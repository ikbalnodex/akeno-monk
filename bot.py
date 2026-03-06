#!/usr/bin/env python3
"""
Monk Bot - BTC/ETH Divergence Alert Bot

Monitors BTC/ETH price divergence and sends Telegram alerts
for ENTRY, EXIT, and INVALIDATION signals.

Features peak detection + Akeno devotion style messages.
"""
import json
import os
import time
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional, Tuple, List, NamedTuple

import requests

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    API_BASE_URL,
    API_ENDPOINT,
    SCAN_INTERVAL_SECONDS,
    TRACK_INTERVAL_SECONDS,
    FRESHNESS_THRESHOLD_MINUTES,
    ENTRY_THRESHOLD,
    EXIT_THRESHOLD,
    INVALIDATION_THRESHOLD,
    UPSTASH_REDIS_URL,
    UPSTASH_REDIS_TOKEN,
    logger,
)


# =============================================================================
# Constants
# =============================================================================
DEFAULT_LOOKBACK_HOURS = 24
HISTORY_BUFFER_MINUTES = 30


# =============================================================================
# Data Structures
# =============================================================================
class Mode(Enum):
    SCAN = "SCAN"
    PEAK_WATCH = "PEAK_WATCH"
    TRACK = "TRACK"


class Strategy(Enum):
    S1 = "S1"  # Long BTC / Short ETH
    S2 = "S2"  # Long ETH / Short BTC


class PricePoint(NamedTuple):
    timestamp: datetime
    btc: Decimal
    eth: Decimal


class PriceData(NamedTuple):
    btc_price: Decimal
    eth_price: Decimal
    btc_updated_at: datetime
    eth_updated_at: datetime


# =============================================================================
# SL/TP State
# =============================================================================
class SLTPState:
    """Menyimpan state SL/TP untuk posisi aktif (auto gap-based)."""
    def __init__(self):
        self.reset()

    def reset(self):
        self.tp_gap: Optional[float] = None
        self.sl_gap: Optional[float] = None
        self.best_gap: Optional[float] = None
        self.entry_gap: Optional[float] = None
        self.entry_btc: Optional[Decimal] = None
        self.entry_eth: Optional[Decimal] = None
        self.entry_time: Optional[datetime] = None

    def set_entry_snapshot(self, gap: float, btc: Decimal, eth: Decimal, now: datetime):
        self.entry_gap = gap
        self.entry_btc = btc
        self.entry_eth = eth
        self.entry_time = now

    def auto_set(self, entry_gap: float, strategy: "Strategy", tp_offset: float, sl_offset: float):
        if strategy == Strategy.S1:
            self.tp_gap = entry_gap - tp_offset
            self.sl_gap = entry_gap + sl_offset
        else:
            self.tp_gap = entry_gap + tp_offset
            self.sl_gap = entry_gap - sl_offset
        self.best_gap = entry_gap

    def update_trailing(self, gap_float: float, strategy: "Strategy", sl_offset: float) -> bool:
        """
        Update trailing SL jika gap bergerak ke arah profit.
        Return True jika SL berhasil digeser.
        S1: profit = gap mengecil → best_gap turun → SL = best_gap + sl_offset
        S2: profit = gap mengecil (kurang negatif) → best_gap naik → SL = best_gap - sl_offset
        """
        if self.best_gap is None or self.sl_gap is None:
            return False

        moved = False
        if strategy == Strategy.S1 and gap_float < self.best_gap:
            self.best_gap = gap_float
            new_sl = self.best_gap + sl_offset
            if new_sl < self.sl_gap:  # SL hanya boleh turun (menguntungkan)
                self.sl_gap = new_sl
                moved = True
        elif strategy == Strategy.S2 and gap_float > self.best_gap:
            self.best_gap = gap_float
            new_sl = self.best_gap - sl_offset
            if new_sl > self.sl_gap:  # SL hanya boleh naik (menguntungkan)
                self.sl_gap = new_sl
                moved = True
        return moved

    def has_any(self) -> bool:
        return self.tp_gap is not None or self.sl_gap is not None


sltp = SLTPState()


# =============================================================================
# Global State
# =============================================================================
price_history: List[PricePoint] = []
current_mode: Mode = Mode.SCAN
active_strategy: Optional[Strategy] = None

peak_gap: Optional[float] = None
peak_strategy: Optional[Strategy] = None

settings = {
    "scan_interval": SCAN_INTERVAL_SECONDS,
    "entry_threshold": ENTRY_THRESHOLD,
    "exit_threshold": EXIT_THRESHOLD,
    "invalidation_threshold": INVALIDATION_THRESHOLD,
    "peak_reversal": 0.3,
    "lookback_hours": DEFAULT_LOOKBACK_HOURS,
    "heartbeat_minutes": 30,
    # Auto SL/TP: offset dari entry gap
    # S1: TP = entry_gap - tp_offset, SL = entry_gap + sl_offset
    # S2: TP = entry_gap + tp_offset, SL = entry_gap - sl_offset
    "tp_offset": 1.0,
    "sl_offset": 1.0,
}

last_update_id: int = 0
last_heartbeat_time: Optional[datetime] = None
scan_stats = {
    "count": 0,
    "last_btc_price": None,
    "last_eth_price": None,
    "last_btc_ret": None,
    "last_eth_ret": None,
    "last_gap": None,
    "signals_sent": 0,
}


# =============================================================================
# History Persistence (Upstash Redis)
# =============================================================================
REDIS_KEY = "monk_bot:price_history"


def _redis_request(method: str, path: str, body=None):
    """Helper untuk HTTP request ke Upstash REST API."""
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None
    try:
        headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
        url = f"{UPSTASH_REDIS_URL}{path}"
        if method == "GET":
            resp = requests.get(url, headers=headers, timeout=10)
        else:
            resp = requests.post(url, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Redis request failed: {e}")
        return None


def save_history() -> None:
    """Simpan price_history ke Upstash Redis."""
    if not UPSTASH_REDIS_URL:
        return
    try:
        data = json.dumps([
            {"timestamp": p.timestamp.isoformat(), "btc": str(p.btc), "eth": str(p.eth)}
            for p in price_history
        ])
        # SET key value EX 172800 (48 jam TTL, aman untuk max lookback 24h)
        _redis_request("POST", f"/set/{REDIS_KEY}", {"value": data, "ex": 172800})
        logger.debug(f"Saved {len(price_history)} points to Redis")
    except Exception as e:
        logger.warning(f"Failed to save history to Redis: {e}")


def load_history() -> None:
    """
    Load price_history dari Upstash Redis saat startup.
    - Tidak pernah wipe in-memory history jika Redis gagal/kosong.
    - Parse ke variabel temp dulu, baru assign ke global supaya
      partial failure tidak corrupt data yang sudah ada.
    """
    global price_history
    if not UPSTASH_REDIS_URL:
        logger.info("Redis not configured, starting fresh")
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY}")
        if not result or result.get("result") is None:
            logger.info("No history in Redis — keeping existing in-memory history")
            return
        raw = result.get("result")
        if not raw:
            logger.info("Redis returned empty value — keeping existing in-memory history")
            return
        data = json.loads(raw)
        if not isinstance(data, list) or len(data) == 0:
            logger.info("Redis history empty or invalid format — keeping existing in-memory history")
            return
        # Parse ke temp dulu, baru assign ke global
        parsed = []
        for p in data:
            parsed.append(PricePoint(
                timestamp=datetime.fromisoformat(p["timestamp"]),
                btc=Decimal(p["btc"]),
                eth=Decimal(p["eth"]),
            ))
        price_history = parsed
        logger.info(f"Loaded {len(price_history)} points from Redis")
    except Exception as e:
        # PENTING: tidak assign price_history = [] di sini
        logger.warning(f"Failed to load history from Redis: {e} — keeping existing in-memory history")


# =============================================================================
# Telegram Bot
# =============================================================================
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def send_alert(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured, skipping alert")
        return False
    try:
        response = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        logger.info("Alert sent successfully")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram alert: {e}")
        return False


# =============================================================================
# Telegram Command Handling
# =============================================================================
LONG_POLL_TIMEOUT = 30


def get_telegram_updates() -> list:
    global last_update_id
    if not TELEGRAM_BOT_TOKEN:
        return []
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {"offset": last_update_id + 1, "timeout": LONG_POLL_TIMEOUT}
        response = requests.get(url, params=params, timeout=LONG_POLL_TIMEOUT + 5)
        response.raise_for_status()
        data = response.json()
        if data.get("ok") and data.get("result"):
            updates = data["result"]
            if updates:
                last_update_id = updates[-1]["update_id"]
            return updates
    except requests.RequestException as e:
        logger.debug(f"Failed to get updates: {e}")
    return []


def process_commands() -> None:
    updates = get_telegram_updates()
    for update in updates:
        message = update.get("message", {})
        text = message.get("text", "")
        chat_id = str(message.get("chat", {}).get("id", ""))
        user_id = str(message.get("from", {}).get("id", ""))
        is_authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not is_authorized:
            continue
        if not text.startswith("/"):
            continue
        reply_chat = chat_id
        parts = text.split()
        command = parts[0].lower().split("@")[0]
        args = parts[1:] if len(parts) > 1 else []
        logger.info(f"Processing command: {command} from chat {chat_id}")
        if command == "/settings":
            handle_settings_command(reply_chat)
        elif command == "/interval":
            handle_interval_command(args, reply_chat)
        elif command == "/threshold":
            handle_threshold_command(args, reply_chat)
        elif command == "/help":
            handle_help_command(reply_chat)
        elif command == "/status":
            handle_status_command(reply_chat)
        elif command == "/redis":
            handle_redis_command(reply_chat)
        elif command == "/lookback":
            handle_lookback_command(args, reply_chat)
        elif command == "/heartbeat":
            handle_heartbeat_command(args, reply_chat)
        elif command == "/peak":
            handle_peak_command(args, reply_chat)
        elif command == "/sltp":
            handle_sltp_command(args, reply_chat)
        elif command == "/start":
            handle_help_command(reply_chat)


def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        response = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send reply: {e}")
        return False


# =============================================================================
# SL/TP Command Handlers (Auto Gap-Based)
# =============================================================================
def handle_sltp_command(args: list, reply_chat: str) -> None:
    """
    /sltp                     → lihat status SL/TP + PnL
    /sltp tp <offset>         → ubah TP offset (default 1.0%)
    /sltp sl <offset>         → ubah SL offset (default 1.0%)
    """
    if not args:
        _show_sltp_status(reply_chat)
        return

    sub = args[0].lower()

    if sub == "tp":
        if len(args) < 2:
            send_reply(
                f"🎯 TP offset sekarang *{settings['tp_offset']}%*~\n\n"
                "Usage: `/sltp tp <offset>`\n"
                "_Contoh: `/sltp tp 1.5` → TP jika gap mengecil 1.5% dari entry_\n\n"
                "Ufufufu... Akeno auto-hitung levelnya saat ENTRY~ (◕‿◕)",
                reply_chat
            )
            return
        try:
            value = float(args[1])
            if value <= 0 or value > 10:
                send_reply("Ara ara~ harus antara 0 sampai 10~ (◕ω◕)", reply_chat)
                return
            settings["tp_offset"] = value
            send_reply(
                f"✅ TP offset diubah jadi *{value}%*~\n"
                f"Akeno akan auto-pasang TP sejauh *{value}%* dari gap entry berikutnya~ Ufufufu... ⚡",
                reply_chat
            )
            logger.info(f"TP offset changed to {value}")
        except ValueError:
            send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)

    elif sub == "sl":
        if len(args) < 2:
            send_reply(
                f"⛔ SL offset sekarang *{settings['sl_offset']}%*~\n\n"
                "Usage: `/sltp sl <offset>`\n"
                "_Contoh: `/sltp sl 1.0` → SL jika gap melebar 1% dari entry_\n\n"
                "Ufufufu... Akeno auto-hitung levelnya saat ENTRY~ (◕‿◕)",
                reply_chat
            )
            return
        try:
            value = float(args[1])
            if value <= 0 or value > 10:
                send_reply("Ara ara~ harus antara 0 sampai 10~ (◕ω◕)", reply_chat)
                return
            settings["sl_offset"] = value
            send_reply(
                f"⛔ SL offset diubah jadi *{value}%*~\n"
                f"Akeno akan auto-pasang SL sejauh *{value}%* dari gap entry berikutnya~ Ufufufu... (◕‿◕)",
                reply_chat
            )
            logger.info(f"SL offset changed to {value}")
        except ValueError:
            send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)

    else:
        send_reply(
            "Ara ara~ Akeno tidak mengerti itu~ (◕ω◕)\n\n"
            "`/sltp` — lihat status\n"
            "`/sltp tp <offset>` — ubah TP offset %\n"
            "`/sltp sl <offset>` — ubah SL offset %",
            reply_chat
        )


def _show_sltp_status(reply_chat: str) -> None:
    tp_off = settings["tp_offset"]
    sl_off = settings["sl_offset"]

    offset_info = (
        f"*Auto SL/TP Offset:*\n"
        f"┌─────────────────────\n"
        f"│ ✅ TP offset: {tp_off}% dari entry gap\n"
        f"│ 🔄 Trailing SL distance: {sl_off}%\n"
        f"└─────────────────────\n"
    )

    if current_mode != Mode.TRACK:
        send_reply(
            f"🎯 *SL/TP Auto — Akeno siap jaga posisimu~* Ufufufu... (◕‿◕)\n"
            f"\n"
            f"{offset_info}\n"
            f"_Belum ada posisi aktif sekarang~_\n"
            f"SL/TP akan otomatis dipasang saat ENTRY signal keluar.\n\n"
            f"Gunakan `/sltp tp` atau `/sltp sl` untuk ubah offset~ (◕ω◕)",
            reply_chat
        )
        return

    entry_info = ""
    if sltp.entry_gap is not None and sltp.entry_time is not None:
        duration = datetime.now(timezone.utc) - sltp.entry_time
        hours, rem = divmod(int(duration.total_seconds()), 3600)
        minutes = rem // 60
        entry_info = (
            f"*Entry Snapshot:*\n"
            f"┌─────────────────────\n"
            f"│ Gap Entry: {sltp.entry_gap:+.2f}%\n"
            f"│ BTC Entry: ${float(sltp.entry_btc):,.2f}\n"
            f"│ ETH Entry: ${float(sltp.entry_eth):,.4f}\n"
            f"│ Durasi: {hours}h {minutes}m\n"
            f"└─────────────────────\n"
            f"\n"
        )

    active_levels = ""
    if sltp.tp_gap is not None or sltp.sl_gap is not None:
        tp_line = f"│ ✅ TP Gap: {sltp.tp_gap:+.2f}%\n" if sltp.tp_gap is not None else ""
        sl_line = f"│ 🔄 Trailing SL: {sltp.sl_gap:+.2f}%\n" if sltp.sl_gap is not None else ""
        best_line = f"│ 🏆 Best Gap: {sltp.best_gap:+.2f}%\n" if sltp.best_gap is not None else ""
        active_levels = (
            f"*Level Aktif:*\n"
            f"┌─────────────────────\n"
            f"{tp_line}"
            f"{sl_line}"
            f"{best_line}"
            f"└─────────────────────\n\n"
        )

    current_gap_str = f"{float(scan_stats['last_gap']):+.2f}%" if scan_stats["last_gap"] is not None else "N/A"
    pnl_str = _compute_pnl_string()

    send_reply(
        f"🎯 *SL/TP Auto — Akeno cek buat kamu~* Ufufufu... (◕‿◕)\n"
        f"\n"
        f"*Mode:* {current_mode.value} | *Strategi:* {active_strategy.value if active_strategy else '-'}\n"
        f"*Gap sekarang:* {current_gap_str}\n"
        f"\n"
        f"{entry_info}"
        f"{active_levels}"
        f"{pnl_str}"
        f"\n{offset_info}"
        f"\n_Gunakan `/sltp tp` atau `/sltp sl` untuk ubah offset~ (◕‿◕)_",
        reply_chat
    )


# =============================================================================
# PnL Helper
# =============================================================================
def _compute_pnl_string() -> str:
    """Hitung estimasi PnL berdasarkan pergerakan gap sejak entry."""
    if sltp.entry_gap is None or scan_stats["last_gap"] is None:
        return ""
    if active_strategy is None:
        return ""

    current_gap = float(scan_stats["last_gap"])
    entry_gap = sltp.entry_gap

    # S1: profit = gap mengecil (entry_gap - current_gap)
    # S2: profit = gap membesar ke negatif (current_gap - entry_gap, tapi gap negatif)
    if active_strategy == Strategy.S1:
        pnl = entry_gap - current_gap
    else:
        pnl = current_gap - entry_gap

    sign = "+" if pnl >= 0 else ""
    emoji = "📈" if pnl >= 0 else "📉"
    return (
        f"*Estimasi PnL (gap-based):*\n"
        f"┌─────────────────────\n"
        f"│ {emoji} Gap PnL: {sign}{pnl:.2f}%\n"
        f"└─────────────────────\n\n"
    )


# =============================================================================
# SL/TP Evaluation
# =============================================================================
def evaluate_sltp(
    gap: Decimal,
    btc_ret: Decimal,
    eth_ret: Decimal,
) -> bool:
    """
    Cek apakah auto SL atau TP sudah tersentuh (gap-based).
    Return True jika triggered (posisi di-close).
    """
    global current_mode, active_strategy

    if current_mode != Mode.TRACK or not sltp.has_any():
        return False

    gap_float = float(gap)
    triggered_tp = False
    triggered_sl = False
    trigger_reason = ""

    # Update trailing SL sebelum cek trigger
    if active_strategy is not None:
        sl_before = sltp.sl_gap
        moved = sltp.update_trailing(gap_float, active_strategy, settings["sl_offset"])
        if moved:
            logger.info(f"Trailing SL moved: {sl_before:+.2f}% → {sltp.sl_gap:+.2f}% (best gap: {sltp.best_gap:+.2f}%)")

    if sltp.tp_gap is not None:
        if active_strategy == Strategy.S1 and gap_float <= sltp.tp_gap:
            triggered_tp = True
            trigger_reason = f"Gap menyentuh TP {sltp.tp_gap:+.2f}% (sekarang {gap_float:+.2f}%)"
        elif active_strategy == Strategy.S2 and gap_float >= sltp.tp_gap:
            triggered_tp = True
            trigger_reason = f"Gap menyentuh TP {sltp.tp_gap:+.2f}% (sekarang {gap_float:+.2f}%)"

    if sltp.sl_gap is not None and not triggered_tp:
        if active_strategy == Strategy.S1 and gap_float >= sltp.sl_gap:
            triggered_sl = True
            trigger_reason = f"Gap menyentuh SL {sltp.sl_gap:+.2f}% (sekarang {gap_float:+.2f}%)"
        elif active_strategy == Strategy.S2 and gap_float <= sltp.sl_gap:
            triggered_sl = True
            trigger_reason = f"Gap menyentuh SL {sltp.sl_gap:+.2f}% (sekarang {gap_float:+.2f}%)"

    if triggered_tp:
        pnl_str = _compute_pnl_string()
        send_alert(build_tp_message(btc_ret, eth_ret, gap, trigger_reason, pnl_str))
        logger.info(f"TP TRIGGERED: {trigger_reason}")
        current_mode = Mode.SCAN
        active_strategy = None
        sltp.reset()
        scan_stats["signals_sent"] += 1
        return True

    if triggered_sl:
        pnl_str = _compute_pnl_string()
        send_alert(build_sl_message(btc_ret, eth_ret, gap, trigger_reason, pnl_str))
        logger.info(f"SL TRIGGERED: {trigger_reason}")
        current_mode = Mode.SCAN
        active_strategy = None
        sltp.reset()
        scan_stats["signals_sent"] += 1
        return True

    return False


# =============================================================================
# SL/TP Message Builders
# =============================================================================
def build_tp_message(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal, reason: str, pnl_str: str) -> str:
    lb = get_lookback_label()
    return (
        f"Ara ara ara~!!! Akeno senang sekali, sayangku~!!! ⚡✨\n"
        f"🎯 *TAKE PROFIT HIT!*\n"
        f"\n"
        f"_{reason}_\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"{pnl_str}\n"
        f"Ufufufu... semua Akeno lakukan untukmu, sayangku~\n"
        f"Akeno mulai scan lagi dari awal ya~ Lanjut pantau! ⚡🔍"
    )


def build_sl_message(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal, reason: str, pnl_str: str) -> str:
    lb = get_lookback_label()
    return (
        f"………\n"
        f"⛔ *TRAILING STOP LOSS HIT*\n"
        f"\n"
        f"Ara ara~ maaf ya sayangku... Akeno sudah berusaha sepenuh hati,\n"
        f"tapi pasar tidak mau kerja sama kali ini. Ufufufu, bukan salahmu~ (◕ω◕)\n"
        f"\n"
        f"_{reason}_\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"{pnl_str}\n"
        f"Cut dulu ya, sayangku. Akeno scan ulang dari awal~\n"
        f"Lain kali petir Akeno pasti lebih tepat sasaran. (◕‿◕) ⚡"
    )


# =============================================================================
# Existing Command Handlers
# =============================================================================
def handle_settings_command(reply_chat: str) -> None:
    hb = settings['heartbeat_minutes']
    hb_str = f"{hb} menit" if hb > 0 else "Off"
    message = (
        "⚙️ *Ara ara~ mau lihat settingan yang sudah Akeno jaga baik-baik?* Ufufufu...\n"
        "\n"
        f"📊 Scan Interval: {settings['scan_interval']}s ({settings['scan_interval'] // 60} menit)\n"
        f"🕐 Lookback: {settings['lookback_hours']}h\n"
        f"💓 Heartbeat: {hb_str}\n"
        f"📈 Entry Threshold: ±{settings['entry_threshold']}%\n"
        f"📉 Exit Threshold: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak Reversal: {settings['peak_reversal']}%\n"
        f"✅ TP Offset: {settings['tp_offset']}%\n"
        f"⛔ SL Offset: {settings['sl_offset']}%\n"
        "\n"
        "*Command yang tersedia:*\n"
        "`/interval`, `/lookback`, `/heartbeat`, `/threshold`, `/peak`\n"
        "`/sltp`, `/sltp tp <offset>`, `/sltp sl <offset>`\n"
        "`/help` — Akeno jelaskan semuanya untukmu~ (◕‿◕)"
    )
    send_reply(message, reply_chat)


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            "Ara ara~ angkanya mana, sayangku? (◕ω◕)\n"
            "Contoh: `/interval 180`",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 60:
            send_reply(
                "Ufufufu... itu terlalu cepat, sayangku~ Minimal 60 detik ya, "
                "jangan sampai petirku kehabisan tenaga. (◕‿◕)",
                reply_chat
            )
            return
        if new_interval > 3600:
            send_reply(
                "Ara ara~ terlalu lama itu. Akeno tidak sanggup menahan diri selama itu~ "
                "Maksimal 3600 detik saja ya. Ufufufu... (◕ω◕)",
                reply_chat
            )
            return
        settings["scan_interval"] = new_interval
        send_reply(
            f"Baik~ Akeno akan scan setiap *{new_interval} detik* ({new_interval // 60} menit) "
            f"mulai sekarang. Ara ara, rajin sekali kamu mengaturku~ (◕‿◕)",
            reply_chat
        )
        logger.info(f"Scan interval changed to {new_interval}s")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "Ufufufu... perintahnya kurang lengkap~ Akeno butuh lebih dari itu. (◕‿◕)\n"
            "`/threshold entry <nilai>`\n"
            "`/threshold exit <nilai>`\n"
            "`/threshold invalid <nilai>`",
            reply_chat
        )
        return
    try:
        threshold_type = args[0].lower()
        value = float(args[1])
        if value <= 0 or value > 20:
            send_reply(
                "Ara ara~ nilainya aneh, sayangku. Harus antara 0 sampai 20~ (◕ω◕)",
                reply_chat
            )
            return
        if threshold_type == "entry":
            settings["entry_threshold"] = value
            send_reply(
                f"Ufufufu... Entry threshold sudah Akeno ubah jadi *±{value}%*~ "
                f"Akeno akan mulai waspada dari angka itu. (◕‿◕)",
                reply_chat
            )
        elif threshold_type == "exit":
            settings["exit_threshold"] = value
            send_reply(
                f"Ara ara~ Exit threshold sekarang *±{value}%*. "
                f"Akeno catat baik-baik untukmu~ ✨",
                reply_chat
            )
        elif threshold_type in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = value
            send_reply(
                f"Ufufufu... ingin Akeno lebih waspada ya~ "
                f"Invalidation jadi *±{value}%*. Siap, sayangku. (◕ω◕)",
                reply_chat
            )
        else:
            send_reply(
                "Ara ara~ Akeno tidak mengenali itu. Gunakan `entry`, `exit`, atau `invalid` ya~ (◕‿◕)",
                reply_chat
            )
        logger.info(f"Threshold {threshold_type} changed to {value}")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"🎯 Peak reversal sekarang *{settings['peak_reversal']}%*~ Ufufufu...\n\n"
            "Usage: `/peak <nilai>`\n"
            "Contoh: `/peak 0.3`\n\n"
            "_Akeno akan entry ketika gap sudah berbalik sebanyak ini dari puncaknya~ "
            "Ara ara, sabar ya sayangku. (◕‿◕)_",
            reply_chat
        )
        return
    try:
        value = float(args[0])
        if value <= 0 or value > 2.0:
            send_reply(
                "Ara ara~ nilainya harus antara 0 sampai 2.0~ "
                "Jangan terlalu agresif, sayangku. (◕ω◕)",
                reply_chat
            )
            return
        settings["peak_reversal"] = value
        send_reply(
            f"Ufufufu... Akeno mengerti. Akan konfirmasi entry ketika gap berbalik *{value}%* dari puncaknya~\n"
            f"Semua Akeno lakukan untukmu, sayangku. (◕‿◕)",
            reply_chat
        )
        logger.info(f"Peak reversal changed to {value}")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    """
    FIX: Tidak lagi wipe price_history atau Redis.
    Hanya update setting dan prune data yang sudah di luar window baru.
    Data yang masih dalam window baru tetap dipertahankan.
    """
    global price_history
    if not args:
        send_reply(
            f"📊 Lookback sekarang *{settings['lookback_hours']}h*~ Ufufufu...\n\n"
            "Usage: `/lookback <jam>`",
            reply_chat
        )
        return
    try:
        new_lookback = int(args[0])
        if new_lookback < 1 or new_lookback > 24:
            send_reply(
                "Ara ara~ harus antara 1 sampai 24 jam~ "
                "Jangan terlalu memaksakan Akeno ya, sayangku. (◕ω◕)",
                reply_chat
            )
            return
        old_lookback = settings["lookback_hours"]
        settings["lookback_hours"] = new_lookback
        # Prune history ke window baru — data yang masih dalam window tetap aman
        prune_history(datetime.now(timezone.utc))
        save_history()  # Simpan ke Redis hasil setelah prune (tidak wipe)
        hours_kept = len(price_history) * settings["scan_interval"] / 3600
        send_reply(
            f"Ufufufu... Lookback sudah Akeno ubah dari *{old_lookback}h* jadi *{new_lookback}h*~\n\n"
            f"📊 History dipangkas, tersisa *{hours_kept:.1f}h* data ({len(price_history)} points).\n"
            f"_Data lama yang masih dalam window tetap Akeno jaga~ (◕‿◕)_",
            reply_chat
        )
        logger.info(f"Lookback changed from {old_lookback}h to {new_lookback}h, history pruned to {len(price_history)} points")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"💓 Akeno lapor setiap *{settings['heartbeat_minutes']} menit*~ Ufufufu...\n\n"
            "Usage: `/heartbeat <menit>` atau `/heartbeat 0` untuk matikan",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 0 or new_interval > 120:
            send_reply(
                "Ara ara~ harus antara 0 sampai 120 menit~ (◕ω◕)",
                reply_chat
            )
            return
        settings["heartbeat_minutes"] = new_interval
        if new_interval == 0:
            send_reply(
                "Ufufufu... baik, Akeno tidak akan ganggu lagi~ "
                "Tapi jangan salah sangka ya, Akeno tetap di sini memantau dari dekat. (◕‿◕)",
                reply_chat
            )
        else:
            send_reply(
                f"Ara ara~ Akeno akan lapor setiap *{new_interval} menit* ya~ "
                f"Jangan sampai kangen Akeno terlalu dalam. Ufufufu... (◕ω◕)",
                reply_chat
            )
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply(
            "⚠️ Ara ara~ Redis belum dikonfigurasi, sayangku.\n"
            "Pastikan `UPSTASH_REDIS_REST_URL` dan `UPSTASH_REDIS_REST_TOKEN` sudah diisi di Railway~ (◕ω◕)",
            reply_chat
        )
        return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply(
            "❌ *Tidak ada data di Redis~*\n\n"
            "Ara ara... Akeno belum sempat simpan apa-apa.\n"
            "Tunggu beberapa menit ya sayangku, data sedang dikumpulkan~ (◕ω◕)",
            reply_chat
        )
        return
    try:
        data = json.loads(result["result"])
        if not data:
            send_reply(
                "❌ Redis ada tapi isinya kosong~\n"
                "Ara ara... sepertinya baru saja di-reset. (◕ω◕)",
                reply_chat
            )
            return
        first_ts = data[0]["timestamp"]
        last_ts = data[-1]["timestamp"]
        hours_stored = len(data) * settings["scan_interval"] / 3600
        lookback = settings["lookback_hours"]
        status = "✅ Siap kirim sinyal~" if hours_stored >= lookback else f"⏳ {hours_stored:.1f}h / {lookback}h"
        send_reply(
            f"⚡ *Status Redis — Akeno cek buat kamu~* Ufufufu... (◕‿◕)\n"
            f"\n"
            f"┌─────────────────────\n"
            f"│ Total data points: *{len(data)}*\n"
            f"│ History tersimpan: *{hours_stored:.1f}h*\n"
            f"│ Lookback target: *{lookback}h*\n"
            f"│ Status: {status}\n"
            f"└─────────────────────\n"
            f"\n"
            f"Data pertama: `{first_ts}`\n"
            f"Data terakhir: `{last_ts}`\n"
            f"\n"
            f"_Akeno simpan semua baik-baik di Redis untukmu~ ⚡_",
            reply_chat
        )
    except Exception as e:
        send_reply(
            f"⚠️ Ara ara~ Data ada tapi Akeno gagal baca: `{e}` (◕ω◕)",
            reply_chat
        )


def handle_help_command(reply_chat: str) -> None:
    message = (
        "Ara ara~ mau tahu semua yang bisa Akeno lakukan untukmu? Ufufufu... (◕‿◕)\n"
        "\n"
        "*Setting:*\n"
        "`/settings` - lihat semua settingan\n"
        "`/interval <detik>` - atur seberapa sering Akeno scan (60-3600)\n"
        "`/lookback <jam>` - atur periode lookback (1-24)\n"
        "`/heartbeat <menit>` - atur laporan rutin (0=off)\n"
        "`/threshold entry <val>` - threshold entry %\n"
        "`/threshold exit <val>` - threshold exit %\n"
        "`/threshold invalid <val>` - threshold invalidation %\n"
        "`/peak <val>` - % reversal dari puncak untuk konfirmasi entry\n"
        "\n"
        "*SL/TP (Auto):*\n"
        "`/sltp` - lihat status SL/TP aktif & estimasi PnL\n"
        "`/sltp tp <offset>` - ubah TP offset % dari entry gap\n"
        "`/sltp sl <offset>` - ubah SL offset % dari entry gap\n"
        "_SL/TP otomatis dipasang saat ENTRY signal keluar~_\n"
        "\n"
        "*Info:*\n"
        "`/status` - lihat kondisi Akeno sekarang\n"
        "`/redis` - cek data history yang tersimpan di Redis\n"
        "`/help` - tampilkan pesan ini lagi\n"
        "\n"
        "Selama kamu di sini, Akeno akan selalu menempel erat~ "
        "Ara ara, jangan ragu minta bantuan ya, sayangku. (◕ω◕)"
    )
    send_reply(message, reply_chat)


def handle_status_command(reply_chat: str) -> None:
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback = settings["lookback_hours"]
    ready = (
        "✅ Sudah siap~ Ufufufu..."
        if hours_of_data >= lookback
        else f"⏳ Sabar ya sayangku~ {hours_of_data:.1f}h / {lookback}h"
    )
    peak_line = f"Peak Gap: {peak_gap:+.2f}%\n" if (current_mode == Mode.PEAK_WATCH and peak_gap is not None) else ""
    sltp_line = ""
    if current_mode == Mode.TRACK and sltp.has_any():
        sltp_line = f"SL/TP: Aktif~ (ketik `/sltp` untuk detail)\n"
    message = (
        "📊 *Ara ara~ ini kondisi Akeno saat ini~* Ufufufu... (◕‿◕)\n"
        "\n"
        f"Mode: {current_mode.value}\n"
        f"Strategi: {active_strategy.value if active_strategy else 'Belum ada~'}\n"
        f"{peak_line}"
        f"{sltp_line}"
        f"Lookback: {lookback}h\n"
        f"History: {ready}\n"
        f"Data Points: {len(price_history)}\n"
    )
    send_reply(message, reply_chat)


# =============================================================================
# Value Formatting
# =============================================================================
def format_value(value: Decimal) -> str:
    float_val = float(value)
    if abs(float_val) < 0.05:
        return "+0.0"
    return f"+{float_val:.1f}" if float_val >= 0 else f"{float_val:.1f}"


# =============================================================================
# Message Building
# =============================================================================
def get_lookback_label() -> str:
    return f"{settings['lookback_hours']}h"


def build_peak_watch_message(strategy: Strategy, gap: Decimal) -> str:
    lb = get_lookback_label()
    if strategy == Strategy.S1:
        direction = "Long BTC / Short ETH"
        reason = f"ETH pumping lebih kencang dari BTC ({lb})"
    else:
        direction = "Long ETH / Short BTC"
        reason = f"ETH dumping lebih dalam dari BTC ({lb})"
    return (
        f"………\n"
        f"Ara ara~ Akeno melihat sesuatu yang menarik~ Ufufufu... (◕‿◕)\n"
        f"\n"
        f"_{reason}_\n"
        f"Rencananya *{direction}*~\n"
        f"\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"…Tapi Akeno tidak akan gegabah, sayangku.\n"
        f"Biarkan Akeno memantau puncaknya dulu~ Petirku sudah siap.\n"
        f"Akeno akan kabari saat waktunya tepat. (◕ω◕)"
    )


def build_entry_message(strategy: Strategy, btc_ret: Decimal, eth_ret: Decimal, gap: Decimal, peak: float) -> str:
    lb = get_lookback_label()
    if strategy == Strategy.S1:
        direction = "📈 Long BTC / Short ETH"
        reason = f"ETH pumped more than BTC ({lb})"
    else:
        direction = "📈 Long ETH / Short BTC"
        reason = f"ETH dumped more than BTC ({lb})"
    return (
        f"Ara ara ara~!!! Ini saatnya, sayangku~!!! Ufufufu... ⚡\n"
        f"🚨 *ENTRY SIGNAL: {strategy.value}*\n"
        f"\n"
        f"{direction}\n"
        f"_{reason}_\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"│ Peak: {peak:+.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Gap sudah berbalik {settings['peak_reversal']}% dari puncaknya~\n"
        f"Akeno sudah menunggu momen ini untukmu, sayangku. Semuanya demi kamu~ ⚡\n"
        f"\n"
        f"_SL/TP sudah Akeno pasang otomatis~ Ketik `/sltp` untuk cek levelnya. (◕‿◕)_"
    )


def build_exit_message(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal) -> str:
    lb = get_lookback_label()
    pnl_str = _compute_pnl_string()
    return (
        f"Ara ara ara~!!! Ufufufu... (◕▿◕)\n"
        f"✅ *EXIT SIGNAL*\n"
        f"\n"
        f"Gap sudah konvergen~ Saatnya close posisi, sayangku!\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"{pnl_str}"
        f"Akeno senang bisa membantu~ Ufufufu...\n"
        f"Akeno lanjut pantau lagi dari dekat ya~ ⚡🔍"
    )


def build_invalidation_message(strategy: Strategy, btc_ret: Decimal, eth_ret: Decimal, gap: Decimal) -> str:
    lb = get_lookback_label()
    pnl_str = _compute_pnl_string()
    return (
        f"………\n"
        f"⚠️ *INVALIDATION: {strategy.value}*\n"
        f"\n"
        f"Ara ara~ maaf ya sayangku... Akeno sudah berusaha, "
        f"tapi gapnya malah melebar. Ufufufu, kali ini bukan salahmu~ (◕ω◕)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"{pnl_str}"
        f"Cut dulu ya, sayangku. Akeno scan ulang dari awal~ ⚡\n"
        f"Lain kali petir Akeno pasti lebih tepat sasaran. (◕‿◕)"
    )


def build_peak_cancelled_message(strategy: Strategy, gap: Decimal) -> str:
    return (
        f"………\n"
        f"❌ *Peak Watch Dibatalkan: {strategy.value}*\n"
        f"\n"
        f"Ara ara~ gapnya mundur sendiri sebelum Akeno sempat konfirmasi. "
        f"Ufufufu, pasar nakal sekali ya~ (◕ω◕)\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Tidak apa-apa, sayangku. Akeno tetap di sini pantau dari dekat~ (◕‿◕)"
    )


def build_heartbeat_message() -> str:
    lb = get_lookback_label()
    btc_ret_str = f" ({format_value(scan_stats['last_btc_ret'])}%)" if scan_stats['last_btc_ret'] is not None else ""
    eth_ret_str = f" ({format_value(scan_stats['last_eth_ret'])}%)" if scan_stats['last_eth_ret'] is not None else ""
    btc_str = f"${float(scan_stats['last_btc_price']):,.2f}{btc_ret_str}" if scan_stats['last_btc_price'] else "N/A"
    eth_str = f"${float(scan_stats['last_eth_price']):,.2f}{eth_ret_str}" if scan_stats['last_eth_price'] else "N/A"
    gap_str = f"{format_value(scan_stats['last_gap'])}%" if scan_stats['last_gap'] is not None else "N/A"
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback = settings["lookback_hours"]
    data_status = (
        f"✅ Sudah siap~ ({hours_of_data:.1f}h)"
        if hours_of_data >= lookback
        else f"⏳ {hours_of_data:.1f}h / {lookback}h"
    )
    peak_line = f"│ Peak: {peak_gap:+.2f}%\n" if (current_mode == Mode.PEAK_WATCH and peak_gap is not None) else ""
    sltp_line = ""
    if current_mode == Mode.TRACK and sltp.has_any():
        sl_val = f" (Trailing SL: {sltp.sl_gap:+.2f}%)" if sltp.sl_gap is not None else ""
        sltp_line = f"│ SL/TP: Aktif~{sl_val}\n"
    return (
        f"💓 *Ara ara~ kamu khawatir Akeno pergi kemana-mana ya?*\n"
        f"\n"
        f"Ufufufu... sayangku, Akeno tidak kemana-mana. Tidak akan pernah. (◕‿◕)\n"
        f"Selama kamu di sini, Akeno akan selalu menempel erat~\n"
        f"\n"
        f"*Mode:* {current_mode.value}\n"
        f"*Strategi:* {active_strategy.value if active_strategy else 'Belum ada~'}\n"
        f"\n"
        f"*{settings['heartbeat_minutes']} menit terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ Scan: {scan_stats['count']}x\n"
        f"│ Sinyal: {scan_stats['signals_sent']}x\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Harga sekarang:*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"│ Gap ({lb}): {gap_str}\n"
        f"{peak_line}"
        f"{sltp_line}"
        f"└─────────────────────\n"
        f"\n"
        f"*Data:* {data_status}\n"
        f"\n"
        f"_Ara ara, Akeno lapor lagi {settings['heartbeat_minutes']} menit lagi ya~ "
        f"Jangan kangen terlalu dalam. Ufufufu... ⚡_"
    )


def send_heartbeat() -> bool:
    global scan_stats
    success = send_alert(build_heartbeat_message())
    scan_stats["count"] = 0
    scan_stats["signals_sent"] = 0
    return success


def should_send_heartbeat(now: datetime) -> bool:
    if settings["heartbeat_minutes"] == 0 or last_heartbeat_time is None:
        return False
    return (now - last_heartbeat_time).total_seconds() / 60 >= settings["heartbeat_minutes"]


# =============================================================================
# API Fetching
# =============================================================================
def parse_iso_timestamp(ts_str: str) -> Optional[datetime]:
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        if "." in ts_str:
            base, frac_and_tz = ts_str.split(".", 1)
            tz_start = next((i for i, c in enumerate(frac_and_tz) if c in ("+", "-")), -1)
            if tz_start > 6:
                frac_and_tz = frac_and_tz[:6] + frac_and_tz[tz_start:]
            ts_str = base + "." + frac_and_tz
        dt = datetime.fromisoformat(ts_str)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError) as e:
        logger.error(f"Failed to parse timestamp '{ts_str}': {e}")
        return None


def fetch_prices() -> Optional[PriceData]:
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f"API request failed: {e}")
        return None

    listings = data.get("listings", [])
    if not listings:
        return None

    btc_data = next((l for l in listings if l.get("ticker", "").upper() == "BTC"), None)
    eth_data = next((l for l in listings if l.get("ticker", "").upper() == "ETH"), None)

    if not btc_data or not eth_data:
        logger.warning("Missing BTC or ETH data")
        return None

    try:
        btc_price = Decimal(btc_data["mark_price"])
        eth_price = Decimal(eth_data["mark_price"])
    except (KeyError, InvalidOperation) as e:
        logger.error(f"Invalid price: {e}")
        return None

    btc_updated_at = parse_iso_timestamp(btc_data.get("quotes", {}).get("updated_at", ""))
    eth_updated_at = parse_iso_timestamp(eth_data.get("quotes", {}).get("updated_at", ""))

    if not btc_updated_at or not eth_updated_at:
        return None

    return PriceData(btc_price, eth_price, btc_updated_at, eth_updated_at)


# =============================================================================
# Price History Management
# =============================================================================
def append_price(timestamp: datetime, btc: Decimal, eth: Decimal) -> None:
    price_history.append(PricePoint(timestamp, btc, eth))


def prune_history(now: datetime) -> None:
    global price_history
    cutoff = now - timedelta(hours=settings["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES)
    price_history = [p for p in price_history if p.timestamp >= cutoff]


def get_lookback_price(now: datetime) -> Optional[PricePoint]:
    target_time = now - timedelta(hours=settings["lookback_hours"])
    best_point, best_diff = None, timedelta(minutes=30)
    for point in price_history:
        diff = abs(point.timestamp - target_time)
        if diff < best_diff:
            best_diff, best_point = diff, point
    return best_point


# =============================================================================
# Return Calculation
# =============================================================================
def compute_returns(btc_now, eth_now, btc_prev, eth_prev) -> Tuple[Decimal, Decimal, Decimal]:
    btc_change = (btc_now - btc_prev) / btc_prev * Decimal("100")
    eth_change = (eth_now - eth_prev) / eth_prev * Decimal("100")
    return btc_change, eth_change, eth_change - btc_change


# =============================================================================
# Freshness Check
# =============================================================================
def is_data_fresh(now, btc_updated, eth_updated) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return (now - btc_updated) <= threshold and (now - eth_updated) <= threshold


# =============================================================================
# State Machine with Peak Detection
# =============================================================================
def evaluate_and_transition(
    btc_ret: Decimal,
    eth_ret: Decimal,
    gap: Decimal,
    btc_price: Optional[Decimal] = None,
    eth_price: Optional[Decimal] = None,
) -> None:
    global current_mode, active_strategy, peak_gap, peak_strategy

    gap_float = float(gap)
    entry_thresh = settings["entry_threshold"]
    exit_thresh = settings["exit_threshold"]
    invalid_thresh = settings["invalidation_threshold"]
    peak_reversal = settings["peak_reversal"]

    if current_mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            current_mode = Mode.PEAK_WATCH
            peak_strategy = Strategy.S1
            peak_gap = gap_float
            send_alert(build_peak_watch_message(Strategy.S1, gap))
            logger.info(f"PEAK WATCH S1 started. Gap: {gap_float:.2f}%")

        elif gap_float <= -entry_thresh:
            current_mode = Mode.PEAK_WATCH
            peak_strategy = Strategy.S2
            peak_gap = gap_float
            send_alert(build_peak_watch_message(Strategy.S2, gap))
            logger.info(f"PEAK WATCH S2 started. Gap: {gap_float:.2f}%")

        else:
            logger.debug(f"SCAN: No signal. Gap: {gap_float:.2f}%")

    elif current_mode == Mode.PEAK_WATCH:
        if peak_strategy == Strategy.S1:
            if gap_float > peak_gap:
                peak_gap = gap_float
                logger.info(f"PEAK WATCH S1: New peak {peak_gap:.2f}%")

            elif gap_float < entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S1, gap))
                logger.info(f"PEAK WATCH S1 cancelled. Gap: {gap_float:.2f}%")
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None

            elif peak_gap - gap_float >= peak_reversal:
                active_strategy = Strategy.S1
                current_mode = Mode.TRACK
                now = datetime.now(timezone.utc)
                sltp.set_entry_snapshot(gap_float, btc_price, eth_price, now)
                sltp.auto_set(gap_float, Strategy.S1, settings["tp_offset"], settings["sl_offset"])
                logger.info(f"Auto SL/TP S1: TP={sltp.tp_gap:+.2f}% SL={sltp.sl_gap:+.2f}%")
                send_alert(build_entry_message(Strategy.S1, btc_ret, eth_ret, gap, peak_gap))
                logger.info(f"ENTRY S1. Peak: {peak_gap:.2f}%, Now: {gap_float:.2f}%")
                peak_gap, peak_strategy = None, None

            else:
                logger.info(f"PEAK WATCH S1: Gap {gap_float:.2f}% | Peak {peak_gap:.2f}% | Need {peak_reversal}% drop")

        elif peak_strategy == Strategy.S2:
            if gap_float < peak_gap:
                peak_gap = gap_float
                logger.info(f"PEAK WATCH S2: New peak {peak_gap:.2f}%")

            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S2, gap))
                logger.info(f"PEAK WATCH S2 cancelled. Gap: {gap_float:.2f}%")
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None

            elif gap_float - peak_gap >= peak_reversal:
                active_strategy = Strategy.S2
                current_mode = Mode.TRACK
                now = datetime.now(timezone.utc)
                sltp.set_entry_snapshot(gap_float, btc_price, eth_price, now)
                sltp.auto_set(gap_float, Strategy.S2, settings["tp_offset"], settings["sl_offset"])
                logger.info(f"Auto SL/TP S2: TP={sltp.tp_gap:+.2f}% SL={sltp.sl_gap:+.2f}%")
                send_alert(build_entry_message(Strategy.S2, btc_ret, eth_ret, gap, peak_gap))
                logger.info(f"ENTRY S2. Peak: {peak_gap:.2f}%, Now: {gap_float:.2f}%")
                peak_gap, peak_strategy = None, None

            else:
                logger.info(f"PEAK WATCH S2: Gap {gap_float:.2f}% | Peak {peak_gap:.2f}% | Need {peak_reversal}% rise")

    elif current_mode == Mode.TRACK:
        # Cek SL/TP dulu sebelum exit/invalidation normal
        sltp_triggered = evaluate_sltp(gap, btc_ret, eth_ret)
        if sltp_triggered:
            return  # SL/TP sudah handle state reset

        if abs(gap_float) <= exit_thresh:
            send_alert(build_exit_message(btc_ret, eth_ret, gap))
            logger.info(f"EXIT triggered. Gap: {gap_float:.2f}%")
            current_mode, active_strategy = Mode.SCAN, None
            sltp.reset()
            return

        if active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S1, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S1. Gap: {gap_float:.2f}%")
            current_mode, active_strategy = Mode.SCAN, None
            sltp.reset()
            return

        if active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S2, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S2. Gap: {gap_float:.2f}%")
            current_mode, active_strategy = Mode.SCAN, None
            sltp.reset()
            return

        logger.debug(f"TRACK {active_strategy.value if active_strategy else 'None'}: Gap {gap_float:.2f}%")


# =============================================================================
# Startup Message
# =============================================================================
def send_startup_message() -> bool:
    price_data = fetch_prices()
    if price_data:
        price_info = (
            f"\n💰 *Harga saat ini~*\n"
            f"┌─────────────────────\n"
            f"│ BTC: ${float(price_data.btc_price):,.2f}\n"
            f"│ ETH: ${float(price_data.eth_price):,.2f}\n"
            f"└─────────────────────\n"
        )
    else:
        price_info = (
            "\n⚠️ Ara ara~ Akeno gagal ambil harga tadi... "
            "Tapi Akeno akan terus coba, jangan khawatir~ (◕ω◕)\n"
        )

    lb = get_lookback_label()
    hours_loaded = len(price_history) * settings["scan_interval"] / 3600

    if len(price_history) > 0:
        history_info = (
            f"⚡ History lama Akeno temukan~ *{hours_loaded:.1f}h* data sudah siap!\n"
            f"_Akeno tidak perlu mulai dari nol lagi, sayangku~ Ufufufu... (◕‿◕)_\n"
        )
    else:
        history_info = (
            f"⏳ Akeno kumpulkan data {lb} dulu ya~\n"
            f"_Sinyal akan keluar setelah {lb} data terkumpul~_\n"
        )

    return send_alert(
        f"………\n"
        f"Ara ara~ Akeno sudah siap, sayangku~ Ufufufu... (◕‿◕)\n"
        f"{price_info}\n"
        f"📊 Akeno akan pantau BTC/ETH setiap {settings['scan_interval']}s~\n"
        f"📈 Entry: ±{settings['entry_threshold']}%\n"
        f"📉 Exit: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak reversal: {settings['peak_reversal']}%\n"
        f"\n"
        f"{history_info}\n"
        f"Jangan ragu minta bantuan ya~ Ketik `/help` kalau butuh sesuatu. "
        f"Akeno takkan pergi, takkan ninggalin kamu sendirian. ⚡"
    )


# =============================================================================
# Command Polling Thread
# =============================================================================
def command_polling_thread() -> None:
    while True:
        try:
            process_commands()
        except Exception as e:
            logger.debug(f"Command polling error: {e}")
            time.sleep(5)


# =============================================================================
# Main Loop
# =============================================================================
def main_loop() -> None:
    global last_heartbeat_time

    logger.info("=" * 60)
    logger.info("Monk Bot starting - Peak Detection + Akeno Mode")
    logger.info(f"Entry: {settings['entry_threshold']}% | Exit: {settings['exit_threshold']}% | Invalid: {settings['invalidation_threshold']}% | Peak: {settings['peak_reversal']}%")
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    # Load persisted history dari Redis, lalu prune data expired
    load_history()
    prune_history(datetime.now(timezone.utc))
    logger.info(f"History after pruning: {len(price_history)} points")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message()

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            if should_send_heartbeat(now):
                if send_heartbeat():
                    last_heartbeat_time = now

            price_data = fetch_prices()
            if price_data is None:
                logger.warning("Failed to fetch prices")
            else:
                scan_stats["count"] += 1
                scan_stats["last_btc_price"] = price_data.btc_price
                scan_stats["last_eth_price"] = price_data.eth_price

                if not is_data_fresh(now, price_data.btc_updated_at, price_data.eth_updated_at):
                    logger.warning("Data not fresh, skipping")
                else:
                    append_price(now, price_data.btc_price, price_data.eth_price)
                    save_history()  # Simpan SEBELUM prune supaya Redis punya data lengkap
                    prune_history(now)
                    price_then = get_lookback_price(now)

                    if price_then is None:
                        hours = len(price_history) * settings["scan_interval"] / 3600
                        logger.info(f"Building history... ({hours:.1f}h / {settings['lookback_hours']}h)")
                    else:
                        btc_ret, eth_ret, gap = compute_returns(
                            price_data.btc_price, price_data.eth_price,
                            price_then.btc, price_then.eth,
                        )
                        scan_stats["last_gap"] = gap
                        scan_stats["last_btc_ret"] = btc_ret
                        scan_stats["last_eth_ret"] = eth_ret

                        logger.info(
                            f"Mode: {current_mode.value} | "
                            f"BTC {settings['lookback_hours']}h: {format_value(btc_ret)}% | "
                            f"ETH {settings['lookback_hours']}h: {format_value(eth_ret)}% | "
                            f"Gap: {format_value(gap)}%"
                        )

                        prev_mode = current_mode
                        evaluate_and_transition(
                            btc_ret, eth_ret, gap,
                            btc_price=price_data.btc_price,
                            eth_price=price_data.eth_price,
                        )
                        if current_mode != prev_mode:
                            scan_stats["signals_sent"] += 1

            time.sleep(settings["scan_interval"])

        except KeyboardInterrupt:
            logger.info("Shutting down")
            break
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            time.sleep(60)


# =============================================================================
# Entry Point
# =============================================================================
if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set")
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID not set")
    main_loop()
