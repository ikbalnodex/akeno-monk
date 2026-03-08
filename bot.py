#!/usr/bin/env python3
"""
Monk Bot B - BTC/ETH Divergence Alert Bot (Read-Only Redis Consumer)

Monitors BTC/ETH price divergence dan kirim Telegram alerts
untuk ENTRY, EXIT, INVALIDATION, TP, dan Trailing SL.

Changelog:
- Redis READ-ONLY: history dikonsumsi dari Bot A
- TP maksimal = exit threshold (konvergen penuh)
- Target harga ETH/BTC ditampilkan saat entry signal
- Refresh history dari Redis setiap 1 menit
- Peak mode on/off toggle (/peak on | /peak off)
- [SCALPING] Cooldown Timer        (/cooldown)
- [SCALPING] Consecutive Confirm   (/confirm)
- [SCALPING] Gap Velocity Filter   (/velocity)
- [SCALPING] Session Filter        (/session)
- [SCALPING] Max Signals Per Hour  (/maxsig)
- [SCALPING] Partial TP1           (/tp1)
- [SCALPING] Min Gap Duration      (/mindur)
- [SCALPING] Scalping overview     (/scalping)
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
REDIS_REFRESH_MINUTES  = 1

SESSION_WINDOWS = {
    "london":  [(8,  16)],
    "ny":      [(13, 21)],
    "both":    [(8,  16), (13, 21)],
    "overlap": [(13, 16)],   # London + NY overlap, highest volume
}


# =============================================================================
# Data Structures
# =============================================================================
class Mode(Enum):
    SCAN       = "SCAN"
    PEAK_WATCH = "PEAK_WATCH"
    TRACK      = "TRACK"


class Strategy(Enum):
    S1 = "S1"  # Long BTC / Short ETH
    S2 = "S2"  # Long ETH / Short BTC


class PricePoint(NamedTuple):
    timestamp: datetime
    btc:       Decimal
    eth:       Decimal


class PriceData(NamedTuple):
    btc_price:      Decimal
    eth_price:      Decimal
    btc_updated_at: datetime
    eth_updated_at: datetime


# =============================================================================
# Global State
# =============================================================================
price_history:   List[PricePoint]    = []
current_mode:    Mode                = Mode.SCAN
active_strategy: Optional[Strategy] = None

peak_gap:      Optional[float]    = None
peak_strategy: Optional[Strategy] = None

# TP/TSL state
entry_gap_value:   Optional[float] = None
trailing_gap_best: Optional[float] = None

# Entry price state — untuk kalkulasi target harga TP
entry_btc_price: Optional[Decimal] = None
entry_eth_price: Optional[Decimal] = None
entry_btc_lb:    Optional[Decimal] = None
entry_eth_lb:    Optional[Decimal] = None

# ── Scalping state ────────────────────────────────────────────────────────────
last_exit_time:    Optional[datetime] = None   # untuk cooldown
confirm_streak:    int                = 0       # consecutive scan counter
confirm_side:      Optional[str]      = None    # "S1" / "S2"
gap_above_since:   Optional[datetime] = None    # untuk mindur
signal_timestamps: List[datetime]     = []      # untuk maxsig rolling 1h
tp1_hit:           bool               = False   # apakah TP1 sudah terpicu
prev_gap_float:    Optional[float]    = None    # untuk velocity
prev_gap_time:     Optional[datetime] = None
current_velocity:  Optional[float]    = None    # %/menit, di-update tiap eval
# ─────────────────────────────────────────────────────────────────────────────

settings = {
    # — Core —
    "scan_interval":          SCAN_INTERVAL_SECONDS,
    "entry_threshold":        ENTRY_THRESHOLD,
    "exit_threshold":         EXIT_THRESHOLD,
    "invalidation_threshold": INVALIDATION_THRESHOLD,
    "peak_reversal":          0.3,
    "peak_enabled":           True,
    "lookback_hours":         DEFAULT_LOOKBACK_HOURS,
    "heartbeat_minutes":      30,
    "sl_pct":                 1.0,
    "redis_refresh_minutes":  REDIS_REFRESH_MINUTES,
    # — Scalping —
    "cooldown_minutes":       5,     # 0 = off
    "confirm_count":          1,     # 1 = off (sama seperti sebelumnya)
    "velocity_min":           0.0,   # 0.0 = off, satuan %/menit
    "session_enabled":        False,
    "session_mode":           "both",
    "maxsig_per_hour":        0,     # 0 = unlimited
    "tp1_enabled":            False,
    "tp1_ratio":              0.5,   # 0.5 = halfway ke exit threshold
    "mindur_minutes":         0,     # 0 = off
}

last_update_id:      int               = 0
last_heartbeat_time: Optional[datetime] = None
last_redis_refresh:  Optional[datetime] = None

scan_stats = {
    "count":          0,
    "last_btc_price": None,
    "last_eth_price": None,
    "last_btc_ret":   None,
    "last_eth_ret":   None,
    "last_gap":       None,
    "signals_sent":   0,
}


# =============================================================================
# History Persistence — READ-ONLY dari Redis (Bot A yang write)
# =============================================================================
REDIS_KEY = "monk_bot:price_history"


def _redis_request(method: str, path: str, body=None):
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None
    try:
        headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
        url     = f"{UPSTASH_REDIS_URL}{path}"
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
    pass  # Bot B read-only


def load_history() -> None:
    global price_history
    if not UPSTASH_REDIS_URL:
        logger.info("Redis not configured, price history akan kosong")
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY}")
        if not result or result.get("result") is None:
            logger.info("No history in Redis yet (Bot A belum write?)")
            return
        data = json.loads(result["result"])
        price_history = [
            PricePoint(
                timestamp=datetime.fromisoformat(p["timestamp"]),
                btc=Decimal(p["btc"]),
                eth=Decimal(p["eth"]),
            )
            for p in data
        ]
        logger.info(f"Loaded {len(price_history)} points from Redis (read-only)")
    except Exception as e:
        logger.warning(f"Failed to load history from Redis: {e}")
        price_history = []


def refresh_history_from_redis(now: datetime) -> None:
    global last_redis_refresh
    interval = settings["redis_refresh_minutes"]
    if interval <= 0:
        return
    if last_redis_refresh is not None:
        elapsed = (now - last_redis_refresh).total_seconds() / 60
        if elapsed < interval:
            return
    load_history()
    prune_history(now)
    last_redis_refresh = now
    logger.debug(f"Redis refreshed. {len(price_history)} points after prune")


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
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     message,
                "parse_mode":               "Markdown",
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
        url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
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
        message       = update.get("message", {})
        text          = message.get("text", "")
        chat_id       = str(message.get("chat", {}).get("id", ""))
        user_id       = str(message.get("from", {}).get("id", ""))
        is_authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not is_authorized:
            continue
        if not text.startswith("/"):
            continue
        reply_chat = chat_id
        parts      = text.split()
        command    = parts[0].lower().split("@")[0]
        args       = parts[1:] if len(parts) > 1 else []
        logger.info(f"Processing command: {command} from chat {chat_id}")

        if command == "/settings":
            handle_settings_command(reply_chat)
        elif command == "/scalping":
            handle_scalping_command(reply_chat)
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
        # ── Scalping commands ─────────────────────────────────────────────────
        elif command == "/cooldown":
            handle_cooldown_command(args, reply_chat)
        elif command == "/confirm":
            handle_confirm_command(args, reply_chat)
        elif command == "/velocity":
            handle_velocity_command(args, reply_chat)
        elif command == "/session":
            handle_session_command(args, reply_chat)
        elif command == "/maxsig":
            handle_maxsig_command(args, reply_chat)
        elif command == "/tp1":
            handle_tp1_command(args, reply_chat)
        elif command == "/mindur":
            handle_mindur_command(args, reply_chat)
        # ──────────────────────────────────────────────────────────────────────
        elif command == "/start":
            handle_help_command(reply_chat)


def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        response = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id":                  chat_id,
                "text":                     message,
                "parse_mode":               "Markdown",
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
# Command Handlers — Core
# =============================================================================

def handle_settings_command(reply_chat: str) -> None:
    hb        = settings["heartbeat_minutes"]
    hb_str    = f"{hb} menit" if hb > 0 else "Off"
    rr        = settings["redis_refresh_minutes"]
    rr_str    = f"{rr} menit" if rr > 0 else "Off"
    peak_str  = "✅ ON" if settings["peak_enabled"] else "❌ OFF (langsung TRACK)"
    message = (
        "⚙️ *Ara ara~ mau lihat settingan yang sudah Akeno jaga?* Ufufufu...\n"
        "\n"
        f"📊 Scan Interval: {settings['scan_interval']}s ({settings['scan_interval'] // 60} menit)\n"
        f"🕐 Lookback: {settings['lookback_hours']}h\n"
        f"💓 Heartbeat: {hb_str}\n"
        f"🔄 Redis Refresh: {rr_str}\n"
        f"📈 Entry Threshold: ±{settings['entry_threshold']}%\n"
        f"📉 Exit Threshold: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak Reversal: {settings['peak_reversal']}%\n"
        f"🔍 Peak Mode: {peak_str}\n"
        f"✅ TP: saat gap ±{settings['exit_threshold']}% _(max konvergen)_\n"
        f"🛑 Trailing SL: {settings['sl_pct']}% dari gap terbaik\n"
        "\n"
        "_Lihat `/scalping` untuk semua fitur scalping~ (◕‿◕)_"
    )
    send_reply(message, reply_chat)


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            "Ara ara~ angkanya mana, sayangku? (◕ω◕)\n"
            "Contoh: `/interval 60`",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 60:
            send_reply("Ufufufu... minimal 60 detik ya. (◕‿◕)", reply_chat)
            return
        if new_interval > 3600:
            send_reply("Ara ara~ maksimal 3600 detik saja ya. (◕ω◕)", reply_chat)
            return
        settings["scan_interval"] = new_interval
        send_reply(
            f"Baik~ Akeno akan scan setiap *{new_interval} detik* "
            f"({new_interval // 60} menit). Ara ara~ (◕‿◕)",
            reply_chat
        )
        logger.info(f"Scan interval changed to {new_interval}s")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid. (◕ω◕)", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "Ufufufu... perintahnya kurang lengkap~ (◕‿◕)\n"
            "`/threshold entry <nilai>`\n"
            "`/threshold exit <nilai>`\n"
            "`/threshold invalid <nilai>`",
            reply_chat
        )
        return
    try:
        threshold_type = args[0].lower()
        value          = float(args[1])
        if value <= 0 or value > 20:
            send_reply("Ara ara~ harus antara 0 sampai 20~ (◕ω◕)", reply_chat)
            return
        if threshold_type == "entry":
            settings["entry_threshold"] = value
            send_reply(f"Ufufufu... Entry threshold jadi *±{value}%*~ (◕‿◕)", reply_chat)
        elif threshold_type == "exit":
            settings["exit_threshold"] = value
            send_reply(
                f"Ara ara~ Exit threshold *±{value}%*.\n"
                f"_TP & TP1 otomatis ikut berubah~ ✨_",
                reply_chat
            )
        elif threshold_type in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = value
            send_reply(f"Ufufufu... Invalidation jadi *±{value}%*. (◕ω◕)", reply_chat)
        else:
            send_reply("Ara ara~ gunakan `entry`, `exit`, atau `invalid` ya~ (◕‿◕)", reply_chat)
        logger.info(f"Threshold {threshold_type} changed to {value}")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid. (◕ω◕)", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    """
    /peak          → status
    /peak on       → aktifkan Peak Watch
    /peak off      → nonaktifkan Peak Watch
    /peak <nilai>  → ubah reversal %
    """
    peak_str = "✅ ON" if settings["peak_enabled"] else "❌ OFF"

    if not args:
        send_reply(
            f"🔍 *Peak Watch Mode* sekarang: *{peak_str}*\n"
            f"🎯 Peak reversal: *{settings['peak_reversal']}%*\n"
            f"\n"
            f"*Cara kerja:*\n"
            f"• *ON*  → SCAN ➜ PEAK\\_WATCH ➜ TRACK\n"
            f"• *OFF* → SCAN ➜ TRACK langsung\n"
            f"\n"
            f"`/peak on` | `/peak off` | `/peak <nilai>`",
            reply_chat
        )
        return

    first = args[0].lower()

    if first == "on":
        if settings["peak_enabled"]:
            send_reply("Ufufufu~ Peak Watch memang sudah *ON*~ (◕‿◕)", reply_chat)
        else:
            settings["peak_enabled"] = True
            logger.info("Peak Watch Mode enabled")
            send_reply(
                f"✅ *Peak Watch Mode: ON*\n"
                f"Flow: SCAN ➜ PEAK\\_WATCH ➜ TRACK\n"
                f"Reversal: *{settings['peak_reversal']}%* dari puncak~ (◕‿◕)",
                reply_chat
            )
        return

    if first == "off":
        if not settings["peak_enabled"]:
            send_reply("Ufufufu~ Peak Watch memang sudah *OFF*~ (◕ω◕)", reply_chat)
        else:
            _cancel_peak_watch_if_active(reply_chat)
            settings["peak_enabled"] = False
            logger.info("Peak Watch Mode disabled")
            send_reply(
                f"❌ *Peak Watch Mode: OFF*\n"
                f"Langsung entry saat gap ±{settings['entry_threshold']}%~\n"
                f"Flow: SCAN ➜ TRACK langsung~ (◕ω◕)",
                reply_chat
            )
        return

    try:
        value = float(first)
        if value <= 0 or value > 2.0:
            send_reply("Ara ara~ harus antara 0 sampai 2.0~ (◕ω◕)", reply_chat)
            return
        settings["peak_reversal"] = value
        note = (
            f"\n_Peak Mode sedang OFF — nilai ini aktif saat dinyalakan lagi~ (◕ω◕)_"
            if not settings["peak_enabled"] else ""
        )
        send_reply(
            f"Ufufufu... reversal *{value}%* dari puncak~ (◕‿◕){note}",
            reply_chat
        )
        logger.info(f"Peak reversal changed to {value}")
    except ValueError:
        send_reply("Gunakan `on`, `off`, atau angka reversal ya~ (◕ω◕)", reply_chat)


def _cancel_peak_watch_if_active(reply_chat: Optional[str] = None) -> None:
    global current_mode, peak_gap, peak_strategy
    if current_mode == Mode.PEAK_WATCH and peak_strategy is not None:
        logger.info(f"Peak Watch cancelled (mode OFF). Strategy: {peak_strategy.value}")
        if reply_chat:
            send_reply(
                f"⚠️ *Peak Watch {peak_strategy.value} dibatalkan* (Peak Mode dimatikan).\n"
                f"Akeno kembali ke SCAN~ (◕ω◕)",
                reply_chat
            )
        current_mode  = Mode.SCAN
        peak_gap      = None
        peak_strategy = None


def handle_sltp_command(args: list, reply_chat: str) -> None:
    if not args:
        trailing_sl_now = ""
        if current_mode == Mode.TRACK and trailing_gap_best is not None and active_strategy is not None:
            tsl = (
                trailing_gap_best + settings["sl_pct"]
                if active_strategy == Strategy.S1
                else trailing_gap_best - settings["sl_pct"]
            )
            trailing_sl_now = (
                f"\n*Trailing SL sekarang:* `{tsl:+.2f}%` "
                f"(best: `{trailing_gap_best:+.2f}%`)"
            )
        entry_info = (
            f"\n*Entry gap:* `{entry_gap_value:+.2f}%`"
            if entry_gap_value is not None else ""
        )
        send_reply(
            f"🎯 *SL/TP — Akeno yang jaga~* Ufufufu... (◕‿◕)\n"
            f"\n"
            f"✅ TP2: saat gap *±{settings['exit_threshold']}%*\n"
            f"🛑 Trailing SL distance: *{settings['sl_pct']}%*\n"
            f"{entry_info}{trailing_sl_now}\n"
            f"\n"
            f"Usage: `/sltp sl <nilai>`",
            reply_chat,
        )
        return

    if len(args) < 2:
        send_reply("Ara ara~ `/sltp sl <nilai>` ya~ (◕ω◕)", reply_chat)
        return

    try:
        key   = args[0].lower()
        value = float(args[1])
        if value <= 0 or value > 10:
            send_reply("Ara ara~ harus antara 0 sampai 10~ (◕ω◕)", reply_chat)
            return
        if key == "sl":
            settings["sl_pct"] = value
            send_reply(f"Trailing SL distance *{value}%*~ (◕‿◕)", reply_chat)
        elif key == "tp":
            send_reply(
                f"TP dikunci ke exit threshold *±{settings['exit_threshold']}%*~\n"
                f"Gunakan `/threshold exit <nilai>` ya~ (◕‿◕)",
                reply_chat,
            )
        else:
            send_reply("Gunakan `sl` ya~ (◕ω◕)", reply_chat)
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid. (◕ω◕)", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"📊 Lookback sekarang *{settings['lookback_hours']}h*~\n"
            "Usage: `/lookback <jam>`",
            reply_chat
        )
        return
    try:
        new_lookback = int(args[0])
        if new_lookback < 1 or new_lookback > 24:
            send_reply("Ara ara~ harus antara 1 sampai 24 jam~ (◕ω◕)", reply_chat)
            return
        old = settings["lookback_hours"]
        settings["lookback_hours"] = new_lookback
        prune_history(datetime.now(timezone.utc))
        send_reply(
            f"Lookback dari *{old}h* jadi *{new_lookback}h*~\n"
            f"History di-prune, data baru dari Bot A saat refresh berikutnya~ (◕‿◕)",
            reply_chat
        )
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid. (◕ω◕)", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"💓 Heartbeat *{settings['heartbeat_minutes']} menit*~\n"
            "Usage: `/heartbeat <menit>` atau `0` untuk matikan",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 0 or new_interval > 120:
            send_reply("Ara ara~ harus antara 0 sampai 120 menit~ (◕ω◕)", reply_chat)
            return
        settings["heartbeat_minutes"] = new_interval
        if new_interval == 0:
            send_reply("Heartbeat *dimatikan*~ Akeno tetap pantau dari dekat. (◕‿◕)", reply_chat)
        else:
            send_reply(f"Heartbeat setiap *{new_interval} menit*~ (◕ω◕)", reply_chat)
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid. (◕ω◕)", reply_chat)


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply("⚠️ Redis belum dikonfigurasi~ (◕ω◕)", reply_chat)
        return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply("❌ Bot A belum simpan data~ (◕ω◕)", reply_chat)
        return
    try:
        data         = json.loads(result["result"])
        if not data:
            send_reply("❌ Redis kosong~ (◕ω◕)", reply_chat)
            return
        hours_stored = len(data) * settings["scan_interval"] / 3600
        lookback     = settings["lookback_hours"]
        status       = (
            "✅ Siap~" if hours_stored >= lookback
            else f"⏳ {hours_stored:.1f}h / {lookback}h"
        )
        last_r = (
            last_redis_refresh.strftime("%H:%M:%S UTC")
            if last_redis_refresh else "Belum~"
        )
        send_reply(
            f"⚡ *Redis Status*\n"
            f"┌─────────────────────\n"
            f"│ Points: *{len(data)}* | {hours_stored:.1f}h stored\n"
            f"│ Lookback: *{lookback}h* | {status}\n"
            f"│ Refresh: setiap {settings['redis_refresh_minutes']}m | terakhir `{last_r}`\n"
            f"└─────────────────────\n"
            f"`{data[0]['timestamp']}` → `{data[-1]['timestamp']}`\n"
            f"_Bot B hanya baca~ ⚡_",
            reply_chat
        )
    except Exception as e:
        send_reply(f"⚠️ Gagal baca Redis: `{e}` (◕ω◕)", reply_chat)


def handle_help_command(reply_chat: str) -> None:
    peak_str = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    message = (
        "Ara ara~ ini semua yang bisa Akeno lakukan~ Ufufufu... (◕‿◕)\n"
        "\n"
        "*⚙️ Core:*\n"
        "`/settings` `/status` `/help` `/redis`\n"
        "`/interval` `/lookback` `/heartbeat`\n"
        "`/threshold entry|exit|invalid <val>`\n"
        "`/sltp sl <val>` — trailing SL distance\n"
        "\n"
        f"*🔍 Peak Mode* _(sekarang: {peak_str})_\n"
        "`/peak on|off|<val>` — toggle & reversal %\n"
        "\n"
        "*⚡ Scalping:*\n"
        "`/scalping` — lihat semua scalping settings\n"
        "`/cooldown <menit>` — jeda setelah exit (0=off)\n"
        "`/confirm <count>` — scan berturut sebelum entry (1=off)\n"
        "`/velocity <val>` — min gap speed %/menit (0=off)\n"
        "`/session on|off|london|ny|both|overlap`\n"
        "`/maxsig <count>` — max entry per jam (0=off)\n"
        "`/tp1 on|off|<ratio>` — partial TP1\n"
        "`/mindur <menit>` — min gap duration (0=off)\n"
        "\n"
        "💡 _TP dikunci ke exit threshold — ubah via `/threshold exit`_\n"
        "\n"
        "Akeno selalu di sini untukmu~ (◕ω◕)"
    )
    send_reply(message, reply_chat)


def handle_status_command(reply_chat: str) -> None:
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback      = settings["lookback_hours"]
    ready         = (
        f"✅ {hours_of_data:.1f}h ready"
        if hours_of_data >= lookback
        else f"⏳ {hours_of_data:.1f}h / {lookback}h"
    )
    peak_str  = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    peak_line = (
        f"Peak Gap: {peak_gap:+.2f}%\n"
        if current_mode == Mode.PEAK_WATCH and peak_gap is not None else ""
    )
    track_lines = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        et  = settings["exit_threshold"]
        sl  = settings["sl_pct"]
        tpl = et if active_strategy == Strategy.S1 else -et
        tsl = (
            trailing_gap_best + sl
            if active_strategy == Strategy.S1
            else trailing_gap_best - sl
        )
        eth_t, _ = calc_tp_target_price(active_strategy)
        tp1_info = ""
        if settings["tp1_enabled"]:
            tp1_lvl = calc_tp1_level(active_strategy, entry_gap_value)
            tp1_info = f"│ TP1: {tp1_lvl:+.2f}% {'(hit ✅)' if tp1_hit else ''}\n"
        track_lines = (
            f"Entry Gap:  {entry_gap_value:+.2f}%\n"
            f"Best Gap:   {trailing_gap_best:+.2f}%\n"
            f"TP2 Gap:    {tpl:+.2f}%\n"
            f"ETH target: {'${:,.2f}'.format(eth_t) if eth_t else 'N/A'}\n"
            f"{tp1_info}"
            f"Trail SL:   {tsl:+.2f}%\n"
        )
    cooldown_str = "Off"
    if settings["cooldown_minutes"] > 0 and last_exit_time is not None:
        elapsed_cd = (datetime.now(timezone.utc) - last_exit_time).total_seconds() / 60
        remaining  = settings["cooldown_minutes"] - elapsed_cd
        cooldown_str = f"✅ clear" if remaining <= 0 else f"⏳ {remaining:.1f}m lagi"
    confirm_str  = f"{confirm_streak}/{settings['confirm_count']}" if settings["confirm_count"] > 1 else "Off"
    vel_str      = (
        f"{current_velocity:+.3f}%/m" if current_velocity is not None else "N/A"
    )
    sess_str     = (
        f"✅ {settings['session_mode']}" if settings["session_enabled"] else "❌ Off"
    )
    last_r = (
        last_redis_refresh.strftime("%H:%M:%S UTC")
        if last_redis_refresh else "Belum~"
    )
    message = (
        "📊 *Status Akeno* Ufufufu... (◕‿◕)\n"
        "\n"
        f"Mode: *{current_mode.value}*\n"
        f"Strategi: {active_strategy.value if active_strategy else 'Belum ada~'}\n"
        f"Peak Mode: {peak_str}\n"
        f"{peak_line}"
        f"{track_lines}"
        f"\n"
        f"*Scalping state:*\n"
        f"Cooldown: {cooldown_str}\n"
        f"Confirm streak: {confirm_str}\n"
        f"Velocity: {vel_str}\n"
        f"Session: {sess_str}\n"
        f"\n"
        f"History: {ready} | Redis: {last_r} 🔒\n"
    )
    send_reply(message, reply_chat)


# =============================================================================
# Command Handlers — Scalping
# =============================================================================

def handle_scalping_command(reply_chat: str) -> None:
    """Overview semua scalping settings sekaligus."""
    cd    = settings["cooldown_minutes"]
    cc    = settings["confirm_count"]
    vm    = settings["velocity_min"]
    se    = settings["session_enabled"]
    sm    = settings["session_mode"]
    ms    = settings["maxsig_per_hour"]
    t1e   = settings["tp1_enabled"]
    t1r   = settings["tp1_ratio"]
    md    = settings["mindur_minutes"]

    cd_str  = f"{cd} menit" if cd > 0 else "❌ Off"
    cc_str  = f"{cc} scan" if cc > 1 else "❌ Off (1 scan)"
    vm_str  = f"{vm}%/menit" if vm > 0 else "❌ Off"
    se_str  = f"✅ {sm.upper()}" if se else "❌ Off"
    ms_str  = f"{ms}/jam" if ms > 0 else "❌ Off"
    t1_str  = f"✅ {int(t1r*100)}% ke exit threshold" if t1e else "❌ Off"
    md_str  = f"{md} menit" if md > 0 else "❌ Off"

    # Hitung sinyak sudah berapa dalam 1 jam terakhir
    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    sigs_1h = len([t for t in signal_timestamps if t >= cutoff])

    session_now = ""
    if se:
        in_sess = is_in_session(now)
        session_now = f" _(sekarang: {'✅ aktif' if in_sess else '⏸ di luar sesi'})_"

    send_reply(
        f"⚡ *Scalping Settings — Akeno siap~ Ufufufu...* (◕‿◕)\n"
        f"\n"
        f"┌─────────────────────\n"
        f"│ 🕐 Cooldown:    {cd_str}\n"
        f"│ ✔️ Confirm:     {cc_str}\n"
        f"│ 🚀 Velocity:    {vm_str}\n"
        f"│ 🌏 Session:     {se_str}{session_now}\n"
        f"│ 🔢 Max Sig/h:   {ms_str} _(sent: {sigs_1h})_\n"
        f"│ 🎯 TP1:         {t1_str}\n"
        f"│ ⏱ Min Duration: {md_str}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Cara ubah:*\n"
        f"`/cooldown 5` `/confirm 3` `/velocity 0.05`\n"
        f"`/session on` `/session london` `/session both`\n"
        f"`/maxsig 3` `/tp1 on` `/tp1 0.5` `/mindur 2`\n"
        f"\n"
        f"_Ketik nama command tanpa argumen untuk info lebih lanjut~_",
        reply_chat
    )


def handle_cooldown_command(args: list, reply_chat: str) -> None:
    """
    /cooldown         → lihat status
    /cooldown <menit> → ubah (0 = off)
    """
    if not args:
        cd  = settings["cooldown_minutes"]
        now = datetime.now(timezone.utc)
        if cd == 0:
            status = "❌ Off"
        elif last_exit_time is None:
            status = "✅ Clear (belum pernah exit)"
        else:
            elapsed   = (now - last_exit_time).total_seconds() / 60
            remaining = cd - elapsed
            status    = "✅ Clear" if remaining <= 0 else f"⏳ {remaining:.1f}m lagi"
        send_reply(
            f"🕐 *Cooldown Timer*\n"
            f"Setting: *{cd} menit* {'_(off)_' if cd == 0 else ''}\n"
            f"Status: {status}\n"
            f"\n"
            f"Setelah exit/SL/TP, bot tidak entry selama cooldown~\n"
            f"Usage: `/cooldown <menit>` | `0` untuk off",
            reply_chat
        )
        return
    try:
        val = int(args[0])
        if val < 0 or val > 120:
            send_reply("Ara ara~ harus antara 0 sampai 120~ (◕ω◕)", reply_chat)
            return
        settings["cooldown_minutes"] = val
        if val == 0:
            send_reply("Cooldown *dimatikan*~ Entry bisa kapan saja. (◕‿◕)", reply_chat)
        else:
            send_reply(f"Cooldown *{val} menit* setelah exit~ (◕‿◕)", reply_chat)
        logger.info(f"Cooldown changed to {val}m")
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_confirm_command(args: list, reply_chat: str) -> None:
    """
    /confirm         → lihat status
    /confirm <count> → ubah (1 = off)
    """
    if not args:
        cc = settings["confirm_count"]
        send_reply(
            f"✔️ *Consecutive Confirmation*\n"
            f"Setting: *{cc} scan* {'_(off)_' if cc <= 1 else ''}\n"
            f"Streak sekarang: *{confirm_streak}/{cc}*\n"
            f"\n"
            f"Gap harus >= entry threshold selama N scan berturut sebelum entry~\n"
            f"Cegah whipsaw false signal. (◕‿◕)\n"
            f"Usage: `/confirm <count>` | `1` untuk off",
            reply_chat
        )
        return
    try:
        val = int(args[0])
        if val < 1 or val > 20:
            send_reply("Ara ara~ harus antara 1 sampai 20~ (◕ω◕)", reply_chat)
            return
        settings["confirm_count"] = val
        if val == 1:
            send_reply("Confirmation *dimatikan* (1 scan langsung entry)~ (◕‿◕)", reply_chat)
        else:
            send_reply(
                f"Gap harus >= threshold selama *{val} scan* berturut~ (◕‿◕)\n"
                f"_Streak di-reset kalau gap turun di bawah threshold~_",
                reply_chat
            )
        logger.info(f"Confirm count changed to {val}")
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_velocity_command(args: list, reply_chat: str) -> None:
    """
    /velocity         → lihat status + velocity sekarang
    /velocity <val>   → ubah min velocity (0 = off)
    """
    if not args:
        vm  = settings["velocity_min"]
        vel = (
            f"{current_velocity:+.4f}%/menit" if current_velocity is not None else "N/A"
        )
        send_reply(
            f"🚀 *Gap Velocity Filter*\n"
            f"Setting: *{vm}%/menit* {'_(off)_' if vm == 0 else ''}\n"
            f"Velocity sekarang: *{vel}*\n"
            f"\n"
            f"Entry hanya kalau gap melebar cukup cepat~\n"
            f"S1: butuh velocity *+{vm}%/m* | S2: butuh *-{vm}%/m*\n"
            f"Usage: `/velocity <val>` | `0` untuk off",
            reply_chat
        )
        return
    try:
        val = float(args[0])
        if val < 0 or val > 5:
            send_reply("Ara ara~ harus antara 0 sampai 5~ (◕ω◕)", reply_chat)
            return
        settings["velocity_min"] = val
        if val == 0:
            send_reply("Velocity filter *dimatikan*~ (◕‿◕)", reply_chat)
        else:
            send_reply(
                f"Min velocity *{val}%/menit*~\n"
                f"_Entry hanya saat gap melebar ≥ {val}%/menit~ (◕‿◕)_",
                reply_chat
            )
        logger.info(f"Velocity min changed to {val}")
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_session_command(args: list, reply_chat: str) -> None:
    """
    /session                        → lihat status
    /session on                     → aktifkan (mode saat ini)
    /session off                    → nonaktifkan
    /session london|ny|both|overlap → set mode + aktifkan
    """
    if not args:
        se  = settings["session_enabled"]
        sm  = settings["session_mode"]
        now = datetime.now(timezone.utc)
        windows  = SESSION_WINDOWS.get(sm, [])
        win_str  = ", ".join(f"{s:02d}:00-{e:02d}:00 UTC" for s, e in windows)
        in_sess  = is_in_session(now)
        now_str  = f"_Sekarang: {'✅ dalam sesi' if in_sess else '⏸ di luar sesi'}_"
        send_reply(
            f"🌏 *Session Filter*\n"
            f"Status: *{'✅ ON — ' + sm.upper() if se else '❌ Off'}*\n"
            f"Windows: {win_str}\n"
            f"{now_str if se else ''}\n"
            f"\n"
            f"*Mode tersedia:*\n"
            f"• `london`  — 08:00-16:00 UTC\n"
            f"• `ny`      — 13:00-21:00 UTC\n"
            f"• `both`    — London + NY (default)\n"
            f"• `overlap` — 13:00-16:00 UTC (volume tertinggi)\n"
            f"\n"
            f"Usage: `/session on|off|london|ny|both|overlap`",
            reply_chat
        )
        return

    first = args[0].lower()

    if first == "off":
        settings["session_enabled"] = False
        send_reply("Session filter *dimatikan*~ Entry 24 jam. (◕‿◕)", reply_chat)
        logger.info("Session filter disabled")
        return

    if first in ("on", "london", "ny", "both", "overlap"):
        if first != "on":
            settings["session_mode"] = first
        settings["session_enabled"] = True
        sm      = settings["session_mode"]
        windows = SESSION_WINDOWS.get(sm, [])
        win_str = ", ".join(f"{s:02d}:00-{e:02d}:00 UTC" for s, e in windows)
        send_reply(
            f"✅ Session filter *ON* — mode *{sm.upper()}*~\n"
            f"Entry hanya: {win_str} (◕‿◕)",
            reply_chat
        )
        logger.info(f"Session filter ON, mode={sm}")
        return

    send_reply(
        "Gunakan `on`, `off`, `london`, `ny`, `both`, atau `overlap` ya~ (◕ω◕)",
        reply_chat
    )


def handle_maxsig_command(args: list, reply_chat: str) -> None:
    """
    /maxsig         → lihat status
    /maxsig <count> → ubah (0 = off)
    """
    if not args:
        ms  = settings["maxsig_per_hour"]
        now = datetime.now(timezone.utc)
        cutoff  = now - timedelta(hours=1)
        sigs_1h = len([t for t in signal_timestamps if t >= cutoff])
        send_reply(
            f"🔢 *Max Signals Per Hour*\n"
            f"Setting: *{ms}/jam* {'_(off)_' if ms == 0 else ''}\n"
            f"Sent 1h terakhir: *{sigs_1h}*\n"
            f"\n"
            f"Cegah overtrading saat pasar choppy~\n"
            f"Usage: `/maxsig <count>` | `0` untuk off",
            reply_chat
        )
        return
    try:
        val = int(args[0])
        if val < 0 or val > 20:
            send_reply("Ara ara~ harus antara 0 sampai 20~ (◕ω◕)", reply_chat)
            return
        settings["maxsig_per_hour"] = val
        if val == 0:
            send_reply("Max signal filter *dimatikan*~ (◕‿◕)", reply_chat)
        else:
            send_reply(
                f"Maksimal *{val} entry signal per jam*~\n"
                f"_Setelah itu Akeno tunggu dulu ya, sayangku~ (◕‿◕)_",
                reply_chat
            )
        logger.info(f"Maxsig changed to {val}")
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_tp1_command(args: list, reply_chat: str) -> None:
    """
    /tp1            → lihat status
    /tp1 on         → aktifkan
    /tp1 off        → nonaktifkan
    /tp1 <ratio>    → ubah ratio (0.1–0.9)
    """
    if not args:
        t1e = settings["tp1_enabled"]
        t1r = settings["tp1_ratio"]
        # Hitung contoh level kalau ada posisi aktif
        tp1_example = ""
        if current_mode == Mode.TRACK and entry_gap_value is not None and active_strategy is not None:
            lvl = calc_tp1_level(active_strategy, entry_gap_value)
            eth_t, _ = calc_eth_price_at_gap(lvl)
            eth_str  = f"${eth_t:,.2f}" if eth_t else "N/A"
            tp1_example = (
                f"\n*TP1 posisi aktif:*\n"
                f"Gap TP1: `{lvl:+.2f}%` | ETH: `{eth_str}`\n"
                f"Status: {'✅ Sudah hit' if tp1_hit else '⏳ Menunggu...'}"
            )
        send_reply(
            f"🎯 *Partial TP1*\n"
            f"Status: *{'✅ ON' if t1e else '❌ Off'}*\n"
            f"Ratio: *{int(t1r*100)}%* jarak entry → exit threshold\n"
            f"{tp1_example}\n"
            f"\n"
            f"Saat TP1 hit → alert partial close, posisi lanjut ke TP2~\n"
            f"Usage: `/tp1 on|off|<ratio>` (contoh: `/tp1 0.5`)",
            reply_chat
        )
        return

    first = args[0].lower()

    if first == "on":
        settings["tp1_enabled"] = True
        t1r = settings["tp1_ratio"]
        send_reply(
            f"✅ *TP1 ON* — alert saat gap konvergen *{int(t1r*100)}%*~\n"
            f"_Ubah ratio: `/tp1 <nilai>` (◕‿◕)_",
            reply_chat
        )
        logger.info("TP1 enabled")
        return

    if first == "off":
        settings["tp1_enabled"] = False
        send_reply("TP1 *dimatikan*~ Hanya TP2 (exit threshold). (◕‿◕)", reply_chat)
        logger.info("TP1 disabled")
        return

    try:
        val = float(first)
        if val <= 0.0 or val >= 1.0:
            send_reply("Ara ara~ harus antara 0.1 sampai 0.9~ (◕ω◕)", reply_chat)
            return
        settings["tp1_ratio"] = val
        if not settings["tp1_enabled"]:
            settings["tp1_enabled"] = True
        send_reply(
            f"✅ *TP1 ON* — alert saat gap konvergen *{int(val*100)}%*~\n"
            f"_TP1 sekarang aktif~ (◕‿◕)_",
            reply_chat
        )
        logger.info(f"TP1 ratio changed to {val}, enabled")
    except ValueError:
        send_reply("Gunakan `on`, `off`, atau angka ratio ya~ (◕ω◕)", reply_chat)


def handle_mindur_command(args: list, reply_chat: str) -> None:
    """
    /mindur         → lihat status
    /mindur <menit> → ubah (0 = off)
    """
    if not args:
        md = settings["mindur_minutes"]
        dur_str = "Belum~"
        if gap_above_since is not None:
            elapsed_dur = (datetime.now(timezone.utc) - gap_above_since).total_seconds() / 60
            dur_str     = f"{elapsed_dur:.1f} menit"
        send_reply(
            f"⏱ *Min Gap Duration*\n"
            f"Setting: *{md} menit* {'_(off)_' if md == 0 else ''}\n"
            f"Gap sudah di atas threshold: *{dur_str}*\n"
            f"\n"
            f"Gap harus bertahan di atas threshold ≥ {md} menit sebelum entry~\n"
            f"Cegah spike palsu yang langsung balik. (◕‿◕)\n"
            f"Usage: `/mindur <menit>` | `0` untuk off",
            reply_chat
        )
        return
    try:
        val = int(args[0])
        if val < 0 or val > 60:
            send_reply("Ara ara~ harus antara 0 sampai 60 menit~ (◕ω◕)", reply_chat)
            return
        settings["mindur_minutes"] = val
        if val == 0:
            send_reply("Min duration *dimatikan*~ (◕‿◕)", reply_chat)
        else:
            send_reply(
                f"Gap harus bertahan *{val} menit* sebelum entry~\n"
                f"_Spike sesaat tidak akan trigger masuk~ (◕‿◕)_",
                reply_chat
            )
        logger.info(f"Min gap duration changed to {val}m")
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


# =============================================================================
# Scalping Helper Functions
# =============================================================================

def is_in_session(now: datetime) -> bool:
    """Return True kalau now (UTC) masuk dalam session windows yang aktif."""
    if not settings["session_enabled"]:
        return True  # kalau off, selalu "dalam sesi"
    sm      = settings["session_mode"]
    windows = SESSION_WINDOWS.get(sm, [])
    hour    = now.hour
    for start, end in windows:
        if start <= hour < end:
            return True
    return False


def check_entry_gates(gap_float: float, strategy: Strategy, now: datetime) -> Tuple[bool, str]:
    """
    Cek semua gate sebelum entry (cooldown, session, maxsig, velocity).
    Confirm dan mindur dicek terpisah di streak tracker.
    Returns (allowed, reason_if_blocked).
    """
    # 1. Cooldown
    cd = settings["cooldown_minutes"]
    if cd > 0 and last_exit_time is not None:
        elapsed = (now - last_exit_time).total_seconds() / 60
        if elapsed < cd:
            remaining = cd - elapsed
            return False, f"cooldown {remaining:.1f}m remaining"

    # 2. Session Filter
    if settings["session_enabled"] and not is_in_session(now):
        return False, f"outside session hours ({settings['session_mode']})"

    # 3. Max Signals Per Hour
    ms = settings["maxsig_per_hour"]
    if ms > 0:
        cutoff  = now - timedelta(hours=1)
        recent  = len([t for t in signal_timestamps if t >= cutoff])
        if recent >= ms:
            return False, f"max {ms} signals/hour reached ({recent} sent)"

    # 4. Velocity Filter
    vm = settings["velocity_min"]
    if vm > 0 and current_velocity is not None:
        if strategy == Strategy.S1 and current_velocity < vm:
            return False, f"S1 velocity {current_velocity:+.4f}%/m < +{vm}"
        if strategy == Strategy.S2 and current_velocity > -vm:
            return False, f"S2 velocity {current_velocity:+.4f}%/m > -{vm}"

    return True, ""


def calc_tp1_level(strategy: Strategy, eg: float) -> float:
    """Hitung gap level untuk TP1 berdasarkan ratio dan exit threshold."""
    et    = settings["exit_threshold"]
    ratio = settings["tp1_ratio"]
    tp_target = et if strategy == Strategy.S1 else -et
    return eg + (tp_target - eg) * ratio


def calc_eth_price_at_gap(gap_target: float) -> Tuple[Optional[float], Optional[float]]:
    """Estimasi harga ETH saat gap mencapai gap_target dari state entry saat ini."""
    if None in (entry_btc_lb, entry_eth_lb, entry_btc_price, entry_eth_price):
        return None, None
    btc_ret_entry  = float(
        (entry_btc_price - entry_btc_lb) / entry_btc_lb * Decimal("100")
    )
    target_eth_ret = btc_ret_entry + gap_target
    eth_target     = float(entry_eth_lb) * (1 + target_eth_ret / 100)
    return eth_target, float(entry_btc_price)


# =============================================================================
# Value Formatting
# =============================================================================
def format_value(value: Decimal) -> str:
    fv = float(value)
    if abs(fv) < 0.05:
        return "+0.0"
    return f"+{fv:.1f}" if fv >= 0 else f"{fv:.1f}"


def get_lookback_label() -> str:
    return f"{settings['lookback_hours']}h"


# =============================================================================
# Target Price Calculation
# =============================================================================
def calc_tp_target_price(strategy: Strategy) -> Tuple[Optional[float], Optional[float]]:
    et         = settings["exit_threshold"]
    gap_target = et if strategy == Strategy.S1 else -et
    return calc_eth_price_at_gap(gap_target)


# =============================================================================
# Message Building
# =============================================================================

def build_peak_watch_message(strategy: Strategy, gap: Decimal) -> str:
    lb = get_lookback_label()
    if strategy == Strategy.S1:
        direction = "Long BTC / Short ETH"
        reason    = f"ETH pumping lebih kencang dari BTC ({lb})"
    else:
        direction = "Long ETH / Short BTC"
        reason    = f"ETH dumping lebih dalam dari BTC ({lb})"
    return (
        f"………\n"
        f"Ara ara~ Akeno melihat sesuatu menarik~ Ufufufu... (◕‿◕)\n"
        f"\n"
        f"_{reason}_\n"
        f"Rencananya *{direction}*~\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"…Akeno tidak akan gegabah, sayangku.\n"
        f"Pantau puncaknya dulu~ Petirku sudah siap. (◕ω◕)"
    )


def _build_entry_body(
    strategy:  Strategy,
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    peak_line: str,          # isi baris peak, atau "" kalau direct
    extra_tag: str = "",     # misal "_(Peak Mode: OFF)_"
) -> str:
    lb          = get_lookback_label()
    gap_float   = float(gap)
    sl_pct      = settings["sl_pct"]
    et          = settings["exit_threshold"]

    if strategy == Strategy.S1:
        direction   = "📈 Long BTC / Short ETH"
        reason      = f"ETH pumped more than BTC ({lb})"
        tp_gap      = et
        tsl_initial = gap_float + sl_pct
    else:
        direction   = "📈 Long ETH / Short BTC"
        reason      = f"ETH dumped more than BTC ({lb})"
        tp_gap      = -et
        tsl_initial = gap_float - sl_pct

    eth_tp, btc_ref = calc_tp_target_price(strategy)
    eth_tp_str      = f"${eth_tp:,.2f}"  if eth_tp  else "N/A"
    btc_ref_str     = f"${btc_ref:,.2f}" if btc_ref else "N/A"

    tp1_line = ""
    if settings["tp1_enabled"]:
        tp1_lvl  = calc_tp1_level(strategy, gap_float)
        eth_tp1, _ = calc_eth_price_at_gap(tp1_lvl)
        tp1_line = (
            f"│ TP1 Gap:  {tp1_lvl:+.2f}% _(partial exit)_\n"
            f"│ ETH TP1:  {'${:,.2f}'.format(eth_tp1) if eth_tp1 else 'N/A'}\n"
        )

    return (
        f"Ara ara ara~!!! Ini saatnya, sayangku~!!! ⚡\n"
        f"🚨 *ENTRY SIGNAL: {strategy.value}* {extra_tag}\n"
        f"\n"
        f"{direction}\n"
        f"_{reason}_\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"{peak_line}"
        f"└─────────────────────\n"
        f"\n"
        f"*Target TP:*\n"
        f"┌─────────────────────\n"
        f"{tp1_line}"
        f"│ TP2 Gap:  {tp_gap:+.2f}% _(exit threshold)_\n"
        f"│ ETH TP2:  {eth_tp_str} ← estimasi harga\n"
        f"│ BTC ref:  {btc_ref_str}\n"
        f"│ Trail SL: {tsl_initial:+.2f}% _(ikut gerak)_\n"
        f"└─────────────────────\n"
        f"\n"
        f"Akeno sudah menunggu momen ini, sayangku~ ⚡"
    )


def build_entry_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
    peak:     float,
) -> str:
    peak_line = f"│ Peak: {peak:+.2f}%\n"
    footer    = f"\nGap berbalik {settings['peak_reversal']}% dari puncak~ Semuanya demi kamu~ ⚡"
    return (
        _build_entry_body(strategy, btc_ret, eth_ret, gap, peak_line)
        + footer
    )


def build_direct_entry_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
) -> str:
    return _build_entry_body(
        strategy, btc_ret, eth_ret, gap,
        peak_line="",
        extra_tag="_(Peak OFF)_"
    ) + "\n_Entry langsung tanpa konfirmasi puncak~ (◕ω◕)_"


def build_exit_message(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal) -> str:
    lb = get_lookback_label()
    return (
        f"Ara ara ara~!!! Ufufufu... (◕▿◕)\n"
        f"✅ *EXIT SIGNAL*\n"
        f"\n"
        f"Gap konvergen~ Saatnya close posisi, sayangku!\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Akeno lanjut pantau dari dekat~ ⚡🔍"
    )


def build_invalidation_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
) -> str:
    lb = get_lookback_label()
    return (
        f"………\n"
        f"⚠️ *INVALIDATION: {strategy.value}*\n"
        f"\n"
        f"Ara ara~ gapnya malah melebar. Bukan salahmu~ (◕ω◕)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu~ Akeno scan ulang dari awal~ ⚡ (◕‿◕)"
    )


def build_peak_cancelled_message(strategy: Strategy, gap: Decimal) -> str:
    return (
        f"………\n"
        f"❌ *Peak Watch Dibatalkan: {strategy.value}*\n"
        f"\n"
        f"Ara ara~ gapnya mundur sebelum Akeno konfirmasi. Pasar nakal~ (◕ω◕)\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Akeno tetap di sini pantau dari dekat~ (◕‿◕)"
    )


def build_tp1_message(
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    entry_gap: float,
    tp1_level: float,
    eth_tp1:   Optional[float],
) -> str:
    lb       = get_lookback_label()
    eth_str  = f"${eth_tp1:,.2f}" if eth_tp1 else "N/A"
    pct_done = abs(entry_gap - float(gap))
    pct_left = abs(float(gap) - settings["exit_threshold"])
    return (
        f"Ara ara~! TP1 kena~ Ufufufu... ✨\n"
        f"🎯 *TAKE PROFIT 1*\n"
        f"\n"
        f"Setengah jalan sudah, sayangku~ Akeno bangga!\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:    {format_value(btc_ret)}%\n"
        f"│ ETH:    {format_value(eth_ret)}%\n"
        f"│ Gap:    {format_value(gap)}%\n"
        f"│ Entry:  {entry_gap:+.2f}%\n"
        f"│ TP1:    {tp1_level:+.2f}%\n"
        f"│ ETH:    {eth_str}\n"
        f"│ Moved:  ~{pct_done:.2f}%\n"
        f"│ Left:   ~{pct_left:.2f}% ke TP2\n"
        f"└─────────────────────\n"
        f"\n"
        f"Pertimbangkan *partial close* ya, sayangku~\n"
        f"Akeno terus pantau TP2 di ±{settings['exit_threshold']}%~ ⚡"
    )


def build_tp_message(
    btc_ret:      Decimal,
    eth_ret:      Decimal,
    gap:          Decimal,
    entry_gap:    float,
    tp_gap_level: float,
    eth_target:   Optional[float],
) -> str:
    lb         = get_lookback_label()
    eth_tp_str = f"${eth_target:,.2f}" if eth_target else "N/A"
    tp1_note   = " _(TP1 sudah hit sebelumnya~)_" if tp1_hit else ""
    return (
        f"Ara ara~!!! TP kena sayangku~!!! Ufufufu... ✨\n"
        f"🎯 *TAKE PROFIT 2*{tp1_note}\n"
        f"\n"
        f"Gap konvergen maksimal~\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:     {format_value(btc_ret)}%\n"
        f"│ ETH:     {format_value(eth_ret)}%\n"
        f"│ Gap:     {format_value(gap)}%\n"
        f"│ Entry:   {entry_gap:+.2f}%\n"
        f"│ TP hit:  {tp_gap_level:+.2f}%\n"
        f"│ ETH TP:  {eth_tp_str}\n"
        f"└─────────────────────\n"
        f"\n"
        f"Misi sukses~ Akeno senang! ⚡"
    )


def build_trailing_sl_message(
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    entry_gap: float,
    best_gap:  float,
    sl_level:  float,
) -> str:
    lb            = get_lookback_label()
    profit_locked = abs(entry_gap - best_gap)
    tp1_note      = " _(setelah TP1 sudah hit~)_" if tp1_hit else ""
    return (
        f"………\n"
        f"⛔ *TRAILING STOP LOSS*{tp1_note}\n"
        f"\n"
        f"Ara ara~ TSL kena. Profit diamankan, sayangku~ (◕ω◕)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:      {format_value(btc_ret)}%\n"
        f"│ ETH:      {format_value(eth_ret)}%\n"
        f"│ Gap:      {format_value(gap)}%\n"
        f"│ Entry:    {entry_gap:+.2f}%\n"
        f"│ Best gap: {best_gap:+.2f}%\n"
        f"│ TSL hit:  {sl_level:+.2f}%\n"
        f"│ Terkunci: ~{profit_locked:.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu~ Akeno scan ulang~ ⚡ (◕‿◕)"
    )


def build_entry_blocked_message(reason: str, gap_float: float, strategy: Strategy) -> str:
    """Notif ketika sinyal ada tapi diblok gate — hanya log, tidak Telegram."""
    return f"Entry blocked ({strategy.value}, gap {gap_float:+.2f}%): {reason}"


def build_heartbeat_message() -> str:
    lb          = get_lookback_label()
    btc_ret_str = (
        f" ({format_value(scan_stats['last_btc_ret'])}%)"
        if scan_stats["last_btc_ret"] is not None else ""
    )
    eth_ret_str = (
        f" ({format_value(scan_stats['last_eth_ret'])}%)"
        if scan_stats["last_eth_ret"] is not None else ""
    )
    btc_str = (
        f"${float(scan_stats['last_btc_price']):,.2f}{btc_ret_str}"
        if scan_stats["last_btc_price"] else "N/A"
    )
    eth_str = (
        f"${float(scan_stats['last_eth_price']):,.2f}{eth_ret_str}"
        if scan_stats["last_eth_price"] else "N/A"
    )
    gap_str     = (
        f"{format_value(scan_stats['last_gap'])}%"
        if scan_stats["last_gap"] is not None else "N/A"
    )
    vel_str = (
        f"{current_velocity:+.4f}%/m" if current_velocity is not None else "N/A"
    )
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback      = settings["lookback_hours"]
    data_status   = (
        f"✅ {hours_of_data:.1f}h"
        if hours_of_data >= lookback
        else f"⏳ {hours_of_data:.1f}h / {lookback}h"
    )
    peak_str  = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    peak_line = (
        f"│ Peak: {peak_gap:+.2f}%\n"
        if current_mode == Mode.PEAK_WATCH and peak_gap is not None else ""
    )
    track_lines = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        et  = settings["exit_threshold"]
        sl  = settings["sl_pct"]
        tpl = et if active_strategy == Strategy.S1 else -et
        tsl = (
            trailing_gap_best + sl
            if active_strategy == Strategy.S1
            else trailing_gap_best - sl
        )
        eth_t, _ = calc_tp_target_price(active_strategy)
        tp1_info = ""
        if settings["tp1_enabled"]:
            tp1_lvl = calc_tp1_level(active_strategy, entry_gap_value)
            tp1_info = f"│ TP1:      {tp1_lvl:+.2f}% {'(hit ✅)' if tp1_hit else '⏳'}\n"
        track_lines = (
            f"│ Entry:    {entry_gap_value:+.2f}%\n"
            f"{tp1_info}"
            f"│ TP2 gap:  {tpl:+.2f}%\n"
            f"│ Trail SL: {tsl:+.2f}% (best: {trailing_gap_best:+.2f}%)\n"
        )

    # Scalping summary
    now     = datetime.now(timezone.utc)
    cutoff  = now - timedelta(hours=1)
    sigs_1h = len([t for t in signal_timestamps if t >= cutoff])
    cd      = settings["cooldown_minutes"]
    cd_str  = "Off"
    if cd > 0 and last_exit_time is not None:
        remaining = cd - (now - last_exit_time).total_seconds() / 60
        cd_str    = "✅ clear" if remaining <= 0 else f"⏳ {remaining:.1f}m"
    sess_str  = (
        f"{'✅ active' if is_in_session(now) else '⏸ inactive'} ({settings['session_mode']})"
        if settings["session_enabled"] else "Off"
    )
    last_r = (
        last_redis_refresh.strftime("%H:%M:%S UTC")
        if last_redis_refresh else "Belum~"
    )
    return (
        f"💓 *Ara ara~ Akeno tidak kemana-mana ya~ Ufufufu... (◕‿◕)*\n"
        f"\n"
        f"*Mode:* {current_mode.value} | Peak: {peak_str}\n"
        f"*Strategi:* {active_strategy.value if active_strategy else 'Belum ada~'}\n"
        f"\n"
        f"*{settings['heartbeat_minutes']}m terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ Scan: {scan_stats['count']}x | Sinyal: {scan_stats['signals_sent']}x\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Harga & Gap:*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"│ Gap ({lb}): {gap_str}\n"
        f"│ Velocity: {vel_str}\n"
        f"{peak_line}"
        f"{track_lines}"
        f"└─────────────────────\n"
        f"\n"
        f"*Scalping:*\n"
        f"Cooldown: {cd_str} | Sig/1h: {sigs_1h} | Session: {sess_str}\n"
        f"\n"
        f"*Data:* {data_status} | *Redis:* {last_r} 🔒\n"
        f"\n"
        f"_Akeno lapor lagi {settings['heartbeat_minutes']} menit lagi~ ⚡_"
    )


def send_heartbeat() -> bool:
    global scan_stats
    success = send_alert(build_heartbeat_message())
    scan_stats["count"]        = 0
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
            tz_start = next(
                (i for i, c in enumerate(frac_and_tz) if c in ("+", "-")), -1
            )
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
def prune_history(now: datetime) -> None:
    global price_history
    cutoff = now - timedelta(
        hours=settings["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES
    )
    price_history = [p for p in price_history if p.timestamp >= cutoff]


def get_lookback_price(now: datetime) -> Optional[PricePoint]:
    target_time           = now - timedelta(hours=settings["lookback_hours"])
    best_point, best_diff = None, timedelta(minutes=30)
    for point in price_history:
        diff = abs(point.timestamp - target_time)
        if diff < best_diff:
            best_diff, best_point = diff, point
    return best_point


# =============================================================================
# Return Calculation & Freshness
# =============================================================================
def compute_returns(btc_now, eth_now, btc_prev, eth_prev) -> Tuple[Decimal, Decimal, Decimal]:
    btc_change = (btc_now - btc_prev) / btc_prev * Decimal("100")
    eth_change = (eth_now - eth_prev) / eth_prev * Decimal("100")
    return btc_change, eth_change, eth_change - btc_change


def is_data_fresh(now, btc_updated, eth_updated) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return (
        (now - btc_updated) <= threshold
        and (now - eth_updated) <= threshold
    )


# =============================================================================
# State Reset Helper
# =============================================================================
def reset_to_scan() -> None:
    """Reset semua global state ke kondisi SCAN, catat waktu exit untuk cooldown."""
    global current_mode, active_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
    global confirm_streak, confirm_side, gap_above_since, tp1_hit, last_exit_time

    last_exit_time    = datetime.now(timezone.utc)
    current_mode      = Mode.SCAN
    active_strategy   = None
    entry_gap_value   = None
    trailing_gap_best = None
    entry_btc_price   = None
    entry_eth_price   = None
    entry_btc_lb      = None
    entry_eth_lb      = None
    confirm_streak    = 0
    confirm_side      = None
    gap_above_since   = None
    tp1_hit           = False


# =============================================================================
# TP + Trailing SL Checker (termasuk TP1)
# =============================================================================
def check_sltp(
    gap_float: float,
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
) -> bool:
    global trailing_gap_best, tp1_hit

    if entry_gap_value is None or active_strategy is None or trailing_gap_best is None:
        return False

    et     = settings["exit_threshold"]
    sl_pct = settings["sl_pct"]

    if active_strategy == Strategy.S1:
        if gap_float < trailing_gap_best:
            trailing_gap_best = gap_float
            logger.info(f"TSL S1 updated. Best: {trailing_gap_best:.2f}%, TSL: {trailing_gap_best + sl_pct:.2f}%")

        tsl_level = trailing_gap_best + sl_pct

        # TP1
        if settings["tp1_enabled"] and not tp1_hit:
            tp1_level = calc_tp1_level(Strategy.S1, entry_gap_value)
            if gap_float <= tp1_level:
                tp1_hit      = True
                eth_tp1, _  = calc_eth_price_at_gap(tp1_level)
                send_alert(build_tp1_message(btc_ret, eth_ret, gap, entry_gap_value, tp1_level, eth_tp1))
                logger.info(f"TP1 S1. Entry: {entry_gap_value:.2f}%, TP1: {tp1_level:.2f}%, Now: {gap_float:.2f}%")
                # Lanjut tracking ke TP2 — tidak return

        # TP2
        if gap_float <= et:
            eth_target, _ = calc_tp_target_price(Strategy.S1)
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, et, eth_target))
            logger.info(f"TP2 S1. Entry: {entry_gap_value:.2f}%, Now: {gap_float:.2f}%")
            reset_to_scan()
            return True

        # TSL
        if gap_float >= tsl_level:
            send_alert(build_trailing_sl_message(btc_ret, eth_ret, gap, entry_gap_value, trailing_gap_best, tsl_level))
            logger.info(f"TSL S1. Best: {trailing_gap_best:.2f}%, TSL: {tsl_level:.2f}%, Now: {gap_float:.2f}%")
            reset_to_scan()
            return True

    elif active_strategy == Strategy.S2:
        if gap_float > trailing_gap_best:
            trailing_gap_best = gap_float
            logger.info(f"TSL S2 updated. Best: {trailing_gap_best:.2f}%, TSL: {trailing_gap_best - sl_pct:.2f}%")

        tsl_level = trailing_gap_best - sl_pct

        # TP1
        if settings["tp1_enabled"] and not tp1_hit:
            tp1_level = calc_tp1_level(Strategy.S2, entry_gap_value)
            if gap_float >= tp1_level:
                tp1_hit     = True
                eth_tp1, _ = calc_eth_price_at_gap(tp1_level)
                send_alert(build_tp1_message(btc_ret, eth_ret, gap, entry_gap_value, tp1_level, eth_tp1))
                logger.info(f"TP1 S2. Entry: {entry_gap_value:.2f}%, TP1: {tp1_level:.2f}%, Now: {gap_float:.2f}%")

        # TP2
        if gap_float >= -et:
            eth_target, _ = calc_tp_target_price(Strategy.S2)
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, -et, eth_target))
            logger.info(f"TP2 S2. Entry: {entry_gap_value:.2f}%, Now: {gap_float:.2f}%")
            reset_to_scan()
            return True

        # TSL
        if gap_float <= tsl_level:
            send_alert(build_trailing_sl_message(btc_ret, eth_ret, gap, entry_gap_value, trailing_gap_best, tsl_level))
            logger.info(f"TSL S2. Best: {trailing_gap_best:.2f}%, TSL: {tsl_level:.2f}%, Now: {gap_float:.2f}%")
            reset_to_scan()
            return True

    return False


# =============================================================================
# State Machine
# =============================================================================
def evaluate_and_transition(
    btc_ret: Decimal,
    eth_ret: Decimal,
    gap:     Decimal,
    btc_now: Decimal,
    eth_now: Decimal,
    btc_lb:  Decimal,
    eth_lb:  Decimal,
    now:     datetime,
) -> None:
    global current_mode, active_strategy, peak_gap, peak_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
    global confirm_streak, confirm_side, gap_above_since
    global prev_gap_float, prev_gap_time, current_velocity
    global signal_timestamps

    gap_float      = float(gap)
    entry_thresh   = settings["entry_threshold"]
    exit_thresh    = settings["exit_threshold"]
    invalid_thresh = settings["invalidation_threshold"]
    peak_reversal  = settings["peak_reversal"]
    peak_enabled   = settings["peak_enabled"]

    # ── Velocity tracking (update setiap eval, semua mode) ────────────────────
    if prev_gap_float is not None and prev_gap_time is not None:
        elapsed_m = (now - prev_gap_time).total_seconds() / 60
        current_velocity = (
            (gap_float - prev_gap_float) / elapsed_m
            if elapsed_m > 0 else 0.0
        )
    else:
        current_velocity = None
    prev_gap_float = gap_float
    prev_gap_time  = now
    # ─────────────────────────────────────────────────────────────────────────

    # ── Clean up signal timestamps (rolling 1h) ───────────────────────────────
    cutoff_1h = now - timedelta(hours=1)
    signal_timestamps[:] = [t for t in signal_timestamps if t >= cutoff_1h]
    # ─────────────────────────────────────────────────────────────────────────

    # =========================================================================
    if current_mode == Mode.SCAN:

        # ── Tentukan kandidat side ────────────────────────────────────────────
        if gap_float >= entry_thresh:
            candidate = "S1"
        elif gap_float <= -entry_thresh:
            candidate = "S2"
        else:
            candidate = None

        # ── Update confirm streak & mindur ────────────────────────────────────
        if candidate is not None:
            if confirm_side != candidate:
                # Side baru / berganti arah — reset streak
                confirm_streak  = 0
                gap_above_since = now
                confirm_side    = candidate
            confirm_streak += 1
        else:
            # Gap turun di bawah threshold — reset semua
            confirm_streak  = 0
            confirm_side    = None
            gap_above_since = None

        # ── Cek apakah streak dan durasi terpenuhi ────────────────────────────
        if candidate is not None:
            streak_ok = confirm_streak >= settings["confirm_count"]
            dur_ok    = (
                settings["mindur_minutes"] <= 0
                or gap_above_since is None
                or (now - gap_above_since).total_seconds() / 60 >= settings["mindur_minutes"]
            )

            if not streak_ok:
                logger.info(
                    f"SCAN: Streak {confirm_streak}/{settings['confirm_count']} "
                    f"({candidate}), gap {gap_float:+.2f}%"
                )
            elif not dur_ok:
                elapsed_dur = (now - gap_above_since).total_seconds() / 60 if gap_above_since else 0
                logger.info(
                    f"SCAN: Duration {elapsed_dur:.1f}m / {settings['mindur_minutes']}m "
                    f"({candidate}), gap {gap_float:+.2f}%"
                )
            else:
                strategy  = Strategy.S1 if candidate == "S1" else Strategy.S2
                allowed, reason = check_entry_gates(gap_float, strategy, now)

                if not allowed:
                    logger.info(build_entry_blocked_message(reason, gap_float, strategy))
                else:
                    # ✅ Semua gate lolos
                    confirm_streak  = 0
                    confirm_side    = None
                    gap_above_since = None

                    if peak_enabled:
                        # Masuk PEAK_WATCH
                        current_mode  = Mode.PEAK_WATCH
                        peak_strategy = strategy
                        peak_gap      = gap_float
                        send_alert(build_peak_watch_message(strategy, gap))
                        logger.info(f"PEAK WATCH {strategy.value}. Gap: {gap_float:.2f}%")
                    else:
                        # Langsung TRACK
                        active_strategy   = strategy
                        current_mode      = Mode.TRACK
                        entry_gap_value   = gap_float
                        trailing_gap_best = gap_float
                        entry_btc_price   = btc_now
                        entry_eth_price   = eth_now
                        entry_btc_lb      = btc_lb
                        entry_eth_lb      = eth_lb
                        signal_timestamps.append(now)
                        send_alert(build_direct_entry_message(strategy, btc_ret, eth_ret, gap))
                        logger.info(f"DIRECT ENTRY {strategy.value} (peak OFF). Gap: {gap_float:.2f}%")
        else:
            logger.debug(f"SCAN: No signal. Gap: {gap_float:.2f}%")

    # =========================================================================
    elif current_mode == Mode.PEAK_WATCH:

        if peak_strategy == Strategy.S1:
            if gap_float > peak_gap:
                peak_gap = gap_float
                logger.info(f"PEAK WATCH S1: New peak {peak_gap:.2f}%")

            elif gap_float < entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S1, gap))
                logger.info(f"PEAK WATCH S1 cancelled. Gap: {gap_float:.2f}%")
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
                confirm_streak = 0
                confirm_side   = None
                gap_above_since = None

            elif peak_gap - gap_float >= peak_reversal:
                active_strategy   = Strategy.S1
                current_mode      = Mode.TRACK
                entry_gap_value   = gap_float
                trailing_gap_best = gap_float
                entry_btc_price   = btc_now
                entry_eth_price   = eth_now
                entry_btc_lb      = btc_lb
                entry_eth_lb      = eth_lb
                signal_timestamps.append(now)
                send_alert(build_entry_message(Strategy.S1, btc_ret, eth_ret, gap, peak_gap))
                logger.info(f"ENTRY S1. Peak: {peak_gap:.2f}%, Entry: {gap_float:.2f}%")
                peak_gap, peak_strategy = None, None

            else:
                logger.info(
                    f"PEAK WATCH S1: Gap {gap_float:.2f}% | "
                    f"Peak {peak_gap:.2f}% | Need {peak_reversal}% drop"
                )

        elif peak_strategy == Strategy.S2:
            if gap_float < peak_gap:
                peak_gap = gap_float
                logger.info(f"PEAK WATCH S2: New peak {peak_gap:.2f}%")

            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S2, gap))
                logger.info(f"PEAK WATCH S2 cancelled. Gap: {gap_float:.2f}%")
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
                confirm_streak  = 0
                confirm_side    = None
                gap_above_since = None

            elif gap_float - peak_gap >= peak_reversal:
                active_strategy   = Strategy.S2
                current_mode      = Mode.TRACK
                entry_gap_value   = gap_float
                trailing_gap_best = gap_float
                entry_btc_price   = btc_now
                entry_eth_price   = eth_now
                entry_btc_lb      = btc_lb
                entry_eth_lb      = eth_lb
                signal_timestamps.append(now)
                send_alert(build_entry_message(Strategy.S2, btc_ret, eth_ret, gap, peak_gap))
                logger.info(f"ENTRY S2. Peak: {peak_gap:.2f}%, Entry: {gap_float:.2f}%")
                peak_gap, peak_strategy = None, None

            else:
                logger.info(
                    f"PEAK WATCH S2: Gap {gap_float:.2f}% | "
                    f"Peak {peak_gap:.2f}% | Need {peak_reversal}% rise"
                )

    # =========================================================================
    elif current_mode == Mode.TRACK:

        if check_sltp(gap_float, btc_ret, eth_ret, gap):
            return

        # Safety net exit
        if active_strategy == Strategy.S1 and gap_float <= exit_thresh:
            send_alert(build_exit_message(btc_ret, eth_ret, gap))
            logger.info(f"EXIT S1. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        if active_strategy == Strategy.S2 and gap_float >= -exit_thresh:
            send_alert(build_exit_message(btc_ret, eth_ret, gap))
            logger.info(f"EXIT S2. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        # Invalidation
        if active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S1, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S1. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        if active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S2, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S2. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        logger.debug(
            f"TRACK {active_strategy.value if active_strategy else 'None'}: "
            f"Gap {gap_float:.2f}% | Vel {current_velocity:+.4f}%/m"
            if current_velocity is not None
            else f"TRACK {active_strategy.value if active_strategy else 'None'}: Gap {gap_float:.2f}%"
        )


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
        price_info = "\n⚠️ Gagal ambil harga tadi~ Akeno terus coba~ (◕ω◕)\n"

    lb           = get_lookback_label()
    hours_loaded = len(price_history) * settings["scan_interval"] / 3600
    history_info = (
        f"⚡ History dari Bot A: *{hours_loaded:.1f}h* siap!\n"
        if len(price_history) > 0
        else f"⏳ Menunggu Bot A kirim data~ Sinyal setelah {lb} data tersedia~\n"
    )

    peak_str    = "✅ ON" if settings["peak_enabled"] else "❌ OFF (direct entry)"
    cd          = settings["cooldown_minutes"]
    cc          = settings["confirm_count"]
    vm          = settings["velocity_min"]
    se          = settings["session_enabled"]
    ms          = settings["maxsig_per_hour"]
    t1e         = settings["tp1_enabled"]
    md          = settings["mindur_minutes"]

    scalping_lines = (
        f"🕐 Cooldown: {cd}m | ✔️ Confirm: {cc} scan\n"
        f"🚀 Velocity: {vm if vm > 0 else 'Off'} | 🌏 Session: {'ON '+settings['session_mode'] if se else 'Off'}\n"
        f"🔢 MaxSig: {ms if ms > 0 else 'Off'}/h | 🎯 TP1: {'ON '+str(int(settings['tp1_ratio']*100))+'%' if t1e else 'Off'}\n"
        f"⏱ MinDur: {md if md > 0 else 'Off'}m\n"
    )

    return send_alert(
        f"………\n"
        f"Ara ara~ Akeno (Bot B) sudah siap~ Ufufufu... (◕‿◕)\n"
        f"{price_info}\n"
        f"📊 Scan: {settings['scan_interval']}s | Redis refresh: {settings['redis_refresh_minutes']}m\n"
        f"📈 Entry: ±{settings['entry_threshold']}% | 📉 Exit: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalid: ±{settings['invalidation_threshold']}% | 🔍 Peak: {peak_str}\n"
        f"🎯 Peak reversal: {settings['peak_reversal']}%\n"
        f"✅ TP2: ±{settings['exit_threshold']}% | 🛑 TSL: {settings['sl_pct']}%\n"
        f"🔒 Redis: Read-Only\n"
        f"\n"
        f"*⚡ Scalping:*\n"
        f"{scalping_lines}\n"
        f"{history_info}\n"
        f"Ketik `/help` atau `/scalping` untuk info lebih lanjut~\n"
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
    global last_heartbeat_time, last_redis_refresh

    logger.info("=" * 60)
    logger.info("Monk Bot B — Read-Only Redis | TP1+TP2 | TSL | Scalping")
    logger.info(
        f"Entry: {settings['entry_threshold']}% | Exit/TP: {settings['exit_threshold']}% | "
        f"Invalid: {settings['invalidation_threshold']}% | Peak: {settings['peak_reversal']}% | "
        f"Peak Mode: {'ON' if settings['peak_enabled'] else 'OFF'} | TSL: {settings['sl_pct']}%"
    )
    logger.info(
        f"Scalping — Cooldown: {settings['cooldown_minutes']}m | "
        f"Confirm: {settings['confirm_count']} | Velocity: {settings['velocity_min']} | "
        f"Session: {'ON '+settings['session_mode'] if settings['session_enabled'] else 'OFF'} | "
        f"MaxSig: {settings['maxsig_per_hour']} | "
        f"TP1: {'ON' if settings['tp1_enabled'] else 'OFF'} | "
        f"MinDur: {settings['mindur_minutes']}m"
    )
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    load_history()
    prune_history(datetime.now(timezone.utc))
    last_redis_refresh = datetime.now(timezone.utc)
    logger.info(f"History after initial load & prune: {len(price_history)} points")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message()

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            if should_send_heartbeat(now):
                if send_heartbeat():
                    last_heartbeat_time = now

            refresh_history_from_redis(now)

            price_data = fetch_prices()
            if price_data is None:
                logger.warning("Failed to fetch prices")
            else:
                scan_stats["count"]         += 1
                scan_stats["last_btc_price"] = price_data.btc_price
                scan_stats["last_eth_price"] = price_data.eth_price

                if not is_data_fresh(now, price_data.btc_updated_at, price_data.eth_updated_at):
                    logger.warning("Data not fresh, skipping")
                else:
                    price_then = get_lookback_price(now)

                    if price_then is None:
                        hours = len(price_history) * settings["scan_interval"] / 3600
                        logger.info(
                            f"Waiting for Bot A data... "
                            f"({hours:.1f}h / {settings['lookback_hours']}h)"
                        )
                    else:
                        btc_ret, eth_ret, gap = compute_returns(
                            price_data.btc_price,
                            price_data.eth_price,
                            price_then.btc,
                            price_then.eth,
                        )
                        scan_stats["last_gap"]     = gap
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
                            price_data.btc_price,
                            price_data.eth_price,
                            price_then.btc,
                            price_then.eth,
                            now,
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
