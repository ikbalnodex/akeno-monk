#!/usr/bin/env python3
"""
Monk Bot B — BTC/ETH Divergence Bot (Swing / Day Trade Edition)

Fitur utama:
- Redis READ-ONLY consumer dari Bot A
- Peak Watch mode (on/off)
- Trailing SL + TP dengan estimasi harga ETH
- [NEW] Gap Driver Analysis  — ETH-led vs BTC-led
- [NEW] ETH/BTC Ratio Percentile — conviction meter seperti mentor
- [NEW] Dollar-Neutral Sizing Guide
- [NEW] Convergence Path Scenarios (A & B)
- [NEW] Net Combined P&L Tracker (pairs health)
- [NEW] /capital — set modal untuk sizing & dollar P&L
- [NEW] /ratio   — ETH/BTC ratio monitor
- [NEW] /pnl     — net P&L dua leg saat TRACK
- [NEW] /analysis — full market analysis on demand
"""
import json
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
DEFAULT_LOOKBACK_HOURS  = 24
HISTORY_BUFFER_MINUTES  = 30
REDIS_REFRESH_MINUTES   = 1
RATIO_WINDOW_DAYS       = 30   # window untuk hitung percentile ETH/BTC ratio


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

# TP / TSL tracking
entry_gap_value:   Optional[float]   = None
trailing_gap_best: Optional[float]   = None

# Harga & return saat entry — dipakai untuk estimasi harga target dan P&L
entry_btc_price: Optional[Decimal] = None
entry_eth_price: Optional[Decimal] = None
entry_btc_lb:    Optional[Decimal] = None   # harga BTC lookback saat entry
entry_eth_lb:    Optional[Decimal] = None   # harga ETH lookback saat entry
entry_btc_ret:   Optional[float]   = None   # % return BTC saat entry
entry_eth_ret:   Optional[float]   = None   # % return ETH saat entry
entry_driver:    Optional[str]     = None   # "ETH-led" / "BTC-led" / "Mixed"

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
    # — Swing / Day Trade —
    "capital":                0.0,    # modal user dalam USD, 0 = belum diset
    "ratio_window_days":      RATIO_WINDOW_DAYS,
}

last_update_id:      int                = 0
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

# Manual position tracker — persisted to Redis, survive restart
pos_data: dict = {
    "eth_entry_price": None,  # float
    "eth_qty":         None,  # float (+long / -short)
    "eth_leverage":    None,  # float
    "eth_liq_price":   None,  # float (manual override, opsional)
    "btc_entry_price": None,
    "btc_qty":         None,
    "btc_leverage":    None,
    "btc_liq_price":   None,
    "strategy":        None,  # "S1" / "S2"
    "set_at":          None,  # ISO string
}


# =============================================================================
# Redis — READ-ONLY for history, READ-WRITE for pos_data
# =============================================================================
REDIS_KEY     = "monk_bot:price_history"
REDIS_KEY_POS = "monk_bot:pos_data"


def _redis_request(method: str, path: str, body=None):
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None
    try:
        headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
        url     = f"{UPSTASH_REDIS_URL}{path}"
        resp    = (
            requests.get(url, headers=headers, timeout=10)
            if method == "GET"
            else requests.post(url, headers=headers, json=body, timeout=10)
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Redis request failed: {e}")
        return None


def load_history() -> None:
    global price_history
    if not UPSTASH_REDIS_URL:
        logger.info("Redis not configured")
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY}")
        if not result or result.get("result") is None:
            logger.info("No history in Redis yet")
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
        logger.info(f"Loaded {len(price_history)} points from Redis")
    except Exception as e:
        logger.warning(f"Failed to load history: {e}")
        price_history = []


def refresh_history_from_redis(now: datetime) -> None:
    global last_redis_refresh
    interval = settings["redis_refresh_minutes"]
    if interval <= 0:
        return
    if last_redis_refresh is not None:
        if (now - last_redis_refresh).total_seconds() / 60 < interval:
            return
    load_history()
    prune_history(now)
    last_redis_refresh = now
    logger.debug(f"Redis refreshed. {len(price_history)} points after prune")


def save_pos_data() -> bool:
    """Simpan pos_data ke Redis supaya survive restart."""
    if not UPSTASH_REDIS_URL:
        return False
    try:
        payload = json.dumps(pos_data, default=str)
        # Upstash REST: POST /set/<key> dengan body = value string
        result  = _redis_request("POST", f"/set/{REDIS_KEY_POS}", body=payload)
        if result and result.get("result") == "OK":
            logger.info("pos_data saved to Redis")
            return True
        logger.warning(f"save_pos_data unexpected result: {result}")
        return False
    except Exception as e:
        logger.warning(f"save_pos_data failed: {e}")
        return False


def load_pos_data() -> None:
    """Load pos_data dari Redis saat startup."""
    global pos_data
    if not UPSTASH_REDIS_URL:
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY_POS}")
        if not result or result.get("result") is None:
            logger.info("No pos_data in Redis")
            return
        data = json.loads(result["result"])
        # Restore semua field yang valid
        for k in pos_data:
            if k in data and data[k] is not None:
                # Konversi numeric fields
                if k in ("eth_entry_price", "eth_qty", "eth_leverage", "eth_liq_price",
                         "btc_entry_price", "btc_qty", "btc_leverage", "btc_liq_price"):
                    pos_data[k] = float(data[k])
                else:
                    pos_data[k] = data[k]
        logger.info(f"pos_data loaded from Redis: {pos_data.get('strategy')} "
                    f"ETH@{pos_data.get('eth_entry_price')} BTC@{pos_data.get('btc_entry_price')}")
    except Exception as e:
        logger.warning(f"load_pos_data failed: {e}")


def clear_pos_data_redis() -> bool:
    """Hapus pos_data dari Redis."""
    if not UPSTASH_REDIS_URL:
        return False
    try:
        _redis_request("POST", f"/del/{REDIS_KEY_POS}")
        return True
    except Exception as e:
        logger.warning(f"clear_pos_data_redis failed: {e}")
        return False


# =============================================================================
# Telegram
# =============================================================================
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def send_alert(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     message,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        logger.info("Alert sent")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send alert: {e}")
        return False


def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        resp = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id":                  chat_id,
                "text":                     message,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send reply: {e}")
        return False


# =============================================================================
# Command Polling
# =============================================================================
LONG_POLL_TIMEOUT = 30


def get_telegram_updates() -> list:
    global last_update_id
    if not TELEGRAM_BOT_TOKEN:
        return []
    try:
        url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {"offset": last_update_id + 1, "timeout": LONG_POLL_TIMEOUT}
        resp   = requests.get(url, params=params, timeout=LONG_POLL_TIMEOUT + 5)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok") and data.get("result"):
            updates = data["result"]
            if updates:
                last_update_id = updates[-1]["update_id"]
            return updates
    except requests.RequestException as e:
        logger.debug(f"Failed to get updates: {e}")
    return []


def process_commands() -> None:
    for update in get_telegram_updates():
        message       = update.get("message", {})
        text          = message.get("text", "")
        chat_id       = str(message.get("chat", {}).get("id", ""))
        user_id       = str(message.get("from", {}).get("id", ""))
        is_authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not is_authorized or not text.startswith("/"):
            continue
        parts   = text.split()
        command = parts[0].lower().split("@")[0]
        args    = parts[1:] if len(parts) > 1 else []
        logger.info(f"Command: {command} from {chat_id}")

        dispatch = {
            "/settings":  lambda: handle_settings_command(chat_id),
            "/status":    lambda: handle_status_command(chat_id),
            "/help":      lambda: handle_help_command(chat_id),
            "/start":     lambda: handle_help_command(chat_id),
            "/interval":  lambda: handle_interval_command(args, chat_id),
            "/threshold": lambda: handle_threshold_command(args, chat_id),
            "/lookback":  lambda: handle_lookback_command(args, chat_id),
            "/heartbeat": lambda: handle_heartbeat_command(args, chat_id),
            "/peak":      lambda: handle_peak_command(args, chat_id),
            "/sltp":      lambda: handle_sltp_command(args, chat_id),
            "/redis":     lambda: handle_redis_command(chat_id),
            # — Swing / Day Trade —
            "/capital":   lambda: handle_capital_command(args, chat_id),
            "/ratio":     lambda: handle_ratio_command(chat_id),
            "/pnl":       lambda: handle_pnl_command(chat_id),
            "/analysis":  lambda: handle_analysis_command(chat_id),
            # — Position Health Tracker —
            "/setpos":    lambda: handle_setpos_command(args, chat_id),
            "/health":    lambda: handle_health_command(chat_id),
            "/clearpos":  lambda: handle_clearpos_command(chat_id),
        }
        if command in dispatch:
            dispatch[command]()


# =============================================================================
# ─── MENTOR ANALYSIS ENGINE ──────────────────────────────────────────────────
# =============================================================================

def analyze_gap_driver(
    btc_ret: float,
    eth_ret: float,
    gap:     float,
) -> Tuple[str, str, str]:
    """
    Identifikasi siapa yang menggerakkan gap.
    Returns: (driver_label, emoji, explanation)
    """
    abs_btc = abs(btc_ret)
    abs_eth = abs(eth_ret)
    total   = abs_btc + abs_eth

    if total == 0:
        return "Mixed", "⚪", "Keduanya tidak bergerak"

    eth_contrib = abs_eth / total * 100
    btc_contrib = abs_btc / total * 100

    if eth_contrib >= 65:
        driver  = "ETH-led"
        emoji   = "🟡"
        explain = f"ETH {'pumping' if eth_ret > 0 else 'dumping'} dominan ({eth_contrib:.0f}% kontribusi)"
    elif btc_contrib >= 65:
        driver  = "BTC-led"
        emoji   = "🟠"
        explain = f"BTC {'naik' if btc_ret > 0 else 'turun'} dominan ({btc_contrib:.0f}% kontribusi)"
    else:
        driver  = "Mixed"
        emoji   = "⚪"
        explain = f"ETH {eth_contrib:.0f}% / BTC {btc_contrib:.0f}% — keduanya berkontribusi"

    return driver, emoji, explain


def get_convergence_hint(strategy: Strategy, driver: str) -> str:
    """Prediksi cara konvergensi paling mungkin."""
    if strategy == Strategy.S1:
        # Gap > 0 (ETH outperform BTC), konvergensi = gap mengecil
        hints = {
            "ETH-led": "ETH cenderung *pullback* — gap ETH-led biasanya revert lebih cepat",
            "BTC-led": "BTC perlu *catch up* naik — butuh lebih sabar, tapi lebih sustained",
            "Mixed":   "Konvergensi bisa dari kedua arah — pantau leg mana yang duluan bergerak",
        }
    else:
        # Gap < 0 (ETH underperform BTC), konvergensi = gap mengecil ke nol
        hints = {
            "ETH-led": "ETH cenderung *bounce* dari oversold — ETH-led dump biasanya cepat recover",
            "BTC-led": "BTC perlu *koreksi* turun — BTC-led gap = BTC kuat, butuh sabar lebih",
            "Mixed":   "Konvergensi bisa dari kedua arah — pantau mana yang duluan bergerak",
        }
    return hints.get(driver, "")


def calc_ratio_percentile() -> Tuple[
    Optional[float], Optional[float],
    Optional[float], Optional[float], Optional[int]
]:
    """
    Hitung ETH/BTC ratio sekarang dan percentile-nya.
    Returns: (current, avg, high, low, percentile)
    """
    if not price_history or len(price_history) < 10:
        return None, None, None, None, None

    now     = datetime.now(timezone.utc)
    cutoff  = now - timedelta(days=settings["ratio_window_days"])
    window  = [p for p in price_history if p.timestamp >= cutoff]
    if len(window) < 5:
        window = price_history  # fallback semua history

    ratios = [float(p.eth / p.btc) for p in window]
    if not ratios:
        return None, None, None, None, None

    current    = ratios[-1]
    avg        = sum(ratios) / len(ratios)
    high       = max(ratios)
    low        = min(ratios)
    below      = sum(1 for r in ratios if r <= current)
    percentile = int(below / len(ratios) * 100)

    return current, avg, high, low, percentile


def get_ratio_conviction(strategy: Strategy, pct: Optional[int]) -> Tuple[str, str]:
    """Returns: (stars, description)"""
    if pct is None:
        return "⭐⭐⭐", "Data terbatas"

    if strategy == Strategy.S2:  # Long ETH → bagus kalau ETH murah (pct rendah)
        if pct <= 10:   return "⭐⭐⭐⭐⭐", "ETH *sangat murah* vs BTC — conviction tertinggi"
        elif pct <= 25: return "⭐⭐⭐⭐",   "ETH *murah* vs BTC — setup bagus"
        elif pct <= 40: return "⭐⭐⭐",     "ETH cukup murah — setup moderat"
        elif pct <= 60: return "⭐⭐",       "ETH di area tengah — gap bisa melebar lebih lanjut"
        else:           return "⭐",         "ETH *mahal* vs BTC — S2 berisiko"
    else:  # S1, Long BTC → bagus kalau ETH mahal (pct tinggi)
        if pct >= 90:   return "⭐⭐⭐⭐⭐", "ETH *sangat mahal* vs BTC — conviction tertinggi"
        elif pct >= 75: return "⭐⭐⭐⭐",   "ETH *mahal* vs BTC — setup bagus"
        elif pct >= 60: return "⭐⭐⭐",     "ETH cukup mahal — setup moderat"
        elif pct >= 40: return "⭐⭐",       "ETH di area tengah — gap bisa melebar lebih lanjut"
        else:           return "⭐",         "ETH *murah* vs BTC — S1 berisiko"


def calc_sizing(
    btc_price: Decimal,
    eth_price: Decimal,
) -> Tuple[float, float, float]:
    """
    Dollar-neutral sizing. Returns: (half_capital, eth_qty, btc_qty)
    """
    capital = settings["capital"]
    if capital <= 0 or float(btc_price) <= 0 or float(eth_price) <= 0:
        return 0.0, 0.0, 0.0
    half    = capital / 2.0
    eth_qty = half / float(eth_price)
    btc_qty = half / float(btc_price)
    return half, eth_qty, btc_qty


def calc_convergence_scenarios(
    strategy: Strategy,
    btc_now:  Decimal,
    eth_now:  Decimal,
    btc_lb:   Decimal,
    eth_lb:   Decimal,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Hitung dua skenario target harga saat gap konvergen ke TP.
    Scenario A: leg ETH yang bergerak, BTC flat
    Scenario B: leg BTC yang bergerak, ETH flat

    Returns:
        S1: (eth_price_scen_A, btc_price_scen_B)
        S2: (eth_price_scen_A, btc_price_scen_B)
    """
    et = settings["exit_threshold"]
    try:
        btc_ret_now = float((btc_now - btc_lb) / btc_lb * Decimal("100"))
        eth_ret_now = float((eth_now - eth_lb) / eth_lb * Decimal("100"))

        if strategy == Strategy.S1:
            # Gap menyempit → gap target = +et
            # A: ETH turun sehingga eth_ret → btc_ret_now - et  (gap = eth_ret - btc_ret = et)
            target_eth_ret_a = btc_ret_now - et
            eth_a = float(eth_lb) * (1 + target_eth_ret_a / 100)
            # B: BTC naik sehingga btc_ret → eth_ret_now - et
            target_btc_ret_b = eth_ret_now - et
            btc_b = float(btc_lb) * (1 + target_btc_ret_b / 100)
        else:
            # Gap menyempit → gap target = -et (negatif)
            # A: ETH naik sehingga eth_ret → btc_ret_now + et
            target_eth_ret_a = btc_ret_now + et
            eth_a = float(eth_lb) * (1 + target_eth_ret_a / 100)
            # B: BTC turun sehingga btc_ret → eth_ret_now + et
            target_btc_ret_b = eth_ret_now + et
            btc_b = float(btc_lb) * (1 + target_btc_ret_b / 100)

        return eth_a, btc_b
    except Exception:
        return None, None


def calc_net_pnl(
    strategy:    Strategy,
    current_gap: float,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Estimasi net P&L dari entry sampai sekarang.
    Returns: (leg_eth_pct, leg_btc_pct, net_pct)
    """
    if entry_gap_value is None:
        return None, None, None

    # Kalau tidak ada harga entry → pakai gap movement saja
    if entry_btc_price is None or entry_eth_price is None:
        if strategy == Strategy.S1:
            net = entry_gap_value - current_gap  # konvergen = gap mengecil
        else:
            net = current_gap - entry_gap_value
        return None, None, net

    try:
        btc_now = scan_stats.get("last_btc_price")
        eth_now = scan_stats.get("last_eth_price")
        if btc_now is None or eth_now is None:
            return None, None, None

        if strategy == Strategy.S1:
            # Long BTC, Short ETH
            leg_btc = float((btc_now - entry_btc_price) / entry_btc_price * 100)
            leg_eth = float((entry_eth_price - eth_now)  / entry_eth_price * 100)
        else:
            # Long ETH, Short BTC
            leg_eth = float((eth_now - entry_eth_price) / entry_eth_price * 100)
            leg_btc = float((entry_btc_price - btc_now) / entry_btc_price * 100)

        net = (leg_eth + leg_btc) / 2
        return leg_eth, leg_btc, net
    except Exception:
        return None, None, None


def get_pairs_health(
    strategy:    Strategy,
    current_gap: float,
) -> Tuple[str, str]:
    """Returns: (emoji, description)"""
    if entry_gap_value is None:
        return "❓", "Tidak ada posisi aktif"

    et = settings["exit_threshold"]

    if strategy == Strategy.S1:
        progress = (entry_gap_value - current_gap) / (entry_gap_value - et) * 100 if entry_gap_value != et else 100
    else:
        progress = (current_gap - entry_gap_value) / (-et - entry_gap_value) * 100 if entry_gap_value != -et else 100

    progress = max(0.0, min(100.0, progress))

    if progress >= 80:   return "🟢", f"Hampir TP! Progress {progress:.0f}%"
    elif progress >= 50: return "🟡", f"Setengah jalan. Progress {progress:.0f}%"
    elif progress >= 20: return "🔵", f"Mulai bergerak. Progress {progress:.0f}%"
    elif progress >= 0:  return "⚪", f"Belum banyak bergerak. Progress {progress:.0f}%"
    else:                return "🔴", f"Berlawanan arah. Progress {progress:.0f}%"


# =============================================================================
# ─── POSITION HEALTH ENGINE ──────────────────────────────────────────────────
# =============================================================================

def calc_position_pnl() -> dict:
    """
    Hitung P&L, margin, dan kesehatan tiap leg dari pos_data + harga sekarang.
    Returns empty dict jika data tidak lengkap.
    """
    if pos_data["eth_entry_price"] is None or pos_data["btc_entry_price"] is None:
        return {}
    btc_now = scan_stats.get("last_btc_price")
    eth_now = scan_stats.get("last_eth_price")
    if btc_now is None or eth_now is None:
        return {}
    try:
        eth_entry = pos_data["eth_entry_price"]
        eth_qty   = pos_data["eth_qty"]           # +long / -short
        eth_lev   = pos_data["eth_leverage"] or 1.0
        btc_entry = pos_data["btc_entry_price"]
        btc_qty   = pos_data["btc_qty"]
        btc_lev   = pos_data["btc_leverage"] or 1.0
        eth_p     = float(eth_now)
        btc_p     = float(btc_now)

        eth_notional   = abs(eth_qty) * eth_entry
        btc_notional   = abs(btc_qty) * btc_entry
        eth_margin     = eth_notional / eth_lev
        btc_margin     = btc_notional / btc_lev
        total_margin   = eth_margin + btc_margin
        total_notional = eth_notional + btc_notional

        # PnL signed: long profit saat naik, short profit saat turun
        eth_pnl = eth_qty * (eth_p - eth_entry)
        btc_pnl = btc_qty * (btc_p - btc_entry)
        net_pnl = eth_pnl + btc_pnl

        eth_pnl_pct = eth_pnl / eth_margin * 100 if eth_margin > 0 else 0
        btc_pnl_pct = btc_pnl / btc_margin * 100 if btc_margin > 0 else 0
        net_pnl_pct = net_pnl / total_margin * 100 if total_margin > 0 else 0

        total_equity = total_margin + net_pnl
        margin_ratio = total_equity / total_notional * 100 if total_notional > 0 else 0

        # Maintenance margin ~0.5% dari notional (estimasi)
        maint_margin    = total_notional * 0.005
        liq_buffer_usd  = total_equity - maint_margin
        liq_buffer_pct  = liq_buffer_usd / total_equity * 100 if total_equity > 0 else 0

        # Estimasi liq price per leg kalau tidak diisi manual
        eth_liq = pos_data["eth_liq_price"]
        btc_liq = pos_data["btc_liq_price"]
        if eth_liq is None and eth_qty != 0:
            eth_liq = eth_entry - (eth_margin / eth_qty)  # long: entry - margin/qty; short: entry + margin/|qty|
        if btc_liq is None and btc_qty != 0:
            btc_liq = btc_entry - (btc_margin / btc_qty)

        eth_dist_liq = abs(eth_p - eth_liq) / eth_p * 100 if eth_liq else None
        btc_dist_liq = abs(btc_p - btc_liq) / btc_p * 100 if btc_liq else None
        eth_danger   = eth_dist_liq is not None and eth_dist_liq < 10
        btc_danger   = btc_dist_liq is not None and btc_dist_liq < 10

        if margin_ratio >= 10:   health_e, health_label = "🟢", "SEHAT"
        elif margin_ratio >= 5:  health_e, health_label = "🟡", "PERHATIKAN"
        elif margin_ratio >= 3:  health_e, health_label = "🟠", "WASPADA"
        else:                    health_e, health_label = "🔴", "BAHAYA — Dekat Liquidasi!"

        return {
            "eth_pnl": eth_pnl, "eth_pnl_pct": eth_pnl_pct,
            "eth_notional": eth_notional, "eth_margin": eth_margin,
            "eth_lev": eth_lev, "eth_liq_est": eth_liq,
            "eth_dist_liq": eth_dist_liq, "eth_danger": eth_danger,
            "btc_pnl": btc_pnl, "btc_pnl_pct": btc_pnl_pct,
            "btc_notional": btc_notional, "btc_margin": btc_margin,
            "btc_lev": btc_lev, "btc_liq_est": btc_liq,
            "btc_dist_liq": btc_dist_liq, "btc_danger": btc_danger,
            "net_pnl": net_pnl, "net_pnl_pct": net_pnl_pct,
            "total_margin": total_margin, "total_notional": total_notional,
            "total_equity": total_equity, "margin_ratio": margin_ratio,
            "liq_buffer_usd": liq_buffer_usd, "liq_buffer_pct": liq_buffer_pct,
            "health_emoji": health_e, "health_label": health_label,
        }
    except Exception as e:
        logger.warning(f"calc_position_pnl error: {e}")
        return {}


def build_position_health_message(h: dict) -> str:
    now     = datetime.now(timezone.utc)
    set_at  = pos_data.get("set_at")
    age_str = ""
    if set_at:
        try:
            sa      = datetime.fromisoformat(set_at) if isinstance(set_at, str) else set_at
            age_min = int((now - sa).total_seconds() / 60)
            age_str = f" _(diset {age_min}m lalu)_"
        except Exception:
            pass

    strat    = pos_data.get("strategy") or "?"
    eth_p    = float(scan_stats["last_eth_price"]) if scan_stats.get("last_eth_price") else 0
    btc_p    = float(scan_stats["last_btc_price"]) if scan_stats.get("last_btc_price") else 0
    eth_qty  = pos_data["eth_qty"]
    btc_qty  = pos_data["btc_qty"]
    eth_dir  = "Long 📈" if eth_qty and eth_qty > 0 else "Short 📉"
    btc_dir  = "Long 📈" if btc_qty and btc_qty > 0 else "Short 📉"

    def _sign(v): return "+" if v >= 0 else ""
    def _pnl_emoji(v): return "🟢" if v >= 0 else "🔴"

    eth_liq_s  = f"${h['eth_liq_est']:,.2f}" if h.get("eth_liq_est") else "N/A"
    btc_liq_s  = f"${h['btc_liq_est']:,.2f}" if h.get("btc_liq_est") else "N/A"
    eth_dist_s = f"{h['eth_dist_liq']:.1f}% jauh" if h.get("eth_dist_liq") else "N/A"
    btc_dist_s = f"{h['btc_dist_liq']:.1f}% jauh" if h.get("btc_dist_liq") else "N/A"
    eth_warn   = " ⚠️" if h.get("eth_danger") else " ✅"
    btc_warn   = " ⚠️" if h.get("btc_danger") else " ✅"

    mr       = h["margin_ratio"]
    mr_fill  = min(10, int(mr / 2))
    mr_bar   = "█" * mr_fill + "░" * (10 - mr_fill)
    lb_pct   = max(0.0, h["liq_buffer_pct"])
    lb_fill  = min(10, int(lb_pct / 10))
    lb_bar   = "█" * lb_fill + "░" * (10 - lb_fill)

    danger_note = ""
    if h.get("eth_danger") or h.get("btc_danger"):
        legs = []
        if h.get("eth_danger"): legs.append("ETH")
        if h.get("btc_danger"): legs.append("BTC")
        danger_note = f"\n🚨 *PERINGATAN: {'/'.join(legs)} mendekati liq price!*\n"

    return (
        f"🏥 *Position Health — {strat}*{age_str}\n"
        f"💰 ETH: ${eth_p:,.2f} | BTC: ${btc_p:,.2f}\n"
        f"\n"
        f"*📊 ETH Leg ({eth_dir}):*\n"
        f"┌─────────────────────\n"
        f"│ Entry:     ${pos_data['eth_entry_price']:,.2f}\n"
        f"│ Qty:       {abs(eth_qty):.4f} ETH\n"
        f"│ Leverage:  {h['eth_lev']:.0f}x\n"
        f"│ Notional:  ${h['eth_notional']:,.2f}\n"
        f"│ Margin:    ${h['eth_margin']:,.2f}\n"
        f"│ UPnL:      {_pnl_emoji(h['eth_pnl'])} {_sign(h['eth_pnl'])}${h['eth_pnl']:,.2f} ({_sign(h['eth_pnl_pct'])}{h['eth_pnl_pct']:.2f}%)\n"
        f"│ Liq:       {eth_liq_s}{eth_warn} | {eth_dist_s}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*📊 BTC Leg ({btc_dir}):*\n"
        f"┌─────────────────────\n"
        f"│ Entry:     ${pos_data['btc_entry_price']:,.2f}\n"
        f"│ Qty:       {abs(btc_qty):.6f} BTC\n"
        f"│ Leverage:  {h['btc_lev']:.0f}x\n"
        f"│ Notional:  ${h['btc_notional']:,.2f}\n"
        f"│ Margin:    ${h['btc_margin']:,.2f}\n"
        f"│ UPnL:      {_pnl_emoji(h['btc_pnl'])} {_sign(h['btc_pnl'])}${h['btc_pnl']:,.2f} ({_sign(h['btc_pnl_pct'])}{h['btc_pnl_pct']:.2f}%)\n"
        f"│ Liq:       {btc_liq_s}{btc_warn} | {btc_dist_s}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*⚖️ Net Pairs:*\n"
        f"┌─────────────────────\n"
        f"│ Notional:  ${h['total_notional']:,.2f}\n"
        f"│ Margin:    ${h['total_margin']:,.2f}\n"
        f"│ Equity:    ${h['total_equity']:,.2f}\n"
        f"│ Net UPnL:  {_pnl_emoji(h['net_pnl'])} {_sign(h['net_pnl'])}${h['net_pnl']:,.2f} ({_sign(h['net_pnl_pct'])}{h['net_pnl_pct']:.2f}%)\n"
        f"└─────────────────────\n"
        f"\n"
        f"*🛡️ Kesehatan Margin:*\n"
        f"┌─────────────────────\n"
        f"│ Margin Ratio: {mr:.2f}%\n"
        f"│ `{mr_bar}` {h['health_emoji']} *{h['health_label']}*\n"
        f"│ Liq Buffer:   ${h['liq_buffer_usd']:,.2f} ({lb_pct:.1f}%)\n"
        f"│ `{lb_bar}` buffer sebelum liq\n"
        f"└─────────────────────\n"
        f"{danger_note}\n"
        f"_💡 Pairs trade: nilai NET yang penting, bukan per leg~_\n"
        f"_Mentor: ETH -68% tapi net +$239 (◕‿◕)_"
    )


# =============================================================================
# ─── ENTRY READINESS ENGINE ──────────────────────────────────────────────────
# =============================================================================

def build_entry_readiness(
    strategy: Strategy,
    pct_r:    Optional[int],
    curr_r:   Optional[float],
    avg_r:    Optional[float],
    ext:      dict,
) -> str:
    gap_now = scan_stats.get("last_gap")
    btc_r   = scan_stats.get("last_btc_ret")
    eth_r   = scan_stats.get("last_eth_ret")
    et      = settings["entry_threshold"]
    it      = settings["invalidation_threshold"]
    gap_f   = float(gap_now) if gap_now is not None else 0.0
    gap_abs = abs(gap_f)

    checks   = []   # (bool|None, label, detail)
    warnings = []

    # 1. Gap di threshold?
    correct_side = (gap_f >= et) if strategy == Strategy.S1 else (gap_f <= -et)
    if correct_side:
        checks.append((True,  "Gap di zona entry",
                        f"Gap {gap_f:+.2f}% melewati ±{et}% threshold"))
    elif gap_abs >= et:
        checks.append((False, "Gap sisi berlawanan",
                        f"Gap {gap_f:+.2f}% — salah sisi untuk {strategy.value}"))
        warnings.append("Gap di sisi yang salah untuk strategi ini")
    else:
        checks.append((False, "Gap belum di threshold",
                        f"Gap {gap_f:+.2f}% | perlu {et - gap_abs:.2f}% lagi ke ±{et}%"))
        warnings.append(f"Gap masih {et - gap_abs:.2f}% dari threshold")

    # 2. Ratio conviction
    if pct_r is not None:
        if strategy == Strategy.S1:
            ratio_ok     = pct_r >= 60
            ratio_strong = pct_r >= 75
            detail       = f"Percentile {pct_r}th — ETH {'mahal ✅' if ratio_ok else 'belum cukup mahal'} vs BTC"
        else:
            ratio_ok     = pct_r <= 40
            ratio_strong = pct_r <= 25
            detail       = f"Percentile {pct_r}th — ETH {'murah ✅' if ratio_ok else 'belum cukup murah'} vs BTC"
        label = "Ratio conviction kuat" if ratio_strong else ("Ratio conviction cukup" if ratio_ok else "Ratio conviction lemah")
        checks.append((ratio_ok, label, detail))
        if not ratio_ok:
            warnings.append("ETH/BTC ratio belum ideal — gap bisa melebar lebih jauh sebelum revert")
    else:
        checks.append((None, "Ratio N/A", "Butuh lebih banyak history~"))

    # 3. Driver analysis
    if btc_r is not None and eth_r is not None:
        driver, _, driver_ex = analyze_gap_driver(float(btc_r), float(eth_r), gap_f)
        driver_ok = driver in ("ETH-led", "Mixed")
        checks.append((driver_ok, f"Driver: {driver}", driver_ex))
        if driver == "BTC-led":
            if strategy == Strategy.S1:
                warnings.append("BTC-led di S1: BTC lemah, bukan ETH terlalu mahal — revert lebih lambat")
            else:
                warnings.append("BTC-led di S2: BTC kuat, ETH belum tentu bounce cepat")
    else:
        checks.append((None, "Driver belum tersedia", "Tunggu data scan~"))

    # 4. Buffer ke invalidation
    dist_invalid = (it - gap_f) if strategy == Strategy.S1 else (gap_f - (-it))
    if dist_invalid >= 1.5:
        checks.append((True,  "Buffer invalidation aman",
                        f"{dist_invalid:.2f}% sebelum invalidation ±{it}%"))
    elif dist_invalid >= 0.5:
        checks.append((True,  "Buffer invalidation tipis",
                        f"Hanya {dist_invalid:.2f}% sebelum invalidation ±{it}%"))
        warnings.append(f"Buffer ke invalidation tipis ({dist_invalid:.2f}%) — sizing kecil disarankan")
    else:
        checks.append((False, "Terlalu dekat invalidation",
                        f"Hanya {dist_invalid:.2f}% dari invalidation ±{it}%"))
        warnings.append("Terlalu dekat invalidation — risiko SL langsung kena tinggi")

    # 5. Z-score statistik
    z = ext.get("z_score")
    if z is not None:
        if strategy == Strategy.S1:
            z_ok     = z >= 1.0
            z_detail = f"Z-score {z:+.2f}σ — ETH {'sudah ✅' if z_ok else 'belum'} cukup mahal secara statistik"
        else:
            z_ok     = z <= -1.0
            z_detail = f"Z-score {z:+.2f}σ — ETH {'sudah ✅' if z_ok else 'belum'} cukup murah secara statistik"
        checks.append((z_ok, f"Z-score {z:+.2f}σ", z_detail))
        if abs(z) > 3.0:
            warnings.append(f"Z-score {z:+.2f}σ sangat ekstrem — bisa ada alasan fundamental, bukan sekadar divergence")
    else:
        checks.append((None, "Z-score N/A", "Data kurang~"))

    # Hitung skor
    true_count  = sum(1 for c in checks if c[0] is True)
    false_count = sum(1 for c in checks if c[0] is False)
    total_valid = sum(1 for c in checks if c[0] is not None)
    score_pct   = true_count / total_valid * 100 if total_valid > 0 else 0

    # Verdict
    if false_count == 0 and true_count >= 4:
        v_e, verdict, v_d = "🟢", "READY TO ENTRY", "Semua faktor oke — kondisi optimal~"
    elif not correct_side or false_count >= 2:
        v_e, verdict, v_d = "🔴", "JANGAN ENTRY DULU", "Terlalu banyak faktor tidak terpenuhi~"
    elif false_count <= 1 and true_count >= 3:
        v_e, verdict, v_d = "🟡", "BISA ENTRY, TAPI HATI-HATI", "1 faktor lemah — pertimbangkan sizing lebih kecil~"
    else:
        v_e, verdict, v_d = "🟠", "TUNGGU KONFIRMASI", "Beberapa faktor masih meragukan~"

    checklist = ""
    for ok, label, detail in checks:
        icon      = "✅" if ok is True else ("❌" if ok is False else "⚪")
        checklist += f"{icon} *{label}*\n   _{detail}_\n"

    warn_block = ""
    if warnings:
        warn_block = "\n*⚠️ Peringatan:*\n" + "".join(f"• {w}\n" for w in warnings)

    return (
        f"*── Entry Readiness: {strategy.value} ──*\n"
        f"{v_e} *{verdict}*\n"
        f"_{v_d}_\n"
        f"Score: {true_count}/{total_valid} ✅ ({score_pct:.0f}%)\n"
        f"\n"
        f"*Checklist:*\n"
        f"{checklist}"
        f"{warn_block}"
    )


# =============================================================================
# Formatting
# =============================================================================
def format_value(value) -> str:
    fv = float(value)
    if abs(fv) < 0.05:
        return "+0.0"
    return f"+{fv:.1f}" if fv >= 0 else f"{fv:.1f}"


def get_lookback_label() -> str:
    return f"{settings['lookback_hours']}h"


# =============================================================================
# Target Price Helpers
# =============================================================================
def calc_eth_price_at_gap(gap_target: float) -> Tuple[Optional[float], Optional[float]]:
    """Estimasi harga ETH saat gap mencapai gap_target."""
    if None in (entry_btc_lb, entry_eth_lb, entry_btc_price):
        return None, None
    try:
        btc_ret_entry  = float((entry_btc_price - entry_btc_lb) / entry_btc_lb * Decimal("100"))
        target_eth_ret = btc_ret_entry + gap_target
        eth_target     = float(entry_eth_lb) * (1 + target_eth_ret / 100)
        return eth_target, float(entry_btc_price)
    except Exception:
        return None, None


def calc_tp_target_price(strategy: Strategy) -> Tuple[Optional[float], Optional[float]]:
    et         = settings["exit_threshold"]
    gap_target = et if strategy == Strategy.S1 else -et
    return calc_eth_price_at_gap(gap_target)


# =============================================================================
# ─── MESSAGE BUILDERS ────────────────────────────────────────────────────────
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
        f"Rencana: *{direction}*\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Akeno tidak gegabah, sayangku~\n"
        f"Pantau puncaknya dulu sebelum entry ⚡"
    )


def build_entry_message(
    strategy:  Strategy,
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    peak:      float,
    btc_now:   Decimal,
    eth_now:   Decimal,
    btc_lb:    Decimal,
    eth_lb:    Decimal,
    is_direct: bool = False,
) -> str:
    lb        = get_lookback_label()
    gap_float = float(gap)
    sl_pct    = settings["sl_pct"]
    et        = settings["exit_threshold"]

    if strategy == Strategy.S1:
        direction   = "Long BTC / Short ETH"
        tp_gap      = et
        tsl_initial = gap_float + sl_pct
    else:
        direction   = "Long ETH / Short BTC"
        tp_gap      = -et
        tsl_initial = gap_float - sl_pct

    # ── 1. Gap Driver Analysis ────────────────────────────────────────────────
    driver, driver_emoji, driver_explain = analyze_gap_driver(
        float(btc_ret), float(eth_ret), gap_float
    )
    conv_hint = get_convergence_hint(strategy, driver)

    # ── 2. ETH/BTC Ratio Percentile ──────────────────────────────────────────
    curr_r, avg_r, hi_r, lo_r, pct_r = calc_ratio_percentile()
    stars, conviction = get_ratio_conviction(strategy, pct_r)
    ratio_str = f"{curr_r:.5f}" if curr_r else "N/A"
    avg_str   = f"{avg_r:.5f}"  if avg_r  else "N/A"
    pct_str   = f"{pct_r}th"    if pct_r is not None else "N/A"

    # ── 3. Target Harga ───────────────────────────────────────────────────────
    eth_tp, btc_ref = calc_tp_target_price(strategy)
    eth_tp_str      = f"${eth_tp:,.2f}"  if eth_tp  else "N/A"
    btc_ref_str     = f"${btc_ref:,.2f}" if btc_ref else "N/A"
    eth_tsl, _      = calc_eth_price_at_gap(tsl_initial)
    eth_tsl_str     = f"${eth_tsl:,.2f}" if eth_tsl else "N/A"

    # ── 4. Convergence Scenarios ──────────────────────────────────────────────
    eth_a, btc_b = calc_convergence_scenarios(strategy, btc_now, eth_now, btc_lb, eth_lb)
    if strategy == Strategy.S1:
        scen_a_label = "ETH pullback"
        scen_b_label = "BTC catch-up"
        scen_a_str   = f"ETH turun ke *${eth_a:,.2f}*" if eth_a else "N/A"
        scen_b_str   = f"BTC naik ke *${btc_b:,.2f}*"  if btc_b else "N/A"
    else:
        scen_a_label = "ETH bounce"
        scen_b_label = "BTC koreksi"
        scen_a_str   = f"ETH naik ke *${eth_a:,.2f}*"  if eth_a else "N/A"
        scen_b_str   = f"BTC turun ke *${btc_b:,.2f}*" if btc_b else "N/A"

    # ── 5. Dollar-Neutral Sizing ──────────────────────────────────────────────
    sizing_section = ""
    half, eth_qty, btc_qty = calc_sizing(btc_now, eth_now)
    if half > 0:
        if strategy == Strategy.S1:
            sizing_section = (
                f"\n"
                f"💰 *Sizing Dollar-Neutral (${settings['capital']:,.0f}):*\n"
                f"┌─────────────────────\n"
                f"│ Long BTC:  ${half:,.0f} → {btc_qty:.6f} BTC\n"
                f"│ Short ETH: ${half:,.0f} → {eth_qty:.4f} ETH\n"
                f"└─────────────────────\n"
            )
        else:
            sizing_section = (
                f"\n"
                f"💰 *Sizing Dollar-Neutral (${settings['capital']:,.0f}):*\n"
                f"┌─────────────────────\n"
                f"│ Long ETH:  ${half:,.0f} → {eth_qty:.4f} ETH\n"
                f"│ Short BTC: ${half:,.0f} → {btc_qty:.6f} BTC\n"
                f"└─────────────────────\n"
            )
    else:
        sizing_section = "\n_💡 `/capital <modal>` untuk sizing guide dollar-neutral~_\n"

    peak_line     = f"│ Peak:     {peak:+.2f}%\n" if not is_direct and peak else ""
    direct_tag    = " _(Peak OFF)_\n" if is_direct else "\n"
    reversal_note = (
        f"_Gap berbalik {settings['peak_reversal']}% dari puncak → entry terkonfirmasi~_\n"
        if not is_direct else ""
    )

    return (
        f"Ara ara ara~!!! Ini saatnya, sayangku~!!! ⚡\n"
        f"🚨 *ENTRY SIGNAL: {strategy.value}*{direct_tag}"
        f"📈 *{direction}*\n"
        f"\n"
        f"*── 1. Gap Analysis ({lb}) ──*\n"
        f"┌─────────────────────\n"
        f"│ BTC:      {format_value(btc_ret)}%\n"
        f"│ ETH:      {format_value(eth_ret)}%\n"
        f"│ Gap:      *{format_value(gap)}%*\n"
        f"{peak_line}"
        f"│ Driver:   {driver_emoji} *{driver}*\n"
        f"│ {driver_explain}\n"
        f"└─────────────────────\n"
        f"_💡 {conv_hint}_\n"
        f"\n"
        f"*── 2. ETH/BTC Ratio ──*\n"
        f"┌─────────────────────\n"
        f"│ Sekarang:   {ratio_str}\n"
        f"│ {lb} avg:   {avg_str}\n"
        f"│ Percentile: *{pct_str}*\n"
        f"│ Conviction: {stars}\n"
        f"│ _{conviction}_\n"
        f"└─────────────────────\n"
        f"\n"
        f"*── 3. Target & Proteksi ──*\n"
        f"┌─────────────────────\n"
        f"│ TP gap:   {tp_gap:+.2f}% _(exit threshold)_\n"
        f"│ ETH TP:   {eth_tp_str}\n"
        f"│ BTC ref:  {btc_ref_str}\n"
        f"│ Trail SL: {tsl_initial:+.2f}% → ETH {eth_tsl_str}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*── 4. Skenario Konvergensi ──*\n"
        f"• *A — {scen_a_label}:* {scen_a_str}\n"
        f"• *B — {scen_b_label}:* {scen_b_str}\n"
        f"\n"
        f"{sizing_section}"
        f"{reversal_note}"
        f"\n"
        f"Akeno sudah menunggu momen ini~ Ufufufu... ⚡"
    )


def build_exit_message(
    btc_ret: Decimal,
    eth_ret: Decimal,
    gap:     Decimal,
) -> str:
    lb            = get_lookback_label()
    gap_f         = float(gap)
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_f) if active_strategy else (None, None, None)
    net_section   = _build_pnl_section(leg_e, leg_b, net)
    return (
        f"Ara ara~!!! Ufufufu... (◕▿◕)\n"
        f"✅ *EXIT — Gap Konvergen!*\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  *{format_value(gap)}%*\n"
        f"└─────────────────────\n"
        f"{net_section}"
        f"Saatnya close posisi~ Akeno lanjut pantau. ⚡🔍"
    )


def build_tp_message(
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    entry_gap: float,
    tp_level:  float,
    eth_target: Optional[float],
) -> str:
    lb          = get_lookback_label()
    eth_str     = f"${eth_target:,.2f}" if eth_target else "N/A"
    gap_f       = float(gap)
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_f) if active_strategy else (None, None, None)
    net_section = _build_pnl_section(leg_e, leg_b, net, emoji="🎉")
    return (
        f"Ara ara~!!! TP kena sayangku~!!! ✨\n"
        f"🎯 *TAKE PROFIT*\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:    {format_value(btc_ret)}%\n"
        f"│ ETH:    {format_value(eth_ret)}%\n"
        f"│ Gap:    *{format_value(gap)}%*\n"
        f"│ Entry:  {entry_gap:+.2f}%\n"
        f"│ TP hit: {tp_level:+.2f}%\n"
        f"│ ETH:    {eth_str}\n"
        f"└─────────────────────\n"
        f"{net_section}"
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
    eth_sl, _     = calc_eth_price_at_gap(sl_level)
    eth_sl_str    = f"${eth_sl:,.2f}" if eth_sl else "N/A"
    gap_f         = float(gap)
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_f) if active_strategy else (None, None, None)
    net_section   = _build_pnl_section(leg_e, leg_b, net)
    return (
        f"………\n"
        f"⛔ *TRAILING STOP LOSS*\n"
        f"\n"
        f"Ara ara~ TSL kena. Profit diamankan~ (◕ω◕)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:      {format_value(btc_ret)}%\n"
        f"│ ETH:      {format_value(eth_ret)}%\n"
        f"│ Gap:      {format_value(gap)}%\n"
        f"│ Entry:    {entry_gap:+.2f}%\n"
        f"│ Best gap: {best_gap:+.2f}%\n"
        f"│ TSL hit:  {sl_level:+.2f}% → ETH {eth_sl_str}\n"
        f"│ Terkunci: ~{profit_locked:.2f}%\n"
        f"└─────────────────────\n"
        f"{net_section}"
        f"Cut dulu~ Akeno scan ulang~ ⚡ (◕‿◕)"
    )


def build_invalidation_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
) -> str:
    lb            = get_lookback_label()
    gap_f         = float(gap)
    leg_e, leg_b, net = calc_net_pnl(strategy, gap_f)
    net_section   = _build_pnl_section(leg_e, leg_b, net)
    return (
        f"………\n"
        f"⚠️ *INVALIDATION: {strategy.value}*\n"
        f"\n"
        f"Ara ara~ gap malah melebar. Cut dulu~ (◕ω◕)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"{net_section}"
        f"Akeno scan ulang dari awal~ ⚡ (◕‿◕)"
    )


def build_peak_cancelled_message(strategy: Strategy, gap: Decimal) -> str:
    return (
        f"………\n"
        f"❌ *Peak Watch Dibatalkan: {strategy.value}*\n"
        f"\n"
        f"Gap mundur sebelum konfirmasi~ (◕ω◕)\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Akeno pantau terus dari dekat~ (◕‿◕)"
    )


def _build_pnl_section(
    leg_e: Optional[float],
    leg_b: Optional[float],
    net:   Optional[float],
    emoji: str = "",
) -> str:
    """Helper: bangun section P&L untuk message builder."""
    if net is None:
        return ""
    capital = settings["capital"]
    if leg_e is not None and leg_b is not None and capital > 0:
        half     = capital / 2.0
        usd_e    = leg_e / 100 * half
        usd_b    = leg_b / 100 * half
        usd_net  = usd_e + usd_b
        e_suffix = f"(${usd_e:+.2f})"
        b_suffix = f"(${usd_b:+.2f})"
        n_suffix = f"(${usd_net:+.2f})"
        if active_strategy == Strategy.S1:
            return (
                f"\n*Estimasi P&L pairs:*\n"
                f"┌─────────────────────\n"
                f"│ Long BTC:  {leg_b:+.2f}% {b_suffix}\n"
                f"│ Short ETH: {leg_e:+.2f}% {e_suffix}\n"
                f"│ *Net: {net:+.2f}% {n_suffix}* {emoji}\n"
                f"└─────────────────────\n"
                f"\n"
            )
        else:
            return (
                f"\n*Estimasi P&L pairs:*\n"
                f"┌─────────────────────\n"
                f"│ Long ETH:  {leg_e:+.2f}% {e_suffix}\n"
                f"│ Short BTC: {leg_b:+.2f}% {b_suffix}\n"
                f"│ *Net: {net:+.2f}% {n_suffix}* {emoji}\n"
                f"└─────────────────────\n"
                f"\n"
            )
    elif leg_e is not None and leg_b is not None:
        if active_strategy == Strategy.S1:
            return (
                f"\n*Estimasi P&L pairs:*\n"
                f"│ Long BTC: {leg_b:+.2f}% | Short ETH: {leg_e:+.2f}%\n"
                f"│ *Net: {net:+.2f}%* {emoji}\n\n"
            )
        else:
            return (
                f"\n*Estimasi P&L pairs:*\n"
                f"│ Long ETH: {leg_e:+.2f}% | Short BTC: {leg_b:+.2f}%\n"
                f"│ *Net: {net:+.2f}%* {emoji}\n\n"
            )
    else:
        return f"\n_Net gap movement: {net:+.2f}%_\n\n"


def build_heartbeat_message() -> str:
    lb          = get_lookback_label()
    now         = datetime.now(timezone.utc)
    btc_str     = (
        f"${float(scan_stats['last_btc_price']):,.2f} ({format_value(scan_stats['last_btc_ret'])}%)"
        if scan_stats["last_btc_price"] else "N/A"
    )
    eth_str     = (
        f"${float(scan_stats['last_eth_price']):,.2f} ({format_value(scan_stats['last_eth_ret'])}%)"
        if scan_stats["last_eth_price"] else "N/A"
    )
    gap_str     = f"{format_value(scan_stats['last_gap'])}%" if scan_stats["last_gap"] is not None else "N/A"
    hours_data  = len(price_history) * settings["scan_interval"] / 3600
    data_status = (
        f"✅ {hours_data:.1f}h"
        if hours_data >= settings["lookback_hours"]
        else f"⏳ {hours_data:.1f}h / {settings['lookback_hours']}h"
    )
    peak_str  = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    peak_line = (
        f"│ Peak: {peak_gap:+.2f}%\n"
        if current_mode == Mode.PEAK_WATCH and peak_gap is not None else ""
    )

    # ETH/BTC ratio snapshot
    curr_r, avg_r, _, _, pct_r = calc_ratio_percentile()
    ratio_line = (
        f"ETH/BTC: {curr_r:.5f} | Percentile: {pct_r}th\n"
        if curr_r is not None and pct_r is not None else ""
    )

    # Track section
    track_section = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        et       = settings["exit_threshold"]
        sl       = settings["sl_pct"]
        tpl      = et if active_strategy == Strategy.S1 else -et
        tsl      = (
            trailing_gap_best + sl if active_strategy == Strategy.S1
            else trailing_gap_best - sl
        )
        eth_tp, _  = calc_tp_target_price(active_strategy)
        eth_sl, _  = calc_eth_price_at_gap(tsl)
        eth_sl_str = f"${eth_sl:,.2f}" if eth_sl else "N/A"
        eth_tp_str = f"${eth_tp:,.2f}" if eth_tp  else "N/A"

        gap_now = float(scan_stats["last_gap"]) if scan_stats["last_gap"] is not None else entry_gap_value
        leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_now)
        net_str   = f"{net:+.2f}%" if net is not None else "N/A"
        health_e, health_d = get_pairs_health(active_strategy, gap_now)

        # Driver sekarang vs entry
        driver_line = ""
        if scan_stats.get("last_btc_ret") is not None and scan_stats.get("last_eth_ret") is not None:
            drv, drv_e, _ = analyze_gap_driver(
                float(scan_stats["last_btc_ret"]),
                float(scan_stats["last_eth_ret"]),
                gap_now,
            )
            driver_line = f"│ Driver now: {drv_e} {drv}\n"

        track_section = (
            f"\n*Posisi aktif {active_strategy.value}:*\n"
            f"┌─────────────────────\n"
            f"│ Entry:    {entry_gap_value:+.2f}%\n"
            f"│ Gap now:  {gap_str}\n"
            f"│ Best:     {trailing_gap_best:+.2f}%\n"
            f"{driver_line}"
            f"│ TP:       {tpl:+.2f}% → ETH {eth_tp_str}\n"
            f"│ TSL:      {tsl:+.2f}% → ETH {eth_sl_str}\n"
            f"│ Net P&L:  {net_str}\n"
            f"│ Health:   {health_e} {health_d}\n"
            f"└─────────────────────\n"
        )

    last_r = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum~"
    return (
        f"💓 *Akeno masih di sini~ Ufufufu... (◕‿◕)*\n"
        f"\n"
        f"Mode: *{current_mode.value}* | Peak: {peak_str}\n"
        f"Strategi: {active_strategy.value if active_strategy else '—'}\n"
        f"\n"
        f"*{settings['heartbeat_minutes']}m terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ Scan: {scan_stats['count']}x | Sinyal: {scan_stats['signals_sent']}x\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Harga & Gap ({lb}):*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"│ Gap: {gap_str}\n"
        f"{peak_line}"
        f"└─────────────────────\n"
        f"{ratio_line}"
        f"{track_section}\n"
        f"Data: {data_status} | Redis: {last_r} 🔒\n"
        f"\n"
        f"_Lapor lagi {settings['heartbeat_minutes']} menit~ ⚡_"
    )


# =============================================================================
# Heartbeat
# =============================================================================
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
# API & Price History
# =============================================================================
def parse_iso_timestamp(ts_str: str):
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        if "." in ts_str:
            base, rest = ts_str.split(".", 1)
            tz_start   = next((i for i, c in enumerate(rest) if c in ("+", "-")), -1)
            if tz_start > 6:
                rest = rest[:6] + rest[tz_start:]
            ts_str = base + "." + rest
        dt = datetime.fromisoformat(ts_str)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError) as e:
        logger.error(f"Failed to parse timestamp '{ts_str}': {e}")
        return None


def fetch_prices() -> Optional[PriceData]:
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f"API request failed: {e}")
        return None

    listings = data.get("listings", [])
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

    btc_upd = parse_iso_timestamp(btc_data.get("quotes", {}).get("updated_at", ""))
    eth_upd = parse_iso_timestamp(eth_data.get("quotes", {}).get("updated_at", ""))
    if not btc_upd or not eth_upd:
        return None

    return PriceData(btc_price, eth_price, btc_upd, eth_upd)


def prune_history(now: datetime) -> None:
    global price_history
    cutoff       = now - timedelta(hours=settings["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES)
    price_history = [p for p in price_history if p.timestamp >= cutoff]


def get_lookback_price(now: datetime) -> Optional[PricePoint]:
    target    = now - timedelta(hours=settings["lookback_hours"])
    best, diff = None, timedelta(minutes=30)
    for point in price_history:
        d = abs(point.timestamp - target)
        if d < diff:
            diff, best = d, point
    return best


def compute_returns(btc_now, eth_now, btc_prev, eth_prev):
    btc_chg = (btc_now - btc_prev) / btc_prev * Decimal("100")
    eth_chg = (eth_now - eth_prev) / eth_prev * Decimal("100")
    return btc_chg, eth_chg, eth_chg - btc_chg


def is_data_fresh(now, btc_upd, eth_upd) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return (now - btc_upd) <= threshold and (now - eth_upd) <= threshold


# =============================================================================
# State Reset
# =============================================================================
def reset_to_scan() -> None:
    global current_mode, active_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
    global entry_btc_ret, entry_eth_ret, entry_driver

    current_mode      = Mode.SCAN
    active_strategy   = None
    entry_gap_value   = None
    trailing_gap_best = None
    entry_btc_price   = None
    entry_eth_price   = None
    entry_btc_lb      = None
    entry_eth_lb      = None
    entry_btc_ret     = None
    entry_eth_ret     = None
    entry_driver      = None


# =============================================================================
# TP + TSL Check
# =============================================================================
def check_sltp(
    gap_float: float,
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
) -> bool:
    global trailing_gap_best

    if entry_gap_value is None or active_strategy is None or trailing_gap_best is None:
        return False

    et     = settings["exit_threshold"]
    sl_pct = settings["sl_pct"]

    if active_strategy == Strategy.S1:
        if gap_float < trailing_gap_best:
            trailing_gap_best = gap_float
        tsl_level = trailing_gap_best + sl_pct

        if gap_float <= et:
            eth_target, _ = calc_tp_target_price(Strategy.S1)
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, et, eth_target))
            logger.info(f"TP S1. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return True

        if gap_float >= tsl_level:
            send_alert(build_trailing_sl_message(btc_ret, eth_ret, gap, entry_gap_value, trailing_gap_best, tsl_level))
            logger.info(f"TSL S1. Best: {trailing_gap_best:.2f}% TSL: {tsl_level:.2f}%")
            reset_to_scan()
            return True

    elif active_strategy == Strategy.S2:
        if gap_float > trailing_gap_best:
            trailing_gap_best = gap_float
        tsl_level = trailing_gap_best - sl_pct

        if gap_float >= -et:
            eth_target, _ = calc_tp_target_price(Strategy.S2)
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, -et, eth_target))
            logger.info(f"TP S2. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return True

        if gap_float <= tsl_level:
            send_alert(build_trailing_sl_message(btc_ret, eth_ret, gap, entry_gap_value, trailing_gap_best, tsl_level))
            logger.info(f"TSL S2. Best: {trailing_gap_best:.2f}% TSL: {tsl_level:.2f}%")
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
) -> None:
    global current_mode, active_strategy, peak_gap, peak_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
    global entry_btc_ret, entry_eth_ret, entry_driver

    gap_float      = float(gap)
    entry_thresh   = settings["entry_threshold"]
    exit_thresh    = settings["exit_threshold"]
    invalid_thresh = settings["invalidation_threshold"]
    peak_reversal  = settings["peak_reversal"]
    peak_enabled   = settings["peak_enabled"]

    def do_entry(strategy: Strategy, is_direct: bool = False):
        global current_mode, active_strategy, entry_gap_value, trailing_gap_best
        global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
        global entry_btc_ret, entry_eth_ret, entry_driver, peak_gap, peak_strategy

        active_strategy   = strategy
        current_mode      = Mode.TRACK
        entry_gap_value   = gap_float
        trailing_gap_best = gap_float
        entry_btc_price   = btc_now
        entry_eth_price   = eth_now
        entry_btc_lb      = btc_lb
        entry_eth_lb      = eth_lb
        entry_btc_ret     = float(btc_ret)
        entry_eth_ret     = float(eth_ret)
        drv, _, _         = analyze_gap_driver(float(btc_ret), float(eth_ret), gap_float)
        entry_driver      = drv
        peak              = peak_gap if not is_direct else 0.0
        peak_gap, peak_strategy = None, None

        send_alert(build_entry_message(
            strategy, btc_ret, eth_ret, gap,
            peak, btc_now, eth_now, btc_lb, eth_lb,
            is_direct=is_direct,
        ))
        scan_stats["signals_sent"] += 1

    # ── SCAN ──────────────────────────────────────────────────────────────────
    if current_mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            if peak_enabled:
                current_mode  = Mode.PEAK_WATCH
                peak_strategy = Strategy.S1
                peak_gap      = gap_float
                send_alert(build_peak_watch_message(Strategy.S1, gap))
                logger.info(f"PEAK WATCH S1. Gap: {gap_float:.2f}%")
            else:
                do_entry(Strategy.S1, is_direct=True)
                logger.info(f"DIRECT ENTRY S1. Gap: {gap_float:.2f}%")

        elif gap_float <= -entry_thresh:
            if peak_enabled:
                current_mode  = Mode.PEAK_WATCH
                peak_strategy = Strategy.S2
                peak_gap      = gap_float
                send_alert(build_peak_watch_message(Strategy.S2, gap))
                logger.info(f"PEAK WATCH S2. Gap: {gap_float:.2f}%")
            else:
                do_entry(Strategy.S2, is_direct=True)
                logger.info(f"DIRECT ENTRY S2. Gap: {gap_float:.2f}%")

        else:
            logger.debug(f"SCAN: No signal. Gap: {gap_float:.2f}%")

    # ── PEAK_WATCH ────────────────────────────────────────────────────────────
    elif current_mode == Mode.PEAK_WATCH:
        if peak_strategy == Strategy.S1:
            if gap_float > peak_gap:
                peak_gap = gap_float
                logger.info(f"S1 new peak: {peak_gap:.2f}%")
            elif gap_float < entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S1, gap))
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
                logger.info(f"S1 peak cancelled. Gap: {gap_float:.2f}%")
            elif peak_gap - gap_float >= peak_reversal:
                do_entry(Strategy.S1, is_direct=False)
                logger.info(f"ENTRY S1. Peak: {peak_gap:.2f}% Entry: {gap_float:.2f}%")
            else:
                logger.info(f"S1 peak watch: {gap_float:.2f}% | Peak {peak_gap:.2f}% | Need {peak_reversal}% drop")

        elif peak_strategy == Strategy.S2:
            if gap_float < peak_gap:
                peak_gap = gap_float
                logger.info(f"S2 new peak: {peak_gap:.2f}%")
            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S2, gap))
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
                logger.info(f"S2 peak cancelled. Gap: {gap_float:.2f}%")
            elif gap_float - peak_gap >= peak_reversal:
                do_entry(Strategy.S2, is_direct=False)
                logger.info(f"ENTRY S2. Peak: {peak_gap:.2f}% Entry: {gap_float:.2f}%")
            else:
                logger.info(f"S2 peak watch: {gap_float:.2f}% | Peak {peak_gap:.2f}% | Need {peak_reversal}% rise")

    # ── TRACK ─────────────────────────────────────────────────────────────────
    elif current_mode == Mode.TRACK:
        if check_sltp(gap_float, btc_ret, eth_ret, gap):
            return

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

        logger.debug(f"TRACK {active_strategy.value}: Gap {gap_float:.2f}%")


# =============================================================================
# ─── COMMAND HANDLERS ────────────────────────────────────────────────────────
# =============================================================================

def handle_settings_command(reply_chat: str) -> None:
    hb      = settings["heartbeat_minutes"]
    hb_str  = f"{hb} menit" if hb > 0 else "Off"
    rr      = settings["redis_refresh_minutes"]
    rr_str  = f"{rr} menit" if rr > 0 else "Off"
    peak_s  = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    cap_str = f"${settings['capital']:,.0f}" if settings["capital"] > 0 else "Belum diset~"
    send_reply(
        f"⚙️ *Settings — Akeno jaga semuanya~ Ufufufu...*\n"
        f"\n"
        f"📊 Scan Interval:  {settings['scan_interval']}s\n"
        f"🕐 Lookback:       {settings['lookback_hours']}h\n"
        f"💓 Heartbeat:      {hb_str}\n"
        f"🔄 Redis Refresh:  {rr_str}\n"
        f"📈 Entry:          ±{settings['entry_threshold']}%\n"
        f"📉 Exit/TP:        ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation:   ±{settings['invalidation_threshold']}%\n"
        f"🔍 Peak Mode:      {peak_s} ({settings['peak_reversal']}% reversal)\n"
        f"🛑 Trailing SL:    {settings['sl_pct']}%\n"
        f"💰 Modal:          {cap_str}\n"
        f"📈 Ratio Window:   {settings['ratio_window_days']}d\n"
        f"\n"
        f"_Lihat `/help` untuk semua command~ (◕‿◕)_",
        reply_chat,
    )


def handle_status_command(reply_chat: str) -> None:
    hours_data  = len(price_history) * settings["scan_interval"] / 3600
    lookback    = settings["lookback_hours"]
    ready       = (
        f"✅ {hours_data:.1f}h"
        if hours_data >= lookback
        else f"⏳ {hours_data:.1f}h / {lookback}h"
    )
    peak_s      = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    last_r      = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum~"

    # SCAN section
    scan_section = ""
    if current_mode == Mode.SCAN:
        gap_now = scan_stats.get("last_gap")
        btc_r   = scan_stats.get("last_btc_ret")
        eth_r   = scan_stats.get("last_eth_ret")
        et      = settings["entry_threshold"]
        gap_str = format_value(gap_now) + "%" if gap_now is not None else "N/A"

        driver_line = ""
        if gap_now is not None and btc_r is not None and eth_r is not None:
            drv, drv_e, drv_ex = analyze_gap_driver(float(btc_r), float(eth_r), float(gap_now))
            driver_line = f"│ Driver: {drv_e} {drv} — _{drv_ex}_\n"

        curr_r, _, _, _, pct_r = calc_ratio_percentile()
        ratio_line = f"│ Ratio:  {curr_r:.5f} ({pct_r}th pct)\n" if curr_r and pct_r is not None else ""

        scan_section = (
            f"\n*Gap sekarang ({lookback}h):*\n"
            f"┌─────────────────────\n"
            f"│ BTC: {format_value(btc_r)}% | ETH: {format_value(eth_r)}%\n"
            f"│ Gap: *{gap_str}* (threshold ±{et}%)\n"
            f"{driver_line}"
            f"{ratio_line}"
            f"└─────────────────────\n"
        )

    # PEAK_WATCH section
    peak_section = ""
    if current_mode == Mode.PEAK_WATCH and peak_gap is not None:
        gap_now     = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else peak_gap
        reversal_now = abs(peak_gap - gap_now)
        needed      = settings["peak_reversal"]
        filled      = min(10, int(reversal_now / needed * 10) if needed > 0 else 10)
        bar         = "█" * filled + "░" * (10 - filled)
        peak_section = (
            f"\n*Peak Watch {peak_strategy.value if peak_strategy else ''}:*\n"
            f"┌─────────────────────\n"
            f"│ Peak:    {peak_gap:+.2f}%\n"
            f"│ Gap now: {format_value(scan_stats['last_gap'])}%\n"
            f"│ Reversal: `{bar}` {reversal_now:.2f}% / {needed}%\n"
            f"└─────────────────────\n"
        )

    # TRACK section
    track_section = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        et        = settings["exit_threshold"]
        sl        = settings["sl_pct"]
        tpl       = et if active_strategy == Strategy.S1 else -et
        tsl       = (
            trailing_gap_best + sl if active_strategy == Strategy.S1
            else trailing_gap_best - sl
        )
        eth_tp, _ = calc_tp_target_price(active_strategy)
        eth_sl, _ = calc_eth_price_at_gap(tsl)
        eth_tp_s  = f"${eth_tp:,.2f}" if eth_tp else "N/A"
        eth_sl_s  = f"${eth_sl:,.2f}" if eth_sl else "N/A"
        gap_now   = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else entry_gap_value
        moved     = abs(entry_gap_value - gap_now)

        leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_now)
        net_str   = f"{net:+.2f}%" if net is not None else "N/A"
        health_e, health_d = get_pairs_health(active_strategy, gap_now)

        driver_now_line = ""
        if scan_stats.get("last_btc_ret") is not None:
            drv, drv_e, _ = analyze_gap_driver(
                float(scan_stats["last_btc_ret"]),
                float(scan_stats["last_eth_ret"]),
                gap_now,
            )
            driver_now_line = f"│ Driver now:  {drv_e} {drv}\n"

        pnl_detail = ""
        if leg_e is not None and leg_b is not None:
            if active_strategy == Strategy.S1:
                pnl_detail = (
                    f"│ Long BTC:    {leg_b:+.2f}%\n"
                    f"│ Short ETH:   {leg_e:+.2f}%\n"
                )
            else:
                pnl_detail = (
                    f"│ Long ETH:    {leg_e:+.2f}%\n"
                    f"│ Short BTC:   {leg_b:+.2f}%\n"
                )

        track_section = (
            f"\n*Posisi aktif {active_strategy.value}:*\n"
            f"┌─────────────────────\n"
            f"│ Entry:      {entry_gap_value:+.2f}%\n"
            f"│ Gap now:    {format_value(scan_stats['last_gap'])}%\n"
            f"│ Moved:      ~{moved:.2f}%\n"
            f"│ Best:       {trailing_gap_best:+.2f}%\n"
            f"{driver_now_line}"
            f"{pnl_detail}"
            f"│ Net P&L:    {net_str}\n"
            f"│ Health:     {health_e} {health_d}\n"
            f"│ TP:         {tpl:+.2f}% → ETH {eth_tp_s}\n"
            f"│ TSL:        {tsl:+.2f}% → ETH {eth_sl_s}\n"
            f"└─────────────────────\n"
        )

    send_reply(
        f"📊 *Status Akeno* Ufufufu... (◕‿◕)\n"
        f"\n"
        f"Mode: *{current_mode.value}* | Peak: {peak_s}\n"
        f"Strategi: {active_strategy.value if active_strategy else '—'}\n"
        f"{scan_section}"
        f"{peak_section}"
        f"{track_section}"
        f"History: {ready} | Redis: {last_r} 🔒\n",
        reply_chat,
    )


def handle_pnl_command(reply_chat: str) -> None:
    """Net combined P&L posisi aktif — seperti cara mentor evaluate."""
    if current_mode != Mode.TRACK or active_strategy is None or entry_gap_value is None:
        send_reply(
            "Ara ara~ tidak ada posisi aktif sekarang~ (◕ω◕)\n"
            "Akeno masih mode SCAN~",
            reply_chat,
        )
        return

    gap_now       = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else entry_gap_value
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_now)
    health_e, health_d = get_pairs_health(active_strategy, gap_now)

    # Driver entry vs sekarang
    entry_d_str = f"Driver entry:    *{entry_driver}*\n" if entry_driver else ""
    curr_d_str  = ""
    if scan_stats.get("last_btc_ret") is not None:
        drv, drv_e, drv_ex = analyze_gap_driver(
            float(scan_stats["last_btc_ret"]),
            float(scan_stats["last_eth_ret"]),
            gap_now,
        )
        curr_d_str = f"Driver sekarang: {drv_e} *{drv}* — _{drv_ex}_\n"

    # P&L per leg
    capital = settings["capital"]
    if leg_e is not None and leg_b is not None and capital > 0:
        half    = capital / 2.0
        usd_e   = leg_e / 100 * half
        usd_b   = leg_b / 100 * half
        usd_net = usd_e + usd_b
        if active_strategy == Strategy.S1:
            pnl_body = (
                f"│ Long BTC:  {leg_b:+.2f}% (${usd_b:+.2f})\n"
                f"│ Short ETH: {leg_e:+.2f}% (${usd_e:+.2f})\n"
                f"│ *Net:      {net:+.2f}% (${usd_net:+.2f})*\n"
            )
        else:
            pnl_body = (
                f"│ Long ETH:  {leg_e:+.2f}% (${usd_e:+.2f})\n"
                f"│ Short BTC: {leg_b:+.2f}% (${usd_b:+.2f})\n"
                f"│ *Net:      {net:+.2f}% (${usd_net:+.2f})*\n"
            )
    elif net is not None:
        pnl_body = f"│ Net gap movement: *{net:+.2f}%*\n"
        if capital <= 0:
            pnl_body += "│ _/capital <modal> untuk P&L dalam $~_\n"
    else:
        pnl_body = "│ Data tidak cukup~\n"

    # Jarak ke TP dan TSL
    et   = settings["exit_threshold"]
    sl   = settings["sl_pct"]
    tpl  = et if active_strategy == Strategy.S1 else -et
    tsl  = (
        trailing_gap_best + sl if trailing_gap_best is not None and active_strategy == Strategy.S1
        else trailing_gap_best - sl if trailing_gap_best is not None
        else None
    )
    dist_tp  = abs(gap_now - tpl)
    dist_tsl = abs(gap_now - tsl) if tsl is not None else None
    tsl_str  = f"{dist_tsl:.2f}% lagi ke TSL\n" if dist_tsl is not None else ""

    send_reply(
        f"📊 *Net P&L — Pairs Trade Analysis*\n"
        f"\n"
        f"Posisi: *{active_strategy.value}* | Entry: {entry_gap_value:+.2f}%\n"
        f"Gap now: *{format_value(scan_stats['last_gap'])}%*\n"
        f"\n"
        f"{entry_d_str}"
        f"{curr_d_str}"
        f"\n"
        f"*Estimasi P&L per leg:*\n"
        f"┌─────────────────────\n"
        f"{pnl_body}"
        f"└─────────────────────\n"
        f"\n"
        f"*Jarak ke target:*\n"
        f"├─ {dist_tp:.2f}% lagi ke TP\n"
        f"{'├─ ' + tsl_str if tsl_str else ''}"
        f"\n"
        f"*Health:* {health_e} {health_d}\n"
        f"\n"
        f"_💡 Pairs trade: nilai dari NET, bukan per leg~_\n"
        f"_Seperti mentor: leg ETH -68% tapi net +$239~ (◕‿◕)_",
        reply_chat,
    )


def handle_ratio_command(reply_chat: str) -> None:
    """ETH/BTC ratio percentile monitor."""
    curr_r, avg_r, hi_r, lo_r, pct_r = calc_ratio_percentile()

    if curr_r is None:
        send_reply(
            f"Ara ara~ belum cukup data~ (◕ω◕)\n"
            f"Sekarang: {len(price_history)} points. Butuh minimal 10~",
            reply_chat,
        )
        return

    window       = settings["ratio_window_days"]
    revert_pct   = (avg_r - curr_r) / curr_r * 100 if avg_r else 0
    stars_s1, d1 = get_ratio_conviction(Strategy.S1, pct_r)
    stars_s2, d2 = get_ratio_conviction(Strategy.S2, pct_r)

    if pct_r <= 20:     signal = "🟢 *ETH sangat murah vs BTC* — momentum S2 kuat"
    elif pct_r <= 40:   signal = "🟡 *ETH relatif murah* — setup S2 cukup bagus"
    elif pct_r >= 80:   signal = "🔴 *ETH sangat mahal vs BTC* — momentum S1 kuat"
    elif pct_r >= 60:   signal = "🟠 *ETH relatif mahal* — setup S1 cukup bagus"
    else:               signal = "⚪ *Neutral* — ETH di area tengah vs BTC"

    bar_pos = min(10, int(pct_r / 10))
    bar     = "─" * bar_pos + "●" + "─" * (10 - bar_pos)

    send_reply(
        f"📈 *ETH/BTC Ratio Monitor*\n"
        f"\n"
        f"┌─────────────────────\n"
        f"│ Sekarang:   {curr_r:.5f}\n"
        f"│ {window}d avg:   {avg_r:.5f}\n"
        f"│ {window}d high:  {hi_r:.5f}\n"
        f"│ {window}d low:   {lo_r:.5f}\n"
        f"│ Percentile: *{pct_r}th*\n"
        f"│ Revert est: {revert_pct:+.2f}% ke avg\n"
        f"└─────────────────────\n"
        f"\n"
        f"`[lo]─{bar}─[hi]`\n"
        f"_(0 = ETH sangat murah | 100 = ETH sangat mahal)_\n"
        f"\n"
        f"{signal}\n"
        f"\n"
        f"*Conviction per strategi:*\n"
        f"S1 (Long BTC): {stars_s1} — {d1}\n"
        f"S2 (Long ETH): {stars_s2} — {d2}\n"
        f"\n"
        f"_Dari {len(price_history)} price points~_",
        reply_chat,
    )


def handle_analysis_command(reply_chat: str) -> None:
    """Full market analysis on demand."""
    gap_now = scan_stats.get("last_gap")
    btc_r   = scan_stats.get("last_btc_ret")
    eth_r   = scan_stats.get("last_eth_ret")
    btc_p   = scan_stats.get("last_btc_price")
    eth_p   = scan_stats.get("last_eth_price")

    if gap_now is None:
        send_reply("Ara ara~ belum ada data harga~ Tunggu scan pertama~ (◕ω◕)", reply_chat)
        return

    lb     = get_lookback_label()
    gap_f  = float(gap_now)
    et     = settings["entry_threshold"]

    # Driver
    drv, drv_e, drv_ex = analyze_gap_driver(float(btc_r), float(eth_r), gap_f)

    # Ratio
    curr_r, avg_r, _, _, pct_r = calc_ratio_percentile()
    ratio_str = f"{curr_r:.5f} ({pct_r}th percentile)" if curr_r else "N/A"

    # Gap status
    gap_abs = abs(gap_f)
    if gap_abs < et * 0.5:   gap_status = "💤 Jauh dari threshold — pasar seimbang"
    elif gap_abs < et:        gap_status = f"🔔 Mendekati ±{et}% — mulai perhatikan"
    elif gap_abs < et * 1.5:  gap_status = f"🚨 Di atas ±{et}% — zona entry"
    else:                     gap_status = f"⚡ Divergence ekstrem"

    # Kandidat
    if gap_f >= et:
        cand_str  = f"🔍 Kandidat *S1* (Long BTC / Short ETH)"
        stars, cv = get_ratio_conviction(Strategy.S1, pct_r)
        hint      = get_convergence_hint(Strategy.S1, drv)
    elif gap_f <= -et:
        cand_str  = f"🔍 Kandidat *S2* (Long ETH / Short BTC)"
        stars, cv = get_ratio_conviction(Strategy.S2, pct_r)
        hint      = get_convergence_hint(Strategy.S2, drv)
    else:
        cand_str  = "💤 Belum ada kandidat entry"
        stars, cv = "—", "—"
        hint      = f"Tunggu gap ±{et}%~"

    # Sizing preview
    sizing_str = ""
    if settings["capital"] > 0 and btc_p and eth_p:
        half, eth_qty, btc_qty = calc_sizing(btc_p, eth_p)
        sizing_str = (
            f"\n*💰 Sizing Preview (${settings['capital']:,.0f}):*\n"
            f"├─ Per sisi:  ${half:,.0f}\n"
            f"├─ ETH qty:   {eth_qty:.4f} ETH\n"
            f"└─ BTC qty:   {btc_qty:.6f} BTC\n"
        )

    # Posisi aktif
    pos_str = ""
    if current_mode == Mode.TRACK and active_strategy is not None and entry_gap_value is not None:
        g       = float(gap_now)
        _, _, net = calc_net_pnl(active_strategy, g)
        he, hd  = get_pairs_health(active_strategy, g)
        net_s   = f"{net:+.2f}%" if net is not None else "N/A"
        pos_str = (
            f"\n*📍 Posisi Aktif {active_strategy.value}:*\n"
            f"Entry: {entry_gap_value:+.2f}% | Now: {format_value(gap_now)}%\n"
            f"Net P&L: {net_s} | {he} {hd}\n"
        )

    send_reply(
        f"🧠 *Full Market Analysis*\n"
        f"_Akeno analisis semuanya~ Ufufufu... (◕‿◕)_\n"
        f"\n"
        f"*📊 Gap ({lb}):*\n"
        f"┌─────────────────────\n"
        f"│ BTC:    {format_value(btc_r)}%\n"
        f"│ ETH:    {format_value(eth_r)}%\n"
        f"│ Gap:    *{format_value(gap_now)}%*\n"
        f"│ Driver: {drv_e} *{drv}*\n"
        f"│ _{drv_ex}_\n"
        f"└─────────────────────\n"
        f"{gap_status}\n"
        f"\n"
        f"*📈 ETH/BTC Ratio:*\n"
        f"{ratio_str}\n"
        f"\n"
        f"*🔍 Setup:*\n"
        f"{cand_str}\n"
        f"Conviction: {stars} — _{cv}_\n"
        f"_Hint: {hint}_\n"
        f"{sizing_str}"
        f"{pos_str}\n"
        f"Mode: *{current_mode.value}* | Peak: {'✅ ON' if settings['peak_enabled'] else '❌ OFF'}\n"
        f"\n"
        f"_`/ratio` detail ratio | `/pnl` P&L posisi aktif~_",
        reply_chat,
    )


def handle_capital_command(args: list, reply_chat: str) -> None:
    if not args:
        cap = settings["capital"]
        if cap > 0:
            btc_p = scan_stats.get("last_btc_price")
            eth_p = scan_stats.get("last_eth_price")
            preview = ""
            if btc_p and eth_p:
                half, eth_qty, btc_qty = calc_sizing(btc_p, eth_p)
                preview = (
                    f"\n*Preview sizing sekarang:*\n"
                    f"Per sisi: ${half:,.0f}\n"
                    f"ETH: {eth_qty:.4f} @ ${float(eth_p):,.2f}\n"
                    f"BTC: {btc_qty:.6f} @ ${float(btc_p):,.2f}\n"
                )
            send_reply(
                f"💰 *Modal:* ${cap:,.0f}\n{preview}\nUsage: `/capital <jumlah USD>`",
                reply_chat,
            )
        else:
            send_reply(
                "💰 *Modal belum diset~*\n\n"
                "Set modal untuk sizing guide & P&L dalam dollar~\n"
                "Usage: `/capital 1000`",
                reply_chat,
            )
        return

    try:
        val = float(args[0])
        if val < 0 or val > 10_000_000:
            send_reply("Harus antara $0 sampai $10,000,000~ (◕ω◕)", reply_chat)
            return
        settings["capital"] = val
        if val == 0:
            send_reply("Modal di-reset. Sizing & dollar P&L dimatikan~ (◕‿◕)", reply_chat)
        else:
            send_reply(
                f"💰 Modal *${val:,.0f}* disimpan~\n"
                f"Sizing guide & P&L aktif~ Ufufufu... (◕‿◕)",
                reply_chat,
            )
        logger.info(f"Capital set to {val}")
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    peak_s = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    if not args:
        send_reply(
            f"🔍 *Peak Watch:* {peak_s} | Reversal: *{settings['peak_reversal']}%*\n"
            f"\n"
            f"*ON:* SCAN ➜ PEAK\\_WATCH ➜ TRACK\n"
            f"*OFF:* SCAN ➜ TRACK langsung\n"
            f"\n"
            f"Usage: `/peak on|off|<nilai reversal>`",
            reply_chat,
        )
        return

    first = args[0].lower()
    if first == "on":
        settings["peak_enabled"] = True
        send_reply("✅ *Peak Watch ON*~ (◕‿◕)", reply_chat)
        return
    if first == "off":
        _cancel_peak_watch_if_active(reply_chat)
        settings["peak_enabled"] = False
        send_reply("❌ *Peak Watch OFF*~ Langsung entry saat threshold~ (◕ω◕)", reply_chat)
        return
    try:
        val = float(first)
        if val <= 0 or val > 3.0:
            send_reply("Harus antara 0–3.0~ (◕ω◕)", reply_chat)
            return
        settings["peak_reversal"] = val
        send_reply(f"Reversal *{val}%* dari puncak~ (◕‿◕)", reply_chat)
    except ValueError:
        send_reply("Gunakan `on`, `off`, atau angka reversal~ (◕ω◕)", reply_chat)


def _cancel_peak_watch_if_active(reply_chat=None) -> None:
    global current_mode, peak_gap, peak_strategy
    if current_mode == Mode.PEAK_WATCH and peak_strategy is not None:
        if reply_chat:
            send_reply(
                f"⚠️ Peak Watch *{peak_strategy.value}* dibatalkan.\n"
                "Kembali ke SCAN~ (◕ω◕)",
                reply_chat,
            )
        current_mode  = Mode.SCAN
        peak_gap      = None
        peak_strategy = None


def handle_sltp_command(args: list, reply_chat: str) -> None:
    if not args:
        tsl_info = ""
        if current_mode == Mode.TRACK and trailing_gap_best is not None and active_strategy is not None:
            tsl      = (
                trailing_gap_best + settings["sl_pct"] if active_strategy == Strategy.S1
                else trailing_gap_best - settings["sl_pct"]
            )
            eth_sl, _ = calc_eth_price_at_gap(tsl)
            eth_s    = f" → ETH `${eth_sl:,.2f}`" if eth_sl else ""
            tsl_info = (
                f"\n*TSL sekarang:* `{tsl:+.2f}%`{eth_s}\n"
                f"_(best gap: `{trailing_gap_best:+.2f}%`)_"
            )
        entry_s = f"\n*Entry gap:* `{entry_gap_value:+.2f}%`" if entry_gap_value is not None else ""
        send_reply(
            f"🛑 *Trailing SL*\n"
            f"Distance: *{settings['sl_pct']}%* dari best gap\n"
            f"TP: ±{settings['exit_threshold']}% _(exit threshold)_\n"
            f"{entry_s}{tsl_info}\n"
            f"\nUsage: `/sltp sl <nilai>`",
            reply_chat,
        )
        return

    if len(args) < 2:
        send_reply("Usage: `/sltp sl <nilai>`~ (◕ω◕)", reply_chat)
        return

    try:
        key, val = args[0].lower(), float(args[1])
        if val <= 0 or val > 10:
            send_reply("Harus antara 0–10~ (◕ω◕)", reply_chat)
            return
        if key == "sl":
            settings["sl_pct"] = val
            send_reply(f"Trailing SL distance *{val}%*~ (◕‿◕)", reply_chat)
        elif key == "tp":
            send_reply(
                f"TP mengikuti exit threshold ±{settings['exit_threshold']}%~\n"
                f"Gunakan `/threshold exit <nilai>` ya~ (◕‿◕)",
                reply_chat,
            )
        else:
            send_reply("Gunakan `sl`~ (◕ω◕)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(f"Interval: *{settings['scan_interval']}s*~\nUsage: `/interval <60-3600>`", reply_chat)
        return
    try:
        val = int(args[0])
        if val < 60 or val > 3600:
            send_reply("Harus 60–3600 detik~ (◕ω◕)", reply_chat)
            return
        settings["scan_interval"] = val
        send_reply(f"Scan setiap *{val}s*~ (◕‿◕)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "Usage:\n`/threshold entry <val>`\n`/threshold exit <val>`\n`/threshold invalid <val>`",
            reply_chat,
        )
        return
    try:
        t_type, val = args[0].lower(), float(args[1])
        if val <= 0 or val > 20:
            send_reply("Harus antara 0–20~ (◕ω◕)", reply_chat)
            return
        if t_type == "entry":
            settings["entry_threshold"] = val
            send_reply(f"Entry threshold *±{val}%*~ (◕‿◕)", reply_chat)
        elif t_type == "exit":
            settings["exit_threshold"] = val
            send_reply(f"Exit/TP threshold *±{val}%*~ (◕‿◕)", reply_chat)
        elif t_type in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = val
            send_reply(f"Invalidation *±{val}%*~ (◕‿◕)", reply_chat)
        else:
            send_reply("Gunakan `entry`, `exit`, atau `invalid`~ (◕ω◕)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(f"Lookback: *{settings['lookback_hours']}h*~\nUsage: `/lookback <1-24>`", reply_chat)
        return
    try:
        val = int(args[0])
        if val < 1 or val > 24:
            send_reply("Harus 1–24 jam~ (◕ω◕)", reply_chat)
            return
        old = settings["lookback_hours"]
        settings["lookback_hours"] = val
        prune_history(datetime.now(timezone.utc))
        send_reply(f"Lookback *{old}h* → *{val}h*~ History di-prune. (◕‿◕)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"Heartbeat: *{settings['heartbeat_minutes']} menit*~\nUsage: `/heartbeat <0-120>`",
            reply_chat,
        )
        return
    try:
        val = int(args[0])
        if val < 0 or val > 120:
            send_reply("Harus 0–120 menit~ (◕ω◕)", reply_chat)
            return
        settings["heartbeat_minutes"] = val
        send_reply(
            "Heartbeat *dimatikan*~" if val == 0
            else f"Heartbeat setiap *{val} menit*~",
            reply_chat,
        )
    except ValueError:
        send_reply("Angkanya tidak valid~ (◕ω◕)", reply_chat)


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply("Redis belum dikonfigurasi~ (◕ω◕)", reply_chat)
        return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply("❌ Bot A belum simpan data~ (◕ω◕)", reply_chat)
        return
    try:
        data       = json.loads(result["result"])
        hrs_stored = len(data) * settings["scan_interval"] / 3600
        lookback   = settings["lookback_hours"]
        status     = "✅ Siap" if hrs_stored >= lookback else f"⏳ {hrs_stored:.1f}h / {lookback}h"
        last_r     = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum~"
        send_reply(
            f"⚡ *Redis Status*\n"
            f"Points: {len(data)} | {hrs_stored:.1f}h | {status}\n"
            f"Refresh: {last_r}\n"
            f"`{data[0]['timestamp'][:19]}` → `{data[-1]['timestamp'][:19]}`",
            reply_chat,
        )
    except Exception as e:
        send_reply(f"Gagal baca: `{e}` (◕ω◕)", reply_chat)


def handle_setpos_command(args: list, reply_chat: str) -> None:
    """
    /setpos <S1|S2> eth <entry> <qty> <lev>x btc <entry> <qty> <lev>x

    qty positif = Long, negatif = Short
    Angka boleh pakai koma ribuan: 2,011.56 ✅

    Contoh S1 (Long BTC / Short ETH):
      /setpos S1 eth 2,011.56 -1.4907 50x btc 67,794.76 0.029491 50x

    Contoh S2 (Long ETH / Short BTC):
      /setpos S2 eth 1,956.40 15.58 10x btc 67,586.10 -0.4439 10x
    """
    usage = (
        "*Usage:*\n"
        "`/setpos <S1|S2> eth <entry> <qty> <lev>x btc <entry> <qty> <lev>x`\n"
        "\n"
        "qty *positif* = Long ↑ | qty *negatif* = Short ↓\n"
        "\n"
        "*S1* (Long BTC / Short ETH):\n"
        "`/setpos S1 eth 2011.56 -1.4907 50x btc 67794.76 0.029491 50x`\n"
        "\n"
        "*S2* (Long ETH / Short BTC):\n"
        "`/setpos S2 eth 1956.40 15.58 10x btc 67586.10 -0.4439 10x`"
    )
    if len(args) < 9:
        send_reply(usage, reply_chat)
        return
    try:
        strat_str = args[0].upper()
        if strat_str not in ("S1", "S2"):
            send_reply("Strategi harus *S1* atau *S2*~ (◕ω◕)", reply_chat)
            return
        if args[1].lower() != "eth" or args[5].lower() != "btc":
            send_reply(usage, reply_chat)
            return

        def _pf(s: str) -> float:
            return float(s.replace(",", ""))

        eth_entry = _pf(args[2])
        eth_qty   = _pf(args[3])
        eth_lev   = _pf(args[4].lower().replace("x", ""))
        btc_entry = _pf(args[6])
        btc_qty   = _pf(args[7])
        btc_lev   = _pf(args[8].lower().replace("x", ""))

        if eth_entry <= 0 or btc_entry <= 0:
            send_reply("Entry price harus positif~ (◕ω◕)", reply_chat)
            return
        if not (1 <= eth_lev <= 200) or not (1 <= btc_lev <= 200):
            send_reply("Leverage harus antara 1x–200x~ (◕ω◕)", reply_chat)
            return

        # Optional liq override: /setpos ... ethliq 1431.47 btcliq 85802.45
        eth_liq, btc_liq = None, None
        extra = args[9:]
        for i in range(0, len(extra) - 1, 2):
            key = extra[i].lower()
            try:
                val = _pf(extra[i + 1])
                if key == "ethliq":   eth_liq = val
                elif key == "btcliq": btc_liq = val
            except (ValueError, IndexError):
                pass

        now_iso = datetime.now(timezone.utc).isoformat()
        pos_data.update({
            "eth_entry_price": eth_entry,
            "eth_qty":         eth_qty,
            "eth_leverage":    eth_lev,
            "eth_liq_price":   eth_liq,
            "btc_entry_price": btc_entry,
            "btc_qty":         btc_qty,
            "btc_leverage":    btc_lev,
            "btc_liq_price":   btc_liq,
            "strategy":        strat_str,
            "set_at":          now_iso,
        })

        # Simpan ke Redis supaya survive restart
        saved  = save_pos_data()
        sv_str = "✅ Tersimpan ke Redis~" if saved else "⚠️ Redis tidak tersedia, data hanya di memory~"

        eth_dir = "Long 📈" if eth_qty > 0 else "Short 📉"
        btc_dir = "Long 📈" if btc_qty > 0 else "Short 📉"
        liq_note = ""
        if eth_liq or btc_liq:
            liq_note = (
                f"\nLiq price manual:\n"
                f"ETH: ${eth_liq:,.2f}\n" if eth_liq else ""
            ) + (f"BTC: ${btc_liq:,.2f}\n" if btc_liq else "")

        logger.info(f"pos_data set: {strat_str} ETH {eth_dir} {eth_qty}@{eth_entry} {eth_lev}x | "
                    f"BTC {btc_dir} {btc_qty}@{btc_entry} {btc_lev}x")
        send_reply(
            f"✅ *Posisi {strat_str} disimpan~* Ufufufu... (◕‿◕)\n"
            f"\n"
            f"ETH: *{eth_dir}* {abs(eth_qty):.4f} @ ${eth_entry:,.2f} | {eth_lev:.0f}x\n"
            f"BTC: *{btc_dir}* {abs(btc_qty):.6f} @ ${btc_entry:,.2f} | {btc_lev:.0f}x\n"
            f"{liq_note}\n"
            f"{sv_str}\n"
            f"\n"
            f"Ketik `/health` untuk cek kesehatan posisi~",
            reply_chat,
        )
    except (ValueError, IndexError) as e:
        send_reply(f"Format salah~ (◕ω◕)\n\n{usage}", reply_chat)
        logger.warning(f"setpos parse error: {e}")


def handle_health_command(reply_chat: str) -> None:
    """Tampilkan health posisi — leverage, margin, UPnL, liq price, pairs net."""
    if pos_data["eth_entry_price"] is None:
        send_reply(
            "Ara ara~ belum ada posisi yang diset~ (◕ω◕)\n\n"
            "Gunakan `/setpos` dulu ya~\n\n"
            "*Contoh S1* (Long BTC / Short ETH):\n"
            "`/setpos S1 eth 2011.56 -1.4907 50x btc 67794.76 0.029491 50x`\n\n"
            "*Contoh S2* (Long ETH / Short BTC):\n"
            "`/setpos S2 eth 1956.40 15.58 10x btc 67586.10 -0.4439 10x`",
            reply_chat,
        )
        return
    if scan_stats.get("last_btc_price") is None:
        send_reply("Tunggu sebentar~ Akeno belum dapat harga terbaru~ (◕ω◕)", reply_chat)
        return
    h = calc_position_pnl()
    if not h:
        send_reply("Gagal hitung P&L~ Coba `/setpos` ulang~ (◕ω◕)", reply_chat)
        return
    send_reply(build_position_health_message(h), reply_chat)


def handle_clearpos_command(reply_chat: str) -> None:
    """Hapus pos_data dari memory dan Redis."""
    for k in pos_data:
        pos_data[k] = None
    clear_pos_data_redis()
    send_reply("🗑️ *Data posisi dihapus~* (◕‿◕)\nRedis juga dibersihkan~", reply_chat)


def handle_help_command(reply_chat: str) -> None:
    peak_s  = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    cap_str = f"${settings['capital']:,.0f}" if settings["capital"] > 0 else "belum diset"
    pos_str = pos_data.get("strategy") or "belum diset"
    send_reply(
        "Ara ara~ ini semua yang bisa Akeno lakukan~ Ufufufu... (◕‿◕)\n"
        "\n"
        "*⚙️ Core:*\n"
        "`/settings` `/status` `/redis`\n"
        "`/interval` `/lookback` `/heartbeat`\n"
        "`/threshold entry|exit|invalid <val>`\n"
        "`/sltp sl <val>` | `/peak on|off|<val>`\n"
        f"_Peak sekarang: {peak_s}_\n"
        "\n"
        "*🧠 Analysis:*\n"
        f"`/capital <usd>` _(sekarang: {cap_str})_\n"
        "`/ratio` — ratio monitor + conviction + entry readiness\n"
        "`/pnl` — net P&L posisi bot\n"
        "`/analysis` — full snapshot\n"
        "\n"
        "*🏥 Position Health Tracker:*\n"
        f"`/setpos S1|S2 eth <entry> <qty> <lev>x btc <entry> <qty> <lev>x`\n"
        f"_Posisi: {pos_str}_ | _Tersimpan di Redis~_\n"
        "`/health` — leverage, margin ratio, UPnL, liq price\n"
        "`/clearpos` — hapus data posisi\n"
        "\n"
        "*S1* (Long BTC / Short ETH):\n"
        "`/setpos S1 eth 2011 -1.49 50x btc 67794 0.029 50x`\n"
        "*S2* (Long ETH / Short BTC):\n"
        "`/setpos S2 eth 1956 15.58 10x btc 67586 -0.44 10x`\n"
        "\n"
        "_Entry signal otomatis: driver, ratio, sizing, skenario, TP/TSL harga~ ⚡_",
        reply_chat,
    )


# =============================================================================
# Startup Message
# =============================================================================
def send_startup_message() -> bool:
    price_data  = fetch_prices()
    price_info  = (
        f"\n💰 BTC: ${float(price_data.btc_price):,.2f} | ETH: ${float(price_data.eth_price):,.2f}\n"
        if price_data
        else "\n⚠️ Gagal ambil harga~ Akeno terus coba~ (◕ω◕)\n"
    )
    hrs_loaded  = len(price_history) * settings["scan_interval"] / 3600
    hist_info   = (
        f"⚡ History Bot A: *{hrs_loaded:.1f}h* siap!\n"
        if price_history
        else f"⏳ Menunggu Bot A~ Sinyal setelah {settings['lookback_hours']}h tersedia~\n"
    )
    peak_s      = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    cap_str     = f"${settings['capital']:,.0f}" if settings["capital"] > 0 else "belum diset (gunakan /capital)"

    # Posisi yang di-restore dari Redis
    pos_info = ""
    if pos_data.get("strategy") and pos_data.get("eth_entry_price"):
        strat    = pos_data["strategy"]
        eth_dir  = "Long" if (pos_data["eth_qty"] or 0) > 0 else "Short"
        btc_dir  = "Long" if (pos_data["btc_qty"] or 0) > 0 else "Short"
        pos_info = (
            f"\n🏥 *Posisi {strat} di-restore:*\n"
            f"ETH {eth_dir} @ ${pos_data['eth_entry_price']:,.2f} | "
            f"BTC {btc_dir} @ ${pos_data['btc_entry_price']:,.2f}\n"
            f"_Ketik `/health` untuk cek kesehatan~_\n"
        )

    return send_alert(
        f"………\n"
        f"Ara ara~ *Akeno (Bot B) sudah siap~* Ufufufu... (◕‿◕)\n"
        f"_Swing / Day Trade Edition — Mentor Analysis_\n"
        f"{price_info}\n"
        f"📊 Scan: {settings['scan_interval']}s | Lookback: {settings['lookback_hours']}h\n"
        f"📈 Entry: ±{settings['entry_threshold']}% | 📉 Exit: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalid: ±{settings['invalidation_threshold']}% | 🛑 TSL: {settings['sl_pct']}%\n"
        f"🔍 Peak Mode: {peak_s} | 💰 Modal: {cap_str}\n"
        f"{pos_info}\n"
        f"*🧠 Analisis aktif di setiap entry signal:*\n"
        f"• Gap Driver (ETH-led vs BTC-led)\n"
        f"• ETH/BTC Ratio Percentile + Conviction\n"
        f"• Dollar-Neutral Sizing Guide\n"
        f"• Convergence Scenarios A & B\n"
        f"• Net Combined P&L Tracker\n"
        f"\n"
        f"{hist_info}\n"
        f"Ketik `/help` untuk semua command~\n"
        f"Akeno takkan pergi~ ⚡"
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
    logger.info("Monk Bot B — Swing/Day Trade | Mentor Analysis Edition")
    logger.info(
        f"Entry: ±{settings['entry_threshold']}% | Exit: ±{settings['exit_threshold']}% | "
        f"Invalid: ±{settings['invalidation_threshold']}% | "
        f"Peak: {'ON' if settings['peak_enabled'] else 'OFF'} | TSL: {settings['sl_pct']}%"
    )
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    load_history()
    prune_history(datetime.now(timezone.utc))
    last_redis_refresh = datetime.now(timezone.utc)
    logger.info(f"History loaded: {len(price_history)} points")

    load_pos_data()
    if pos_data.get("strategy"):
        logger.info(f"pos_data restored: {pos_data['strategy']} "
                    f"ETH@{pos_data['eth_entry_price']} BTC@{pos_data['btc_entry_price']}")

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
                scan_stats["count"]          += 1
                scan_stats["last_btc_price"]  = price_data.btc_price
                scan_stats["last_eth_price"]  = price_data.eth_price

                if not is_data_fresh(now, price_data.btc_updated_at, price_data.eth_updated_at):
                    logger.warning("Data not fresh, skipping")
                else:
                    price_then = get_lookback_price(now)

                    if price_then is None:
                        hrs = len(price_history) * settings["scan_interval"] / 3600
                        logger.info(f"Waiting for data... ({hrs:.1f}h / {settings['lookback_hours']}h)")
                    else:
                        btc_ret, eth_ret, gap = compute_returns(
                            price_data.btc_price, price_data.eth_price,
                            price_then.btc, price_then.eth,
                        )
                        scan_stats["last_gap"]     = gap
                        scan_stats["last_btc_ret"] = btc_ret
                        scan_stats["last_eth_ret"] = eth_ret

                        logger.info(
                            f"Mode: {current_mode.value} | "
                            f"BTC {settings['lookback_hours']}h: {format_value(btc_ret)}% | "
                            f"ETH: {format_value(eth_ret)}% | Gap: {format_value(gap)}%"
                        )

                        evaluate_and_transition(
                            btc_ret, eth_ret, gap,
                            price_data.btc_price, price_data.eth_price,
                            price_then.btc, price_then.eth,
                        )

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
