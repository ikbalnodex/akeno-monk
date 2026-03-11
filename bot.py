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
    "entry_threshold":        1.5,
    "exit_threshold":         0.2,
    "invalidation_threshold": INVALIDATION_THRESHOLD,
    "peak_reversal":          0.3,
    "peak_enabled":           False,
    "lookback_hours":         DEFAULT_LOOKBACK_HOURS,
    "heartbeat_minutes":      30,
    "sl_pct":                 1.0,
    "redis_refresh_minutes":  REDIS_REFRESH_MINUTES,
    # — Swing / Day Trade —
    "capital":                0.0,
    "ratio_window_days":      RATIO_WINDOW_DAYS,
    # — Exit Confirmation (anti false-exit) —
    # Lapis 1: gap harus stay di zona exit selama N scan berturut-turut
    "exit_confirm_scans":     2,       # 0 = langsung exit (behaviour lama)
    # Lapis 2: gap harus konvergen sejauh X% lebih dalam dari exit_threshold
    "exit_confirm_buffer":    0.0,     # 0.0 = disable; misal 0.3 = exit di threshold - 0.3%
    # Lapis 3: P&L gate — exit hanya kalau net P&L ≥ X% dari margin (pakai pos_data)
    "exit_pnl_gate":          0.0,     # 0.0 = disable; misal 0.5 = minimal +0.5% net
    # — Sizing Ratio —
    # eth_size_ratio: % dari modal ke ETH leg (0-100), sisanya ke BTC
    # 50.0 = dollar-neutral | 60.0 = ETH 60% / BTC 40%
    "eth_size_ratio":         50.0,
    # — Regime Filter —
    # Blokir entry kalau arah gap berlawanan dengan kondisi market
    # S1 hanya valid saat BULLISH | S2 hanya valid saat BEARISH
    # pump + ETH < BTC (gap -) = SKIP S2 | dump + ETH > BTC (gap +) = SKIP S1
    "regime_filter_enabled":  True,
    # — Simulation Mode —
    "sim_enabled":            False,   # bot otomatis open/close posisi saat sinyal
    "sim_margin_usd":         100.0,   # margin per leg dalam USD
    "sim_leverage":           10.0,    # leverage (sama untuk dua leg)
    "sim_fee_pct":            0.06,    # taker fee % per side (default Bybit/OKX)
}

# Simulation trade state — diisi otomatis saat entry signal, dikosongkan saat exit
sim_trade: dict = {
    "active":        False,
    "strategy":      None,    # "S1" / "S2"
    "eth_entry":     None,    # float, harga ETH saat open
    "btc_entry":     None,    # float, harga BTC saat open
    "eth_qty":       None,    # float, signed (+long / -short)
    "btc_qty":       None,    # float, signed
    "eth_notional":  None,    # float, USD
    "btc_notional":  None,    # float, USD
    "eth_margin":    None,    # float, USD
    "btc_margin":    None,    # float, USD
    "fee_open":      None,    # float, total fee saat buka 2 leg
    "opened_at":     None,    # ISO string
    # Rekap closed trades
    "history":       [],      # list of dict per trade
}

last_update_id:      int                = 0
last_heartbeat_time: Optional[datetime] = None
last_redis_refresh:  Optional[datetime] = None

# Exit confirmation counter — reset setiap kali gap keluar zona exit
exit_confirm_count:  int                = 0

scan_stats = {
    "count":          0,
    "last_btc_price": None,
    "last_eth_price": None,
    "last_btc_ret":   None,
    "last_eth_ret":   None,
    "last_gap":       None,
    "signals_sent":   0,
}

# Gap history untuk velocity tracking (ringkasan gap per scan)
gap_history: List[Tuple[datetime, float]] = []   # (timestamp, gap_value)
MAX_GAP_HISTORY = 120   # simpan ~2 jam kalau scan setiap 60s

# Manual position tracker — persisted to Redis, survive restart
pos_data: dict = {
    # Legs
    "eth_entry_price":  None,   # float
    "eth_qty":          None,   # float (+long / -short)
    "eth_notional_usd": None,   # float, USD value saat entry (opsional, untuk display)
    "eth_leverage":     None,   # float
    "eth_liq_price":    None,   # float (manual override, opsional)
    "eth_funding_rate": None,   # float, % per 8h (positif = kamu bayar)
    "btc_entry_price":  None,
    "btc_qty":          None,
    "btc_notional_usd": None,
    "btc_leverage":     None,
    "btc_liq_price":    None,
    "btc_funding_rate": None,   # float, % per 8h
    # Meta
    "strategy":         None,   # "S1" / "S2"
    "set_at":           None,   # ISO string
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
            "/help": lambda: (
                handle_help_market_command(chat_id)  if args and args[0].lower() in ("market", "m") else
                handle_help_pos_command(chat_id)     if args and args[0].lower() in ("pos", "p", "health") else
                handle_help_config_command(chat_id)  if args and args[0].lower() in ("config", "c", "conf") else
                handle_help_example_command(chat_id) if args and args[0].lower() in ("example", "ex", "contoh") else
                handle_help_command(chat_id)
            ),
            "/start":     lambda: handle_help_command(chat_id),
            "/interval":  lambda: handle_interval_command(args, chat_id),
            "/threshold": lambda: handle_threshold_command(args, chat_id),
            "/lookback":  lambda: handle_lookback_command(args, chat_id),
            "/heartbeat": lambda: handle_heartbeat_command(args, chat_id),
            "/peak":      lambda: handle_peak_command(args, chat_id),
            "/sltp":      lambda: handle_sltp_command(args, chat_id),
            "/redis":     lambda: handle_redis_command(chat_id),
            # — Swing / Day Trade —
            "/capital":    lambda: handle_capital_command(args, chat_id),
            "/sizeratio":     lambda: handle_sizeratio_command(args, chat_id),
            "/regimefilter":  lambda: handle_regimefilter_command(args, chat_id),
            "/sim":        lambda: handle_sim_command(args, chat_id),
            "/simstats":   lambda: handle_simstats_command(chat_id),
            "/ratio":     lambda: handle_ratio_command(chat_id),
            "/pnl":       lambda: handle_pnl_command(chat_id),
            "/analysis":  lambda: handle_analysis_command(chat_id),
            # — Position Health Tracker —
            "/setpos":      lambda: handle_setpos_command(args, chat_id),
            "/health":      lambda: handle_health_command(chat_id),
            "/clearpos":    lambda: handle_clearpos_command(chat_id),
            "/setfunding":  lambda: handle_setfunding_command(args, chat_id),
            "/velocity":    lambda: handle_velocity_command(chat_id),
            "/exitconf":    lambda: handle_exitconf_command(args, chat_id),
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
    Identifikasi siapa yang menggerakkan gap (lookback window saja).
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


def calc_dominance_score() -> dict:
    """
    Hitung ETH vs BTC dominance score dari multi-timeframe (1h, 4h, 24h).

    Logika:
      per timeframe: dominance = ETH_ret - BTC_ret
      ETH > BTC → ETH dominant (positif)
      BTC > ETH → BTC dominant (negatif)

    Weighted score: 1h×1 + 4h×2 + 24h×3 (max ±6 normalized units)

    Score > 0  → ETH dominant  → S1 signal (Short ETH yang terlalu tinggi)
    Score < 0  → BTC dominant  → S2 signal (Short BTC yang terlalu tinggi)

    Returns:
      score          : float (raw weighted)
      score_norm     : float (-1.0 to +1.0 normalized)
      dominant       : "ETH" / "BTC" / "Seimbang"
      strength       : "Ekstrem" / "Kuat" / "Moderat" / "Lemah"
      signal         : "S1" / "S2" / None
      aligned_with   : lambda strategy → bool
      tf             : dict {1h/4h/24h: {btc, eth, diff}}
      label          : str (human readable)
      conviction_adj : int (-2 / -1 / 0 / +1 / +2) — adjustment ke conviction
    """
    if not price_history or len(price_history) < 3:
        return {
            "score": 0, "score_norm": 0, "dominant": "N/A",
            "strength": "N/A", "signal": None, "tf": {},
            "label": "Data belum cukup", "conviction_adj": 0,
        }

    now_pt   = price_history[-1]
    btc_now  = float(now_pt.btc)
    eth_now  = float(now_pt.eth)
    interval = settings["scan_interval"]

    def _ret(minutes: int):
        scans_back = max(1, int(minutes * 60 / interval))
        if len(price_history) <= scans_back:
            return None, None
        old = price_history[-scans_back - 1]
        return (
            (btc_now - float(old.btc)) / float(old.btc) * 100,
            (eth_now - float(old.eth)) / float(old.eth) * 100,
        )

    btc_1h,  eth_1h  = _ret(60)
    btc_4h,  eth_4h  = _ret(240)
    btc_24h, eth_24h = _ret(1440)

    tf = {}
    weighted_sum = 0.0
    weight_total = 0.0

    for (label, btc_v, eth_v, weight) in [
        ("1h",  btc_1h,  eth_1h,  1),
        ("4h",  btc_4h,  eth_4h,  2),
        ("24h", btc_24h, eth_24h, 3),
    ]:
        if btc_v is not None and eth_v is not None:
            diff = eth_v - btc_v   # positive = ETH dominant, negative = BTC dominant
            tf[label] = {"btc": btc_v, "eth": eth_v, "diff": diff}
            weighted_sum += diff * weight
            weight_total += weight

    if weight_total == 0:
        return {
            "score": 0, "score_norm": 0, "dominant": "N/A",
            "strength": "N/A", "signal": None, "tf": tf,
            "label": "Data belum cukup", "conviction_adj": 0,
        }

    score      = weighted_sum / weight_total   # avg weighted diff %
    abs_score  = abs(score)

    # Normalize ke -1..+1 menggunakan 3% sebagai "extreme" reference
    score_norm = max(-1.0, min(1.0, score / 3.0))

    # Dominance & strength
    if abs_score >= 2.0:   strength = "Ekstrem";  conv_adj = 2
    elif abs_score >= 1.0: strength = "Kuat";     conv_adj = 1
    elif abs_score >= 0.3: strength = "Moderat";  conv_adj = 0
    else:                  strength = "Lemah";    conv_adj = -1

    if score > 0.3:
        dominant = "ETH"
        signal   = Strategy.S1   # ETH terlalu tinggi → Short ETH
        emoji    = "🟡"
        label    = f"ETH dominant {strength} ({score:+.2f}%) → S1 favored"
    elif score < -0.3:
        dominant = "BTC"
        signal   = Strategy.S2   # BTC terlalu tinggi → Short BTC
        emoji    = "🟠"
        label    = f"BTC dominant {strength} ({score:+.2f}%) → S2 favored"
    else:
        dominant = "Seimbang"
        signal   = None
        emoji    = "⚪"
        label    = f"Seimbang ({score:+.2f}%) — tidak ada dominance jelas"
        conv_adj = -1

    return {
        "score":         score,
        "score_norm":    score_norm,
        "dominant":      dominant,
        "strength":      strength,
        "signal":        signal,
        "emoji":         emoji,
        "tf":            tf,
        "label":         label,
        "conviction_adj": conv_adj,
    }


def check_dominance_alignment(strategy: Strategy, dom: dict) -> Tuple[str, str, int]:
    """
    Cek apakah dominance score align dengan strategi yang akan dientry.

    Returns: (status, message, conviction_adj)
      status: "STRONG" / "OK" / "WEAK" / "AGAINST"
    """
    dom_signal = dom.get("signal")
    strength   = dom.get("strength", "N/A")
    score      = dom.get("score", 0)
    conv_adj   = dom.get("conviction_adj", 0)

    if dom_signal == strategy:
        # Align sempurna
        if strength == "Ekstrem":
            return "STRONG", f"✅ Dominance *Ekstrem* align — {dom['label']}", +2
        elif strength == "Kuat":
            return "STRONG", f"✅ Dominance *Kuat* align — {dom['label']}", +1
        else:
            return "OK", f"✅ Dominance align ({strength}) — {dom['label']}", 0

    elif dom_signal is None:
        # Seimbang — netral
        return "WEAK", f"⚠️ Dominance lemah/seimbang — conviction berkurang", -1

    else:
        # Berlawanan
        opp = "ETH" if strategy == Strategy.S1 else "BTC"
        return "AGAINST", (
            f"⚠️ Dominance berlawanan — *{dom['dominant']} dominant* tapi kamu entry {strategy.value}\n"
            f"  {opp} sedang outperform, gap bisa melebar lebih lanjut"
        ), -2


def detect_market_regime() -> dict:
    """
    Deteksi regime pasar BTC (dan ETH) dari price_history.
    Menggunakan multi-timeframe: 1h, 4h, 24h + volatility (ATR proxy).

    Returns dict:
      regime       : "BULLISH" / "BEARISH" / "CONSOLIDASI"
      strength     : "Kuat" / "Moderat" / "Lemah"
      emoji        : str
      btc_1h/4h/24h: % change
      eth_1h/4h/24h: % change
      volatility   : "Tinggi" / "Normal" / "Rendah"
      vol_pct      : float (avg candle range % per period)
      description  : str (satu baris penjelasan)
      implications : str (implikasi untuk pairs strategy)
    """
    if len(price_history) < 3:
        return {"regime": "N/A", "emoji": "⚪", "strength": "—",
                "description": "Data belum cukup", "implications": "—",
                "btc_1h": None, "btc_4h": None, "btc_24h": None,
                "eth_1h": None, "eth_4h": None, "eth_24h": None,
                "volatility": "—", "vol_pct": 0.0}

    now      = price_history[-1]
    btc_now  = float(now.btc)
    eth_now  = float(now.eth)
    interval = settings["scan_interval"]   # detik per scan

    def _pct_change(minutes: int):
        """Ambil price N menit lalu, hitung % change."""
        scans_back = max(1, int(minutes * 60 / interval))
        if len(price_history) <= scans_back:
            return None, None
        old = price_history[-scans_back - 1]
        btc_ret = (btc_now - float(old.btc)) / float(old.btc) * 100
        eth_ret = (eth_now - float(old.eth)) / float(old.eth) * 100
        return btc_ret, eth_ret

    btc_1h,  eth_1h  = _pct_change(60)
    btc_4h,  eth_4h  = _pct_change(240)
    btc_24h, eth_24h = _pct_change(1440)

    # Volatility: rata-rata |Δ%| per scan selama 60 menit terakhir
    scans_1h   = max(2, int(3600 / interval))
    window_pts = price_history[-scans_1h:] if len(price_history) >= scans_1h else price_history
    vol_samples = []
    for i in range(1, len(window_pts)):
        prev_b = float(window_pts[i-1].btc)
        curr_b = float(window_pts[i].btc)
        if prev_b > 0:
            vol_samples.append(abs(curr_b - prev_b) / prev_b * 100)
    avg_vol = sum(vol_samples) / len(vol_samples) if vol_samples else 0.0

    if avg_vol >= 0.15:     vol_label = "Tinggi 🔥"
    elif avg_vol >= 0.05:   vol_label = "Normal 📊"
    else:                   vol_label = "Rendah 😴"

    # Regime logic — pakai weighted vote dari 3 timeframe
    # Bobot: 1h=1, 4h=2, 24h=3 (4h dan 24h lebih penting)
    def _vote(ret, threshold=0.5):
        if ret is None: return 0
        if ret > threshold:  return 1    # bull
        if ret < -threshold: return -1   # bear
        return 0                          # konsolidasi

    votes = (
        _vote(btc_1h,  0.3) * 1 +
        _vote(btc_4h,  0.8) * 2 +
        _vote(btc_24h, 1.5) * 3
    )

    # Tentukan regime dari total votes (-6 s/d +6)
    if votes >= 4:      regime, emoji = "BULLISH",     "🟢"
    elif votes >= 1:    regime, emoji = "BULLISH",     "🟡"
    elif votes <= -4:   regime, emoji = "BEARISH",     "🔴"
    elif votes <= -1:   regime, emoji = "BEARISH",     "🟠"
    else:               regime, emoji = "KONSOLIDASI", "⚪"

    if abs(votes) >= 4: strength = "Kuat"
    elif abs(votes) >= 2: strength = "Moderat"
    else: strength = "Lemah"

    # Description
    if regime == "BULLISH":
        desc = f"BTC dalam tren naik — momentum {'kuat' if strength == 'Kuat' else 'moderat'}"
    elif regime == "BEARISH":
        desc = f"BTC dalam tren turun — momentum {'kuat' if strength == 'Kuat' else 'moderat'}"
    else:
        desc = "BTC bergerak sideways — tidak ada tren jelas"

    # Implikasi untuk pairs strategy
    if regime == "BULLISH" and strength == "Kuat":
        impl = "✅ *S1 setup* — pump kuat, ETH cenderung outperform BTC. Monitor gap, kalau ETH pump lebih % dari BTC → entry S1."
    elif regime == "BULLISH":
        impl = "S1 moderat — pump ada tapi belum kuat. Tunggu ETH jelas outperform BTC sebelum entry S1."
    elif regime == "BEARISH" and strength == "Kuat":
        impl = "✅ *S2 setup* — dump kuat, ETH cenderung underperform BTC. Monitor gap, kalau ETH dump lebih % dari BTC → entry S2."
    elif regime == "BEARISH":
        impl = "S2 moderat — dump ada tapi belum kuat. Tunggu ETH jelas underperform BTC sebelum entry S2."
    else:
        impl = "⚠️ Sideways — gap kecil dan tidak sustained. Kedua strategi sulit, lebih baik tunggu arah jelas."

    return {
        "regime":     regime,
        "emoji":      emoji,
        "strength":   strength,
        "votes":      votes,
        "btc_1h":     btc_1h,  "eth_1h":  eth_1h,
        "btc_4h":     btc_4h,  "eth_4h":  eth_4h,
        "btc_24h":    btc_24h, "eth_24h": eth_24h,
        "volatility": vol_label,
        "vol_pct":    avg_vol,
        "description": desc,
        "implications": impl,
    }


def get_convergence_hint(strategy: Strategy, driver: str) -> str:
    """Prediksi cara konvergensi paling mungkin."""
    if strategy == Strategy.S1:
        # Entry saat pump + ETH outperform BTC (gap > 0)
        # Profit saat ETH pullback atau BTC catch up
        hints = {
            "ETH-led": "ETH pump yang dominan → kemungkinan *ETH pullback* dulu. Revert biasanya lebih cepat.",
            "BTC-led": "BTC yang ketinggalan naik → tunggu *BTC catch up*. Lebih lambat tapi lebih sustained.",
            "Mixed":   "Keduanya berkontribusi → bisa revert dari ETH pullback atau BTC catch up.",
        }
    else:
        # Entry saat dump + ETH underperform BTC (gap < 0)
        # Profit saat ETH bounce atau BTC koreksi lebih dalam
        hints = {
            "ETH-led": "ETH dump yang dominan → kemungkinan *ETH bounce* dulu. Revert biasanya lebih cepat.",
            "BTC-led": "BTC yang terlalu kuat → tunggu *BTC koreksi*. Lebih lambat, seperti yang terjadi 2x ini.",
            "Mixed":   "Keduanya berkontribusi → bisa revert dari ETH bounce atau BTC koreksi.",
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


def _calc_ratio_extended_stats(
    curr_r: float,
    avg_r:  float,
    hi_r:   float,
    lo_r:   float,
    pct_r:  int,
) -> dict:
    """Statistik lanjutan ratio untuk conviction detail."""
    pct_from_high = (curr_r - hi_r) / hi_r * 100 if hi_r else 0
    pct_from_low  = (curr_r - lo_r) / lo_r * 100  if lo_r else 0
    range_total   = hi_r - lo_r if hi_r and lo_r else 0
    pos_in_range  = (curr_r - lo_r) / range_total * 100 if range_total > 0 else 50
    revert_to_avg = (avg_r - curr_r) / curr_r * 100 if avg_r else 0

    ratios  = [float(p.eth / p.btc) for p in price_history]
    z_score = None
    if len(ratios) >= 10:
        mean = sum(ratios) / len(ratios)
        std  = (sum((r - mean) ** 2 for r in ratios) / len(ratios)) ** 0.5
        if std > 0:
            z_score = (curr_r - mean) / std

    return {
        "pct_from_high": pct_from_high,
        "pct_from_low":  pct_from_low,
        "pos_in_range":  pos_in_range,
        "z_score":       z_score,
        "revert_to_avg": revert_to_avg,
    }


def _build_conviction_detail(
    strategy: Strategy,
    stars:    str,
    pct_r:    int,
    curr_r:   float,
    avg_r:    float,
    hi_r:     float,
    lo_r:     float,
    ext:      dict,
) -> str:
    """Teks conviction detail untuk satu strategi."""
    window = settings["ratio_window_days"]
    z      = ext["z_score"]
    z_str  = f"{z:+.2f}σ dari avg" if z is not None else "N/A"

    if strategy == Strategy.S1:
        label = "S1 — Long BTC / Short ETH"
        reasons = []
        if pct_r >= 75:
            reasons.append(f"Ratio *{pct_r}th percentile* — ETH mahal secara historis ({window}d)")
        if ext["pct_from_high"] >= -1.0:
            reasons.append(f"Ratio *{abs(ext['pct_from_high']):.2f}%* dari {window}d high — mendekati puncak")
        elif ext["pct_from_high"] >= -3.0:
            reasons.append(f"Ratio *{abs(ext['pct_from_high']):.2f}%* di bawah {window}d high")
        if z is not None and z >= 1.0:
            reasons.append(f"Z-score *{z:+.2f}σ* — ETH secara statistik mahal vs BTC")
        if ext["revert_to_avg"] < -0.5:
            reasons.append(f"Mean revert ke avg butuh ETH turun *{abs(ext['revert_to_avg']):.2f}%* vs BTC")

        if pct_r >= 90:   timing = "🟢 Timing sangat baik — ratio di zona ekstrem, revert probability tinggi"
        elif pct_r >= 75: timing = "🟡 Timing baik — ratio elevated, tapi belum di puncak ekstrem"
        elif pct_r >= 60: timing = "🟠 Timing cukup — ratio di atas avg, bisa naik lebih dulu sebelum revert"
        else:             timing = "🔴 Timing kurang — ratio belum cukup tinggi untuk S1 yang optimal"

        if pct_r >= 90:   risk = "⚠️ *Risk:* Ratio bisa terus naik sebelum revert (trend ETH bullish bisa override)"
        elif pct_r >= 75: risk = f"⚠️ *Risk:* Kalau ratio tembus {hi_r:.5f} (high), gap bisa melebar lebih jauh"
        else:             risk = "⚠️ *Risk:* Ratio belum di zona optimal S1 — conviction rendah"

        entry_note = (
            f"_💡 Mentor rule: ratio ≥75th pct = konfirmasi tambahan untuk S1_\n"
            f"_Sekarang {pct_r}th → {'✅ terpenuhi' if pct_r >= 75 else '❌ belum'}_"
        )
        reason_block = "\n".join(f"│ ✅ {r}" for r in reasons) if reasons else "│ Belum ada sinyal kuat"

        return (
            f"*{stars} {label}*\n"
            f"┌─────────────────────\n"
            f"│ Percentile:  *{pct_r}th* dari {window}d history\n"
            f"│ Dari high:   {ext['pct_from_high']:+.2f}% ({abs(ext['pct_from_high']):.2f}% di bawah puncak)\n"
            f"│ Dari avg:    revert *{ext['revert_to_avg']:+.2f}%* ke {avg_r:.5f}\n"
            f"│ Z-score:     {z_str}\n"
            f"│ Pos range:   {ext['pos_in_range']:.0f}% (0=low, 100=high)\n"
            f"├─────────────────────\n"
            f"{reason_block}\n"
            f"├─────────────────────\n"
            f"│ {timing}\n"
            f"└─────────────────────\n"
            f"{risk}\n"
            f"{entry_note}"
        )

    else:
        label = "S2 — Long ETH / Short BTC"
        reasons = []
        if pct_r <= 25:
            reasons.append(f"Ratio *{pct_r}th percentile* — ETH murah secara historis ({window}d)")
        if ext["pct_from_low"] <= 3.0:
            reasons.append(f"Ratio *{ext['pct_from_low']:.2f}%* dari {window}d low — mendekati dasar")
        if z is not None and z <= -1.0:
            reasons.append(f"Z-score *{z:+.2f}σ* — ETH secara statistik murah vs BTC")
        if ext["revert_to_avg"] > 0.5:
            reasons.append(f"Mean revert ke avg butuh ETH naik *{ext['revert_to_avg']:.2f}%* vs BTC")

        if pct_r <= 10:   timing = "🟢 Timing sangat baik — ratio di zona ekstrem bawah, bounce probability tinggi"
        elif pct_r <= 25: timing = "🟡 Timing baik — ratio depressed, tapi belum di dasar ekstrem"
        elif pct_r <= 40: timing = "🟠 Timing cukup — ratio di bawah avg, bisa turun lebih dulu sebelum bounce"
        else:             timing = "🔴 Timing kurang — ratio belum cukup rendah untuk S2 yang optimal"

        if pct_r <= 10:   risk = "⚠️ *Risk:* Ratio bisa terus turun (ETH bisa terus underperform BTC)"
        elif pct_r <= 25: risk = f"⚠️ *Risk:* Kalau ratio tembus {lo_r:.5f} (low), gap bisa melebar lebih jauh"
        else:             risk = "⚠️ *Risk:* Ratio belum di zona optimal S2 — conviction rendah"

        entry_note = (
            f"_💡 Mentor rule: ratio ≤25th pct = konfirmasi tambahan untuk S2_\n"
            f"_Sekarang {pct_r}th → {'✅ terpenuhi' if pct_r <= 25 else '❌ belum'}_"
        )
        reason_block = "\n".join(f"│ ✅ {r}" for r in reasons) if reasons else "│ Belum ada sinyal kuat untuk S2"

        return (
            f"*{stars} {label}*\n"
            f"┌─────────────────────\n"
            f"│ Percentile:  *{pct_r}th* dari {window}d history\n"
            f"│ Dari low:    +{ext['pct_from_low']:.2f}% ({ext['pct_from_low']:.2f}% di atas dasar)\n"
            f"│ Dari avg:    revert *{ext['revert_to_avg']:+.2f}%* ke {avg_r:.5f}\n"
            f"│ Z-score:     {z_str}\n"
            f"│ Pos range:   {ext['pos_in_range']:.0f}% (0=low, 100=high)\n"
            f"├─────────────────────\n"
            f"{reason_block}\n"
            f"├─────────────────────\n"
            f"│ {timing}\n"
            f"└─────────────────────\n"
            f"{risk}\n"
            f"{entry_note}"
        )


def calc_sizing(
    btc_price: Decimal,
    eth_price: Decimal,
) -> Tuple[float, float, float, float]:
    """
    Asymmetric sizing berdasarkan eth_size_ratio.
    Returns: (eth_alloc, btc_alloc, eth_qty, btc_qty)
    eth_size_ratio = 50 → dollar-neutral (50/50)
    eth_size_ratio = 60 → ETH 60% / BTC 40%
    """
    capital   = settings["capital"]
    eth_ratio = max(1.0, min(99.0, float(settings["eth_size_ratio"]))) / 100.0
    btc_ratio = 1.0 - eth_ratio
    if capital <= 0 or float(btc_price) <= 0 or float(eth_price) <= 0:
        return 0.0, 0.0, 0.0, 0.0
    eth_alloc = capital * eth_ratio
    btc_alloc = capital * btc_ratio
    eth_qty   = eth_alloc / float(eth_price)
    btc_qty   = btc_alloc / float(btc_price)
    return eth_alloc, btc_alloc, eth_qty, btc_qty


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

def sim_open_position(strategy: Strategy, btc_price: float, eth_price: float) -> str:
    """
    Otomatis open posisi simulasi saat entry signal.
    Returns: pesan ringkas untuk dilampirkan ke entry alert.
    """
    if not settings["sim_enabled"]:
        return ""
    if sim_trade["active"]:
        return "_⚠️ Sim: posisi sudah aktif, skip open~_\n"

    margin  = float(settings["sim_margin_usd"])
    lev     = float(settings["sim_leverage"])
    fee_pct = float(settings["sim_fee_pct"]) / 100.0

    notional   = margin * lev
    eth_ratio  = float(settings["eth_size_ratio"]) / 100.0
    btc_ratio  = 1.0 - eth_ratio

    eth_notional = notional * eth_ratio
    btc_notional = notional * btc_ratio
    eth_margin_  = eth_notional / lev
    btc_margin_  = btc_notional / lev

    eth_qty_abs = eth_notional / eth_price
    btc_qty_abs = btc_notional / btc_price

    # S1: Long BTC (+), Short ETH (-)
    # S2: Long ETH (+), Short BTC (-)
    if strategy == Strategy.S1:
        eth_qty = -eth_qty_abs
        btc_qty = +btc_qty_abs
    else:
        eth_qty = +eth_qty_abs
        btc_qty = -btc_qty_abs

    fee_open = (eth_notional + btc_notional) * fee_pct

    sim_trade.update({
        "active":       True,
        "strategy":     strategy.value,
        "eth_entry":    eth_price,
        "btc_entry":    btc_price,
        "eth_qty":      eth_qty,
        "btc_qty":      btc_qty,
        "eth_notional": eth_notional,
        "btc_notional": btc_notional,
        "eth_margin":   eth_margin_,
        "btc_margin":   btc_margin_,
        "fee_open":     fee_open,
        "opened_at":    datetime.now(timezone.utc).isoformat(),
    })

    # Sync ke pos_data agar /health langsung bisa dipakai
    pos_data.update({
        "eth_entry_price":  eth_price,
        "eth_qty":          eth_qty,
        "eth_notional_usd": eth_notional,
        "eth_leverage":     lev,
        "eth_liq_price":    None,
        "eth_funding_rate": None,
        "btc_entry_price":  btc_price,
        "btc_qty":          btc_qty,
        "btc_notional_usd": btc_notional,
        "btc_leverage":     lev,
        "btc_liq_price":    None,
        "btc_funding_rate": None,
        "strategy":         strategy.value,
        "set_at":           sim_trade["opened_at"],
    })
    save_pos_data()

    eth_dir = "Long 📈" if eth_qty > 0 else "Short 📉"
    btc_dir = "Long 📈" if btc_qty > 0 else "Short 📉"
    er = settings["eth_size_ratio"]; br = 100 - er
    logger.info(f"SIM OPEN {strategy.value}: ETH {eth_dir} {eth_qty:.4f}@{eth_price} | "
                f"BTC {btc_dir} {btc_qty:.6f}@{btc_price} | fee ${fee_open:.3f}")
    return (
        f"\n"
        f"🤖 *[SIM] Posisi Dibuka Otomatis*\n"
        f"┌─────────────────────\n"
        f"│ ETH: *{eth_dir}* {abs(eth_qty):.4f} @ ${eth_price:,.2f}\n"
        f"│      Notional: ${eth_notional:,.2f} | Margin: ${eth_margin_:,.2f}\n"
        f"│ BTC: *{btc_dir}* {abs(btc_qty):.6f} @ ${btc_price:,.2f}\n"
        f"│      Notional: ${btc_notional:,.2f} | Margin: ${btc_margin_:,.2f}\n"
        f"│ Leverage: {lev:.0f}x | Ratio: {er:.0f}/{br:.0f}\n"
        f"│ Fee open: ${fee_open:.3f}\n"
        f"└─────────────────────\n"
        f"_Ketik `/health` untuk memantau P&L secara live._\n"
    )


def sim_close_position(btc_price: float, eth_price: float, reason: str = "EXIT") -> str:
    """
    Otomatis close posisi simulasi saat exit/invalidasi signal.
    Returns: recap PnL string untuk dilampirkan ke exit alert.
    """
    if not settings["sim_enabled"] or not sim_trade["active"]:
        return ""

    eth_qty      = sim_trade["eth_qty"]
    btc_qty      = sim_trade["btc_qty"]
    eth_entry    = sim_trade["eth_entry"]
    btc_entry    = sim_trade["btc_entry"]
    eth_notional = sim_trade["eth_notional"]
    btc_notional = sim_trade["btc_notional"]
    eth_margin   = sim_trade["eth_margin"]
    btc_margin   = sim_trade["btc_margin"]
    fee_pct      = float(settings["sim_fee_pct"]) / 100.0
    fee_open     = sim_trade["fee_open"]
    fee_close    = (eth_notional + btc_notional) * fee_pct
    total_fee    = fee_open + fee_close
    total_margin = eth_margin + btc_margin

    # PnL per leg
    eth_pnl = eth_qty * (eth_price - eth_entry)
    btc_pnl = btc_qty * (btc_price - btc_entry)
    gross_pnl = eth_pnl + btc_pnl
    net_pnl   = gross_pnl - total_fee
    net_pct   = net_pnl / total_margin * 100

    # Duration
    opened_at = sim_trade.get("opened_at")
    dur_str   = "N/A"
    if opened_at:
        try:
            sa      = datetime.fromisoformat(opened_at)
            dur_min = int((datetime.now(timezone.utc) - sa).total_seconds() / 60)
            h, m    = divmod(dur_min, 60)
            dur_str = f"{h}h {m}m" if h > 0 else f"{m}m"
        except Exception:
            pass

    result_e = "🟢" if net_pnl >= 0 else "🔴"
    result_s = "PROFIT" if net_pnl >= 0 else "LOSS"
    sign     = "+" if net_pnl >= 0 else ""

    # Simpan ke history
    sim_trade["history"].append({
        "strategy":   sim_trade["strategy"],
        "reason":     reason,
        "eth_entry":  eth_entry, "eth_exit": eth_price,
        "btc_entry":  btc_entry, "btc_exit": btc_price,
        "gross_pnl":  gross_pnl, "fee": total_fee,
        "net_pnl":    net_pnl,   "net_pct": net_pct,
        "duration":   dur_str,
        "closed_at":  datetime.now(timezone.utc).isoformat(),
    })

    # Reset sim state
    sim_trade.update({
        "active": False, "strategy": None,
        "eth_entry": None, "btc_entry": None,
        "eth_qty": None, "btc_qty": None,
        "eth_notional": None, "btc_notional": None,
        "eth_margin": None, "btc_margin": None,
        "fee_open": None, "opened_at": None,
    })
    # Bersihkan pos_data
    for k in ["eth_entry_price","eth_qty","eth_notional_usd","eth_leverage","eth_liq_price",
              "btc_entry_price","btc_qty","btc_notional_usd","btc_leverage","btc_liq_price","strategy","set_at"]:
        pos_data[k] = None
    save_pos_data()

    logger.info(f"SIM CLOSE {reason}: net P&L ${net_pnl:.2f} ({net_pct:.2f}%) | fee ${total_fee:.3f}")
    return (
        f"\n"
        f"🤖 *[SIM] Posisi Ditutup — {result_e} {result_s}*\n"
        f"┌─────────────────────\n"
        f"│ ETH: {eth_entry:,.2f} → {eth_price:,.2f} "
        f"({'▲' if eth_price>eth_entry else '▼'}{abs(eth_price-eth_entry)/eth_entry*100:.2f}%)\n"
        f"│ BTC: {btc_entry:,.2f} → {btc_price:,.2f} "
        f"({'▲' if btc_price>btc_entry else '▼'}{abs(btc_price-btc_entry)/btc_entry*100:.2f}%)\n"
        f"├─────────────────────\n"
        f"│ Gross PnL:  {'+' if gross_pnl>=0 else ''}${gross_pnl:.2f}\n"
        f"│ Fee total:  -${total_fee:.3f}\n"
        f"│ *Net PnL:   {sign}${net_pnl:.2f} ({sign}{net_pct:.2f}%)*\n"
        f"│ Modal:      ${total_margin:.2f} | Durasi: {dur_str}\n"
        f"└─────────────────────\n"
        f"Total trade: {len(sim_trade['history'])} | "
        f"Ketik `/simstats` untuk rekap semua\n"
    )


def calc_gap_velocity() -> dict:
    """
    Analisis kecepatan dan arah gap dari gap_history.
    Returns dict dengan velocity metrics, atau empty dict kalau data kurang.
    """
    if len(gap_history) < 3:
        return {}
    try:
        now     = datetime.now(timezone.utc)
        # Ambil window berbeda untuk short/medium term
        def _slice(minutes: int):
            cutoff = now - timedelta(minutes=minutes)
            pts    = [(ts, g) for ts, g in gap_history if ts >= cutoff]
            return pts

        pts_15  = _slice(15)
        pts_30  = _slice(30)
        pts_60  = _slice(60)

        def _delta(pts):
            if len(pts) < 2:
                return None
            return pts[-1][1] - pts[0][1]   # gap change over window

        def _velocity(pts):
            """Gap change per menit"""
            if len(pts) < 2:
                return None
            dt = (pts[-1][0] - pts[0][0]).total_seconds() / 60
            if dt <= 0:
                return None
            return (pts[-1][1] - pts[0][1]) / dt

        d15 = _delta(pts_15)
        d30 = _delta(pts_30)
        d60 = _delta(pts_60)
        v15 = _velocity(pts_15)   # gap/menit terbaru (15m window)
        v60 = _velocity(pts_60)

        curr_gap = gap_history[-1][1]

        # Trend: apakah gap bergerak menuju TP atau menjauhinya?
        # S1 TP = gap mengecil (positif ke nol), S2 TP = gap mengecil (negatif ke nol)
        # Konverging = abs(gap) mengecil
        abs_delta_15 = None
        if d15 is not None:
            abs_delta_15 = abs(gap_history[-1][1]) - abs(gap_history[-len(pts_15)][1]) if pts_15 else None

        # Apakah gap accelerating atau decelerating menuju TP?
        accel = None
        if v15 is not None and v60 is not None and v60 != 0:
            accel = v15 / v60    # > 1 = accelerating, < 1 = decelerating

        # ETA ke TP (entry_threshold sebagai target konvergensi)
        eta_minutes = None
        et  = settings["exit_threshold"]
        if v15 is not None and v15 != 0:
            # Berapa jauh lagi abs(gap) perlu berubah menuju TP
            gap_to_tp = abs(curr_gap) - et
            if gap_to_tp > 0:
                # Kecepatan konvergensi = -abs(gap) per menit
                conv_rate = -abs(v15) if v15 * curr_gap > 0 else abs(v15)
                if conv_rate != 0:
                    eta_minutes = gap_to_tp / abs(conv_rate)

        return {
            "curr_gap": curr_gap,
            "delta_15m": d15,
            "delta_30m": d30,
            "delta_60m": d60,
            "vel_15m":   v15,
            "vel_60m":   v60,
            "accel":     accel,
            "eta_min":   eta_minutes,
            "n_pts":     len(gap_history),
        }
    except Exception as e:
        logger.warning(f"calc_gap_velocity error: {e}")
        return {}


def calc_position_pnl() -> dict:
    """
    Hitung P&L lengkap: unrealized, funding cost, net after funding,
    break-even timer, time-in-trade, liq distance.
    Returns empty dict jika data tidak lengkap.
    """
    if pos_data["eth_entry_price"] is None or pos_data["btc_entry_price"] is None:
        return {}
    btc_now = scan_stats.get("last_btc_price")
    eth_now = scan_stats.get("last_eth_price")
    if btc_now is None or eth_now is None:
        return {}
    try:
        eth_entry  = pos_data["eth_entry_price"]
        eth_qty    = pos_data["eth_qty"]
        eth_lev    = pos_data["eth_leverage"] or 1.0
        eth_fr     = pos_data.get("eth_funding_rate") or 0.0   # % per 8h
        btc_entry  = pos_data["btc_entry_price"]
        btc_qty    = pos_data["btc_qty"]
        btc_lev    = pos_data["btc_leverage"] or 1.0
        btc_fr     = pos_data.get("btc_funding_rate") or 0.0
        eth_p      = float(eth_now)
        btc_p      = float(btc_now)

        # ── Notional & Margin ─────────────────────────────────────────────────
        # Gunakan notional_usd manual kalau ada (lebih akurat dari exchange)
        eth_notional   = pos_data.get("eth_notional_usd") or (abs(eth_qty) * eth_entry)
        btc_notional   = pos_data.get("btc_notional_usd") or (abs(btc_qty) * btc_entry)
        eth_margin     = eth_notional / eth_lev
        btc_margin     = btc_notional / btc_lev
        total_margin   = eth_margin + btc_margin
        total_notional = eth_notional + btc_notional

        # ── Nilai sekarang ───────────────────────────────────────────────────
        eth_value_now  = abs(eth_qty) * eth_p
        btc_value_now  = abs(btc_qty) * btc_p

        # ── Unrealized PnL ───────────────────────────────────────────────────
        eth_pnl = eth_qty * (eth_p - eth_entry)
        btc_pnl = btc_qty * (btc_p - btc_entry)
        net_pnl = eth_pnl + btc_pnl

        eth_pnl_pct = eth_pnl / eth_margin * 100 if eth_margin > 0 else 0
        btc_pnl_pct = btc_pnl / btc_margin * 100 if btc_margin > 0 else 0
        net_pnl_pct = net_pnl / total_margin * 100 if total_margin > 0 else 0

        # ── Time-in-trade ────────────────────────────────────────────────────
        set_at_str  = pos_data.get("set_at")
        time_in_min = None
        time_label  = "N/A"
        if set_at_str:
            try:
                sa          = datetime.fromisoformat(set_at_str)
                time_in_min = (datetime.now(timezone.utc) - sa).total_seconds() / 60
                h_part      = int(time_in_min // 60)
                m_part      = int(time_in_min % 60)
                time_label  = f"{h_part}h {m_part}m" if h_part > 0 else f"{m_part}m"
            except Exception:
                pass

        # ── Funding Cost ─────────────────────────────────────────────────────
        # Funding rate: positif = kamu bayar (long bayar short)
        # ETH leg: kalau long dan fr positif → kamu bayar; kalau short dan fr positif → kamu terima
        # BTC leg: kebalikannya
        def _funding_flow(qty: float, notional: float, fr: float) -> float:
            """Negatif = kamu bayar, positif = kamu terima"""
            direction = 1.0 if qty > 0 else -1.0
            return -direction * notional * (fr / 100)   # per 8h dalam USD

        eth_funding_per_8h = _funding_flow(eth_qty, eth_notional, eth_fr)
        btc_funding_per_8h = _funding_flow(btc_qty, btc_notional, btc_fr)
        net_funding_per_8h = eth_funding_per_8h + btc_funding_per_8h
        net_funding_per_day = net_funding_per_8h * 3

        # Total funding sudah dibayar/diterima berdasarkan time-in-trade
        total_funding_paid = 0.0
        if time_in_min is not None:
            periods_8h         = time_in_min / 480   # 480 menit = 8 jam
            total_funding_paid = net_funding_per_8h * periods_8h

        # Net PnL setelah funding
        net_pnl_after_funding     = net_pnl + total_funding_paid
        net_pnl_af_pct            = net_pnl_after_funding / total_margin * 100 if total_margin > 0 else 0

        # ── Break-even Timer ─────────────────────────────────────────────────
        # Berapa jam lagi funding akan habiskan profit yang ada?
        breakeven_hours = None
        if net_pnl > 0 and net_funding_per_8h < 0:
            # Kamu bayar funding, profit ada — kapan funding = profit?
            breakeven_hours = (net_pnl / abs(net_funding_per_8h)) * 8
        elif net_pnl < 0 and net_funding_per_8h > 0:
            # Kamu terima funding, sedang rugi — kapan funding tutup kerugian?
            breakeven_hours = abs(net_pnl) / abs(net_funding_per_8h) * 8

        # ── Margin Health ────────────────────────────────────────────────────
        total_equity = total_margin + net_pnl
        margin_ratio = total_equity / total_notional * 100 if total_notional > 0 else 0
        maint_margin    = total_notional * 0.005
        liq_buffer_usd  = total_equity - maint_margin
        liq_buffer_pct  = liq_buffer_usd / total_equity * 100 if total_equity > 0 else 0

        # ── Liq Prices ───────────────────────────────────────────────────────
        eth_liq = pos_data["eth_liq_price"]
        btc_liq = pos_data["btc_liq_price"]
        if eth_liq is None and eth_qty != 0:
            eth_liq = eth_entry - (eth_margin / eth_qty)
        if btc_liq is None and btc_qty != 0:
            btc_liq = btc_entry - (btc_margin / btc_qty)

        eth_dist_liq = abs(eth_p - eth_liq) / eth_p * 100 if eth_liq else None
        btc_dist_liq = abs(btc_p - btc_liq) / btc_p * 100 if btc_liq else None
        eth_danger   = eth_dist_liq is not None and eth_dist_liq < 10
        btc_danger   = btc_dist_liq is not None and btc_dist_liq < 10

        # ── Health Label ─────────────────────────────────────────────────────
        if margin_ratio >= 10:   health_e, health_label = "🟢", "SEHAT"
        elif margin_ratio >= 5:  health_e, health_label = "🟡", "PERHATIKAN"
        elif margin_ratio >= 3:  health_e, health_label = "🟠", "WASPADA"
        else:                    health_e, health_label = "🔴", "BAHAYA — Dekat Liquidasi!"

        return {
            # ETH leg
            "eth_pnl": eth_pnl, "eth_pnl_pct": eth_pnl_pct,
            "eth_notional": eth_notional, "eth_margin": eth_margin,
            "eth_value_now": eth_value_now,
            "eth_lev": eth_lev, "eth_liq_est": eth_liq,
            "eth_dist_liq": eth_dist_liq, "eth_danger": eth_danger,
            "eth_funding_per_8h": eth_funding_per_8h,
            # BTC leg
            "btc_pnl": btc_pnl, "btc_pnl_pct": btc_pnl_pct,
            "btc_notional": btc_notional, "btc_margin": btc_margin,
            "btc_value_now": btc_value_now,
            "btc_lev": btc_lev, "btc_liq_est": btc_liq,
            "btc_dist_liq": btc_dist_liq, "btc_danger": btc_danger,
            "btc_funding_per_8h": btc_funding_per_8h,
            # Net
            "net_pnl": net_pnl, "net_pnl_pct": net_pnl_pct,
            "net_pnl_after_funding": net_pnl_after_funding,
            "net_pnl_af_pct": net_pnl_af_pct,
            "total_margin": total_margin, "total_notional": total_notional,
            "total_equity": total_equity, "margin_ratio": margin_ratio,
            "liq_buffer_usd": liq_buffer_usd, "liq_buffer_pct": liq_buffer_pct,
            # Funding
            "net_funding_per_8h": net_funding_per_8h,
            "net_funding_per_day": net_funding_per_day,
            "total_funding_paid": total_funding_paid,
            "breakeven_hours": breakeven_hours,
            # Time
            "time_in_min": time_in_min,
            "time_label": time_label,
            # Health
            "health_emoji": health_e, "health_label": health_label,
        }
    except Exception as e:
        logger.warning(f"calc_position_pnl error: {e}")
        return {}


def build_position_health_message(h: dict) -> str:
    strat   = pos_data.get("strategy") or "?"
    eth_p   = float(scan_stats["last_eth_price"]) if scan_stats.get("last_eth_price") else 0
    btc_p   = float(scan_stats["last_btc_price"]) if scan_stats.get("last_btc_price") else 0
    eth_qty = pos_data["eth_qty"]
    btc_qty = pos_data["btc_qty"]
    eth_dir = "Long 📈" if eth_qty and eth_qty > 0 else "Short 📉"
    btc_dir = "Long 📈" if btc_qty and btc_qty > 0 else "Short 📉"

    def _s(v):  return "+" if v >= 0 else ""
    def _e(v):  return "🟢" if v >= 0 else "🔴"
    def _fe(v): return "🟢" if v >= 0 else "🔴"   # funding: pos = receive

    # ── Liq strings ──────────────────────────────────────────────────────────
    eth_liq_s  = f"${h['eth_liq_est']:,.2f}" if h.get("eth_liq_est") else "N/A"
    btc_liq_s  = f"${h['btc_liq_est']:,.2f}" if h.get("btc_liq_est") else "N/A"
    eth_dist_s = f"{h['eth_dist_liq']:.1f}% jauh" if h.get("eth_dist_liq") else "N/A"
    btc_dist_s = f"{h['btc_dist_liq']:.1f}% jauh" if h.get("btc_dist_liq") else "N/A"
    eth_liq_e  = "⚠️" if h.get("eth_danger") else "✅"
    btc_liq_e  = "⚠️" if h.get("btc_danger") else "✅"

    # ── Value size (USD sekarang) ─────────────────────────────────────────────
    eth_val_s = f"${h['eth_value_now']:,.2f}" if h.get("eth_value_now") else "N/A"
    btc_val_s = f"${h['btc_value_now']:,.2f}" if h.get("btc_value_now") else "N/A"

    # ── Funding strings ───────────────────────────────────────────────────────
    eth_fr     = pos_data.get("eth_funding_rate") or 0.0
    btc_fr     = pos_data.get("btc_funding_rate") or 0.0
    has_funding = eth_fr != 0.0 or btc_fr != 0.0
    eth_f8h    = h.get("eth_funding_per_8h", 0)
    btc_f8h    = h.get("btc_funding_per_8h", 0)
    net_f8h    = h.get("net_funding_per_8h", 0)
    net_fday   = h.get("net_funding_per_day", 0)
    total_fp   = h.get("total_funding_paid", 0)
    be_h       = h.get("breakeven_hours")

    funding_block = ""
    if has_funding:
        eth_f_dir = "terima 🟢" if eth_f8h >= 0 else "bayar 🔴"
        btc_f_dir = "terima 🟢" if btc_f8h >= 0 else "bayar 🔴"
        net_f_dir = "terima 🟢" if net_f8h >= 0 else "bayar 🔴"
        be_str    = f"{be_h:.1f}h" if be_h is not None else "N/A"
        be_label  = (
            "waktu tersisa sebelum funding habiskan profit" if h["net_pnl"] > 0 and net_f8h < 0
            else "waktu untuk funding tutup kerugian" if h["net_pnl"] < 0 and net_f8h > 0
            else "—"
        )
        funding_block = (
            f"\n*💸 Funding Cost:*\n"
            f"┌─────────────────────\n"
            f"│ ETH: {eth_fr:+.4f}%/8h → {_s(eth_f8h)}${abs(eth_f8h):.3f} ({eth_f_dir})\n"
            f"│ BTC: {btc_fr:+.4f}%/8h → {_s(btc_f8h)}${abs(btc_f8h):.3f} ({btc_f_dir})\n"
            f"│ Net: {_s(net_f8h)}${abs(net_f8h):.3f}/8h | {_s(net_fday)}${abs(net_fday):.2f}/hari ({net_f_dir})\n"
            f"│ Total dibayar: {_s(total_fp)}${abs(total_fp):.2f}\n"
            f"│ Net PnL after funding: {_e(h['net_pnl_after_funding'])} "
            f"{_s(h['net_pnl_after_funding'])}${h['net_pnl_after_funding']:,.2f} "
            f"({_s(h['net_pnl_af_pct'])}{h['net_pnl_af_pct']:.2f}%)\n"
            + (f"│ ⏱️ Break-even: *{be_str}* lagi ({be_label})\n" if be_h is not None else "")
            + f"└─────────────────────\n"
        )
    else:
        funding_block = "\n_💸 Funding: belum diset — gunakan `/setfunding`~_\n"

    # ── Gap Velocity ─────────────────────────────────────────────────────────
    vel  = calc_gap_velocity()
    vel_block = ""
    if vel:
        d15 = vel.get("delta_15m")
        d60 = vel.get("delta_60m")
        eta = vel.get("eta_min")
        curr_gap = vel.get("curr_gap", 0)

        if d15 is not None:
            conv   = abs(curr_gap) > 0 and abs(curr_gap + d15) < abs(curr_gap)
            d15_e  = "⬆️ melebar" if not conv else "⬇️ konvergen"
            d15_s  = f"{d15:+.3f}%"
        else:
            d15_e, d15_s = "—", "N/A"

        if d60 is not None:
            d60_s = f"{d60:+.3f}%"
        else:
            d60_s = "N/A"

        accel = vel.get("accel")
        if accel is not None:
            accel_s = "📈 accelerating" if accel > 1.2 else ("📉 decelerating" if accel < 0.8 else "➡️ steady")
        else:
            accel_s = "N/A"

        eta_s = f"{int(eta)}m ({eta/60:.1f}h)" if eta is not None and eta < 10000 else "tidak bisa hitung"

        vel_block = (
            f"\n*📡 Gap Velocity:*\n"
            f"┌─────────────────────\n"
            f"│ Gap sekarang: {curr_gap:+.3f}%\n"
            f"│ Δ 15m: {d15_s} {d15_e}\n"
            f"│ Δ 60m: {d60_s}\n"
            f"│ Trend: {accel_s}\n"
            f"│ ETA ke TP: ~{eta_s}\n"
            f"│ Data: {vel['n_pts']} pts\n"
            f"└─────────────────────\n"
        )

    # ── Margin bars ───────────────────────────────────────────────────────────
    mr      = h["margin_ratio"]
    mr_fill = min(10, int(mr / 2))
    mr_bar  = "█" * mr_fill + "░" * (10 - mr_fill)
    lb_pct  = max(0.0, h["liq_buffer_pct"])
    lb_fill = min(10, int(lb_pct / 10))
    lb_bar  = "█" * lb_fill + "░" * (10 - lb_fill)

    danger_note = ""
    if h.get("eth_danger") or h.get("btc_danger"):
        legs = []
        if h.get("eth_danger"): legs.append("ETH")
        if h.get("btc_danger"): legs.append("BTC")
        danger_note = f"\n🚨 *PERINGATAN: {'/'.join(legs)} mendekati liq price!*\n"

    return (
        f"🏥 *Position Health — {strat}*\n"
        f"⏱️ Time in trade: *{h['time_label']}*\n"
        f"💰 ETH: ${eth_p:,.2f} | BTC: ${btc_p:,.2f}\n"
        f"\n"
        f"*📊 ETH Leg ({eth_dir}):*\n"
        f"┌─────────────────────\n"
        f"│ Entry:    ${pos_data['eth_entry_price']:,.2f}\n"
        f"│ Qty:      {abs(eth_qty):.4f} ETH\n"
        f"│ Leverage: {h['eth_lev']:.0f}x\n"
        f"│ Notional: ${h['eth_notional']:,.2f} → value now: {eth_val_s}\n"
        f"│ Margin:   ${h['eth_margin']:,.2f}\n"
        f"│ UPnL:     {_e(h['eth_pnl'])} {_s(h['eth_pnl'])}${h['eth_pnl']:,.2f} ({_s(h['eth_pnl_pct'])}{h['eth_pnl_pct']:.2f}%)\n"
        f"│ Liq:      {eth_liq_s} {eth_liq_e} | {eth_dist_s}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*📊 BTC Leg ({btc_dir}):*\n"
        f"┌─────────────────────\n"
        f"│ Entry:    ${pos_data['btc_entry_price']:,.2f}\n"
        f"│ Qty:      {abs(btc_qty):.6f} BTC\n"
        f"│ Leverage: {h['btc_lev']:.0f}x\n"
        f"│ Notional: ${h['btc_notional']:,.2f} → value now: {btc_val_s}\n"
        f"│ Margin:   ${h['btc_margin']:,.2f}\n"
        f"│ UPnL:     {_e(h['btc_pnl'])} {_s(h['btc_pnl'])}${h['btc_pnl']:,.2f} ({_s(h['btc_pnl_pct'])}{h['btc_pnl_pct']:.2f}%)\n"
        f"│ Liq:      {btc_liq_s} {btc_liq_e} | {btc_dist_s}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*⚖️ Net Pairs:*\n"
        f"┌─────────────────────\n"
        f"│ Notional:  ${h['total_notional']:,.2f}\n"
        f"│ Margin:    ${h['total_margin']:,.2f}\n"
        f"│ Equity:    ${h['total_equity']:,.2f}\n"
        f"│ Net UPnL:  {_e(h['net_pnl'])} {_s(h['net_pnl'])}${h['net_pnl']:,.2f} ({_s(h['net_pnl_pct'])}{h['net_pnl_pct']:.2f}%)\n"
        f"└─────────────────────\n"
        f"{funding_block}"
        f"{vel_block}"
        f"*🛡️ Margin Health:*\n"
        f"┌─────────────────────\n"
        f"│ Margin Ratio: {mr:.2f}%\n"
        f"│ `{mr_bar}` {h['health_emoji']} *{h['health_label']}*\n"
        f"│ Liq Buffer: ${h['liq_buffer_usd']:,.2f} ({lb_pct:.1f}%)\n"
        f"│ `{lb_bar}` sebelum likuidasi\n"
        f"└─────────────────────\n"
        f"{danger_note}\n"
        f"_💡 Yang penting adalah net P&L. Contoh: ETH -68% tapi net +$239._"
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
        checks.append((None, "Ratio N/A", "Butuh lebih banyak history"))

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
        checks.append((None, "Driver belum tersedia", "Tunggu data scan"))

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
        checks.append((None, "Z-score N/A", "Data kurang"))

    # Hitung skor
    true_count  = sum(1 for c in checks if c[0] is True)
    false_count = sum(1 for c in checks if c[0] is False)
    total_valid = sum(1 for c in checks if c[0] is not None)
    score_pct   = true_count / total_valid * 100 if total_valid > 0 else 0

    # Verdict
    if false_count == 0 and true_count >= 4:
        v_e, verdict, v_d = "🟢", "READY TO ENTRY", "Semua faktor oke — kondisi optimal"
    elif not correct_side or false_count >= 2:
        v_e, verdict, v_d = "🔴", "JANGAN ENTRY DULU", "Terlalu banyak faktor tidak terpenuhi"
    elif false_count <= 1 and true_count >= 3:
        v_e, verdict, v_d = "🟡", "BISA ENTRY, TAPI HATI-HATI", "1 faktor lemah — pertimbangkan sizing lebih kecil"
    else:
        v_e, verdict, v_d = "🟠", "TUNGGU KONFIRMASI", "Beberapa faktor masih meragukan"

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

def check_regime_filter(strategy: Strategy) -> Tuple[bool, str]:
    """
    Cek apakah kondisi market mendukung strategi yang akan dientry.

    S1 (Long BTC / Short ETH):
      - Butuh BULLISH — market pump, ETH outperform BTC
      - Blokir kalau BEARISH atau Mixed dengan BTC dominan

    S2 (Short BTC / Long ETH):
      - Butuh BEARISH — market dump, ETH underperform BTC
      - Blokir kalau BULLISH atau Mixed dengan BTC dominan

    Returns: (allowed: bool, reason: str)
    """
    if not settings["regime_filter_enabled"]:
        return True, ""

    reg = detect_market_regime()
    regime   = reg["regime"]    # BULLISH / BEARISH / KONSOLIDASI
    strength = reg["strength"]  # Kuat / Moderat / Lemah
    emoji    = reg["emoji"]

    if strategy == Strategy.S1:
        # S1 = ETH outperform saat pump
        if regime == "BULLISH":
            return True, f"{emoji} {regime} {strength} — ✅ sesuai untuk S1"
        elif regime == "KONSOLIDASI":
            return True, f"{emoji} Sideways {strength} — ⚠️ S1 bisa, tapi lebih hati-hati"
        else:
            # BEARISH saat entry S1 = berbahaya
            reason = (
                f"{emoji} Market *{regime} {strength}* — "
                f"❌ S1 diblokir\n"
                f"Saat dump, Short ETH berisiko karena BTC bisa turun lebih dalam.\n"
                f"Tunggu market berbalik BULLISH sebelum S1."
            )
            return False, reason

    else:
        # S2 = ETH underperform saat dump
        if regime == "BEARISH":
            return True, f"{emoji} {regime} {strength} — ✅ sesuai untuk S2"
        elif regime == "KONSOLIDASI":
            return True, f"{emoji} Sideways {strength} — ⚠️ S2 bisa, tapi lebih hati-hati"
        else:
            # BULLISH saat entry S2 = berbahaya (ini yang kamu alami!)
            reason = (
                f"{emoji} Market *{regime} {strength}* — "
                f"❌ S2 diblokir\n"
                f"Saat pump, Long ETH berisiko karena BTC yang pump bukan ETH lemah.\n"
                f"Tunggu market berbalik BEARISH sebelum S2."
            )
            return False, reason


def build_regime_blocked_message(strategy: Strategy, gap: Decimal, reason: str) -> str:
    """Pesan alert saat entry diblokir karena regime filter."""
    gap_f = float(gap)
    reg   = detect_market_regime()
    btc_r = float(scan_stats.get("last_btc_ret") or 0)
    eth_r = float(scan_stats.get("last_eth_ret") or 0)
    return (
        f"⛔ *ENTRY DIBLOKIR — Regime Filter*\n"
        f"\n"
        f"Sinyal *{strategy.value}* muncul tapi kondisi market tidak mendukung\n"
        f"\n"
        f"Gap sekarang: *{gap_f:+.2f}%*\n"
        f"BTC: {btc_r:+.2f}% | ETH: {eth_r:+.2f}%\n"
        f"\n"
        f"*Alasan diblokir:*\n"
        f"{reason}\n"
        f"\n"
        f"_Sinyal akan muncul saat kondisi market sesuai._\n"
        f"_`/regimefilter off` untuk menonaktifkan filter ini._"
    )


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
        f"Potensi sinyal terdeteksi \n"
        f"\n"
        f"_{reason}_\n"
        f"Rencana: *{direction}*\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Menunggu konfirmasi. \n"
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

    # Dominance score (calculated early, used in regime section + conviction)
    dom      = calc_dominance_score()
    dom_status, dom_msg, dom_conv = check_dominance_alignment(strategy, dom)

    # Adjust conviction stars berdasarkan dominance alignment
    star_list   = ["", "⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"]
    current_lvl = len(stars)  # 1-5
    adj_lvl     = max(1, min(5, current_lvl + dom_conv))
    stars_adj   = star_list[adj_lvl]
    conv_adj_label = (
        f" _(+{dom_conv} dominance)_" if dom_conv > 0 else
        f" _({dom_conv} dominance)_"  if dom_conv < 0 else ""
    )

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

    # ── 5. Market Regime + Dominance ─────────────────────────────────────────
    reg = detect_market_regime()
    dom = calc_dominance_score()
    dom_status, dom_msg, dom_conv = check_dominance_alignment(strategy, dom)

    # Validasi kesesuaian regime dengan strategi
    if strategy == Strategy.S1:
        if reg["regime"] == "BULLISH" and reg["strength"] == "Kuat":
            regime_fit = "✅ Ideal — market pump kuat, ETH outperform BTC"
        elif reg["regime"] == "BULLISH":
            regime_fit = "✅ OK — market pump moderat"
        elif reg["regime"] == "KONSOLIDASI":
            regime_fit = "⚠️ Sideways — S1 bisa tapi gap revert lebih lambat"
        else:
            regime_fit = "⚠️ Market dump — S1 berisiko, BTC ikut turun"
    else:
        if reg["regime"] == "BEARISH" and reg["strength"] == "Kuat":
            regime_fit = "✅ Ideal — market dump kuat, ETH underperform BTC"
        elif reg["regime"] == "BEARISH":
            regime_fit = "✅ OK — market dump moderat"
        elif reg["regime"] == "KONSOLIDASI":
            regime_fit = "⚠️ Sideways — S2 bisa tapi gap revert lebih lambat"
        else:
            regime_fit = "⚠️ Market pump — S2 berisiko, ETH ikut naik"

    # Dominance tf breakdown (1h/4h/24h)
    def _dom_tf_line(key):
        t = dom["tf"].get(key)
        if not t: return ""
        arr = "🟡 ETH" if t["diff"] > 0.1 else ("🟠 BTC" if t["diff"] < -0.1 else "⚪ Seimbang")
        return f"│   {key}: BTC {t['btc']:+.2f}% | ETH {t['eth']:+.2f}% → {arr} ({t['diff']:+.2f}%)\n"

    dom_block = (
        f"│ Dominance: {dom['emoji']} *{dom['dominant']}* {dom.get('strength','')} (score {dom['score']:+.2f}%)\n"
        + _dom_tf_line("1h")
        + _dom_tf_line("4h")
        + _dom_tf_line("24h")
        + f"│ {dom_msg}\n"
    )

    regime_line = (
        f"│ Market:   {reg['emoji']} *{reg['regime']}* {reg['strength']}\n"
        f"│ {regime_fit}\n"
        f"{dom_block}"
    )

    # ── 6. Sizing ─────────────────────────────────────────────────────────────
    sizing_section = ""
    eth_ratio = settings["eth_size_ratio"]
    btc_ratio = 100.0 - eth_ratio
    ratio_tag = f"{eth_ratio:.0f}/{btc_ratio:.0f}" if eth_ratio != 50.0 else "50/50"
    eth_alloc, btc_alloc, eth_qty, btc_qty = calc_sizing(btc_now, eth_now)
    if eth_alloc > 0:
        if strategy == Strategy.S1:
            sizing_section = (
                f"\n"
                f"💰 *Sizing ({ratio_tag} ETH/BTC, ${settings['capital']:,.0f}):*\n"
                f"┌─────────────────────\n"
                f"│ Long BTC:  ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n"
                f"│ Short ETH: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
                f"└─────────────────────\n"
            )
        else:
            sizing_section = (
                f"\n"
                f"💰 *Sizing ({ratio_tag} ETH/BTC, ${settings['capital']:,.0f}):*\n"
                f"┌─────────────────────\n"
                f"│ Long ETH:  ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
                f"│ Short BTC: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n"
                f"└─────────────────────\n"
            )
    else:
        sizing_section = "\n_💡 `/capital <modal>` untuk sizing guide~_\n"

    peak_line     = f"│ Peak:     {peak:+.2f}%\n" if not is_direct and peak else ""
    direct_tag    = " _(Peak OFF)_\n" if is_direct else "\n"
    reversal_note = (
        f"_Gap berbalik {settings['peak_reversal']}% dari puncak → entry terkonfirmasi._\n"
        if not is_direct else ""
    )

    return (
        f"ara   ⚡\n"
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
        f"│ Conviction: {stars_adj}{conv_adj_label}\n"
        f"│ _{conviction}_\n"
        f"└─────────────────────\n"
        f"\n"
        f"*── 3. Market Regime ──*\n"
        f"┌─────────────────────\n"
        f"{regime_line}"
        f"└─────────────────────\n"
        f"\n"
        f"*── 4. Target & Proteksi ──*\n"
        f"┌─────────────────────\n"
        f"│ TP gap:   {tp_gap:+.2f}% _(exit threshold)_\n"
        f"│ ETH TP:   {eth_tp_str}\n"
        f"│ BTC ref:  {btc_ref_str}\n"
        f"│ Trail SL: {tsl_initial:+.2f}% → ETH {eth_tsl_str}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*── 5. Skenario Konvergensi ──*\n"
        f"• *A — {scen_a_label}:* {scen_a_str}\n"
        f"• *B — {scen_b_label}:* {scen_b_str}\n"
        f"\n"
        f"{sizing_section}"
        f"{reversal_note}"
        f"\n"
        f"Akeno sudah menunggu momen ini ⚡"
    )


def build_exit_message(
    btc_ret:      Decimal,
    eth_ret:      Decimal,
    gap:          Decimal,
    confirm_note: str = "",
) -> str:
    lb            = get_lookback_label()
    gap_f         = float(gap)
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_f) if active_strategy else (None, None, None)
    net_section   = _build_pnl_section(leg_e, leg_b, net)
    conf_line     = f"_{confirm_note}_\n" if confirm_note else ""
    return (
        f"\n"
        f"✅ *EXIT — Gap Konvergen!*\n"
        f"{conf_line}"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  *{format_value(gap)}%*\n"
        f"└─────────────────────\n"
        f"{net_section}"
        f"Posisi di-close. Bot kembali memantau. ⚡"
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
        f"🎯 Target Profit tercapai. ✨\n"
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
        f"🎯 Target tercapai. ⚡"
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
        f"TSL kena. Profit diamankan\n"
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
        f"Cut dulu~ Kembali scan. ⚡"
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
        f"gap malah melebar. Cut dulu\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"{net_section}"
        f"Bot kembali ke mode SCAN. ⚡"
    )


def build_peak_cancelled_message(strategy: Strategy, gap: Decimal) -> str:
    return (
        f"………\n"
        f"❌ *Peak Watch Dibatalkan: {strategy.value}*\n"
        f"\n"
        f"Gap mundur sebelum konfirmasi.\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Bot terus memantau."
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

    last_r = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum"
    return (
        f"💓 *Heartbeat — Bot Aktif*\n"
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
    global exit_confirm_count

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
    exit_confirm_count = 0


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
        # Simulation: auto-open posisi
        sim_msg = sim_open_position(strategy, float(btc_now), float(eth_now))
        if sim_msg:
            send_alert(sim_msg)
        scan_stats["signals_sent"] += 1

    # ── SCAN ──────────────────────────────────────────────────────────────────
    if current_mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            allowed, reason = check_regime_filter(Strategy.S1)
            if not allowed:
                send_alert(build_regime_blocked_message(Strategy.S1, gap, reason))
                logger.info(f"S1 BLOCKED by regime filter. Gap: {gap_float:.2f}%")
            elif peak_enabled:
                current_mode  = Mode.PEAK_WATCH
                peak_strategy = Strategy.S1
                peak_gap      = gap_float
                send_alert(build_peak_watch_message(Strategy.S1, gap))
                logger.info(f"PEAK WATCH S1. Gap: {gap_float:.2f}%")
            else:
                do_entry(Strategy.S1, is_direct=True)
                logger.info(f"DIRECT ENTRY S1. Gap: {gap_float:.2f}%")

        elif gap_float <= -entry_thresh:
            allowed, reason = check_regime_filter(Strategy.S2)
            if not allowed:
                send_alert(build_regime_blocked_message(Strategy.S2, gap, reason))
                logger.info(f"S2 BLOCKED by regime filter. Gap: {gap_float:.2f}%")
            elif peak_enabled:
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
                allowed, reason = check_regime_filter(Strategy.S1)
                if not allowed:
                    send_alert(build_regime_blocked_message(Strategy.S1, gap, reason))
                    current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
                    logger.info(f"S1 BLOCKED by regime filter at peak entry. Gap: {gap_float:.2f}%")
                else:
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
                allowed, reason = check_regime_filter(Strategy.S2)
                if not allowed:
                    send_alert(build_regime_blocked_message(Strategy.S2, gap, reason))
                    current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
                    logger.info(f"S2 BLOCKED by regime filter at peak entry. Gap: {gap_float:.2f}%")
                else:
                    do_entry(Strategy.S2, is_direct=False)
                    logger.info(f"ENTRY S2. Peak: {peak_gap:.2f}% Entry: {gap_float:.2f}%")
            else:
                logger.info(f"S2 peak watch: {gap_float:.2f}% | Peak {peak_gap:.2f}% | Need {peak_reversal}% rise")

    # ── TRACK ─────────────────────────────────────────────────────────────────
    elif current_mode == Mode.TRACK:
        if check_sltp(gap_float, btc_ret, eth_ret, gap):
            return

        # Ambil setting konfirmasi
        confirm_scans  = int(settings["exit_confirm_scans"])
        confirm_buffer = float(settings["exit_confirm_buffer"])
        pnl_gate       = float(settings["exit_pnl_gate"])

        # Hitung effective exit level (threshold + buffer ekstra)
        # S1: exit kalau gap ≤ exit_thresh - buffer (lebih dalam ke positif)
        # S2: exit kalau gap ≥ -exit_thresh + buffer (lebih dalam ke negatif)
        s1_exit_level = exit_thresh - confirm_buffer
        s2_exit_level = -exit_thresh + confirm_buffer

        in_exit_zone = (
            (active_strategy == Strategy.S1 and gap_float <= s1_exit_level) or
            (active_strategy == Strategy.S2 and gap_float >= s2_exit_level)
        )

        if in_exit_zone:
            exit_confirm_count += 1

            # Lapis 3: P&L gate check (kalau diset dan pos_data tersedia)
            pnl_gate_ok = True
            pnl_gate_msg = ""
            if pnl_gate > 0 and pos_data.get("eth_entry_price"):
                h = calc_position_pnl()
                if h:
                    net_pct = h.get("net_pnl_pct", 0)
                    if net_pct < pnl_gate:
                        pnl_gate_ok = False
                        pnl_gate_msg = f"net P&L {net_pct:.2f}% < gate {pnl_gate:.2f}%"

            # Lapis 1+2+3: semua harus pass
            if exit_confirm_count >= max(1, confirm_scans) and pnl_gate_ok:
                confirm_note = ""
                if confirm_scans > 1:
                    confirm_note = f" ✅ Konfirmasi {exit_confirm_count} scan"
                if confirm_buffer > 0:
                    confirm_note += f" | buffer +{confirm_buffer}%"
                if pnl_gate > 0:
                    h = calc_position_pnl()
                    net_str = f"{h['net_pnl_pct']:.2f}%" if h else "N/A"
                    confirm_note += f" | P&L gate ✅ ({net_str})"

                send_alert(build_exit_message(btc_ret, eth_ret, gap, confirm_note=confirm_note))
                # Simulation: auto-close posisi
                sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="EXIT")
                if sim_msg:
                    send_alert(sim_msg)
                logger.info(f"EXIT {active_strategy.value}. Gap: {gap_float:.2f}% "
                            f"| Confirmed: {exit_confirm_count} scans | Buffer: {confirm_buffer}%")
                reset_to_scan()
                return
            else:
                # Masih dalam konfirmasi — kirim peringatan saja (bukan exit)
                if exit_confirm_count == 1:
                    # Scan pertama masuk zona — kirim pre-exit alert
                    remaining = max(1, confirm_scans) - exit_confirm_count
                    pnl_wait  = f" | Menunggu P&L ≥{pnl_gate:.1f}%" if not pnl_gate_ok else ""
                    if confirm_scans > 1 or not pnl_gate_ok:
                        send_alert(
                            f"⏳ *Pre-exit: Gap menyentuh TP zone*\n"
                            f"Gap: {gap_float:+.2f}% | TP level: ±{exit_thresh}%\n"
                            f"Menunggu konfirmasi {remaining} scan lagi{pnl_wait}\n"
                            f"_Memproses..._"
                        )
                        logger.info(f"PRE-EXIT {active_strategy.value}. Gap: {gap_float:.2f}% "
                                    f"| Need {remaining} more scans{pnl_wait}")
                elif not pnl_gate_ok:
                    logger.info(f"EXIT HELD by P&L gate: {pnl_gate_msg}")
        else:
            # Gap keluar dari exit zone — reset counter
            if exit_confirm_count > 0:
                logger.info(f"Exit zone lost. Resetting confirm counter ({exit_confirm_count}→0). Gap: {gap_float:.2f}%")
                exit_confirm_count = 0

        if active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S1, btc_ret, eth_ret, gap))
            sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="INVALID")
            if sim_msg: send_alert(sim_msg)
            logger.info(f"INVALIDATION S1. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        if active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S2, btc_ret, eth_ret, gap))
            sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="INVALID")
            if sim_msg: send_alert(sim_msg)
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
    cap_str = f"${settings['capital']:,.0f}" if settings["capital"] > 0 else "Belum diset"
    ec_scans = int(settings["exit_confirm_scans"])
    ec_buf   = float(settings["exit_confirm_buffer"])
    ec_pnl   = float(settings["exit_pnl_gate"])
    eth_sr   = settings["eth_size_ratio"]
    btc_sr   = 100.0 - eth_sr
    ec_str   = (
        f"{ec_scans} scan" + (f" + {ec_buf:.2f}% buffer" if ec_buf > 0 else "") +
        (f" + P&L gate {ec_pnl:.1f}%" if ec_pnl > 0 else "")
        if ec_scans > 0 or ec_buf > 0 or ec_pnl > 0
        else "OFF (langsung exit)"
    )
    send_reply(
        f"⚙️ *Settings — Akeno jaga semuanya~ *\n"
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
        f"🛡️ Exit Confirm:   {ec_str}\n"
        f"📐 Size Ratio:     ETH {eth_sr:.0f}% / BTC {btc_sr:.0f}%\n"
        f"🛡️ Regime Filter:  {'✅ ON' if settings['regime_filter_enabled'] else '❌ OFF'}\n"
        f"💰 Modal:          {cap_str}\n"
        f"📈 Ratio Window:   {settings['ratio_window_days']}d\n"
        f"\n"
        f"_Ketik `/help` untuk daftar perintah lengkap._",
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
    last_r      = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum"

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
        f"📊 *Status Bot* \n"
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
            "tidak ada posisi aktif sekarang\n"
            "Akeno masih mode SCAN",
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
        pnl_body = "│ Data tidak cukup\n"

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
        f"_Seperti mentor: leg ETH -68% tapi net +$239~_",
        reply_chat,
    )


def handle_ratio_command(reply_chat: str) -> None:
    """ETH/BTC ratio percentile monitor — detail conviction + entry readiness."""
    curr_r, avg_r, hi_r, lo_r, pct_r = calc_ratio_percentile()

    if curr_r is None:
        send_reply(
            f"belum cukup data\n"
            f"Sekarang: {len(price_history)} points. Butuh minimal 10",
            reply_chat,
        )
        return

    window      = settings["ratio_window_days"]
    stars_s1, _ = get_ratio_conviction(Strategy.S1, pct_r)
    stars_s2, _ = get_ratio_conviction(Strategy.S2, pct_r)
    ext         = _calc_ratio_extended_stats(curr_r, avg_r, hi_r, lo_r, pct_r)

    if pct_r <= 20:   signal = "🟢 *ETH sangat murah vs BTC* — momentum S2 kuat"
    elif pct_r <= 40: signal = "🟡 *ETH relatif murah* — setup S2 cukup bagus"
    elif pct_r >= 80: signal = "🔴 *ETH sangat mahal vs BTC* — momentum S1 kuat"
    elif pct_r >= 60: signal = "🟠 *ETH relatif mahal* — setup S1 cukup bagus"
    else:             signal = "⚪ *Neutral* — ETH di area tengah vs BTC"

    bar_pos  = min(10, int(pct_r / 10))
    bar      = "─" * bar_pos + "●" + "─" * (10 - bar_pos)
    detail_s1 = _build_conviction_detail(Strategy.S1, stars_s1, pct_r, curr_r, avg_r, hi_r, lo_r, ext)
    detail_s2 = _build_conviction_detail(Strategy.S2, stars_s2, pct_r, curr_r, avg_r, hi_r, lo_r, ext)
    ready_s1  = build_entry_readiness(Strategy.S1, pct_r, curr_r, avg_r, ext)
    ready_s2  = build_entry_readiness(Strategy.S2, pct_r, curr_r, avg_r, ext)

    send_reply(
        f"📈 *ETH/BTC Ratio Monitor*\n"
        f"\n"
        f"┌─────────────────────\n"
        f"│ Sekarang:   {curr_r:.5f}\n"
        f"│ {window}d avg:   {avg_r:.5f}\n"
        f"│ {window}d high:  {hi_r:.5f}\n"
        f"│ {window}d low:   {lo_r:.5f}\n"
        f"│ Percentile: *{pct_r}th*\n"
        f"│ Revert est: {ext['revert_to_avg']:+.2f}% ke avg\n"
        f"└─────────────────────\n"
        f"\n"
        f"`[lo]─{bar}─[hi]`\n"
        f"_(0 = ETH sangat murah | 100 = ETH sangat mahal)_\n"
        f"\n"
        f"{signal}\n"
        f"\n"
        f"──────────────────────\n"
        f"{detail_s1}\n"
        f"\n"
        f"──────────────────────\n"
        f"{detail_s2}\n"
        f"\n"
        f"══════════════════════\n"
        f"{ready_s1}\n"
        f"══════════════════════\n"
        f"{ready_s2}\n"
        f"\n"
        f"_Dari {len(price_history)} price points~_",
        reply_chat,
    )


def build_price_recap_block() -> str:
    """
    Rekap harga BTC & ETH untuk 24h, 48h, 72h terakhir.
    Format: Tgl HH:MM | BTC $X → $Y (±Z%) | ETH $X → $Y (±Z%)
    """
    if not price_history or len(price_history) < 2:
        return ""

    now_pt  = price_history[-1]
    now_ts  = now_pt.timestamp
    btc_now = float(now_pt.btc)
    eth_now = float(now_pt.eth)
    interval = settings["scan_interval"]

    def _find_point(hours_ago: int):
        scans_back = max(1, int(hours_ago * 3600 / interval))
        if len(price_history) <= scans_back:
            # Ambil yang paling jauh tersedia
            return price_history[0]
        return price_history[-scans_back - 1]

    def _row(label: str, pt) -> str:
        ts_str  = pt.timestamp.strftime("%-d %b %H:%M")
        btc_old = float(pt.btc)
        eth_old = float(pt.eth)
        btc_pct = (btc_now - btc_old) / btc_old * 100
        eth_pct = (eth_now - eth_old) / eth_old * 100
        btc_arr = "📈" if btc_pct >= 0 else "📉"
        eth_arr = "📈" if eth_pct >= 0 else "📉"
        return (
            f"│ *{label}*  _(dari {ts_str})_\n"
            f"│  BTC: ${btc_old:,.0f} → ${btc_now:,.0f} {btc_arr} {btc_pct:+.2f}%\n"
            f"│  ETH: ${eth_old:,.2f} → ${eth_now:,.2f} {eth_arr} {eth_pct:+.2f}%"
        )

    pt_24h = _find_point(24)
    pt_48h = _find_point(48)
    pt_72h = _find_point(72)

    # Hanya tampilkan baris kalau datanya berbeda (ada historynya)
    rows = [_row("24h", pt_24h)]
    if pt_48h.timestamp < pt_24h.timestamp:
        rows.append(_row("48h", pt_48h))
    if pt_72h.timestamp < pt_48h.timestamp:
        rows.append(_row("72h", pt_72h))

    now_str = now_ts.strftime("%-d %b %H:%M")
    block   = (
        f"*📅 Rekap Harga (sekarang: {now_str} UTC)*\n"
        f"┌─────────────────────\n"
        + "\n├─────────────────────\n".join(rows) +
        f"\n└─────────────────────\n"
    )
    return block


def handle_analysis_command(reply_chat: str) -> None:
    """Full market analysis on demand."""
    gap_now = scan_stats.get("last_gap")
    btc_r   = scan_stats.get("last_btc_ret")
    eth_r   = scan_stats.get("last_eth_ret")
    btc_p   = scan_stats.get("last_btc_price")
    eth_p   = scan_stats.get("last_eth_price")

    if gap_now is None:
        send_reply("belum ada data harga~ Tunggu scan pertama", reply_chat)
        return

    lb     = get_lookback_label()
    gap_f  = float(gap_now)
    et     = settings["entry_threshold"]

    # Driver
    drv, drv_e, drv_ex = analyze_gap_driver(float(btc_r), float(eth_r), gap_f)

    # Market Regime
    reg = detect_market_regime()
    dom = calc_dominance_score()

    def _pct(v): return f"{v:+.2f}%" if v is not None else "N/A"

    def _recap(btc_v, eth_v, label):
        if btc_v is None or eth_v is None:
            return f"│ {label}: N/A"
        gap_tf  = eth_v - btc_v
        abs_gap = abs(gap_tf)
        if abs_gap < 0.1:
            who = "⚪ Seimbang"
        elif eth_v > btc_v:
            who = f"🟡 ETH lebih {'pump' if eth_v > 0 else 'sedikit turun'} {abs_gap:.2f}%"
        else:
            who = f"🟠 BTC lebih {'pump' if btc_v > 0 else 'sedikit turun'} {abs_gap:.2f}%"
        return f"│ {label}: BTC {_pct(btc_v)} | ETH {_pct(eth_v)} → {who}"

    # Dominance tf rows
    def _dom_row(key):
        t = dom["tf"].get(key)
        if not t: return ""
        arr = "🟡 ETH" if t["diff"] > 0.1 else ("🟠 BTC" if t["diff"] < -0.1 else "⚪")
        return f"│ {key}: BTC {t['btc']:+.2f}% | ETH {t['eth']:+.2f}% → {arr} ({t['diff']:+.2f}%)\n"

    dom_signal_str = f"→ *{dom['signal'].value}* favored" if dom.get("signal") else "→ tidak ada sinyal dominance"

    reg_block = (
        f"*🌍 Market Regime:*\n"
        f"┌─────────────────────\n"
        f"│ {reg['emoji']} *{reg['regime']}* — {reg['strength']}\n"
        f"│ _{reg['description']}_\n"
        f"├─────────────────────\n"
        f"{_recap(reg['btc_1h'],  reg['eth_1h'],  ' 1h')}\n"
        f"{_recap(reg['btc_4h'],  reg['eth_4h'],  ' 4h')}\n"
        f"{_recap(reg['btc_24h'], reg['eth_24h'], '24h')}\n"
        f"├─────────────────────\n"
        f"│ 📊 *Dominance Score: {dom['emoji']} {dom['dominant']} {dom.get('strength','')}*\n"
        f"│ _{dom['label']}_\n"
        f"│ Score: {dom['score']:+.2f}% weighted avg {dom_signal_str}\n"
        + _dom_row("1h") + _dom_row("4h") + _dom_row("24h") +
        f"├─────────────────────\n"
        f"│ Volatilitas: {reg['volatility']} ({reg['vol_pct']:.3f}%/scan)\n"
        f"└─────────────────────\n"
        f"_{reg['implications']}_\n"
    )

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
        hint      = f"Tunggu gap ±{et}%"

    # Sizing preview
    sizing_str = ""
    if settings["capital"] > 0 and btc_p and eth_p:
        eth_r  = settings["eth_size_ratio"]
        btc_r  = 100.0 - eth_r
        rtag   = f"{eth_r:.0f}/{btc_r:.0f}" if eth_r != 50.0 else "50/50"
        eth_alloc, btc_alloc, eth_qty, btc_qty = calc_sizing(btc_p, eth_p)
        sizing_str = (
            f"\n*💰 Sizing Preview ({rtag} ETH/BTC, ${settings['capital']:,.0f}):*\n"
            f"├─ ETH: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
            f"└─ BTC: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n"
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

    # Price recap 24h/48h/72h
    recap_block = build_price_recap_block()

    send_reply(
        f"🧠 *Full Market Analysis*\n"
        f"_Snapshot kondisi pasar terkini._\n"
        f"\n"
        f"{recap_block}\n"
        f"{reg_block}\n"
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
                eth_r = settings["eth_size_ratio"]
                btc_r = 100.0 - eth_r
                rtag  = f"{eth_r:.0f}/{btc_r:.0f}" if eth_r != 50.0 else "50/50"
                eth_alloc, btc_alloc, eth_qty, btc_qty = calc_sizing(btc_p, eth_p)
                preview = (
                    f"\n*Preview sizing ({rtag}):*\n"
                    f"ETH: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
                    f"BTC: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n"
                )
            send_reply(
                f"💰 *Modal:* ${cap:,.0f}\n{preview}\nUsage: `/capital <jumlah USD>`",
                reply_chat,
            )
        else:
            send_reply(
                "💰 *Modal belum diset~*\n\n"
                "Set modal untuk sizing guide & P&L dalam dollar\n"
                "Usage: `/capital 1000`",
                reply_chat,
            )
        return

    try:
        val = float(args[0])
        if val < 0 or val > 10_000_000:
            send_reply("Nilai harus antara $0 hingga $10.000.000.", reply_chat)
            return
        settings["capital"] = val
        if val == 0:
            send_reply("Modal direset. Fitur sizing & P&L dinonaktifkan.", reply_chat)
        else:
            send_reply(
                f"💰 Modal *${val:,.0f}* disimpan\n"
                f"Fitur sizing & P&L dalam dollar aktif. ",
                reply_chat,
            )
        logger.info(f"Capital set to {val}")
    except ValueError:
        send_reply("Nilai tidak valid.", reply_chat)


def handle_sizeratio_command(args: list, reply_chat: str) -> None:
    """
    Set rasio alokasi modal ETH vs BTC.

    /sizeratio           — tampilkan setting sekarang
    /sizeratio <eth_pct> — set % modal ke ETH (sisanya ke BTC)
    /sizeratio 50        — dollar-neutral (default)
    /sizeratio 60        — ETH 60% / BTC 40%
    /sizeratio 70        — ETH 70% / BTC 30%
    """
    curr_eth = settings["eth_size_ratio"]
    curr_btc = 100.0 - curr_eth

    if not args:
        btc_p = scan_stats.get("last_btc_price")
        eth_p = scan_stats.get("last_eth_price")
        preview = ""
        if btc_p and eth_p and settings["capital"] > 0:
            eth_alloc, btc_alloc, eth_qty, btc_qty = calc_sizing(btc_p, eth_p)
            preview = (
                f"\n*Preview sizing sekarang (${settings['capital']:,.0f}):*\n"
                f"ETH leg: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
                f"BTC leg: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n"
            )
        send_reply(
            f"📐 *Sizing Ratio*\n"
            f"\n"
            f"ETH leg: *{curr_eth:.0f}%* | BTC leg: *{curr_btc:.0f}%*\n"
            f"{preview}\n"
            f"Usage: `/sizeratio <eth_pct>`\n"
            f"Contoh: `/sizeratio 60` → ETH 60% / BTC 40%\n"
            f"`/sizeratio 50` → kembali ke dollar-neutral",
            reply_chat,
        )
        return

    try:
        val = float(args[0])
        if not (10.0 <= val <= 90.0):
            send_reply(
                "Nilai harus antara 10–90%.\n"
                "Contoh: `/sizeratio 60` untuk ETH 60% / BTC 40%",
                reply_chat,
            )
            return

        settings["eth_size_ratio"] = val
        btc_pct = 100.0 - val
        tag     = "dollar-neutral" if val == 50.0 else f"ETH lebih besar" if val > 50 else "BTC lebih besar"

        # Preview langsung
        btc_p = scan_stats.get("last_btc_price")
        eth_p = scan_stats.get("last_eth_price")
        preview = ""
        if btc_p and eth_p and settings["capital"] > 0:
            eth_alloc, btc_alloc, eth_qty, btc_qty = calc_sizing(btc_p, eth_p)
            preview = (
                f"\n*Preview sizing (${settings['capital']:,.0f}):*\n"
                f"ETH leg: *${eth_alloc:,.0f}* → {eth_qty:.4f} ETH\n"
                f"BTC leg: *${btc_alloc:,.0f}* → {btc_qty:.6f} BTC\n"
            )

        logger.info(f"Size ratio set: ETH {val:.0f}% / BTC {btc_pct:.0f}%")
        send_reply(
            f"✅ *Sizing ratio diperbarui.*\n"
            f"\n"
            f"ETH leg: *{val:.0f}%* | BTC leg: *{btc_pct:.0f}%*\n"
            f"_{tag}_\n"
            f"{preview}\n"
            f"Berlaku untuk sinyal entry berikutnya.",
            reply_chat,
        )
    except ValueError:
        send_reply("Nilai tidak valid. Contoh: `/sizeratio 60`", reply_chat)


def handle_regimefilter_command(args: list, reply_chat: str) -> None:
    """
    /regimefilter        — tampilkan status
    /regimefilter on     — aktifkan filter (default)
    /regimefilter off    — matikan filter (sinyal masuk walau market berlawanan)
    """
    curr = settings["regime_filter_enabled"]

    if not args:
        reg   = detect_market_regime()
        status = "✅ ON" if curr else "❌ OFF"
        send_reply(
            f"🛡️ *Regime Filter: {status}*\n"
            f"\n"
            f"Market sekarang: {reg['emoji']} *{reg['regime']}* {reg['strength']}\n"
            f"\n"
            f"*Aturan filter:*\n"
            f"S1 (Short ETH) → hanya saat *BULLISH* (market pump)\n"
            f"S2 (Long ETH)  → hanya saat *BEARISH* (market dump)\n"
            f"\n"
            f"Tanpa filter ini, bot akan sinyal S2 saat pump seperti yang\n"
            f"menyebabkan drawdown kemarin\n"
            f"\n"
            f"`/regimefilter on` | `/regimefilter off`",
            reply_chat,
        )
        return

    cmd = args[0].lower()
    if cmd == "on":
        settings["regime_filter_enabled"] = True
        send_reply(
            "✅ *Regime filter aktif~*\n"
            "\n"
            "Bot hanya entry kalau arah gap sesuai kondisi market:\n"
            "S1 → saat pump | S2 → saat dump\n"
            "\n"
            "_Entry yang berlawanan dengan regime akan diblokir + notif~_",
            reply_chat,
        )
    elif cmd == "off":
        settings["regime_filter_enabled"] = False
        send_reply(
            "⚠️ *Regime filter dimatikan~*\n"
            "\n"
            "Bot akan entry S1/S2 berdasarkan gap saja tanpa cek kondisi market.\n"
            "_Hati-hati — ini bisa menyebabkan entry saat kondisi berlawanan~_",
            reply_chat,
        )
    else:
        send_reply("Usage: `/regimefilter on` atau `/regimefilter off`", reply_chat)


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
        send_reply("✅ *Peak Watch ON*", reply_chat)
        return
    if first == "off":
        _cancel_peak_watch_if_active(reply_chat)
        settings["peak_enabled"] = False
        send_reply("❌ *Peak Watch OFF*~ Langsung entry saat threshold", reply_chat)
        return
    try:
        val = float(first)
        if val <= 0 or val > 3.0:
            send_reply("Nilai harus antara 0–3.0.", reply_chat)
            return
        settings["peak_reversal"] = val
        send_reply(f"Reversal *{val}%* dari puncak.", reply_chat)
    except ValueError:
        send_reply("Gunakan `on`, `off`, atau angka reversal.", reply_chat)


def _cancel_peak_watch_if_active(reply_chat=None) -> None:
    global current_mode, peak_gap, peak_strategy
    if current_mode == Mode.PEAK_WATCH and peak_strategy is not None:
        if reply_chat:
            send_reply(
                f"⚠️ Peak Watch *{peak_strategy.value}* dibatalkan.\n"
                "Bot kembali ke mode SCAN.",
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
        send_reply("Usage: `/sltp sl <nilai>`", reply_chat)
        return

    try:
        key, val = args[0].lower(), float(args[1])
        if val <= 0 or val > 10:
            send_reply("Nilai harus antara 0–10.", reply_chat)
            return
        if key == "sl":
            settings["sl_pct"] = val
            send_reply(f"Trailing SL distance *{val}%*.", reply_chat)
        elif key == "tp":
            send_reply(
                f"TP mengikuti exit threshold ±{settings['exit_threshold']}%\n"
                f"Gunakan `/threshold exit <nilai>` untuk menyesuaikan.",
                reply_chat,
            )
        else:
            send_reply("Gunakan parameter `sl`.", reply_chat)
    except ValueError:
        send_reply("Nilai tidak valid.", reply_chat)


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(f"Interval: *{settings['scan_interval']}s*\nUsage: `/interval <60-3600>`", reply_chat)
        return
    try:
        val = int(args[0])
        if val < 60 or val > 3600:
            send_reply("Nilai harus antara 60–3600 detik.", reply_chat)
            return
        settings["scan_interval"] = val
        send_reply(f"Interval scan diatur ke *{val}s*.", reply_chat)
    except ValueError:
        send_reply("Nilai tidak valid.", reply_chat)


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
            send_reply("Nilai harus antara 0–20.", reply_chat)
            return
        if t_type == "entry":
            settings["entry_threshold"] = val
            send_reply(f"Entry threshold diatur ke *±{val}%*.", reply_chat)
        elif t_type == "exit":
            settings["exit_threshold"] = val
            send_reply(f"Exit/TP threshold diatur ke *±{val}%*.", reply_chat)
        elif t_type in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = val
            send_reply(f"Invalidation threshold diatur ke *±{val}%*.", reply_chat)
        else:
            send_reply("Gunakan parameter `entry`, `exit`, atau `invalid`.", reply_chat)
    except ValueError:
        send_reply("Nilai tidak valid.", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(f"Lookback: *{settings['lookback_hours']}h*\nUsage: `/lookback <1-24>`", reply_chat)
        return
    try:
        val = int(args[0])
        if val < 1 or val > 24:
            send_reply("Nilai harus antara 1–24 jam.", reply_chat)
            return
        old = settings["lookback_hours"]
        settings["lookback_hours"] = val
        prune_history(datetime.now(timezone.utc))
        send_reply(f"Lookback diperbarui: *{old}h* → *{val}h*. History di-prune.", reply_chat)
    except ValueError:
        send_reply("Nilai tidak valid.", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"Heartbeat: *{settings['heartbeat_minutes']} menit*\nUsage: `/heartbeat <0-120>`",
            reply_chat,
        )
        return
    try:
        val = int(args[0])
        if val < 0 or val > 120:
            send_reply("Nilai harus antara 0–120 menit.", reply_chat)
            return
        settings["heartbeat_minutes"] = val
        send_reply(
            "Heartbeat *dimatikan*" if val == 0
            else f"Heartbeat setiap *{val} menit*",
            reply_chat,
        )
    except ValueError:
        send_reply("Nilai tidak valid.", reply_chat)


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply("Redis belum dikonfigurasi.", reply_chat)
        return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply("❌ Bot A belum menyimpan data.", reply_chat)
        return
    try:
        data       = json.loads(result["result"])
        hrs_stored = len(data) * settings["scan_interval"] / 3600
        lookback   = settings["lookback_hours"]
        status     = "✅ Siap" if hrs_stored >= lookback else f"⏳ {hrs_stored:.1f}h / {lookback}h"
        last_r     = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum"
        send_reply(
            f"⚡ *Redis Status*\n"
            f"Points: {len(data)} | {hrs_stored:.1f}h | {status}\n"
            f"Refresh: {last_r}\n"
            f"`{data[0]['timestamp'][:19]}` → `{data[-1]['timestamp'][:19]}`",
            reply_chat,
        )
    except Exception as e:
        send_reply(f"Gagal baca: `{e}`", reply_chat)


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
            send_reply("Strategi harus *S1* atau *S2*.", reply_chat)
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
            send_reply("Harga entry harus bernilai positif.", reply_chat)
            return
        if not (1 <= eth_lev <= 200) or not (1 <= btc_lev <= 200):
            send_reply("Leverage harus antara 1x–200x.", reply_chat)
            return

        # Optional extras: ethliq btcliq ethval btcval
        # Contoh: /setpos S1 ... ethliq 1431 btcliq 85000 ethval 3000 btcval 2000
        eth_liq, btc_liq, eth_val, btc_val = None, None, None, None
        extra = args[9:]
        for i in range(0, len(extra) - 1, 2):
            key = extra[i].lower()
            try:
                val = _pf(extra[i + 1])
                if key == "ethliq":   eth_liq = val
                elif key == "btcliq": btc_liq = val
                elif key == "ethval": eth_val = val   # override notional USD
                elif key == "btcval": btc_val = val
            except (ValueError, IndexError):
                pass

        now_iso = datetime.now(timezone.utc).isoformat()
        pos_data.update({
            "eth_entry_price":  eth_entry,
            "eth_qty":          eth_qty,
            "eth_notional_usd": eth_val,
            "eth_leverage":     eth_lev,
            "eth_liq_price":    eth_liq,
            "btc_entry_price":  btc_entry,
            "btc_qty":          btc_qty,
            "btc_notional_usd": btc_val,
            "btc_leverage":     btc_lev,
            "btc_liq_price":    btc_liq,
            "strategy":         strat_str,
            "set_at":           now_iso,
        })

        # Simpan ke Redis supaya survive restart
        saved  = save_pos_data()
        sv_str = "✅ Tersimpan ke Redis" if saved else "⚠️ Redis tidak tersedia, data hanya di memory"

        eth_dir  = "Long 📈" if eth_qty > 0 else "Short 📉"
        btc_dir  = "Long 📈" if btc_qty > 0 else "Short 📉"
        val_note = ""
        if eth_val or btc_val:
            val_note = (
                f"\nValue size (override):\n"
                + (f"ETH notional: ${eth_val:,.2f}\n" if eth_val else "")
                + (f"BTC notional: ${btc_val:,.2f}\n" if btc_val else "")
            )
        liq_note = ""
        if eth_liq or btc_liq:
            liq_note = (
                f"\nLiq override:\n"
                + (f"ETH liq: ${eth_liq:,.2f}\n" if eth_liq else "")
                + (f"BTC liq: ${btc_liq:,.2f}\n" if btc_liq else "")
            )

        logger.info(f"pos_data set: {strat_str} ETH {eth_dir} {eth_qty}@{eth_entry} {eth_lev}x | "
                    f"BTC {btc_dir} {btc_qty}@{btc_entry} {btc_lev}x")
        send_reply(
            f"✅ *Posisi {strat_str} tersimpan.* \n"
            f"\n"
            f"ETH: *{eth_dir}* {abs(eth_qty):.4f} @ ${eth_entry:,.2f} | {eth_lev:.0f}x\n"
            f"BTC: *{btc_dir}* {abs(btc_qty):.6f} @ ${btc_entry:,.2f} | {btc_lev:.0f}x\n"
            f"{val_note}{liq_note}\n"
            f"{sv_str}\n"
            f"\n"
            f"Ketik `/health` untuk cek, `/setfunding` untuk set funding rate",
            reply_chat,
        )
    except (ValueError, IndexError) as e:
        send_reply(f"Format tidak sesuai.\n\n{usage}", reply_chat)
        logger.warning(f"setpos parse error: {e}")


def handle_setfunding_command(args: list, reply_chat: str) -> None:
    """
    Set funding rate untuk dua leg — biar kalkulasi break-even akurat.

    /setfunding eth <rate> btc <rate>

    rate = % per 8 jam dari exchange (bisa negatif)
    Positif = kamu bayar (kalau long), terima (kalau short)
    Negatif = kamu terima (kalau long), bayar (kalau short)

    Contoh: /setfunding eth 0.0100 btc 0.0080
    """
    usage = (
        "*Usage:*\n"
        "`/setfunding eth <rate%> btc <rate%>`\n"
        "\n"
        "Rate = % per 8h dari exchange (cek di funding history)\n"
        "Positif = long bayar | Negatif = long terima\n"
        "\n"
        "Contoh:\n"
        "`/setfunding eth 0.0100 btc 0.0080`\n"
        "`/setfunding eth -0.0050 btc 0.0100`"
    )
    if len(args) < 4:
        send_reply(usage, reply_chat)
        return
    try:
        def _pf(s): return float(s.replace(",", ""))
        if args[0].lower() != "eth" or args[2].lower() != "btc":
            send_reply(usage, reply_chat)
            return
        eth_fr = _pf(args[1])
        btc_fr = _pf(args[3])

        pos_data["eth_funding_rate"] = eth_fr
        pos_data["btc_funding_rate"] = btc_fr
        save_pos_data()

        # Preview cost kalau posisi sudah diset
        preview = ""
        if pos_data.get("eth_entry_price"):
            eth_qty     = pos_data["eth_qty"] or 0
            btc_qty     = pos_data["btc_qty"] or 0
            eth_lev     = pos_data["eth_leverage"] or 1.0
            btc_lev     = pos_data["btc_leverage"] or 1.0
            eth_not     = pos_data.get("eth_notional_usd") or (abs(eth_qty) * pos_data["eth_entry_price"])
            btc_not     = pos_data.get("btc_notional_usd") or (abs(btc_qty) * pos_data["btc_entry_price"])
            eth_margin  = eth_not / eth_lev
            btc_margin  = btc_not / btc_lev

            def _flow(qty, notional, fr):
                return -(1.0 if qty > 0 else -1.0) * notional * (fr / 100)

            ef8h = _flow(eth_qty, eth_not, eth_fr)
            bf8h = _flow(btc_qty, btc_not, btc_fr)
            net8 = ef8h + bf8h
            netd = net8 * 3
            net_dir = "terima 🟢" if net8 >= 0 else "bayar 🔴"
            preview = (
                f"\n*Preview biaya funding:*\n"
                f"ETH: {'+' if ef8h>=0 else ''}${ef8h:.3f}/8h\n"
                f"BTC: {'+' if bf8h>=0 else ''}${bf8h:.3f}/8h\n"
                f"Net: *{'+' if net8>=0 else ''}${net8:.3f}/8h* | *{'+' if netd>=0 else ''}${netd:.2f}/hari* ({net_dir})\n"
            )

        send_reply(
            f"✅ *Funding rate tersimpan.*\n"
            f"\n"
            f"ETH: {eth_fr:+.4f}%/8h\n"
            f"BTC: {btc_fr:+.4f}%/8h\n"
            f"{preview}\n"
            f"Ketik `/health` untuk lihat break-even timer",
            reply_chat,
        )
    except (ValueError, IndexError) as e:
        send_reply(f"Format tidak sesuai.\n\n{usage}", reply_chat)
        logger.warning(f"setfunding parse error: {e}")


def handle_velocity_command(reply_chat: str) -> None:
    """Gap velocity & ETA ke TP."""
    vel = calc_gap_velocity()
    if not vel:
        send_reply(
            "Data belum mencukupi untuk kalkulasi velocity.\n"
            f"Sekarang: {len(gap_history)} pts | Butuh minimal 3",
            reply_chat,
        )
        return

    curr_gap = vel["curr_gap"]
    et       = settings["exit_threshold"]
    it       = settings["invalidation_threshold"]
    d15      = vel.get("delta_15m")
    d30      = vel.get("delta_30m")
    d60      = vel.get("delta_60m")
    eta      = vel.get("eta_min")
    accel    = vel.get("accel")

    def _ds(v): return f"{v:+.3f}%" if v is not None else "N/A"

    # Konverging atau melebar?
    conv_15 = (d15 is not None and abs(curr_gap + d15) < abs(curr_gap))
    trend_e = "⬇️ konvergen" if conv_15 else "⬆️ melebar"
    if accel is not None:
        if accel > 1.2:    momentum = "📈 *makin cepat*"
        elif accel < 0.8:  momentum = "📉 *makin lambat*"
        else:              momentum = "➡️ *stabil*"
    else:
        momentum = "N/A"

    eta_s = f"~{int(eta)}m ({eta/60:.1f}h)" if eta is not None and eta < 10000 else "tidak bisa dihitung"
    dist_to_tp  = abs(curr_gap) - et
    dist_to_inv = it - abs(curr_gap)

    send_reply(
        f"📡 *Gap Velocity Monitor*\n"
        f"\n"
        f"┌─────────────────────\n"
        f"│ Gap sekarang: *{curr_gap:+.3f}%*\n"
        f"│ Jarak ke TP:   {dist_to_tp:+.3f}% (exit ±{et}%)\n"
        f"│ Jarak ke Invalid: {dist_to_inv:.3f}% (invalid ±{it}%)\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Δ Gap (perubahan gap):*\n"
        f"┌─────────────────────\n"
        f"│ 15m: {_ds(d15)} {trend_e}\n"
        f"│ 30m: {_ds(d30)}\n"
        f"│ 60m: {_ds(d60)}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Momentum:* {momentum}\n"
        f"*ETA ke TP:* {eta_s}\n"
        f"\n"
        f"_Dari {vel['n_pts']} pts terakhir~_",
        reply_chat,
    )


def handle_exitconf_command(args: list, reply_chat: str) -> None:
    """
    Konfigurasi 3-lapis exit confirmation.

    /exitconf scans <n>       — gap harus stay N scan berturut-turut (default 2)
    /exitconf buffer <pct>    — gap harus masuk buffer% lebih dalam dari threshold
    /exitconf pnl <pct>       — exit hanya kalau net P&L ≥ pct% dari margin
    /exitconf off             — matikan semua konfirmasi (langsung exit)
    /exitconf show            — tampilkan setting sekarang
    """
    conf_s = int(settings["exit_confirm_scans"])
    conf_b = float(settings["exit_confirm_buffer"])
    pnl_g  = float(settings["exit_pnl_gate"])

    if not args or args[0].lower() == "show":
        mode_s1 = settings["exit_threshold"] - conf_b
        mode_s2 = settings["exit_threshold"] - conf_b
        send_reply(
            f"*🛡️ Exit Confirmation Settings*\n"
            f"\n"
            f"┌─────────────────────\n"
            f"│ Lapis 1 — Scan konfirmasi: *{conf_s}x*\n"
            f"│  Gap harus stay {conf_s} scan berturut-turut sebelum exit\n"
            f"│  _(0 = langsung exit, behaviour lama)_\n"
            f"│\n"
            f"│ Lapis 2 — Buffer: *{conf_b:.2f}%*\n"
            f"│  Efektif exit S1 di gap ≤ +{mode_s1:.2f}%\n"
            f"│  Efektif exit S2 di gap ≥ -{mode_s2:.2f}%\n"
            f"│  _(0.0 = tepat di threshold)_\n"
            f"│\n"
            f"│ Lapis 3 — P&L gate: *{pnl_g:.2f}%*\n"
            f"│  Exit hanya kalau net P&L ≥ {pnl_g:.2f}% dari margin\n"
            f"│  _(0.0 = disable, tidak cek P&L)_\n"
            f"└─────────────────────\n"
            f"\n"
            f"*Commands:*\n"
            f"`/exitconf scans 3` — konfirmasi 3 scan\n"
            f"`/exitconf buffer 0.3` — buffer 0.3% lebih dalam\n"
            f"`/exitconf pnl 0.5` — exit kalau net P&L ≥ 0.5%\n"
            f"`/exitconf off` — matikan semua (langsung exit)\n",
            reply_chat,
        )
        return

    if args[0].lower() == "off":
        settings["exit_confirm_scans"]  = 0
        settings["exit_confirm_buffer"] = 0.0
        settings["exit_pnl_gate"]       = 0.0
        send_reply(
            "⚡ *Exit confirmation dimatikan~*\n"
            "Bot akan exit langsung saat gap menyentuh threshold~ (behaviour lama)\n",
            reply_chat,
        )
        return

    if len(args) < 2:
        send_reply("Usage: `/exitconf scans|buffer|pnl <nilai>` atau `/exitconf off`", reply_chat)
        return

    try:
        key = args[0].lower()
        val = float(args[1].replace(",", ""))
        if key == "scans":
            settings["exit_confirm_scans"] = max(0, int(val))
            send_reply(
                f"✅ Scan konfirmasi: *{int(val)}x*\n"
                f"_Gap harus stay {int(val)} scan sebelum exit~_",
                reply_chat,
            )
        elif key == "buffer":
            settings["exit_confirm_buffer"] = max(0.0, val)
            et     = settings["exit_threshold"]
            eff_s1 = et - val
            eff_s2 = et - val
            send_reply(
                f"✅ Exit buffer: *{val:.2f}%*\n"
                f"_Efektif exit S1 di gap ≤ +{eff_s1:.2f}% | S2 di gap ≥ -{eff_s2:.2f}%~_",
                reply_chat,
            )
        elif key in ("pnl", "pnlgate"):
            settings["exit_pnl_gate"] = max(0.0, val)
            if val > 0 and pos_data.get("eth_entry_price") is None:
                send_reply(
                    f"✅ P&L gate: *{val:.2f}%*\n"
                    f"_Bot akan tahan exit sampai net P&L ≥ {val:.2f}%~_\n"
                    f"⚠️ `/setpos` belum diset — P&L gate butuh data posisi",
                    reply_chat,
                )
            else:
                send_reply(
                    f"✅ P&L gate: *{val:.2f}%*\n"
                    f"_Bot akan tahan exit sampai net P&L ≥ {val:.2f}%~_",
                    reply_chat,
                )
        else:
            send_reply("Key tidak dikenal~ Gunakan: `scans`, `buffer`, atau `pnl`", reply_chat)
    except (ValueError, IndexError):
        send_reply("Format tidak sesuai. Contoh: `/exitconf scans 2`", reply_chat)


def handle_health_command(reply_chat: str) -> None:
    """Tampilkan health posisi — leverage, margin, UPnL, liq price, pairs net."""
    if pos_data["eth_entry_price"] is None:
        send_reply(
            "belum ada posisi yang diset\n\n"
            "Gunakan `/setpos` dulu ya\n\n"
            "*Contoh S1* (Long BTC / Short ETH):\n"
            "`/setpos S1 eth 2011.56 -1.4907 50x btc 67794.76 0.029491 50x`\n\n"
            "*Contoh S2* (Long ETH / Short BTC):\n"
            "`/setpos S2 eth 1956.40 15.58 10x btc 67586.10 -0.4439 10x`",
            reply_chat,
        )
        return
    if scan_stats.get("last_btc_price") is None:
        send_reply("Harga belum tersedia. Coba lagi sesaat.", reply_chat)
        return
    h = calc_position_pnl()
    if not h:
        send_reply("Gagal menghitung P&L. Coba `/setpos` ulang.", reply_chat)
        return
    send_reply(build_position_health_message(h), reply_chat)


def handle_clearpos_command(reply_chat: str) -> None:
    """Hapus pos_data dari memory dan Redis."""
    for k in pos_data:
        pos_data[k] = None
    clear_pos_data_redis()
    send_reply("🗑️ *Data posisi dihapus~*\nRedis juga dibersihkan.", reply_chat)


def handle_sim_command(args: list, reply_chat: str) -> None:
    """
    /sim              — status simulasi sekarang
    /sim on           — aktifkan simulasi
    /sim off          — matikan simulasi
    /sim margin <usd> — set margin per leg
    /sim lev <n>      — set leverage
    /sim fee <pct>    — set taker fee % (default 0.06)
    /sim reset        — hapus history trades
    """
    enabled    = settings["sim_enabled"]
    margin     = settings["sim_margin_usd"]
    lev        = settings["sim_leverage"]
    fee        = settings["sim_fee_pct"]
    active     = sim_trade["active"]
    n_trades   = len(sim_trade["history"])

    if not args or args[0].lower() == "status":
        btc_p  = scan_stats.get("last_btc_price")
        eth_p  = scan_stats.get("last_eth_price")
        pnl_str = ""
        if active and btc_p and eth_p:
            h = calc_position_pnl()
            if h:
                s   = "+" if h["net_pnl"] >= 0 else ""
                pnl_str = (
                    f"\n*PnL sekarang:*\n"
                    f"ETH leg: {'+' if h['eth_pnl']>=0 else ''}${h['eth_pnl']:.2f} ({h['eth_pnl_pct']:.2f}%)\n"
                    f"BTC leg: {'+' if h['btc_pnl']>=0 else ''}${h['btc_pnl']:.2f} ({h['btc_pnl_pct']:.2f}%)\n"
                    f"*Net:    {s}${h['net_pnl']:.2f} ({s}{h['net_pnl_pct']:.2f}%)*\n"
                )
        sim_state = f"{'🟢 AKTIF' if enabled else '🔴 MATI'}"
        pos_state = f"{'📍 Posisi terbuka — ' + (sim_trade['strategy'] or '') if active else '💤 Tidak ada posisi'}"
        send_reply(
            f"🤖 *Simulation Mode*\n"
            f"\n"
            f"Status:  {sim_state}\n"
            f"Posisi:  {pos_state}\n"
            f"Margin:  ${margin:,.0f} per leg\n"
            f"Lev:     {lev:.0f}x\n"
            f"Fee:     {fee:.3f}% per side\n"
            f"Trades:  {n_trades} total\n"
            f"{pnl_str}\n"
            f"*Commands:*\n"
            f"`/sim on` `/sim off`\n"
            f"`/sim margin <usd>`\n"
            f"`/sim lev <n>`\n"
            f"`/sim fee <pct>`\n"
            f"`/sim reset` — hapus history\n"
            f"`/simstats` — rekap semua trade",
            reply_chat,
        )
        return

    cmd = args[0].lower()

    if cmd == "on":
        settings["sim_enabled"] = True
        send_reply(
            f"🟢 *Simulation Mode: ON*\n"
            f"\n"
            f"Margin: ${margin:,.0f} per leg | Lev: {lev:.0f}x | Fee: {fee:.3f}%\n"
            f"Total eksposur per trade: ${margin * lev * 2:,.0f}\n"
            f"\n"
            f"Bot akan otomatis open posisi saat sinyal entry,\n"
            f"dan close saat exit/invalidasi\n"
            f"\n"
            f"_Ubah setting: `/sim margin 200` `/sim lev 20`_",
            reply_chat,
        )

    elif cmd == "off":
        settings["sim_enabled"] = False
        if sim_trade["active"]:
            send_reply(
                "🔴 *Simulation mode OFF~*\n"
                "⚠️ Masih ada posisi terbuka — gunakan `/sim on` lagi atau tunggu exit signal",
                reply_chat,
            )
        else:
            send_reply("🔴 *Simulation mode OFF~* Bot tidak akan auto-trade lagi", reply_chat)

    elif cmd == "margin":
        try:
            val = float(args[1])
            if val <= 0 or val > 100000:
                send_reply("Margin harus antara $1 sampai $100,000", reply_chat)
                return
            settings["sim_margin_usd"] = val
            send_reply(
                f"✅ Margin per leg: *${val:,.0f}*\n"
                f"Total eksposur per trade: ${val * settings['sim_leverage'] * 2:,.0f}",
                reply_chat,
            )
        except (IndexError, ValueError):
            send_reply("Usage: `/sim margin <usd>` — contoh: `/sim margin 200`", reply_chat)

    elif cmd == "lev":
        try:
            val = float(args[1])
            if not 1 <= val <= 200:
                send_reply("Leverage harus 1x–200x", reply_chat)
                return
            settings["sim_leverage"] = val
            send_reply(
                f"✅ Leverage: *{val:.0f}x*\n"
                f"Total eksposur per trade: ${settings['sim_margin_usd'] * val * 2:,.0f}",
                reply_chat,
            )
        except (IndexError, ValueError):
            send_reply("Usage: `/sim lev <n>` — contoh: `/sim lev 20`", reply_chat)

    elif cmd == "fee":
        try:
            val = float(args[1])
            settings["sim_fee_pct"] = val
            send_reply(f"✅ Fee taker: *{val:.4f}%* per side", reply_chat)
        except (IndexError, ValueError):
            send_reply("Usage: `/sim fee <pct>` — contoh: `/sim fee 0.06`", reply_chat)

    elif cmd == "reset":
        sim_trade["history"].clear()
        send_reply("✅ History trade direset.", reply_chat)

    else:
        send_reply("Command tidak dikenal~ Ketik `/sim` untuk daftar perintah", reply_chat)


def handle_simstats_command(reply_chat: str) -> None:
    """Rekap statistik semua sim trades."""
    history = sim_trade["history"]

    # Status posisi aktif dulu
    active_str = ""
    if sim_trade["active"]:
        h = calc_position_pnl()
        if h:
            s = "+" if h["net_pnl"] >= 0 else ""
            active_str = (
                f"📍 *Posisi terbuka: {sim_trade['strategy']}*\n"
                f"Net PnL sekarang: *{s}${h['net_pnl']:.2f} ({s}{h['net_pnl_pct']:.2f}%)*\n"
                f"Time: {h['time_label']}\n\n"
            )

    if not history:
        send_reply(
            f"{active_str}"
            f"📊 *Sim Stats*\n\n"
            f"Belum ada trade yang selesai.\n"
            f"Aktifkan `/sim on` dan tunggu sinyal entry",
            reply_chat,
        )
        return

    total     = len(history)
    wins      = [t for t in history if t["net_pnl"] >= 0]
    losses    = [t for t in history if t["net_pnl"] < 0]
    total_net = sum(t["net_pnl"] for t in history)
    total_fee = sum(t["fee"] for t in history)
    win_rate  = len(wins) / total * 100
    avg_win   = sum(t["net_pnl"] for t in wins) / len(wins) if wins else 0
    avg_loss  = sum(t["net_pnl"] for t in losses) / len(losses) if losses else 0
    best      = max(history, key=lambda t: t["net_pnl"])
    worst     = min(history, key=lambda t: t["net_pnl"])

    # Risk/reward ratio
    rr = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    sign  = "+" if total_net >= 0 else ""
    emoji = "🟢" if total_net >= 0 else "🔴"

    # 5 trade terakhir
    recent = history[-5:]
    recent_lines = ""
    for t in reversed(recent):
        s    = "+" if t["net_pnl"] >= 0 else ""
        e    = "✅" if t["net_pnl"] >= 0 else "❌"
        rc   = "INV" if t["reason"] == "INVALID" else "TP"
        recent_lines += f"│ {e} {t['strategy']} {rc}: {s}${t['net_pnl']:.2f} ({s}{t['net_pct']:.2f}%) {t['duration']}\n"

    send_reply(
        f"{active_str}"
        f"📊 *Sim Stats — {total} trades*\n"
        f"┌─────────────────────\n"
        f"│ {emoji} *Net P&L:  {sign}${total_net:.2f}*\n"
        f"│ Total fee: -${total_fee:.3f}\n"
        f"├─────────────────────\n"
        f"│ Win rate:  {len(wins)}W / {len(losses)}L ({win_rate:.0f}%)\n"
        f"│ Avg win:   +${avg_win:.2f}\n"
        f"│ Avg loss:  ${avg_loss:.2f}\n"
        f"│ R:R ratio: 1:{rr:.2f}\n"
        f"├─────────────────────\n"
        f"│ Best:  +${best['net_pnl']:.2f} ({best['strategy']} {best['reason']})\n"
        f"│ Worst:  ${worst['net_pnl']:.2f} ({worst['strategy']} {worst['reason']})\n"
        f"├─────────────────────\n"
        f"│ *5 Trade Terakhir:*\n"
        f"{recent_lines}"
        f"└─────────────────────\n"
        f"_Ketik `/sim reset` untuk hapus history~_",
        reply_chat,
    )


def handle_help_command(reply_chat: str) -> None:
    """
    /help          — menu utama
    /help market   — perintah analisis pasar
    /help pos      — position health tracker
    /help config   — konfigurasi bot
    /help examples — contoh lengkap setpos & setfunding
    """
    args_raw = getattr(handle_help_command, "_last_args", [])

    peak_s  = "✅" if settings["peak_enabled"] else "❌"
    cap_str = f"${settings['capital']:,.0f}" if settings["capital"] > 0 else "—"
    pos_str = pos_data.get("strategy") or "—"
    mode_s  = current_mode.value if current_mode else "SCAN"
    gap_s   = f"{float(scan_stats['last_gap']):+.2f}%" if scan_stats.get("last_gap") is not None else "—"

    send_reply(
        f"📟 *Menu Utama*\n"
        f"_Gap: {gap_s} | Mode: {mode_s} | Peak: {peak_s} | Modal: {cap_str} | Posisi: {pos_str}_\n"
        f"\n"
        f"Ketik salah satu untuk detail:\n"
        f"\n"
        f"📈 `/help market` — Analisis & sinyal\n"
        f"🏥 `/help pos`    — Position tracker\n"
        f"⚙️ `/help config` — Konfigurasi bot\n"
        f"📋 `/help example`— Contoh perintah\n"
        f"\n"
        f"*Shortcut cepat:*\n"
        f"`/analysis` `/ratio` `/health` `/status`",
        reply_chat,
    )


def handle_help_market_command(reply_chat: str) -> None:
    send_reply(
        f"📈 *Market Analysis*\n"
        f"───────────────────\n"
        f"`/analysis`  — Snapshot lengkap\n"
        f"             _(regime + gap + ratio + setup)_\n"
        f"\n"
        f"`/ratio`     — ETH/BTC ratio detail\n"
        f"             _(conviction + entry readiness)_\n"
        f"\n"
        f"`/velocity`  — Kecepatan gap & ETA TP\n"
        f"\n"
        f"`/pnl`       — Net P&L posisi bot aktif\n"
        f"\n"
        f"`/capital <usd>`\n"
        f"             — Set modal untuk sizing\n"
        f"\n"
        f"───────────────────\n"
        f"_Sinyal otomatis dikirim saat gap ±threshold~_",
        reply_chat,
    )


def handle_help_pos_command(reply_chat: str) -> None:
    eth_fr = pos_data.get("eth_funding_rate")
    btc_fr = pos_data.get("btc_funding_rate")
    fr_str = f"ETH {eth_fr:+.4f}% / BTC {btc_fr:+.4f}%" if eth_fr is not None else "belum diset"
    pos_s  = pos_data.get("strategy") or "belum diset"
    send_reply(
        f"🏥 *Position Health Tracker*\n"
        f"_Posisi aktif: {pos_s}_\n"
        f"───────────────────\n"
        f"`/health`    — Cek health posisi\n"
        f"             _(margin, UPnL, liq, funding)_\n"
        f"\n"
        f"`/setpos`    — Daftarkan posisi baru\n"
        f"             _/help example untuk format_\n"
        f"\n"
        f"`/setfunding eth <r> btc <r>`\n"
        f"             — Set funding rate/8h\n"
        f"             _{fr_str}_\n"
        f"\n"
        f"`/clearpos`  — Hapus data posisi\n"
        f"\n"
        f"───────────────────\n"
        f"_Data tersimpan di Redis dan akan bertahan saat restart._",
        reply_chat,
    )


def handle_help_config_command(reply_chat: str) -> None:
    et    = settings["entry_threshold"]
    xt    = settings["exit_threshold"]
    it    = settings["invalidation_threshold"]
    sl    = settings["sl_pct"]
    peak  = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    pr    = settings["peak_reversal"]
    ec_s  = int(settings["exit_confirm_scans"])
    ec_b  = float(settings["exit_confirm_buffer"])
    ec_p  = float(settings["exit_pnl_gate"])
    hb    = settings["heartbeat_minutes"]
    iv    = settings["scan_interval"]
    lk    = settings["lookback_hours"]
    send_reply(
        f"⚙️ *Konfigurasi Bot*\n"
        f"───────────────────\n"
        f"*Threshold:*\n"
        f"  Entry:      ±{et}%\n"
        f"  Exit/TP:    ±{xt}%\n"
        f"  Invalid:    ±{it}%\n"
        f"  Trail SL:   {sl}%\n"
        f"\n"
        f"*Peak Mode:* {peak} ({pr}% reversal)\n"
        f"\n"
        f"*Exit Confirmation:*\n"
        f"  Scans:  {ec_s}x\n"
        f"  Buffer: {ec_b:.2f}%\n"
        f"  P&L gate: {ec_p:.2f}%\n"
        f"\n"
        f"*Timing:*\n"
        f"  Scan:      {iv}s\n"
        f"  Lookback:  {lk}h\n"
        f"  Heartbeat: {hb}m\n"
        f"\n"
        f"───────────────────\n"
        f"*Ubah dengan:*\n"
        f"`/threshold entry|exit|invalid <val>`\n"
        f"`/sltp sl <val>`\n"
        f"`/peak on|off|<val>`\n"
        f"`/exitconf scans|buffer|pnl <val>`\n"
        f"`/sizeratio <eth_pct>` _(sekarang ETH {int(settings['eth_size_ratio'])}%)_\n"
        f"`/regimefilter on|off` _(sekarang: {'✅ ON' if settings['regime_filter_enabled'] else '❌ OFF'})_\n"
        f"`/interval <detik>`\n"
        f"`/lookback <jam>`\n"
        f"`/heartbeat <menit>`",
        reply_chat,
    )


def handle_help_example_command(reply_chat: str) -> None:
    et = settings["exit_threshold"]
    send_reply(
        f"📋 *Contoh Perintah*\n"
        f"───────────────────\n"
        f"*S1 — Long BTC / Short ETH:*\n"
        f"`/setpos S1`\n"
        f"`  eth 2011.56 -1.4907 50x`\n"
        f"`  btc 67794.76 0.029491 50x`\n"
        f"\n"
        f"_Optional tambahan di akhir:_\n"
        f"`  ethval 3000 btcval 2000`\n"
        f"`  ethliq 1431 btcliq 85000`\n"
        f"\n"
        f"*S2 — Long ETH / Short BTC:*\n"
        f"`/setpos S2`\n"
        f"`  eth 1956.40 15.58 10x`\n"
        f"`  btc 67586.10 -0.4439 10x`\n"
        f"\n"
        f"*Funding rate:*\n"
        f"`/setfunding eth 0.0100 btc 0.0080`\n"
        f"\n"
        f"*Exit confirmation:*\n"
        f"`/exitconf scans 2`\n"
        f"`/exitconf buffer 0.2`\n"
        f"`/exitconf pnl 0.3`\n"
        f"\n"
        f"*Threshold cepat:*\n"
        f"`/threshold entry 1.2`\n"
        f"`/threshold exit {et}`\n"
        f"\n"
        f"───────────────────\n"
        f"_qty positif = Long | negatif = Short_\n"
        f"_angka boleh pakai koma: 2,011.56 ✅_",
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
        else "\n⚠️ Gagal mengambil data harga. Bot akan mencoba kembali.\n"
    )
    hrs_loaded  = len(price_history) * settings["scan_interval"] / 3600
    hist_info   = (
        f"⚡ History Bot A: *{hrs_loaded:.1f}h* siap!\n"
        if price_history
        else f"⏳ Menunggu Bot A~ Sinyal setelah {settings['lookback_hours']}h tersedia\n"
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
        f"✅ *Bot B — Aktif*\n"
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
        f"Ketik `/help` untuk semua command\n"
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
                        scan_stats["last_btc_ret"] = eth_ret
                        scan_stats["last_eth_ret"] = eth_ret

                        # Gap velocity history
                        _now = datetime.now(timezone.utc)
                        gap_history.append((_now, float(gap)))
                        if len(gap_history) > MAX_GAP_HISTORY:
                            gap_history.pop(0)

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
