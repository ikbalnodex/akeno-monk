#!/usr/bin/env python3
"""
Lookback Bot - BTC/ETH Price History Collector

Tugasnya satu: kumpulkan harga BTC/ETH setiap 3 menit
dan simpan ke Redis — siap dipakai bot utama kapanpun.

Tidak ada sinyal, tidak ada threshold, tidak ada trading logic.
"""
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional, List, NamedTuple

import requests

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    API_BASE_URL,
    API_ENDPOINT,
    SCAN_INTERVAL_SECONDS,
    HEARTBEAT_MINUTES,
    FRESHNESS_THRESHOLD_MINUTES,
    MAX_LOOKBACK_HOURS,
    UPSTASH_REDIS_URL,
    UPSTASH_REDIS_TOKEN,
    logger,
)


# =============================================================================
# Data Structures
# =============================================================================
class PricePoint(NamedTuple):
    timestamp: datetime
    btc: Decimal
    eth: Decimal


# =============================================================================
# Global State
# =============================================================================
price_history: List[PricePoint] = []

settings = {
    "scan_interval": SCAN_INTERVAL_SECONDS,
    "heartbeat_minutes": HEARTBEAT_MINUTES,
    "max_lookback_hours": MAX_LOOKBACK_HOURS,
}

last_update_id: int = 0
last_heartbeat_time: Optional[datetime] = None
_save_counter: int = 0
SAVE_EVERY_N_SCANS: int = 3  # 3 scan × 3 menit = simpan tiap ~9 menit
scan_stats = {
    "count": 0,
    "last_btc_price": None,
    "last_eth_price": None,
    "errors": 0,
}

# =============================================================================
# Redis — key SAMA dengan bot utama agar bisa langsung dipakai
# =============================================================================
REDIS_KEY = "monk_bot:price_history"
HISTORY_BUFFER_MINUTES = 30


def _redis_request(method: str, path: str, body=None):
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
    if not UPSTASH_REDIS_URL:
        return
    try:
        data = json.dumps([
            {"timestamp": p.timestamp.isoformat(), "btc": str(p.btc), "eth": str(p.eth)}
            for p in price_history
        ])
        # Upstash REST pipeline: POST / dengan array command
        headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
        resp = requests.post(
            f"{UPSTASH_REDIS_URL}/pipeline",
            headers=headers,
            json=[["SET", REDIS_KEY, data], ["EXPIRE", REDIS_KEY, 90000]],
            timeout=10,
        )
        resp.raise_for_status()
        logger.debug(f"Saved {len(price_history)} points to Redis")
    except Exception as e:
        logger.warning(f"Failed to save to Redis: {e}")


def _unwrap_redis_data(raw) -> list:
    """Handle berbagai format response Upstash: string, list, atau wrapped dict."""
    # Kalau string, parse dulu
    if isinstance(raw, str):
        raw = json.loads(raw)
    # Kalau masih string (double encoded)
    if isinstance(raw, str):
        raw = json.loads(raw)
    # Kalau wrapped {"value": "..."}
    if isinstance(raw, dict) and "value" in raw:
        raw = raw["value"]
        if isinstance(raw, str):
            raw = json.loads(raw)
    return raw


def load_history() -> None:
    global price_history
    if not UPSTASH_REDIS_URL:
        logger.info("Redis not configured, starting fresh")
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY}")
        if not result or result.get("result") is None:
            logger.info("No history in Redis, starting fresh")
            return
        data = _unwrap_redis_data(result["result"])
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
        logger.warning(f"Failed to load history from Redis: {e}")
        price_history = []


def get_redis_status() -> dict:
    """Ambil info ringkas dari Redis untuk heartbeat."""
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        return {"ok": False, "points": 0, "hours": 0.0, "first": "-", "last": "-"}
    try:
        data = _unwrap_redis_data(result["result"])
        hours = len(data) * settings["scan_interval"] / 3600
        return {
            "ok": True,
            "points": len(data),
            "hours": hours,
            "first": data[0]["timestamp"] if data else "-",
            "last": data[-1]["timestamp"] if data else "-",
        }
    except Exception as e:
        logger.warning(f"get_redis_status error: {e}")
        return {"ok": False, "points": 0, "hours": 0.0, "first": "-", "last": "-"}


# =============================================================================
# History Management
# =============================================================================
def append_price(timestamp: datetime, btc: Decimal, eth: Decimal) -> None:
    price_history.append(PricePoint(timestamp, btc, eth))


def prune_history(now: datetime) -> None:
    global price_history
    cutoff = now - timedelta(
        hours=settings["max_lookback_hours"],
        minutes=HISTORY_BUFFER_MINUTES
    )
    price_history = [p for p in price_history if p.timestamp >= cutoff]


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
    except (ValueError, AttributeError):
        return None


def fetch_prices():
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f"API request failed: {e}")
        return None

    listings = data.get("listings", [])
    btc_data = next((l for l in listings if l.get("ticker", "").upper() == "BTC"), None)
    eth_data = next((l for l in listings if l.get("ticker", "").upper() == "ETH"), None)

    if not btc_data or not eth_data:
        return None

    try:
        btc_price = Decimal(btc_data["mark_price"])
        eth_price = Decimal(eth_data["mark_price"])
    except (KeyError, InvalidOperation):
        return None

    btc_updated = parse_iso_timestamp(btc_data.get("quotes", {}).get("updated_at", ""))
    eth_updated = parse_iso_timestamp(eth_data.get("quotes", {}).get("updated_at", ""))

    if not btc_updated or not eth_updated:
        return None

    return btc_price, eth_price, btc_updated, eth_updated


def is_data_fresh(now, btc_updated, eth_updated) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return (now - btc_updated) <= threshold and (now - eth_updated) <= threshold


# =============================================================================
# Telegram
# =============================================================================
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def send_message(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
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
        return True
    except requests.RequestException as e:
        logger.error(f"Telegram failed: {e}")
        return False


# =============================================================================
# Telegram Commands
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
        if not is_authorized or not text.startswith("/"):
            continue
        parts = text.split()
        command = parts[0].lower().split("@")[0]
        args = parts[1:] if len(parts) > 1 else []
        logger.info(f"Command: {command} from {chat_id}")

        if command in ("/start", "/help"):
            handle_help(chat_id)
        elif command == "/status":
            handle_status(chat_id)
        elif command == "/redis":
            handle_redis(chat_id)
        elif command == "/interval":
            handle_interval(args, chat_id)
        elif command == "/heartbeat":
            handle_heartbeat_cmd(args, chat_id)


def send_reply(message: str, chat_id: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
    except requests.RequestException as e:
        logger.error(f"Reply failed: {e}")


def handle_help(chat_id: str) -> None:
    send_reply(
        "Senpai, ini aku Lookback Bot.\n"
        "\n"
        "Tugasku satu — kumpulin harga BTC/ETH tiap beberapa menit\n"
        "dan simpen ke Redis buat bot utama.\n"
        "\n"
        "*Command:*\n"
        "`/status` - lihat kondisi sekarang\n"
        "`/redis` - cek data yang tersimpan di Redis\n"
        "`/interval <detik>` - ubah scan interval (60-3600)\n"
        "`/heartbeat <menit>` - ubah laporan rutin (0=off)\n"
        "`/help` - tampilkan pesan ini\n"
        "\n"
        "_Aku kerja di sini ya senpai, tenang aja._",
        chat_id
    )


def handle_status(chat_id: str) -> None:
    hours = len(price_history) * settings["scan_interval"] / 3600
    max_h = settings["max_lookback_hours"]
    btc_str = f"${float(scan_stats['last_btc_price']):,.2f}" if scan_stats["last_btc_price"] else "N/A"
    eth_str = f"${float(scan_stats['last_eth_price']):,.2f}" if scan_stats["last_eth_price"] else "N/A"
    redis_ok = "✅ Terhubung" if UPSTASH_REDIS_URL else "❌ Tidak dikonfigurasi"
    data_status = f"✅ Penuh ({hours:.1f}h)" if hours >= max_h else f"⏳ {hours:.1f}h / {max_h}h"
    send_reply(
        "📊 *Status Lookback Bot*\n"
        "\n"
        f"┌─────────────────────\n"
        f"│ Scan interval: {settings['scan_interval']}s\n"
        f"│ Heartbeat: {settings['heartbeat_minutes']} menit\n"
        f"│ Max lookback: {max_h}h\n"
        f"└─────────────────────\n"
        "\n"
        "*Harga terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"│ Scan count: {scan_stats['count']}x\n"
        f"│ Errors: {scan_stats['errors']}x\n"
        f"└─────────────────────\n"
        "\n"
        f"*Redis:* {redis_ok}\n"
        f"*Data lokal:* {data_status}\n"
        "\n"
        "_Masih jalan kok senpai._",
        chat_id
    )


def handle_redis(chat_id: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply(
            "⚠️ Redis belum dikonfigurasi senpai.\n"
            "Isi `UPSTASH_REDIS_REST_URL` dan token di Railway dulu ya.",
            chat_id
        )
        return
    s = get_redis_status()
    if not s["ok"]:
        send_reply(
            "❌ *Belum ada data di Redis.*\n\n"
            "Lagi ngumpulin data senpai, tunggu beberapa menit lagi.",
            chat_id
        )
        return
    max_h = settings["max_lookback_hours"]
    bar_filled = int((s["hours"] / max_h) * 10)
    bar = "█" * bar_filled + "░" * (10 - bar_filled)
    ready = "✅ Siap dipakai bot utama" if s["hours"] >= max_h else f"⏳ {s['hours']:.1f}h / {max_h}h"
    send_reply(
        f"⚡ *Redis Status*\n"
        f"\n"
        f"┌─────────────────────\n"
        f"│ Data points: *{s['points']}*\n"
        f"│ History: *{s['hours']:.1f}h / {max_h}h*\n"
        f"│ [{bar}]\n"
        f"│ Status: {ready}\n"
        f"└─────────────────────\n"
        f"\n"
        f"Data pertama: `{s['first'][:19]}`\n"
        f"Data terakhir: `{s['last'][:19]}`\n"
        f"\n"
        f"_Key: `{REDIS_KEY}`_",
        chat_id
    )


def handle_interval(args: list, chat_id: str) -> None:
    if not args:
        send_reply(f"Scan interval sekarang *{settings['scan_interval']}s* senpai.\nContoh: `/interval 180`", chat_id)
        return
    try:
        val = int(args[0])
        if not (60 <= val <= 3600):
            send_reply("Harus antara 60 sampai 3600 detik senpai.", chat_id)
            return
        settings["scan_interval"] = val
        send_reply(f"Oke senpai, scan interval jadi *{val}s* ({val // 60} menit).", chat_id)
    except ValueError:
        send_reply("Angkanya ga valid senpai, coba lagi.", chat_id)


def handle_heartbeat_cmd(args: list, chat_id: str) -> None:
    if not args:
        send_reply(f"Heartbeat sekarang *{settings['heartbeat_minutes']} menit* senpai.\nContoh: `/heartbeat 30`", chat_id)
        return
    try:
        val = int(args[0])
        if not (0 <= val <= 120):
            send_reply("Harus antara 0 sampai 120 menit senpai.", chat_id)
            return
        settings["heartbeat_minutes"] = val
        if val == 0:
            send_reply("Oke senpai, heartbeat dimatiin. Aku tetap jalan kok.", chat_id)
        else:
            send_reply(f"Siap, aku lapor setiap *{val} menit* ya senpai.", chat_id)
    except ValueError:
        send_reply("Angkanya ga valid senpai, coba lagi.", chat_id)


# =============================================================================
# Heartbeat
# =============================================================================
def should_send_heartbeat(now: datetime) -> bool:
    if settings["heartbeat_minutes"] == 0 or last_heartbeat_time is None:
        return False
    return (now - last_heartbeat_time).total_seconds() / 60 >= settings["heartbeat_minutes"]


def send_heartbeat() -> bool:
    hours = len(price_history) * settings["scan_interval"] / 3600
    max_h = settings["max_lookback_hours"]
    btc_str = f"${float(scan_stats['last_btc_price']):,.2f}" if scan_stats["last_btc_price"] else "N/A"
    eth_str = f"${float(scan_stats['last_eth_price']):,.2f}" if scan_stats["last_eth_price"] else "N/A"
    bar_filled = int(min(hours / max_h, 1.0) * 10)
    bar = "█" * bar_filled + "░" * (10 - bar_filled)
    data_status = f"✅ Penuh ({hours:.1f}h)" if hours >= max_h else f"⏳ {hours:.1f}h / {max_h}h"
    success = send_message(
        f"💓 *Lookback Bot — Laporan Rutin*\n"
        f"\n"
        f"Masih jalan senpai, tenang aja.\n"
        f"\n"
        f"*{settings['heartbeat_minutes']} menit terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ Scan: {scan_stats['count']}x\n"
        f"│ Errors: {scan_stats['errors']}x\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Harga terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Redis:*\n"
        f"[{bar}] {hours:.1f}h / {max_h}h\n"
        f"Status: {data_status}"
    )
    scan_stats["count"] = 0
    scan_stats["errors"] = 0
    return success


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
# Startup Message
# =============================================================================
def send_startup_message() -> None:
    hours = len(price_history) * settings["scan_interval"] / 3600
    max_h = settings["max_lookback_hours"]

    if len(price_history) > 0:
        history_info = (
            f"Data lama ketemu di Redis — *{hours:.1f}h* sudah tersimpan.\n"
            f"_Bot utama bisa langsung pakai._"
        )
    else:
        history_info = (
            f"Mulai dari nol, ngumpulin {max_h}h data dulu.\n"
            f"_Estimasi siap dalam {max_h} jam._"
        )

    send_message(
        f"*Lookback Bot* nyala senpai.\n"
        f"\n"
        f"Tugasku — kumpulin harga BTC/ETH tiap {settings['scan_interval']}s\n"
        f"dan simpen ke Redis buat bot utama.\n"
        f"\n"
        f"{history_info}\n"
        f"\n"
        f"📊 Scan interval: {settings['scan_interval']}s\n"
        f"💓 Heartbeat: setiap {settings['heartbeat_minutes']} menit\n"
        f"🗄️ Redis key: `{REDIS_KEY}`"
    )


# =============================================================================
# Main Loop
# =============================================================================
def main_loop() -> None:
    global last_heartbeat_time, _save_counter

    logger.info("=" * 60)
    logger.info("Lookback Bot starting - History Collector Mode")
    logger.info(f"Scan: {settings['scan_interval']}s | Lookback: {settings['max_lookback_hours']}h | Redis key: {REDIS_KEY}")
    logger.info("=" * 60)

    # Load existing history dari Redis
    load_history()
    now = datetime.now(timezone.utc)
    prune_history(now)
    logger.info(f"History after load+prune: {len(price_history)} points")

    # Start command polling thread
    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message()

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            # Heartbeat
            if should_send_heartbeat(now):
                if send_heartbeat():
                    last_heartbeat_time = now

            # Fetch & store harga
            result = fetch_prices()
            if result is None:
                scan_stats["errors"] += 1
                logger.warning("Failed to fetch prices")
            else:
                btc_price, eth_price, btc_updated, eth_updated = result
                scan_stats["last_btc_price"] = btc_price
                scan_stats["last_eth_price"] = eth_price
                scan_stats["count"] += 1

                if not is_data_fresh(now, btc_updated, eth_updated):
                    logger.warning("Data not fresh, skipping")
                else:
                    append_price(now, btc_price, eth_price)
                    prune_history(now)

                    _save_counter += 1
                    if _save_counter >= SAVE_EVERY_N_SCANS:
                        save_history()
                        _save_counter = 0

                    hours = len(price_history) * settings["scan_interval"] / 3600
                    logger.info(
                        f"Stored | BTC: ${float(btc_price):,.2f} | "
                        f"ETH: ${float(eth_price):,.2f} | "
                        f"History: {hours:.1f}h / {settings['max_lookback_hours']}h "
                        f"({len(price_history)} pts)"
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
        logger.warning("TELEGRAM_BOT_TOKEN not set — heartbeat disabled")
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID not set — heartbeat disabled")
    if not UPSTASH_REDIS_URL:
        logger.warning("UPSTASH_REDIS_REST_URL not set — data will NOT persist!")
    main_loop()
