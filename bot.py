"""
=============================================================================
PATCH: Auto SL/TP berdasarkan % dari entry gap
=============================================================================
Cara pakai:
  1. Tempel setiap blok di bawah ke bot.py sesuai instruksi komentar #INSERT_AT
  2. Tidak ada kode lama yang perlu diubah, semua additive
=============================================================================
"""


# =============================================================================
# [1] #INSERT_AT: setelah baris `peak_gap: Optional[float] = None`
#     di bagian Global State
# =============================================================================

entry_gap_value: Optional[float] = None   # gap (float) saat ENTRY signal terpicu


# =============================================================================
# [2] #INSERT_AT: di dalam dict `settings = { ... }`
#     tambahkan dua key baru sebelum tanda kurung tutup `}`
# =============================================================================

#   "tp_pct": 1.0,   # TP: gap bergerak X% kembali dari entry gap
#   "sl_pct": 1.0,   # SL: gap bergerak X% menjauhi dari entry gap


# =============================================================================
# [3] #INSERT_AT: setelah fungsi `build_peak_cancelled_message()`
#     tambahkan dua message builder baru
# =============================================================================

def build_tp_message(
    btc_ret: "Decimal", eth_ret: "Decimal", gap: "Decimal",
    entry_gap: float, tp_level: float
) -> str:
    lb = get_lookback_label()
    return (
        f"Ara ara~!!! TP kena sayangku~!!! Ufufufu... ✨\n"
        f"🎯 *TAKE PROFIT*\n"
        f"\n"
        f"Gap sudah konvergen sesuai target Akeno~\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:    {format_value(btc_ret)}%\n"
        f"│ ETH:    {format_value(eth_ret)}%\n"
        f"│ Gap:    {format_value(gap)}%\n"
        f"│ Entry:  {entry_gap:+.2f}%\n"
        f"│ TP hit: {tp_level:+.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Akeno senang~ Misi sukses untukmu, sayangku! ⚡"
    )


def build_sl_message(
    btc_ret: "Decimal", eth_ret: "Decimal", gap: "Decimal",
    entry_gap: float, sl_level: float
) -> str:
    lb = get_lookback_label()
    return (
        f"………\n"
        f"⛔ *STOP LOSS*\n"
        f"\n"
        f"Ara ara~ maaf ya sayangku... gapnya malah melebar dari entry. "
        f"Akeno sudah berusaha keras. (◕ω◕)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:    {format_value(btc_ret)}%\n"
        f"│ ETH:    {format_value(eth_ret)}%\n"
        f"│ Gap:    {format_value(gap)}%\n"
        f"│ Entry:  {entry_gap:+.2f}%\n"
        f"│ SL hit: {sl_level:+.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu ya sayangku. Akeno scan ulang~ ⚡\n"
        f"Lain kali petir Akeno pasti lebih tepat. (◕‿◕)"
    )


# =============================================================================
# [4] #INSERT_AT: setelah fungsi `build_sl_message()` di atas
#     fungsi inti SL/TP checker — dipanggil dari evaluate_and_transition()
# =============================================================================

def check_sltp(gap_float: float, btc_ret: "Decimal", eth_ret: "Decimal", gap: "Decimal") -> bool:
    """
    Cek apakah SL atau TP terpicu saat Mode TRACK.
    Return True jika terpicu (mode sudah di-reset ke SCAN di dalam sini).
    Dipanggil SEBELUM logika exit/invalidation yang sudah ada.
    """
    global current_mode, active_strategy, entry_gap_value

    if entry_gap_value is None or active_strategy is None:
        return False

    tp_pct = settings["tp_pct"]
    sl_pct = settings["sl_pct"]

    if active_strategy == Strategy.S1:
        # S1 = Long BTC / Short ETH → gap harusnya turun (konvergen)
        # TP: gap turun tp_pct dari entry  →  gap <= entry - tp_pct
        # SL: gap naik  sl_pct dari entry  →  gap >= entry + sl_pct
        tp_level = entry_gap_value - tp_pct
        sl_level = entry_gap_value + sl_pct

        if gap_float <= tp_level:
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, tp_level))
            logger.info(f"TP S1 triggered. Entry: {entry_gap_value:.2f}%, Now: {gap_float:.2f}%, TP: {tp_level:.2f}%")
            current_mode, active_strategy, entry_gap_value = Mode.SCAN, None, None
            return True

        if gap_float >= sl_level:
            send_alert(build_sl_message(btc_ret, eth_ret, gap, entry_gap_value, sl_level))
            logger.info(f"SL S1 triggered. Entry: {entry_gap_value:.2f}%, Now: {gap_float:.2f}%, SL: {sl_level:.2f}%")
            current_mode, active_strategy, entry_gap_value = Mode.SCAN, None, None
            return True

    elif active_strategy == Strategy.S2:
        # S2 = Long ETH / Short BTC → gap harusnya naik (konvergen ke 0 dari negatif)
        # TP: gap naik  tp_pct dari entry  →  gap >= entry + tp_pct
        # SL: gap turun sl_pct dari entry  →  gap <= entry - sl_pct
        tp_level = entry_gap_value + tp_pct
        sl_level = entry_gap_value - sl_pct

        if gap_float >= tp_level:
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, tp_level))
            logger.info(f"TP S2 triggered. Entry: {entry_gap_value:.2f}%, Now: {gap_float:.2f}%, TP: {tp_level:.2f}%")
            current_mode, active_strategy, entry_gap_value = Mode.SCAN, None, None
            return True

        if gap_float <= sl_level:
            send_alert(build_sl_message(btc_ret, eth_ret, gap, entry_gap_value, sl_level))
            logger.info(f"SL S2 triggered. Entry: {entry_gap_value:.2f}%, Now: {gap_float:.2f}%, SL: {sl_level:.2f}%")
            current_mode, active_strategy, entry_gap_value = Mode.SCAN, None, None
            return True

    return False


# =============================================================================
# [5] #INSERT_AT: di dalam `evaluate_and_transition()`, bagian PEAK_WATCH
#     tepat setelah baris `current_mode = Mode.TRACK` untuk S1 maupun S2
#     tambahkan 1 baris untuk simpan entry gap
# =============================================================================

# Contoh untuk S1 (cari blok ini di kode asli, tambahkan baris entry_gap_value):
#
#   elif peak_gap - gap_float >= peak_reversal:
#       active_strategy = Strategy.S1
#       current_mode = Mode.TRACK
#       entry_gap_value = gap_float                    # ← TAMBAHKAN INI
#       send_alert(build_entry_message(...))
#
# Contoh untuk S2 (sama):
#
#   elif gap_float - peak_gap >= peak_reversal:
#       active_strategy = Strategy.S2
#       current_mode = Mode.TRACK
#       entry_gap_value = gap_float                    # ← TAMBAHKAN INI
#       send_alert(build_entry_message(...))


# =============================================================================
# [6] #INSERT_AT: di dalam `evaluate_and_transition()`, bagian TRACK
#     tambahkan call check_sltp() sebagai baris PERTAMA di blok TRACK
#     (sebelum `if abs(gap_float) <= exit_thresh:`)
# =============================================================================

# Contoh (cari `elif current_mode == Mode.TRACK:` di kode asli):
#
#   elif current_mode == Mode.TRACK:
#       if check_sltp(gap_float, btc_ret, eth_ret, gap):   # ← TAMBAHKAN INI
#           return                                          # ← TAMBAHKAN INI
#
#       if abs(gap_float) <= exit_thresh:   # ← baris lama, tidak diubah
#           ...


# =============================================================================
# [7] #INSERT_AT: command handler baru — taruh setelah handle_peak_command()
# =============================================================================

def handle_sltp_command(args: list, reply_chat: str) -> None:
    if not args:
        entry_info = (
            f"\n*Entry gap sekarang:* `{entry_gap_value:+.2f}%`"
            if entry_gap_value is not None else ""
        )
        send_reply(
            f"🎯 *SL/TP Otomatis — Akeno yang jaga~* Ufufufu... (◕‿◕)\n"
            f"\n"
            f"TP: *{settings['tp_pct']}%* dari entry gap\n"
            f"SL: *{settings['sl_pct']}%* dari entry gap\n"
            f"{entry_info}\n"
            f"\n"
            f"*Cara kerja:*\n"
            f"S1 → TP saat gap turun {settings['tp_pct']}%, SL saat naik {settings['sl_pct']}%\n"
            f"S2 → TP saat gap naik {settings['tp_pct']}%, SL saat turun {settings['sl_pct']}%\n"
            f"\n"
            f"Usage:\n"
            f"`/sltp tp <nilai>` — ubah TP %\n"
            f"`/sltp sl <nilai>` — ubah SL %",
            reply_chat,
        )
        return

    if len(args) < 2:
        send_reply(
            "Ara ara~ kurang lengkap~ `/sltp tp <nilai>` atau `/sltp sl <nilai>` ya~ (◕ω◕)",
            reply_chat,
        )
        return

    try:
        key = args[0].lower()
        value = float(args[1])
        if value <= 0 or value > 10:
            send_reply(
                "Ara ara~ nilainya harus antara 0 sampai 10~ (◕ω◕)",
                reply_chat,
            )
            return
        if key == "tp":
            settings["tp_pct"] = value
            send_reply(
                f"Ufufufu~ TP sekarang *{value}%* dari entry gap~ "
                f"Akeno catat baik-baik untukmu~ (◕‿◕)",
                reply_chat,
            )
        elif key == "sl":
            settings["sl_pct"] = value
            send_reply(
                f"Ara ara~ SL sekarang *{value}%* dari entry gap~ "
                f"Siap sayangku, Akeno jaga~ (◕‿◕)",
                reply_chat,
            )
        else:
            send_reply(
                "Gunakan `tp` atau `sl` ya~ (◕ω◕)",
                reply_chat,
            )
        logger.info(f"SL/TP {key} changed to {value}")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


# =============================================================================
# [8] #INSERT_AT: di dalam process_commands(), tambahkan 1 elif baru
#     setelah `elif command == "/peak":`
# =============================================================================

#   elif command == "/sltp":
#       handle_sltp_command(args, reply_chat)


# =============================================================================
# [9] #INSERT_AT: di handle_settings_command(), tambahkan 2 baris info
#     setelah baris `f"🎯 Peak Reversal: ..."`
# =============================================================================

#   f"🛑 SL: {settings['sl_pct']}% dari entry gap\n"
#   f"✅ TP: {settings['tp_pct']}% dari entry gap\n"


# =============================================================================
# [10] #INSERT_AT: di handle_help_command(), tambahkan ke bagian Setting
#      setelah baris `/peak`:
# =============================================================================

#   "`/sltp` - lihat SL/TP otomatis\n"
#   "`/sltp tp <val>` - ubah TP %\n"
#   "`/sltp sl <val>` - ubah SL %\n"


# =============================================================================
# RINGKASAN PERUBAHAN
# =============================================================================
# Global    : + entry_gap_value (float | None)
# settings  : + tp_pct (default 1.0), sl_pct (default 1.0)
# Functions : + build_tp_message(), build_sl_message(), check_sltp()
#             + handle_sltp_command()
# Hooks     : evaluate_and_transition() → simpan entry_gap_value saat ENTRY
#                                       → call check_sltp() di blok TRACK
#             process_commands()        → route /sltp
#             handle_settings_command() → tampilkan SL/TP
#             handle_help_command()     → dokumentasi /sltp
# =============================================================================
