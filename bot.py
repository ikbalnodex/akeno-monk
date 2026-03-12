"""
PATCH — monk_bot_b.py
Enhanced Early Signal: Lead Chart + Neutral Alert

Cara integrasi:
  1. Tambah 2 baris di GLOBAL STATE
  2. Tambah 2 key di settings{}
  3. Paste fungsi-fungsi baru
  4. Ganti handle_status_command
  5. Tambah 1 baris di main_loop
  6. Tambah /earlystatus ke dispatch
"""

# =============================================================================
# STEP 1 — GLOBAL STATE
# Tambahkan di bawah baris: exit_confirm_count: int = 0
# =============================================================================
neutral_last_alerted:         None  # Optional[datetime]
neutral_alerted_this_session: False # bool


# =============================================================================
# STEP 2 — SETTINGS
# Tambahkan 2 key ini ke dalam dict settings = { ... }
# =============================================================================
_SETTINGS_TO_ADD = {
    "neutral_alert_buffer":   0.15,   # gap < 0.15% = zona neutral
    "neutral_alert_cooldown": 10,     # menit cooldown antar neutral alert
}


# =============================================================================
# STEP 3A — FUNGSI: build_lead_chart
# Taruh setelah fungsi build_market_snapshot()
# =============================================================================
def build_lead_chart(gap_f: float, entry_threshold: float) -> str:
    """
    Grafik horizontal number line.

    Contoh ETH lead +0.5%:
                      🟡 +0.50%
    [BTC lead]─────────●──────[ETH lead]
                   | Netral |

    Contoh BTC lead -0.8%:
           🟠 -0.80%
    [BTC lead]──●─────────────[ETH lead]
                   | Netral |

    Contoh Neutral:
                      ⚪ 0%
    [BTC lead]──────────●──────[ETH lead]
                   | Netral |
    """
    HALF           = 10
    neutral_buffer = settings.get("neutral_alert_buffer", 0.15)

    # Posisi titik: 0 = ujung kiri, HALF = tengah, HALF*2 = ujung kanan
    clamped = max(-entry_threshold, min(entry_threshold, gap_f))
    pos     = int((clamped / entry_threshold) * HALF) + HALF
    pos     = max(0, min(HALF * 2, pos))

    is_neutral = abs(gap_f) < neutral_buffer
    if is_neutral:
        pos = HALF

    # Bar
    bar     = ["─"] * (HALF * 2 + 1)
    bar[pos] = "●"
    bar_str  = "".join(bar)

    LEFT  = "[BTC lead]"
    RIGHT = "[ETH lead]"

    # Label nilai gap di atas titik
    prefix_len = len(LEFT)
    dot_offset = prefix_len + pos

    if is_neutral:
        gap_label = "⚪ 0%"
    elif gap_f > 0:
        gap_label = f"🟡 +{gap_f:.2f}%"
    else:
        gap_label = f"🟠 {gap_f:.2f}%"

    label_offset = max(0, dot_offset - len(gap_label) // 2)
    value_line   = " " * label_offset + gap_label

    # Label neutral di bawah bar
    neutral_label  = "| Netral |"
    neutral_offset = prefix_len + HALF - len(neutral_label) // 2
    neutral_line   = " " * neutral_offset + neutral_label

    # Status & progress
    gap_abs = abs(gap_f)
    if is_neutral:
        hint      = "💤 BTC & ETH seimbang — tidak ada lead"
        signal    = ""
        prog_line = ""
    elif gap_f > 0:
        pct       = min(100, int(gap_abs / entry_threshold * 100))
        filled    = "█" * min(10, int(gap_abs / entry_threshold * 10))
        empty     = "░" * (10 - len(filled))
        hint      = "Setup: *S1* — Long BTC / Short ETH"
        signal    = (
            f"⚡ *Di threshold! Kandidat S1*"
            if gap_abs >= entry_threshold
            else f"⏳ Butuh {entry_threshold - gap_abs:.2f}% lagi → entry S1"
        )
        prog_line = f"`[{filled}{empty}]` {pct}% menuju ±{entry_threshold:.1f}%"
    else:
        pct       = min(100, int(gap_abs / entry_threshold * 100))
        filled    = "█" * min(10, int(gap_abs / entry_threshold * 10))
        empty     = "░" * (10 - len(filled))
        hint      = "Setup: *S2* — Long ETH / Short BTC"
        signal    = (
            f"⚡ *Di threshold! Kandidat S2*"
            if gap_abs >= entry_threshold
            else f"⏳ Butuh {entry_threshold - gap_abs:.2f}% lagi → entry S2"
        )
        prog_line = f"`[{filled}{empty}]` {pct}% menuju ±{entry_threshold:.1f}%"

    lines = [
        f"`{value_line}`",
        f"`{LEFT}{bar_str}{RIGHT}`",
        f"`{neutral_line}`",
        f"",
        f"_{hint}_",
    ]
    if prog_line:
        lines.append(prog_line)
    if signal:
        lines.append(signal)

    return "\n".join(lines)


# =============================================================================
# STEP 3B — FUNGSI: check_convergence_neutral_alert
# Taruh setelah build_lead_chart()
# =============================================================================
def check_convergence_neutral_alert(
    gap_float: float,
    btc_ret:   float,
    eth_ret:   float,
    btc_price: float,
    eth_price: float,
) -> None:
    """
    Alert otomatis saat gap kembali ke zona neutral.
    Berlaku semua mode: SCAN / PEAK_WATCH / TRACK.
    Ada cooldown agar tidak spam.
    """
    global neutral_last_alerted, neutral_alerted_this_session

    neutral_buffer = settings.get("neutral_alert_buffer",   0.15)
    cooldown_min   = settings.get("neutral_alert_cooldown", 10)
    lb             = get_lookback_label()
    is_neutral     = abs(gap_float) < neutral_buffer

    if not is_neutral:
        neutral_alerted_this_session = False   # reset, siap alert lagi nanti
        return

    now = datetime.now(timezone.utc)
    if neutral_last_alerted is not None:
        if (now - neutral_last_alerted).total_seconds() / 60 < cooldown_min:
            return   # masih cooldown

    if neutral_alerted_this_session:
        return

    neutral_last_alerted         = now
    neutral_alerted_this_session = True

    # ── Isi pesan tergantung mode ─────────────────────────────────────────────
    if current_mode == Mode.TRACK and active_strategy is not None and entry_gap_value is not None:
        leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_float)
        net_str  = f"{net:+.2f}%" if net is not None else "N/A"
        capital  = settings["capital"]
        usd_str  = ""
        if net is not None and capital > 0:
            usd_net = net / 100 * capital
            usd_str = f" (${usd_net:+.2f})"

        et       = settings["exit_threshold"]
        strat_dir = (
            "Long BTC / Short ETH"
            if active_strategy == Strategy.S1
            else "Long ETH / Short BTC"
        )

        msg = (
            f"⚪ *Gap Kembali Neutral — Posisi {active_strategy.value}*\n"
            f"\n"
            f"BTC & ETH sudah bergerak seimbang\n"
            f"_Ini bisa sinyal exit, atau jeda sebelum diverge lagi_\n"
            f"\n"
            f"┌─────────────────────\n"
            f"│ Strategi:  *{strat_dir}*\n"
            f"│ Entry gap: {entry_gap_value:+.2f}%\n"
            f"│ Gap now:   *{gap_float:+.2f}%* ← mendekati nol\n"
            f"│ BTC ({lb}): {btc_ret:+.2f}% — ${btc_price:,.2f}\n"
            f"│ ETH ({lb}): {eth_ret:+.2f}% — ${eth_price:,.2f}\n"
            f"├─────────────────────\n"
            f"│ Net P&L:   *{net_str}{usd_str}*\n"
            f"└─────────────────────\n"
            f"\n"
            f"_`/pnl` untuk detail | `/health` cek posisi~_"
        )

    elif current_mode == Mode.PEAK_WATCH and peak_strategy is not None:
        msg = (
            f"⚪ *Lead Hilang — Peak Watch {peak_strategy.value}*\n"
            f"\n"
            f"Gap kembali neutral sebelum konfirmasi entry\n"
            f"\n"
            f"┌─────────────────────\n"
            f"│ Peak was:  {peak_gap:+.2f}%\n"
            f"│ Gap now:   *{gap_float:+.2f}%*\n"
            f"│ BTC ({lb}): {btc_ret:+.2f}%\n"
            f"│ ETH ({lb}): {eth_ret:+.2f}%\n"
            f"└─────────────────────\n"
            f"\n"
            f"_Bot kembali scan. Tunggu lead baru~_"
        )

    else:
        # SCAN — hanya kirim kalau sebelumnya ada lead (early signal aktif)
        if es_leader is None and not es_lead_alerted:
            return

        prev_strat = (
            "S1 (Long BTC/Short ETH)"
            if es_leader == "ETH"
            else "S2 (Long ETH/Short BTC)"
        )
        msg = (
            f"⚪ *Market Neutral — BTC & ETH Seimbang*\n"
            f"\n"
            f"Gap mengecil ke zona netral\n"
            f"\n"
            f"┌─────────────────────\n"
            f"│ Gap now:   *{gap_float:+.2f}%*\n"
            f"│ BTC ({lb}): {btc_ret:+.2f}% — ${btc_price:,.2f}\n"
            f"│ ETH ({lb}): {eth_ret:+.2f}% — ${eth_price:,.2f}\n"
            f"└─────────────────────\n"
            f"\n"
            f"_Sinyal {prev_strat} melemah — menunggu lead baru~_"
        )

    send_alert(msg)
    logger.info(f"Neutral alert sent | gap: {gap_float:+.2f}% | mode: {current_mode.value}")


# =============================================================================
# STEP 4 — GANTI handle_status_command
# Copy-paste seluruh fungsi ini, hapus yang lama
# =============================================================================
def handle_status_command(reply_chat: str) -> None:
    hours_data = len(price_history) * settings["scan_interval"] / 3600
    lookback   = settings["lookback_hours"]
    ready      = (
        f"✅ {hours_data:.1f}h"
        if hours_data >= lookback
        else f"⏳ {hours_data:.1f}h / {lookback}h"
    )
    peak_s = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    last_r = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum"

    gap_now = scan_stats.get("last_gap")
    btc_r   = scan_stats.get("last_btc_ret")
    eth_r   = scan_stats.get("last_eth_ret")
    et      = settings["entry_threshold"]

    # ── Lead Chart (BARU) ─────────────────────────────────────────────────────
    lead_chart_block = ""
    if gap_now is not None and btc_r is not None and eth_r is not None:
        lead_chart_block = (
            f"\n*📡 Lead Monitor:*\n"
            + build_lead_chart(float(gap_now), et)
            + "\n"
        )

    # ── Early Signal Phase Status (BARU) ──────────────────────────────────────
    early_phase_block = ""
    if settings.get("early_signal_enabled", True):
        if es_leader is not None:
            lead_since  = es_lead_start_ts.strftime("%H:%M UTC") if es_lead_start_ts else "—"
            strat_s     = "S1" if es_leader == "ETH" else "S2"
            phase2_s    = "✅ Terkirim" if es_move_alerted else "⏳ Memantau 5m"
            chg         = _get_5m_changes()
            eth_5ms     = f"{chg['eth_5m']:+.2f}%" if chg.get("eth_5m") is not None else "—"
            btc_5ms     = f"{chg['btc_5m']:+.2f}%" if chg.get("btc_5m") is not None else "—"
            early_phase_block = (
                f"*Early Signal:*\n"
                f"┌─────────────────────\n"
                f"│ Phase-1: ✅ *{es_leader} lead* → {strat_s} | sejak {lead_since}\n"
                f"│ Phase-2: {phase2_s} | ETH {eth_5ms} BTC {btc_5ms}\n"
                f"└─────────────────────\n"
            )
        else:
            early_phase_block = (
                f"*Early Signal:* 💤 Tidak ada lead aktif\n"
            )

    # ── SCAN section ──────────────────────────────────────────────────────────
    scan_section = ""
    if current_mode == Mode.SCAN:
        gap_str = format_value(gap_now) + "%" if gap_now is not None else "N/A"
        driver_line = ""
        if gap_now is not None and btc_r is not None and eth_r is not None:
            drv, drv_e, drv_ex = analyze_gap_driver(float(btc_r), float(eth_r), float(gap_now))
            driver_line = f"│ Driver: {drv_e} {drv} — _{drv_ex}_\n"
        curr_r, _, _, _, pct_r = calc_ratio_percentile()
        ratio_line = f"│ Ratio:  {curr_r:.5f} ({pct_r}th pct)\n" if curr_r and pct_r is not None else ""
        scan_section = (
            f"\n*Gap ({lookback}h):*\n"
            f"┌─────────────────────\n"
            f"│ BTC: {format_value(btc_r)}% | ETH: {format_value(eth_r)}%\n"
            f"│ Gap: *{gap_str}* (threshold ±{et}%)\n"
            f"{driver_line}"
            f"{ratio_line}"
            f"└─────────────────────\n"
        )

    # ── PEAK_WATCH section ────────────────────────────────────────────────────
    peak_section = ""
    if current_mode == Mode.PEAK_WATCH and peak_gap is not None:
        gap_now_f    = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else peak_gap
        reversal_now = abs(peak_gap - gap_now_f)
        needed       = settings["peak_reversal"]
        filled       = min(10, int(reversal_now / needed * 10) if needed > 0 else 10)
        bar          = "█" * filled + "░" * (10 - filled)
        peak_section = (
            f"\n*Peak Watch {peak_strategy.value if peak_strategy else ''}:*\n"
            f"┌─────────────────────\n"
            f"│ Peak:    {peak_gap:+.2f}%\n"
            f"│ Gap now: {format_value(scan_stats['last_gap'])}%\n"
            f"│ Reversal: `{bar}` {reversal_now:.2f}% / {needed}%\n"
            f"└─────────────────────\n"
        )

    # ── TRACK section ─────────────────────────────────────────────────────────
    track_section = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        et_t      = settings["exit_threshold"]
        sl        = settings["sl_pct"]
        tpl       = et_t if active_strategy == Strategy.S1 else -et_t
        tsl       = (
            trailing_gap_best + sl if active_strategy == Strategy.S1
            else trailing_gap_best - sl
        )
        eth_tp, _ = calc_tp_target_price(active_strategy)
        eth_sl, _ = calc_eth_price_at_gap(tsl)
        eth_tp_s  = f"${eth_tp:,.2f}" if eth_tp else "N/A"
        eth_sl_s  = f"${eth_sl:,.2f}" if eth_sl else "N/A"
        gap_now_f = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else entry_gap_value
        moved     = abs(entry_gap_value - gap_now_f)

        leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_now_f)
        net_str   = f"{net:+.2f}%" if net is not None else "N/A"
        health_e, health_d = get_pairs_health(active_strategy, gap_now_f)

        driver_now_line = ""
        if scan_stats.get("last_btc_ret") is not None:
            drv, drv_e, _ = analyze_gap_driver(
                float(scan_stats["last_btc_ret"]),
                float(scan_stats["last_eth_ret"]),
                gap_now_f,
            )
            driver_now_line = f"│ Driver:   {drv_e} {drv}\n"

        pnl_detail = ""
        if leg_e is not None and leg_b is not None:
            if active_strategy == Strategy.S1:
                pnl_detail = f"│ Long BTC:  {leg_b:+.2f}%\n│ Short ETH: {leg_e:+.2f}%\n"
            else:
                pnl_detail = f"│ Long ETH:  {leg_e:+.2f}%\n│ Short BTC: {leg_b:+.2f}%\n"

        track_section = (
            f"\n*Posisi aktif {active_strategy.value}:*\n"
            f"┌─────────────────────\n"
            f"│ Entry:    {entry_gap_value:+.2f}%\n"
            f"│ Gap now:  {format_value(scan_stats['last_gap'])}%\n"
            f"│ Moved:    ~{moved:.2f}%\n"
            f"│ Best:     {trailing_gap_best:+.2f}%\n"
            f"{driver_now_line}"
            f"{pnl_detail}"
            f"│ Net P&L:  {net_str}\n"
            f"│ Health:   {health_e} {health_d}\n"
            f"│ TP:       {tpl:+.2f}% → ETH {eth_tp_s}\n"
            f"│ TSL:      {tsl:+.2f}% → ETH {eth_sl_s}\n"
            f"└─────────────────────\n"
        )

    send_reply(
        f"📊 *Status Bot*\n"
        f"Mode: *{current_mode.value}* | Peak: {peak_s}\n"
        f"Strategi: {active_strategy.value if active_strategy else '—'}\n"
        f"\n"
        f"{lead_chart_block}"          # ← lead chart
        f"{early_phase_block}"         # ← phase status
        f"{scan_section}"
        f"{peak_section}"
        f"{track_section}"
        f"History: {ready} | Redis: {last_r} 🔒\n",
        reply_chat,
    )


# =============================================================================
# STEP 5 — main_loop PATCH
# Tambahkan ini SETELAH baris evaluate_and_transition(...) di main_loop
# =============================================================================
_MAIN_LOOP_ADDITION = """
# ← Paste ini setelah evaluate_and_transition(...)
if price_then is not None and scan_stats.get("last_gap") is not None:
    check_convergence_neutral_alert(
        gap_float = float(scan_stats["last_gap"]),
        btc_ret   = float(btc_ret),
        eth_ret   = float(eth_ret),
        btc_price = float(price_data.btc_price),
        eth_price = float(price_data.eth_price),
    )
"""


# =============================================================================
# STEP 6 — TAMBAHKAN ke dispatch di process_commands()
# =============================================================================
_DISPATCH_ADDITION = """
# Tambahkan 1 baris ini ke dict dispatch:
"/earlystatus": lambda: handle_earlystatus_command(chat_id),
"""


# =============================================================================
# STEP 7 — COMMAND BARU /earlystatus
# Paste fungsi ini ke file, setelah handle_status_command
# =============================================================================
def handle_earlystatus_command(reply_chat: str) -> None:
    """/earlystatus — tampilkan lead chart + phase status detail"""
    gap_now = scan_stats.get("last_gap")
    btc_r   = scan_stats.get("last_btc_ret")
    eth_r   = scan_stats.get("last_eth_ret")

    if gap_now is None:
        send_reply("Belum ada data scan. Tunggu sebentar~", reply_chat)
        return

    gap_f  = float(gap_now)
    btc_rf = float(btc_r) if btc_r is not None else 0.0
    eth_rf = float(eth_r) if eth_r is not None else 0.0
    et     = settings["entry_threshold"]
    lb     = get_lookback_label()

    chart = build_lead_chart(gap_f, et)

    if es_leader is not None:
        lead_since  = es_lead_start_ts.strftime("%H:%M UTC") if es_lead_start_ts else "—"
        elapsed_min = (
            int((datetime.now(timezone.utc) - es_lead_start_ts).total_seconds() / 60)
            if es_lead_start_ts else 0
        )
        elapsed_str = f"{elapsed_min}m" if elapsed_min < 60 else f"{elapsed_min//60}h {elapsed_min%60}m"
        strat_name  = "S1 — Long BTC / Short ETH" if es_leader == "ETH" else "S2 — Long ETH / Short BTC"

        chg      = _get_5m_changes()
        eth_5m   = chg.get("eth_5m")
        btc_5m   = chg.get("btc_5m")
        move_th  = settings["early_5m_move"]
        eth_fill = min(10, int(abs(eth_5m or 0) / move_th * 10))
        btc_fill = min(10, int(abs(btc_5m or 0) / move_th * 10))
        eth_bar  = "█" * eth_fill + "░" * (10 - eth_fill)
        btc_bar  = "█" * btc_fill + "░" * (10 - btc_fill)
        eth_5ms  = f"{eth_5m:+.2f}%" if eth_5m is not None else "—"
        btc_5ms  = f"{btc_5m:+.2f}%" if btc_5m is not None else "—"

        phase_block = (
            f"*Phase-1: ✅ Aktif*\n"
            f"┌─────────────────────\n"
            f"│ Leader:  *{es_leader}* sejak {lead_since} ({elapsed_str})\n"
            f"│ Gap:     *{es_lead_gap:+.2f}%*\n"
            f"│ Setup:   *{strat_name}*\n"
            f"└─────────────────────\n"
            f"\n"
            f"*Phase-2: {'✅ Alert Terkirim' if es_move_alerted else '⏳ Memantau 5m'}*\n"
            f"┌─────────────────────\n"
            f"│ ETH 5m: `[{eth_bar}]` {eth_5ms} / ±{move_th}%\n"
            f"│ BTC 5m: `[{btc_bar}]` {btc_5ms} / ±{move_th}%\n"
            f"└─────────────────────\n"
        )
    else:
        phase_block = (
            f"*Phase-1: 💤 Tidak ada lead*\n"
            f"_Gap {gap_f:+.2f}% — threshold lead ±{settings['early_lead_gap']}%_\n\n"
            f"*Phase-2: 💤 Menunggu Phase-1*\n"
        )

    nb = settings.get("neutral_alert_buffer", 0.15)
    if abs(gap_f) < nb:
        neutral_str = f"\n⚪ *Zona Neutral* — gap {gap_f:+.2f}% (buffer ±{nb}%)"
    else:
        neutral_str = f"\n_Neutral zone: ±{nb}%_"

    send_reply(
        f"📡 *Early Signal Status*\n"
        f"_{'✅ ON' if settings.get('early_signal_enabled', True) else '❌ OFF'} | "
        f"Lead min: ±{settings['early_lead_gap']}% | 5m: ±{settings['early_5m_move']}%_\n"
        f"\n"
        f"{chart}\n"
        f"\n"
        f"{phase_block}"
        f"{neutral_str}\n"
        f"\n"
        f"_`/earlysignal on|off` toggle | `/analysis` full context~_",
        reply_chat,
    )
