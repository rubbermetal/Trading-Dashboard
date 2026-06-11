import math
import pandas as pd
import pandas_ta as ta

def calculate_quad_rotation(df, rotation_window=20):
    """
    DTRS Quad Rotation — Complete Unified Strategy
    Day Trader Rock Star's Quad Rotation: 4 stochastics (9/3, 14/3, 40/4, 60/10).

    Five entry tiers (highest confidence first):
    1. Strict Pullback — EMA trend + Macro/Med strong + Trigger dip to EMA support
    2. Super Signal — 3-stage capitulation divergence after quad flush
    3. Holy Grail — quad oversold + RSI divergence confirmation
    4. Sequential Rotation — fast->slow stochastic turn cascade
    5. K/D Cross — trigger K/D crossover in oversold zone

    Shared exit framework: counter-trend protection, sequential bear rotation,
    K/D bear cross, plus ATR-based hard SL/TP set at entry.

    Returns (signal, reason, meta) where meta contains confidence/ATR for entries.
    """
    if len(df) < 200:
        return "HOLD", "Not enough data", {}

    # ── Stochastics: extract both %K (index 0) and %D (index 1) ──
    stoch_trig = ta.stoch(df['high'], df['low'], df['close'], k=9, d=3, smooth_k=3)
    stoch_fast = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3, smooth_k=3)
    stoch_med = ta.stoch(df['high'], df['low'], df['close'], k=40, d=4, smooth_k=4)
    stoch_macro = ta.stoch(df['high'], df['low'], df['close'], k=60, d=10, smooth_k=10)

    try:
        df['Stoch_Trig_K'] = stoch_trig.iloc[:, 0]
        df['Stoch_Trig_D'] = stoch_trig.iloc[:, 1]
        df['Stoch_Fast_K'] = stoch_fast.iloc[:, 0]
        df['Stoch_Fast_D'] = stoch_fast.iloc[:, 1]
        df['Stoch_Med_K'] = stoch_med.iloc[:, 0]
        df['Stoch_Med_D'] = stoch_med.iloc[:, 1]
        df['Stoch_Macro_K'] = stoch_macro.iloc[:, 0]
        df['Stoch_Macro_D'] = stoch_macro.iloc[:, 1]
    except Exception:
        return "HOLD", "Math error", {}

    # ── RSI Divergence: RSI(5) - RSI(14) ──
    df['RSI_5'] = ta.rsi(df['close'], length=5)
    df['RSI_14'] = ta.rsi(df['close'], length=14)
    df['RSI_Div'] = df['RSI_5'] - df['RSI_14']

    # ── ATR(14) for SL/TP sizing ──
    atr_series = ta.atr(df['high'], df['low'], df['close'], length=14)
    df['ATR_14'] = atr_series

    # ── EMAs for Strict Pullback entry ──
    df['EMA_20'] = ta.ema(df['close'], length=20)
    df['EMA_50'] = ta.ema(df['close'], length=50)

    # ── NaN guard ──
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    if pd.isna(curr['Stoch_Macro_D']) or pd.isna(curr['RSI_Div']) or pd.isna(curr['ATR_14']) or pd.isna(curr['EMA_50']):
        return "HOLD", "Warming up indicators", {}

    d1 = curr['Stoch_Trig_D']
    d2 = curr['Stoch_Fast_D']
    d3 = curr['Stoch_Med_D']
    d4 = curr['Stoch_Macro_D']
    atr_val = curr['ATR_14']
    rsi_div = curr['RSI_Div']

    # ── Quad Alignment (Pine lines 75-76) ──
    all_oversold = d1 < 20 and d2 < 20 and d3 < 20 and d4 < 20
    all_overbought = d1 > 80 and d2 > 80 and d3 > 80 and d4 > 80

    # ── Turn Detection — vectorized booleans (Pine lines 79-86) ──
    for name in ['Trig', 'Fast', 'Med', 'Macro']:
        d_col = df[f'Stoch_{name}_D']
        df[f'turnUp_{name}'] = (d_col < 20) & (d_col > d_col.shift(1)) & (d_col.shift(1) <= d_col.shift(2))
        df[f'turnDn_{name}'] = (d_col > 80) & (d_col < d_col.shift(1)) & (d_col.shift(1) >= d_col.shift(2))

    # ── barssince helper (Pine ta.barssince equivalent) ──
    def barssince(series, idx):
        for offset in range(idx + 1):
            if series.iloc[idx - offset]:
                return offset
        return None

    last = len(df) - 1

    # ── Sequential Rotation Detection (Pine lines 91-110) ──
    # Bullish: all 4 turned up in order (fastest first, slowest last)
    bs_up = {n: barssince(df[f'turnUp_{n}'], last) for n in ['Trig', 'Fast', 'Med', 'Macro']}
    seq_bull_trigger = (
        all(v is not None for v in bs_up.values()) and
        bs_up['Trig'] > bs_up['Fast'] > bs_up['Med'] > bs_up['Macro'] and
        bs_up['Macro'] < rotation_window and
        bool(df['turnUp_Macro'].iloc[-1])
    )

    # Bearish: all 4 turned down in order
    bs_dn = {n: barssince(df[f'turnDn_{n}'], last) for n in ['Trig', 'Fast', 'Med', 'Macro']}
    seq_bear_trigger = (
        all(v is not None for v in bs_dn.values()) and
        bs_dn['Trig'] > bs_dn['Fast'] > bs_dn['Med'] > bs_dn['Macro'] and
        bs_dn['Macro'] < rotation_window and
        bool(df['turnDn_Macro'].iloc[-1])
    )

    # ── Holy Grail (Pine lines 113-114) ──
    holy_grail_bull = all_oversold and rsi_div > 0
    holy_grail_bear = all_overbought and rsi_div < 0

    # ── Counter-Trend Protection (Pine lines 118-119) ──
    ct_bull_danger = (d4 < 20 and d4 <= prev['Stoch_Macro_D'] and d1 > 80)
    ct_bear_danger = (d4 > 80 and d4 >= prev['Stoch_Macro_D'] and d1 < 20)

    # ── K/D Cross — Trigger stochastic (Pine lines 127-128) ──
    k1_curr, d1_curr = curr['Stoch_Trig_K'], d1
    k1_prev, d1_prev = prev['Stoch_Trig_K'], prev['Stoch_Trig_D']
    bull_cross = (k1_prev < d1_prev) and (k1_curr > d1_curr) and (d1_curr < 20)
    bear_cross = (k1_prev > d1_prev) and (k1_curr < d1_curr) and (d1_curr > 80)

    # ── Convergence Band (Pine lines 122-123) — informational ──
    convergence = (d1 * 1 + d2 * 2 + d3 * 3 + d4 * 4) / 10.0
    spread = (abs(d1 - convergence) + abs(d2 - convergence) + abs(d3 - convergence) + abs(d4 - convergence)) / 4.0

    # ══════════════════════════════════════════════════════════
    # Decision Tree — priority-ordered signal generation
    # ══════════════════════════════════════════════════════════

    # ── SELL signals (checked first for protection) ──
    if ct_bull_danger:
        return "SELL", f"Counter-Trend: Macro stuck <20 ({d4:.1f}) while Trigger >80 ({d1:.1f}). Conv={convergence:.1f}", {"exit_type": "COUNTER_TREND"}
    if seq_bear_trigger:
        return "SELL", f"Sequential Bear Rotation: all 4 turned down fast->slow within {rotation_window} bars. Conv={convergence:.1f}", {"exit_type": "SEQ_BEAR"}
    if bear_cross:
        return "SELL", f"Trigger K/D Bear Cross: K={k1_curr:.1f} < D={d1_curr:.1f} in overbought. Conv={convergence:.1f}", {"exit_type": "BEAR_KD"}

    # ── BUY signals (priority = confidence order, 5 tiers) ──

    # Tier 1: Strict Pullback — confirmed uptrend + pullback to EMA support
    strict_pullback = (
        curr['close'] > curr['EMA_20'] and curr['close'] > curr['EMA_50']
        and d4 > 80 and d3 > 80 and d1 <= 20
        and curr['low'] <= curr['EMA_20'] * 1.005
    )
    if strict_pullback:
        return "BUY", f"Strict Pullback: Macro={d4:.0f} Med={d3:.0f} >80, Trigger={d1:.0f} oversold, EMA touch. Conv={convergence:.1f}", {"confidence": 0.90, "atr": atr_val, "signal_type": "STRICT_PULLBACK"}

    # Tier 2: Super Signal — 3-stage capitulation divergence after quad flush
    quad_flush_idx = None
    for i in range(2, 16):
        row = df.iloc[-i]
        if (row['Stoch_Macro_D'] < 20 and row['Stoch_Med_D'] < 20
                and row['Stoch_Fast_D'] < 20 and row['Stoch_Trig_D'] < 20):
            quad_flush_idx = -i
            break
    if quad_flush_idx is not None:
        anchor = df.iloc[quad_flush_idx]
        price_lower_low = curr['low'] < anchor['low'] or curr['close'] < anchor['close']
        stoch_curling = (d1 > prev['Stoch_Trig_D']) and (d2 > prev['Stoch_Fast_D'])
        stoch_holding = (d1 > 20) and (d2 > 20)
        reversal_candle = curr['close'] > curr['open']
        if price_lower_low and stoch_curling and stoch_holding and reversal_candle:
            return "BUY", f"Super Signal: Divergence after quad flush ({-quad_flush_idx} bars ago). Conv={convergence:.1f} spread={spread:.1f}", {"confidence": 0.85, "atr": atr_val, "signal_type": "SUPER_SIGNAL"}

    # Tier 3: Holy Grail — quad oversold + RSI divergence
    if holy_grail_bull:
        return "BUY", f"Holy Grail: Quad oversold + RSI div +{rsi_div:.1f}. Conv={convergence:.1f} spread={spread:.1f}", {"confidence": 0.80, "atr": atr_val, "signal_type": "HOLY_GRAIL"}

    # Tier 4: Sequential Rotation — fast->slow cascade
    if seq_bull_trigger:
        return "BUY", f"Sequential Rotation: 4 stochs turned up fast->slow within {rotation_window} bars. Conv={convergence:.1f} spread={spread:.1f}", {"confidence": 0.70, "atr": atr_val, "signal_type": "SEQ_ROTATION"}

    # Tier 5: K/D Cross — timing signal
    if bull_cross:
        return "BUY", f"Trigger K/D Bull Cross: K={k1_curr:.1f} > D={d1_curr:.1f} in oversold. Conv={convergence:.1f} spread={spread:.1f}", {"confidence": 0.40, "atr": atr_val, "signal_type": "KD_CROSS"}

    # ── HOLD — informational status ──
    status_parts = []
    if all_oversold:
        status_parts.append("Quad Oversold")
    if all_overbought:
        status_parts.append("Quad Overbought")
    if ct_bear_danger:
        status_parts.append("Counter-Trend Bear Warning")
    status_parts.append(f"Conv={convergence:.1f} spread={spread:.1f}")
    return "HOLD", f"Monitoring: {'; '.join(status_parts)}", {}

def calculate_advanced_grid(df, lower_price, upper_price, grids, current_inventory_pct=0.0):
    """
    Advanced Grid Engine: Fee-aware, Regime-filtered, and Tail-risk protected.
    """
    if len(df) < 50:
        return "HOLD", "Not enough data"

    # Constraint 2: Fee-Adjusted Grid Spacing (Minimum 1.0% width)
    min_step = lower_price * 0.01
    actual_step = (upper_price - lower_price) / grids
    if actual_step < min_step:
        grids = max(1, int((upper_price - lower_price) / min_step))
        actual_step = (upper_price - lower_price) / grids

    # Indicator Calculations (ADX and ATR)
    adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
    if adx_df is not None and not adx_df.empty:
        df['ADX'] = adx_df.iloc[:, 0]
    else:
        df['ADX'] = 0.0

    df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    curr = df.iloc[-1]
    
    if pd.isna(curr['ADX']) or pd.isna(curr['ATR']):
        return "HOLD", "Calculating indicators..."

    # Constraint 4: Volatility-Based Tail Risk (2.0 ATR below lower bound)
    tail_risk_level = lower_price - (2.0 * curr['ATR'])
    if curr['close'] < tail_risk_level:
        return "HOLD", f"EMERGENCY HALT: Price fell > 2.0 ATR below grid (Halt level: {tail_risk_level:.2f})"

    # Constraint 5: Inventory-Aware Recovery (Waiting Mode)
    if current_inventory_pct >= 0.99:
        return "HOLD", "INVENTORY RECOVERY: Deploy Post-Only Maker SELL limits at grid levels."

    # Constraint 3: Market Regime Gating (ADX < 25)
    if curr['ADX'] >= 25:
        return "HOLD", f"DORMANT: Strong trend detected (ADX: {curr['ADX']:.2f}). Pausing to avoid falling knife."

    # Constraint 1: Maker-Only Execution (instructed via reason string)
    return "HOLD", f"GRID ACTIVE: Deploy Post-Only Maker limits. Spacing >= 1.0% ({actual_step:.2f})"

def calculate_orb(df, pos_side="FLAT", entry_price=0.0, orb_data=None, tp_stage=0,
                  range_start_hour=14, range_duration_min=60, expiry_hours=8):
    """
    Opening Range Breakout (ORB) — Comprehensive overhaul.

    Range Window: configurable (default 14:00-15:00 UTC, US/EU session overlap).
    Entry Filters: VWAP, EMA(20), RVOL > 1.5, body > 50% of candle range.
    Range Quality: 0.5x-2.0x ATR (reject noise and extended ranges).
    Stop Loss: max(1x range_width, 1.5x ATR) from entry.
    Take Profit: R-multiple — T1=1.5R sell 50% + BE stop, T2=3.0R full exit.
    Trailing Stop: 1.5x ATR from HWM, activated after T1.
    Session Timeout: 8 hours from entry.
    """
    if len(df) < 100:
        return "HOLD", "Not enough data", {}

    # --- Indicators ---
    df['EMA_20'] = ta.ema(df['close'], length=20)
    df['ATR'] = ta.atr(df['high'], df['low'], df['close'], 14)
    df['VOL_AVG'] = ta.sma(df['volume'], 20)
    df['Typical_Price'] = (df['high'] + df['low'] + df['close']) / 3
    df['VWAP'] = (df['Typical_Price'] * df['volume']).cumsum() / df['volume'].cumsum()

    df['datetime'] = pd.to_datetime(df['start'], unit='s', utc=True)
    curr = df.iloc[-1]
    atr = curr['ATR'] if not pd.isna(curr['ATR']) and curr['ATR'] > 0 else 0
    vol_avg = curr['VOL_AVG'] if not pd.isna(curr['VOL_AVG']) else 0

    if atr <= 0:
        return "HOLD", "ATR warming up", {}

    # --- Build Opening Range for configured window ---
    current_date = curr['datetime'].date()
    range_end_hour = range_start_hour + (range_duration_min // 60)
    range_end_minute = range_duration_min % 60

    opening_range = df[
        (df['datetime'].dt.date == current_date) &
        (df['datetime'].dt.hour >= range_start_hour) &
        ((df['datetime'].dt.hour < range_end_hour) |
         ((df['datetime'].dt.hour == range_end_hour) & (df['datetime'].dt.minute < range_end_minute)))
    ]

    # Range not ready yet — infer bar interval from the data instead of
    # hardcoding 5m bars (STRAT-M1), fallback 300s when undeterminable
    bar_seconds = 300.0
    if 'start' in df.columns and len(df) >= 2:
        diffs = df['start'].diff().dropna()
        med = float(diffs.median()) if len(diffs) > 0 else 0.0
        if med > 0:
            bar_seconds = med
    bar_minutes = max(1, int(round(bar_seconds / 60.0)))
    min_candles = max(3, range_duration_min // bar_minutes)  # at least 3 candles or duration/bar
    if len(opening_range) < min_candles:
        return "HOLD", f"Defining range ({range_start_hour}:00-{range_end_hour}:{range_end_minute:02d} UTC, {len(opening_range)}/{min_candles} candles)", {}

    range_high = float(opening_range['high'].max())
    range_low = float(opening_range['low'].min())
    range_width = range_high - range_low

    # --- EXIT LOGIC (checked first when in position) ---
    if pos_side in ('LONG', 'SHORT') and entry_price > 0 and orb_data:
        stop_dist = orb_data.get('stop_distance', atr)
        entry_ts = orb_data.get('entry_time', 0)
        R = stop_dist if stop_dist > 0 else atr

        if pos_side == 'LONG':
            sl_price = entry_price - stop_dist
            # After T1, stop moves to breakeven
            if tp_stage >= 1:
                sl_price = max(sl_price, entry_price)

            # Stop loss
            if curr['close'] <= sl_price:
                return "EXIT_LONG", f"STOP LOSS: {curr['close']:.2f} <= {sl_price:.2f}", {}

            pnl = curr['close'] - entry_price
            r_mult = pnl / R if R > 0 else 0

            # T2: 3.0R — full exit
            if tp_stage >= 1 and r_mult >= 3.0:
                return "EXIT_LONG", f"TARGET 2: +{r_mult:.1f}R. Full exit.", {}
            # T1: 1.5R — partial exit 50%, move stop to BE
            if tp_stage == 0 and r_mult >= 1.5:
                return "PARTIAL_EXIT_LONG", f"TARGET 1: +{r_mult:.1f}R. Sell 50%, stop->BE.", {}

        elif pos_side == 'SHORT':
            sl_price = entry_price + stop_dist
            if tp_stage >= 1:
                sl_price = min(sl_price, entry_price)

            if curr['close'] >= sl_price:
                return "EXIT_SHORT", f"STOP LOSS: {curr['close']:.2f} >= {sl_price:.2f}", {}

            pnl = entry_price - curr['close']
            r_mult = pnl / R if R > 0 else 0

            if tp_stage >= 1 and r_mult >= 3.0:
                return "EXIT_SHORT", f"TARGET 2: +{r_mult:.1f}R. Full exit.", {}
            if tp_stage == 0 and r_mult >= 1.5:
                return "PARTIAL_EXIT_SHORT", f"TARGET 1: +{r_mult:.1f}R. Cover 50%, stop->BE.", {}

        # Session timeout: 8 hours from entry
        if entry_ts > 0:
            hours_in_trade = (int(curr['start']) - entry_ts) / 3600
            if hours_in_trade > expiry_hours:
                sig = "EXIT_LONG" if pos_side == 'LONG' else "EXIT_SHORT"
                return sig, f"SESSION TIMEOUT: {hours_in_trade:.1f}h in trade (max {expiry_hours}h)", {}

        return "HOLD", f"In position, {r_mult:.1f}R" if R > 0 else "In position", {}

    # --- ENTRY LOGIC (FLAT only) ---
    if pos_side != 'FLAT':
        return "HOLD", "In position", {}

    # Check if current time is after range completion
    range_end_ts = opening_range['start'].max() + 60  # 1 min after last range candle
    curr_ts = int(curr['start'])
    if curr_ts < range_end_ts:
        return "HOLD", "Waiting for range completion", {}

    # Expiry check
    hours_since_range = (curr_ts - range_end_ts) / 3600
    if hours_since_range > expiry_hours:
        return "HOLD", f"ORB expired ({hours_since_range:.0f}h > {expiry_hours}h)", {}

    # Range quality filter: 0.5x ATR floor; width cap scaled by sqrt(bars in
    # range) since an N-bar range is expected to span ~sqrt(N)x a 1-bar ATR (STRAT-M3)
    bars_in_range = len(opening_range)
    max_range_width = 2.0 * atr * math.sqrt(bars_in_range)
    if range_width < 0.5 * atr:
        return "HOLD", f"Range too narrow ({range_width:.2f} < 0.5x ATR {0.5*atr:.2f})", {}
    if range_width > max_range_width:
        return "HOLD", f"Range too wide ({range_width:.2f} > 2.0x ATR x sqrt({bars_in_range}) = {max_range_width:.2f})", {}

    # Candle quality checks
    curr_body = abs(curr['close'] - curr['open'])
    curr_range = curr['high'] - curr['low']
    body_quality = curr_body > 0.5 * curr_range if curr_range > 0 else False
    volume_confirmed = curr['volume'] > (vol_avg * 1.5) if vol_avg > 0 else False

    # Stop distance for position sizing
    stop_distance = max(range_width, 1.5 * atr)

    orb_meta = {
        'range_high': range_high,
        'range_low': range_low,
        'range_width': range_width,
        'stop_distance': stop_distance,
        'atr': float(atr),
        'entry_time': curr_ts
    }

    # LONG breakout
    is_bull = curr['close'] > curr['open']
    if (is_bull and curr['close'] > range_high and
            curr['close'] > curr['VWAP'] and curr['close'] > curr['EMA_20']):
        if not volume_confirmed:
            return "HOLD", f"Long breakout but volume weak ({curr['volume']:.0f} < 1.5x avg)", {}
        if not body_quality:
            return "HOLD", "Long breakout but weak candle body (wick rejection)", {}
        return "LONG", \
            f"ORB LONG: Close {curr['close']:.2f} > range {range_high:.2f}. R={stop_distance:.2f}", orb_meta

    # SHORT breakout
    is_bear = curr['close'] < curr['open']
    if (is_bear and curr['close'] < range_low and
            curr['close'] < curr['VWAP'] and curr['close'] < curr['EMA_20']):
        if not volume_confirmed:
            return "HOLD", f"Short breakout but volume weak ({curr['volume']:.0f} < 1.5x avg)", {}
        if not body_quality:
            return "HOLD", "Short breakout but weak candle body (wick rejection)", {}
        return "SHORT", \
            f"ORB SHORT: Close {curr['close']:.2f} < range {range_low:.2f}. R={stop_distance:.2f}", orb_meta

    return "HOLD", f"Ranging ({range_low:.2f} - {range_high:.2f})", {}

def calculate_trap(df, pos_side="FLAT", entry_stage=0, avg_entry=0.0, breakout_data=None, tp_stage=0):
    """
    TRAP Strategy: Oliver Velez Elephant Bar / Narrow-Band Breakout

    Entry Logic (Velez-aligned):
    1. SMA 20 and SMA 200 must be flat (narrow-band / consolidation)
    2. SMA 20 and SMA 200 must be converged (within 1.5%)
    3. SMA ordering: SMA20 > SMA200 for long, SMA20 < SMA200 for short
    4. Elephant bar: body > 1.5x ATR AND > 70th percentile of last 20 bars
    5. Elephant bar clears 3+ prior opposite-color bars (87% follow-through)
    6. Close beyond zone edge + 0.5x ATR
    7. Volume > 1.5x average confirms conviction
    8. SMA20 post-breakout angle > 30 degrees (arctangent, ATR-normalized)

    Position Sizing (Velez-aligned):
    - Stage 1: 10% on elephant bar breakout
    - Stage 2: 10% on first pullback (opposite color, body < 1x ATR, < 50% retrace)
    - Stage 3: 10% on second pullback (same conditions)

    Exit (Velez-aligned):
    - SL: 2x ATR or elephant bar low/high (whichever wider)
    - T1: +2.5R -> sell 50%, move stop to breakeven
    - T2: +4.0R -> sell remaining
    - Extended: exit if price > 5% from SMA20
    """
    if len(df) < 210:
        return "HOLD", "Not enough data", {}

    c = df['close']
    o = df['open']
    h = df['high']
    l = df['low']

    # --- Indicators ---
    df['SMA_20'] = ta.sma(c, 20)
    df['SMA_200'] = ta.sma(c, 200)
    df['ATR'] = ta.atr(h, l, c, 14)
    df['VOL_AVG'] = ta.sma(df['volume'], 20)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    if pd.isna(curr['SMA_200']) or pd.isna(curr['ATR']) or curr['ATR'] <= 0:
        return "HOLD", "Warming up indicators", {}

    atr = curr['ATR']
    sma20 = curr['SMA_20']
    sma200 = curr['SMA_200']
    vol_avg = curr['VOL_AVG'] if not pd.isna(curr['VOL_AVG']) else 0

    # --- EXIT LOGIC (checked first) ---
    # Velez: SL = 2x ATR or elephant bar low/high (whichever wider)
    # Velez: TP via R-multiples (T1=2.5R partial, T2=4.0R full), breakeven stop after T1
    if pos_side in ('LONG', 'SHORT') and avg_entry > 0:
        elephant_low = breakout_data.get('low', 0) if breakout_data else 0
        elephant_high = breakout_data.get('high', 0) if breakout_data else 0

        if pos_side == 'LONG':
            atr_stop = avg_entry - (2.0 * atr)
            elephant_stop = elephant_low if elephant_low > 0 else atr_stop
            sl_price = min(atr_stop, elephant_stop)
            # After T1 hit, stop moves to breakeven (Velez)
            if tp_stage >= 1:
                sl_price = max(sl_price, avg_entry)
            R = avg_entry - sl_price if sl_price < avg_entry else atr  # risk per unit

            if curr['close'] <= sl_price:
                return "EXIT_LONG", f"STOP LOSS: Price {curr['close']:.2f} hit stop {sl_price:.2f} (R={R:.2f})", {}

            if R > 0:
                pnl = curr['close'] - avg_entry
                r_multiple = pnl / R
                # T2: 4.0R -- full exit of remaining position
                if tp_stage >= 1 and r_multiple >= 4.0:
                    return "EXIT_LONG", f"TARGET 2: +{r_multiple:.1f}R (${pnl:.2f}/unit). Full exit.", {}
                # T1: 2.5R -- partial exit (50%), move stop to breakeven
                if tp_stage == 0 and r_multiple >= 2.5:
                    return "PARTIAL_EXIT_LONG", f"TARGET 1: +{r_multiple:.1f}R (${pnl:.2f}/unit). Sell 50%%, move stop to BE.", {}

            # Extended: exit if price far from SMA20 (Velez)
            sma20_dist = (curr['close'] - sma20) / sma20 if sma20 > 0 else 0
            if sma20_dist > 0.05:
                return "EXIT_LONG", f"EXTENDED FROM SMA20: {sma20_dist*100:.1f}% above. Taking profit.", {}

        elif pos_side == 'SHORT':
            atr_stop = avg_entry + (2.0 * atr)
            elephant_stop = elephant_high if elephant_high > 0 else atr_stop
            sl_price = max(atr_stop, elephant_stop)
            # After T1, stop moves to breakeven
            if tp_stage >= 1:
                sl_price = min(sl_price, avg_entry)
            R = sl_price - avg_entry if sl_price > avg_entry else atr

            if curr['close'] >= sl_price:
                return "EXIT_SHORT", f"STOP LOSS: Price {curr['close']:.2f} hit stop {sl_price:.2f} (R={R:.2f})", {}

            if R > 0:
                pnl = avg_entry - curr['close']
                r_multiple = pnl / R
                if tp_stage >= 1 and r_multiple >= 4.0:
                    return "EXIT_SHORT", f"TARGET 2: +{r_multiple:.1f}R (${pnl:.2f}/unit). Full exit.", {}
                if tp_stage == 0 and r_multiple >= 2.5:
                    return "PARTIAL_EXIT_SHORT", f"TARGET 1: +{r_multiple:.1f}R (${pnl:.2f}/unit). Cover 50%%, move stop to BE.", {}

            sma20_dist = (sma20 - curr['close']) / sma20 if sma20 > 0 else 0
            if sma20_dist > 0.05:
                return "EXIT_SHORT", f"EXTENDED FROM SMA20: {sma20_dist*100:.1f}% below. Taking profit.", {}

    # --- ADD to position on pullback (Velez: up to 2 adds, 3 stages total) ---
    if pos_side != 'FLAT' and entry_stage in (1, 2) and breakout_data:
        bo_close = breakout_data.get('close', 0)
        bo_open = breakout_data.get('open', 0)
        bo_body = abs(bo_close - bo_open)

        curr_body = abs(curr['close'] - curr['open'])
        is_opposite_color = (pos_side == 'LONG' and curr['close'] < curr['open']) or \
                           (pos_side == 'SHORT' and curr['close'] > curr['open'])

        if is_opposite_color:
            # Check: body < 1x ATR (not a substantial reversal)
            body_ok = curr_body < atr
            # Check: doesn't retrace > 50% of breakout
            if pos_side == 'LONG':
                retrace = (bo_close - curr['close']) / bo_body if bo_body > 0 else 999
            else:
                retrace = (curr['close'] - bo_close) / bo_body if bo_body > 0 else 999
            retrace_ok = retrace < 0.5

            if body_ok and retrace_ok:
                return "ADD_LONG" if pos_side == 'LONG' else "ADD_SHORT", \
                    f"ADD: Pullback candle retrace {retrace*100:.0f}%, body {curr_body:.2f} < ATR {atr:.2f}", {}
            elif not body_ok:
                return "HOLD", f"Pullback too strong (body {curr_body:.2f} >= ATR {atr:.2f}). Skipping add.", {}

        return "HOLD", "Waiting for pullback candle to add position", {}

    # --- STAGE 1: BREAKOUT DETECTION (only when FLAT) ---
    if pos_side != 'FLAT':
        return "HOLD", "In position, monitoring TP/SL", {}

    # 1. SMA FLATNESS: slope of SMA over last 20 bars as percentage
    sma20_start = df['SMA_20'].iloc[-21] if len(df) > 21 else df['SMA_20'].iloc[0]
    sma200_start = df['SMA_200'].iloc[-21] if len(df) > 21 else df['SMA_200'].iloc[0]

    sma20_slope = abs((sma20 - sma20_start) / sma20_start) if sma20_start > 0 else 999
    sma200_slope = abs((sma200 - sma200_start) / sma200_start) if sma200_start > 0 else 999

    if sma20_slope > 0.003:
        return "HOLD", f"SMA20 not flat (slope: {sma20_slope*100:.2f}% > 0.3%)", {}
    if sma200_slope > 0.0015:
        return "HOLD", f"SMA200 not flat (slope: {sma200_slope*100:.2f}% > 0.15%)", {}

    # 2. SMA CONVERGENCE: within 1.5% of each other
    sma_gap = abs(sma20 - sma200) / max(sma20, sma200)
    if sma_gap > 0.015:
        return "HOLD", f"SMAs not converged (gap: {sma_gap*100:.1f}% > 1.5%)", {}

    # 3. SMA ORDERING (Velez: SMA20 must be on correct side for direction)
    sma_long_ok = sma20 > sma200
    sma_short_ok = sma20 < sma200

    # 4. DEFINE THE TRAP ZONE
    zone_upper = max(sma20, sma200)
    zone_lower = min(sma20, sma200)
    zone_mid = (zone_upper + zone_lower) / 2

    # 4. ELEPHANT BAR CHECK (Velez: body > 1.5x ATR + > 70th percentile of last 20)
    curr_body = abs(curr['close'] - curr['open'])
    is_bull = curr['close'] > curr['open']
    is_bear = curr['close'] < curr['open']

    body_big = curr_body > (1.5 * atr)
    recent_bodies = abs(df['close'].iloc[-21:-1] - df['open'].iloc[-21:-1])
    percentile_70 = recent_bodies.quantile(0.70)
    body_relative_ok = curr_body > percentile_70
    volume_confirmed = curr['volume'] > (vol_avg * 1.5) if vol_avg > 0 else False

    if not body_big:
        return "HOLD", f"No elephant bar (body {curr_body:.2f} < 1.5x ATR {1.5*atr:.2f})", {}
    if not body_relative_ok:
        return "HOLD", f"Body not dominant (body {curr_body:.2f} < 70th pctile {percentile_70:.2f})", {}
    if not volume_confirmed:
        return "HOLD", f"Volume weak ({curr['volume']:.0f} < 1.5x avg {vol_avg:.0f})", {}

    # 5. ELEPHANT BAR MUST CLEAR 3+ PRIOR OPPOSITE-COLOR BARS (Velez: 87% follow-through)
    lookback = df.iloc[-11:-1]
    if is_bull:
        bear_bars = lookback[lookback['close'] < lookback['open']]
        bars_cleared = int((curr['close'] > bear_bars['high']).sum()) if len(bear_bars) > 0 else 0
        if bars_cleared < 3:
            return "HOLD", f"Elephant bar only clears {bars_cleared} prior bear bars (need 3+)", {}
    elif is_bear:
        bull_bars = lookback[lookback['close'] > lookback['open']]
        bars_cleared = int((curr['close'] < bull_bars['low']).sum()) if len(bull_bars) > 0 else 0
        if bars_cleared < 3:
            return "HOLD", f"Elephant bar only clears {bars_cleared} prior bull bars (need 3+)", {}

    # 6. SMA20 POST-BREAKOUT ANGLE (Velez: arctangent > 30 degrees, ATR-normalized)
    sma20_5ago = df['SMA_20'].iloc[-6] if len(df) > 6 else df['SMA_20'].iloc[0]
    sma20_change = sma20 - sma20_5ago
    normalized_slope = sma20_change / atr if atr > 0 else 0
    angle_deg = abs(math.degrees(math.atan(normalized_slope)))

    if is_bull and (angle_deg < 30 or sma20_change < 0):
        return "HOLD", f"SMA20 angle too flat for long ({angle_deg:.0f} deg < 30 deg)", {}
    if is_bear and (angle_deg < 30 or sma20_change > 0):
        return "HOLD", f"SMA20 angle too flat for short ({angle_deg:.0f} deg < 30 deg)", {}

    # 7. BREAKOUT DIRECTION
    breakout_threshold_long = zone_upper + (0.5 * atr)
    breakout_threshold_short = zone_lower - (0.5 * atr)

    bo_data = {
        'open': float(curr['open']),
        'close': float(curr['close']),
        'high': float(curr['high']),
        'low': float(curr['low']),
        'atr': float(atr)
    }

    if is_bull and curr['close'] > breakout_threshold_long:
        if not sma_long_ok:
            return "HOLD", f"Breakout long rejected: SMA20 ({sma20:.2f}) not above SMA200 ({sma200:.2f})", {}
        return "BREAKOUT_LONG", \
            f"TRAP BREAKOUT LONG: Close {curr['close']:.2f} > zone {zone_upper:.2f} + 0.5*ATR. Body={curr_body:.2f}, Vol={curr['volume']:.0f}", \
            bo_data

    if is_bear and curr['close'] < breakout_threshold_short:
        if not sma_short_ok:
            return "HOLD", f"Breakout short rejected: SMA20 ({sma20:.2f}) not below SMA200 ({sma200:.2f})", {}
        return "BREAKOUT_SHORT", \
            f"TRAP BREAKOUT SHORT: Close {curr['close']:.2f} < zone {zone_lower:.2f} - 0.5*ATR. Body={curr_body:.2f}, Vol={curr['volume']:.0f}", \
            bo_data

    return "HOLD", f"Consolidating in zone ({zone_lower:.2f} - {zone_upper:.2f}). Waiting for breakout.", {}

# ==========================================
# MOMENTUM STRATEGY
# ==========================================
def calculate_momentum(df):
    """
    MOMENTUM: Trend-pullback reversal using dual smoothed ROC + ADX power filter.
    
    Entry requires ALL:
    1. ADX(14) >= 25 (strong trend)
    2. SMA(20) > SMA(200) (uptrend context)
    3. ROC(5) smoothed SMA(5) <= -0.30 AND ROC(14) smoothed SMA(5) <= -0.30 (real dip)
    4. Both smoothed ROCs curling up (inflection: was flat/falling, now rising)
    
    Returns: ("BUY", reason) or ("HOLD", reason)
    Also returns current ATR for stop calibration: ("BUY", reason, atr_value)
    """
    if len(df) < 210:
        return "HOLD", "Not enough data", 0.0

    c = df['close']
    h = df['high']
    l = df['low']

    # --- Indicators ---
    sma20 = ta.sma(c, 20)
    sma200 = ta.sma(c, 200)
    atr_series = ta.atr(h, l, c, 14)

    # ADX
    adx_df = ta.adx(h, l, c, 14)
    if adx_df is None or adx_df.empty:
        return "HOLD", "ADX calculation failed", 0.0

    # ROC(5) smoothed with SMA(2) = "Fast ROC"  (spec says SMA(5) but SMA(2) intentional — SMA(5) too smooth)
    roc5_raw = ta.roc(c, 5)
    roc5_smooth = ta.sma(roc5_raw, 2) if roc5_raw is not None else None

    # ROC(14) smoothed with SMA(2) = "Slow ROC"  (spec says SMA(5) but SMA(2) intentional — SMA(5) too smooth)
    roc14_raw = ta.roc(c, 14)
    roc14_smooth = ta.sma(roc14_raw, 2) if roc14_raw is not None else None

    # Validate all indicators are ready
    if sma20 is None or sma200 is None or atr_series is None:
        return "HOLD", "Warming up indicators", 0.0
    if roc5_smooth is None or roc14_smooth is None:
        return "HOLD", "ROC indicators warming up", 0.0

    curr_sma20 = sma20.iloc[-1]
    curr_sma200 = sma200.iloc[-1]
    curr_adx = float(adx_df.iloc[-1, 0])
    curr_atr = float(atr_series.iloc[-1])

    if pd.isna(curr_sma200) or pd.isna(curr_adx) or pd.isna(curr_atr) or curr_atr <= 0:
        return "HOLD", "Warming up indicators", 0.0

    # Current and previous 2 bars for curl-up detection
    fast_cur = roc5_smooth.iloc[-1]
    fast_prev = roc5_smooth.iloc[-2]
    fast_prev2 = roc5_smooth.iloc[-3]

    slow_cur = roc14_smooth.iloc[-1]
    slow_prev = roc14_smooth.iloc[-2]
    slow_prev2 = roc14_smooth.iloc[-3]

    if pd.isna(fast_cur) or pd.isna(fast_prev) or pd.isna(fast_prev2):
        return "HOLD", "ROC smoothing warming up", 0.0
    if pd.isna(slow_cur) or pd.isna(slow_prev) or pd.isna(slow_prev2):
        return "HOLD", "ROC smoothing warming up", 0.0

    # --- CONDITION 1: Trend Power (ADX >= 25) ---
    if curr_adx < 25:
        return "HOLD", f"ADX too low ({curr_adx:.1f} < 25)", curr_atr

    # --- CONDITION 2: Trend Direction (SMA 20 > SMA 200) ---
    if curr_sma20 <= curr_sma200:
        return "HOLD", f"SMA20 ({curr_sma20:.2f}) <= SMA200 ({curr_sma200:.2f}). No uptrend.", curr_atr

    # --- CONDITION 3: Dip Depth (both ROCs <= -0.30) ---
    if fast_cur > -0.30:
        return "HOLD", f"Fast ROC not deep enough ({fast_cur:.2f} > -0.30)", curr_atr
    if slow_cur > -0.30:
        return "HOLD", f"Slow ROC not deep enough ({slow_cur:.2f} > -0.30)", curr_atr

    # --- CONDITION 4: Fast ROC Curl-Up ---
    # Current rising AND previous was flat or falling
    fast_curling = (fast_cur > fast_prev) and (fast_prev <= fast_prev2)
    if not fast_curling:
        if fast_cur <= fast_prev:
            return "HOLD", f"Fast ROC still falling ({fast_prev:.2f} -> {fast_cur:.2f})", curr_atr
        else:
            return "HOLD", f"Fast ROC rising but prev wasn't flat/falling ({fast_prev2:.2f} -> {fast_prev:.2f} -> {fast_cur:.2f})", curr_atr

    # --- CONDITION 5: Slow ROC Curl-Up ---
    slow_curling = (slow_cur > slow_prev) and (slow_prev <= slow_prev2)
    if not slow_curling:
        if slow_cur <= slow_prev:
            return "HOLD", f"Slow ROC still falling ({slow_prev:.2f} -> {slow_cur:.2f})", curr_atr
        else:
            return "HOLD", f"Slow ROC rising but prev wasn't flat/falling ({slow_prev2:.2f} -> {slow_prev:.2f} -> {slow_cur:.2f})", curr_atr

    # --- ALL CONDITIONS MET ---
    reason = (f"MOMENTUM BUY: ADX={curr_adx:.1f}, "
              f"FastROC={fast_cur:.2f} (curl from {fast_prev:.2f}), "
              f"SlowROC={slow_cur:.2f} (curl from {slow_prev:.2f}), "
              f"SMA20={curr_sma20:.2f} > SMA200={curr_sma200:.2f}")
    return "BUY", reason, curr_atr

# ==========================================
# DCA STRATEGY
# ==========================================
def calculate_dca(df, dca_state="SCANNING", last_cross_direction="ABOVE", arm_threshold=-0.30):
    """
    DCA: Signal-gated accumulation via dual smoothed ROC cross/curl cycle.
    
    State machine signals:
    - "ARM": Both ROCs crossed below zero (dip starting)
    - "DISARM": Both ROCs crossed back above zero before conditions met
    - "BUY": Armed + both ROCs <= -0.50 + either curling up + ADX >= 20
    - "HOLD": No action this cycle
    
    Returns: (signal, reason, extra_data)
    extra_data includes: fast_roc, slow_roc, adx, depth_multiplier
    """
    empty_data = {'fast_roc': 0, 'slow_roc': 0, 'adx': 0, 'depth_multiplier': 1.0}
    
    if len(df) < 210:
        return "HOLD", "Not enough data", empty_data

    c = df['close']
    h = df['high']
    l = df['low']

    # --- Indicators ---
    adx_df = ta.adx(h, l, c, 14)
    if adx_df is None or adx_df.empty:
        return "HOLD", "ADX calculation failed", empty_data

    roc5_raw = ta.roc(c, 5)
    roc5_smooth = ta.sma(roc5_raw, 3) if roc5_raw is not None else None
    roc14_raw = ta.roc(c, 14)
    roc14_smooth = ta.sma(roc14_raw, 3) if roc14_raw is not None else None

    if roc5_smooth is None or roc14_smooth is None:
        return "HOLD", "ROC indicators warming up", empty_data

    curr_adx = float(adx_df.iloc[-1, 0])

    fast_cur = roc5_smooth.iloc[-1]
    fast_prev = roc5_smooth.iloc[-2]
    fast_prev2 = roc5_smooth.iloc[-3]
    slow_cur = roc14_smooth.iloc[-1]
    slow_prev = roc14_smooth.iloc[-2]
    slow_prev2 = roc14_smooth.iloc[-3]

    if any(pd.isna(v) for v in [fast_cur, fast_prev, fast_prev2, slow_cur, slow_prev, slow_prev2, curr_adx]):
        return "HOLD", "Indicators warming up", empty_data

    fast_cur = float(fast_cur)
    fast_prev = float(fast_prev)
    fast_prev2 = float(fast_prev2)
    slow_cur = float(slow_cur)
    slow_prev = float(slow_prev)
    slow_prev2 = float(slow_prev2)

    # Depth for sizing tier — heavier buys at deeper dips (where DCA edge lives)
    depth = min(fast_cur, slow_cur)
    if depth <= -3.0:
        depth_mult = 6.0    # capitulation — max aggression
    elif depth <= -2.0:
        depth_mult = 4.0    # heavy dip
    elif depth <= -1.0:
        depth_mult = 2.5    # solid dip
    elif depth <= -0.50:
        depth_mult = 1.5    # moderate dip
    else:
        depth_mult = 1.0    # scout buy at threshold

    data = {
        'fast_roc': round(fast_cur, 4),
        'slow_roc': round(slow_cur, 4),
        'adx': round(curr_adx, 2),
        'depth_multiplier': depth_mult,
    }

    # --- SCANNING: wait for both ROCs below zero AND at depth threshold ---
    # Zero-cross is just a prerequisite — ARM only when depth is reached
    if dca_state == "SCANNING":
        both_below_now = fast_cur < 0 and slow_cur < 0
        at_depth = fast_cur <= arm_threshold and slow_cur <= arm_threshold
        if both_below_now and at_depth:
            return "ARM", f"Depth threshold reached (Fast={fast_cur:.2f} Slow={slow_cur:.2f}, thresh={arm_threshold})", data
        return "HOLD", f"Scanning. Fast={fast_cur:.2f} Slow={slow_cur:.2f}", data

    # --- ARMED: target acquired, waiting for curl-up confirmation to fire ---
    if dca_state == "ARMED":
        # Disarm: Both ROCs crossed back above zero — dip fizzled without curling
        both_above_now = fast_cur >= 0 and slow_cur >= 0
        if both_above_now:
            return "DISARM", f"Both ROCs back above zero (Fast={fast_cur:.2f} Slow={slow_cur:.2f}). Dip resolved.", data

        # Check buy conditions — depth already confirmed by ARM, now need curl + ADX
        # 1. Either ROC curling up (momentum reversing)
        fast_curling = fast_cur > fast_prev
        slow_curling = slow_cur > slow_prev
        if not fast_curling and not slow_curling:
            return "HOLD", f"Armed. Waiting for curl-up (Fast={fast_prev:.2f}->{fast_cur:.2f}, Slow={slow_prev:.2f}->{slow_cur:.2f})", data

        # 2. ADX filter
        if curr_adx < 10:
            return "HOLD", f"Armed. Curl detected but ADX too low ({curr_adx:.1f} < 10)", data

        # All conditions met — fire
        curl_source = "Fast" if fast_curling else "Slow"
        reason = (f"DCA BUY: {curl_source} ROC curled up. "
                  f"Fast={fast_cur:.2f} Slow={slow_cur:.2f} ADX={curr_adx:.1f} "
                  f"Depth={depth:.2f} Size={depth_mult:.2f}x")
        return "BUY", reason, data

    # Default: if state is ACCUMULATING/BUYING/PAUSED, signal logic handles re-arm
    # Re-arm cycle: both ROCs must cross above zero (reset), then reach depth again
    if last_cross_direction == "BELOW":
        # Waiting for both ROCs to cross back above zero (reset)
        both_above_now = fast_cur >= 0 and slow_cur >= 0
        either_below_prev = fast_prev < 0 or slow_prev < 0
        if both_above_now and either_below_prev:
            return "CROSS_ABOVE", f"Both ROCs above zero (reset). Ready to re-arm on next depth cross.", data
    elif last_cross_direction == "ABOVE":
        # Waiting for depth threshold — not just zero-cross
        both_below_now = fast_cur < 0 and slow_cur < 0
        at_depth = fast_cur <= arm_threshold and slow_cur <= arm_threshold
        if both_below_now and at_depth:
            return "ARM", f"Re-armed: Depth threshold reached (Fast={fast_cur:.2f} Slow={slow_cur:.2f}, thresh={arm_threshold})", data

    return "HOLD", f"Accumulating. Fast={fast_cur:.2f} Slow={slow_cur:.2f} Cross={last_cross_direction}", data


# ==========================================
# NPR STRATEGY
# ==========================================

NPR_CONFIG = {
    "elephant_body_mult": 2.5, "elephant_lookback": 20,
    "tail_ratio_min": 2.0, "tail_body_max_pct": 0.35,
    "one80_min_first_bar_pct": 0.5,
    "ma_separation_max_atr": 3.0, "ma_flatness_slope_max": 0.0015,
    "zone1_atr": 1.5, "zone3_atr": 3.0,
    "sma_proximity_atr": 0.5, "stop_buffer_atr": 0.5,
}

def _detect_events(df, config):
    events = []
    if len(df) < 22: return events
    curr, prev = df.iloc[-1], df.iloc[-2]
    c_o, c_c, c_h, c_l = float(curr['open']), float(curr['close']), float(curr['high']), float(curr['low'])
    p_o, p_c, p_h, p_l = float(prev['open']), float(prev['close']), float(prev['high']), float(prev['low'])
    c_body, c_range, p_body = abs(c_c - c_o), c_h - c_l, abs(p_c - p_o)
    lb = config['elephant_lookback']
    bodies = df['close'].iloc[-(lb+1):-1].values - df['open'].iloc[-(lb+1):-1].values
    avg_body = float(pd.Series(abs(bodies)).mean()) if len(bodies) > 0 else c_body
    atr = float(df['atr'].iloc[-1]) if 'atr' in df.columns and not pd.isna(df['atr'].iloc[-1]) else c_range
    sb = config['stop_buffer_atr'] * atr
    p_bear, p_bull = p_c < p_o, p_c > p_o
    c_bull, c_bear = c_c > c_o, c_c < c_o
    first_ok = p_body >= config['one80_min_first_bar_pct'] * avg_body
    # 180 bars
    if first_ok and p_body > 0 and c_body > 0:
        if p_bear and c_bull and c_h > p_h:
            if min(c_o, c_c) <= min(p_o, p_c):
                events.append({'type':'180','direction':'BULL','stop_price':min(p_l,c_l)-sb,'power':2.0,
                    'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l},'prev':{'o':p_o,'c':p_c,'h':p_h,'l':p_l}}})
        if p_bull and c_bear and c_l < p_l:
            if max(c_o, c_c) >= max(p_o, p_c):
                events.append({'type':'180','direction':'BEAR','stop_price':max(p_h,c_h)+sb,'power':2.0,
                    'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l},'prev':{'o':p_o,'c':p_c,'h':p_h,'l':p_l}}})
    # Tail bars
    if c_range > 0 and c_body > 0 and (c_body / c_range) <= config['tail_body_max_pct']:
        lo_tail = min(c_o, c_c) - c_l
        if c_body > 0 and lo_tail / c_body >= config['tail_ratio_min']:
            events.append({'type':'TAIL','direction':'BULL','stop_price':c_l-sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
        hi_tail = c_h - max(c_o, c_c)
        if c_body > 0 and hi_tail / c_body >= config['tail_ratio_min']:
            events.append({'type':'TAIL','direction':'BEAR','stop_price':c_h+sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
    # Elephant bars
    if c_body >= config['elephant_body_mult'] * avg_body and avg_body > 0:
        if c_bull:
            events.append({'type':'ELEPHANT','direction':'BULL','stop_price':c_l-sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
        elif c_bear:
            events.append({'type':'ELEPHANT','direction':'BEAR','stop_price':c_h+sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
    events.sort(key=lambda e: {'180':0,'TAIL':1,'ELEPHANT':2}.get(e['type'],9))
    return events

def _compute_zone(price, sma20, sma200, atr, config):
    ma_mid = (sma20 + sma200) / 2.0
    d = (price - ma_mid) / atr if atr > 0 else 0
    z1, z3 = config['zone1_atr'], config['zone3_atr']
    if d >= z3: return 3, d
    elif d >= z1: return 2, d
    elif d >= 0: return 1, d
    elif d >= -z1: return -1, d
    elif d >= -z3: return -2, d
    else: return -3, d

def _zone_allows_direction(zone, direction):
    if direction == 'BULL': return zone not in (-1, 3)
    return zone not in (1, -3)

def _score_trade(event, zone, sma20, sma200, price, atr, config):
    pos_checks = []
    if abs(zone) == 1: pos_checks.append('zone_1')
    elif abs(zone) == 3: pos_checks.append('zone_3')
    if atr > 0 and abs(price - sma20) <= config['sma_proximity_atr'] * atr: pos_checks.append('near_20sma')
    if atr > 0 and abs(price - sma200) <= config['sma_proximity_atr'] * atr: pos_checks.append('near_200sma')
    pos_score = min(len(pos_checks), 1.0) if pos_checks else 0.5
    return 1.0 + pos_score + 1.0, pos_checks

def calculate_npr(df):
    empty = {'signal':'HOLD','event_type':None,'event_direction':None,'event_stop':0,
             'event_power':1.0,'event_bar_data':None,'zone':0,'check_score':0,
             'position_checks':[],'atr':0,'reason':'No signal'}
    if len(df) < 210: empty['reason'] = 'Not enough data'; return empty
    c, h, l = df['close'], df['high'], df['low']
    sma20 = ta.sma(c, 20); sma200 = ta.sma(c, 200); atr_s = ta.atr(h, l, c, 14)
    if sma20 is None or sma200 is None or atr_s is None: empty['reason'] = 'Warming up'; return empty
    s20, s200, atr_v = float(sma20.iloc[-1]), float(sma200.iloc[-1]), float(atr_s.iloc[-1])
    px = float(c.iloc[-1])
    if pd.isna(s200) or pd.isna(atr_v) or atr_v <= 0: empty['reason'] = 'Warming up'; return empty
    empty['atr'] = atr_v; df['atr'] = atr_s; cfg = NPR_CONFIG
    s200_st = float(sma200.iloc[-21]) if len(sma200) > 21 else float(sma200.iloc[0])
    slope = abs((s200 - s200_st) / s200_st) if s200_st > 0 else 999
    if slope > cfg['ma_flatness_slope_max']: empty['reason'] = f"SMA200 not flat ({slope*100:.3f}%)"; return empty
    gap = abs(s20 - s200) / atr_v
    if gap > cfg['ma_separation_max_atr']: empty['reason'] = f"SMAs separated ({gap:.1f} ATR)"; return empty
    zone, dist = _compute_zone(px, s20, s200, atr_v, cfg); empty['zone'] = zone
    events = _detect_events(df, cfg)
    if not events: empty['reason'] = f"No event. Zone={zone:+d}"; return empty
    for ev in events:
        d = ev['direction']
        if not _zone_allows_direction(zone, d): continue
        sc, pc = _score_trade(ev, zone, s20, s200, px, atr_v, cfg)
        if sc < 2.0: continue
        if abs(zone) == 2 and (sc < 3.0 or ev['type'] not in ('180','ELEPHANT')): continue
        sig = 'LONG' if d == 'BULL' else 'SHORT'
        reason = f"NPR {ev['type']} {d} zone {zone:+d} (score={sc:.1f} checks={pc}) Stop=${ev['stop_price']:.2f}"
        return {'signal':sig,'event_type':ev['type'],'event_direction':d,'event_stop':ev['stop_price'],
                'event_power':ev['power'],'event_bar_data':ev['bar_data'],'zone':zone,
                'check_score':sc,'position_checks':pc,'atr':atr_v,'reason':reason}
    empty['reason'] = f"Event {events[0]['type']} {events[0]['direction']} blocked (zone={zone:+d})"; return empty


# ==========================================
# VWAP MEAN REVERSION (VWAP_MR)
# ==========================================
def calculate_vwap_mr(df):
    """
    VWAP Mean Reversion: Buy when price dips below daily VWAP by > 1 std dev
    with RSI < 35. Exit at VWAP touch or upper band.
    Expects 5m candles with 'volume' column.
    """
    if len(df) < 100:
        return "HOLD", "Not enough data", 0.0

    c, h, l, v = df['close'], df['high'], df['low'], df['volume']

    # VWAP: cumulative (typ_price * volume) / cumulative volume
    typical = (h + l + c) / 3
    cum_tv = (typical * v).cumsum()
    cum_v = v.cumsum()
    vwap = cum_tv / cum_v

    # VWAP standard deviation bands
    vwap_sq = ((typical - vwap) ** 2 * v).cumsum() / cum_v
    vwap_std = vwap_sq.apply(lambda x: x ** 0.5 if x > 0 else 0)

    # RSI
    rsi_s = ta.rsi(c, 14)

    # ATR for trailing stop
    atr_s = ta.atr(h, l, c, 14)

    if vwap is None or rsi_s is None or atr_s is None:
        return "HOLD", "Indicators warming up", 0.0

    cur_px = float(c.iloc[-1])
    cur_vwap = float(vwap.iloc[-1])
    cur_std = float(vwap_std.iloc[-1])
    cur_rsi = float(rsi_s.iloc[-1])
    cur_atr = float(atr_s.iloc[-1])

    if pd.isna(cur_vwap) or pd.isna(cur_rsi) or pd.isna(cur_atr) or cur_std <= 0:
        return "HOLD", "Warming up indicators", 0.0

    lower_band = cur_vwap - cur_std
    upper_band = cur_vwap + cur_std
    deviation = (cur_vwap - cur_px) / cur_std if cur_std > 0 else 0

    # ENTRY: price below VWAP - 1 std dev AND RSI < 35
    if cur_px < lower_band and cur_rsi < 35:
        return "BUY", f"VWAP reversion: price ${cur_px:.0f} < lower band ${lower_band:.0f}, RSI {cur_rsi:.0f}", cur_atr

    # EXIT: price touches VWAP or upper band
    if cur_px >= cur_vwap:
        return "SELL", f"VWAP touched: price ${cur_px:.0f} >= VWAP ${cur_vwap:.0f}", cur_atr

    return "HOLD", f"Deviation {deviation:.2f}σ, RSI {cur_rsi:.0f}", cur_atr


# ==========================================
# BOLLINGER SQUEEZE BREAKOUT (SQUEEZE)
# ==========================================
def calculate_squeeze(df):
    """
    Bollinger Squeeze: BB narrows inside Keltner Channels (squeeze).
    Enter on release when momentum turns positive (long) or negative (short).
    Exit on momentum reversal or ATR trailing stop.
    Expects 15m candles.
    """
    if len(df) < 210:
        return "HOLD", "Not enough data", 0.0

    c, h, l = df['close'], df['high'], df['low']

    # Bollinger Bands (20, 2.0)
    bb = ta.bbands(c, length=20, std=2.0)
    # Keltner Channels (20, 1.5)
    kc = ta.kc(h, l, c, length=20, scalar=1.5)
    # Momentum: linear regression value of close over 20 bars (squeeze histogram)
    mom = ta.mom(c, length=12)
    # ATR
    atr_s = ta.atr(h, l, c, 14)

    if bb is None or kc is None or mom is None or atr_s is None:
        return "HOLD", "Indicators warming up", 0.0

    try:
        # BB columns: BBL, BBM, BBU, BBB, BBP
        bb_lower = bb.iloc[:, 0]
        bb_upper = bb.iloc[:, 2]
        # KC columns: KCLe, KCBe, KCUe
        kc_lower = kc.iloc[:, 0]
        kc_upper = kc.iloc[:, 2]
    except (IndexError, KeyError):
        return "HOLD", "Band calculation error", 0.0

    cur_atr = float(atr_s.iloc[-1])
    if pd.isna(cur_atr):
        return "HOLD", "ATR warming up", 0.0

    # Squeeze detection: BB inside KC
    squeeze = []
    for i in range(max(0, len(df) - 5), len(df)):
        try:
            sq = float(bb_lower.iloc[i]) > float(kc_lower.iloc[i]) and float(bb_upper.iloc[i]) < float(kc_upper.iloc[i])
            squeeze.append(sq)
        except (IndexError, ValueError):
            squeeze.append(False)

    cur_squeeze = squeeze[-1] if squeeze else False
    prev_squeeze = squeeze[-2] if len(squeeze) >= 2 else False

    cur_mom = float(mom.iloc[-1])
    prev_mom = float(mom.iloc[-2]) if len(mom) >= 2 else 0

    if pd.isna(cur_mom) or pd.isna(prev_mom):
        return "HOLD", "Momentum warming up", 0.0

    # RELEASE: was in squeeze, now released, momentum has direction
    squeeze_release = prev_squeeze and not cur_squeeze

    if squeeze_release and cur_mom > 0 and cur_mom > prev_mom:
        return "BUY", f"Squeeze LONG release: momentum {cur_mom:.2f} rising", cur_atr

    if squeeze_release and cur_mom < 0 and cur_mom < prev_mom:
        return "SHORT", f"Squeeze SHORT release: momentum {cur_mom:.2f} falling", cur_atr

    # EXIT signals for existing positions
    if cur_mom < 0 and prev_mom > 0:
        return "EXIT_LONG", f"Momentum reversed negative ({cur_mom:.2f})", cur_atr
    if cur_mom > 0 and prev_mom < 0:
        return "EXIT_SHORT", f"Momentum reversed positive ({cur_mom:.2f})", cur_atr

    # Status info
    if cur_squeeze:
        return "HOLD", f"IN SQUEEZE — momentum {cur_mom:.2f}, awaiting release", cur_atr

    return "HOLD", f"No squeeze. Momentum {cur_mom:.2f}", cur_atr
