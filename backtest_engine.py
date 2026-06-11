"""
Backtest Engine — Walk-forward strategy simulation.
Runs any dashboard strategy against historical candle data with
confidence-based sizing, ATR SL/TP, and fee simulation.
"""
import math
import re
import numpy as np
import pandas as pd
import pandas_ta as ta
from dataclasses import dataclass, field
from strategies import (
    calculate_quad_rotation, calculate_momentum, calculate_squeeze,
    calculate_vwap_mr, calculate_orb, calculate_trap, calculate_dca, calculate_npr
)
from bot_executors import momentum_get_stop_price, _QUAD_SL_MULT, _QUAD_TP_MULT
from grid_engine import (compute_regime, compute_dynamic_step, compute_dynamic_trail,
                         compute_kelly_size, compute_grid_crisis_score, compute_dynamic_sell_price,
                         compute_gtfo_target, compute_quarantine_minutes, compute_weighted_avg)
from logger import get_logger

log = get_logger('backtest')

FEE_RATE = 0.002        # 0.2% per side (taker)
MAKER_FEE_RATE = 0.0006  # 0.06% maker (post_only limit orders)
SLIPPAGE_PCT = 0.0005    # 0.05% adverse slippage on market-style fills (entries at close, market exits, stop fills)
WARMUP_BARS = 210


# ══════════════════════════════════════════════════════════
# Strategy Adapter — normalizes all strategy return formats
# ══════════════════════════════════════════════════════════

def _adapt_signal(strategy, df, state, params):
    """Call strategy function, return normalized (action, reason, meta)."""

    if strategy == 'QUAD':
        rot_win = params.get('rotation_window', 20)
        return calculate_quad_rotation(df, rotation_window=rot_win)

    elif strategy == 'MOMENTUM':
        sig, reason, atr = calculate_momentum(df)
        return sig, reason, {'atr': atr}

    elif strategy == 'SQUEEZE':
        sig, reason, atr = calculate_squeeze(df)
        return sig, reason, {'atr': atr}

    elif strategy == 'VWAP_MR':
        sig, reason, atr = calculate_vwap_mr(df)
        return sig, reason, {'atr': atr}

    elif strategy == 'ORB':
        sig, reason, meta = calculate_orb(
            df,
            pos_side=state.get('position', 'FLAT'),
            entry_price=state.get('entry_price', 0.0),
            orb_data=state.get('orb_data', None),
            tp_stage=state.get('tp_stage', 0),
            range_start_hour=params.get('orb_start_hour', 14) if params else 14,
            range_duration_min=params.get('orb_duration_min', 60) if params else 60,
            expiry_hours=params.get('orb_expiry_hours', 8) if params else 8
        )
        return sig, reason, meta if isinstance(meta, dict) else {}

    elif strategy == 'TRAP':
        sig, reason, meta = calculate_trap(
            df,
            pos_side=state.get('position', 'FLAT'),
            entry_stage=state.get('entry_stage', 0),
            avg_entry=state.get('avg_entry', 0.0),
            breakout_data=state.get('breakout_data', None),
            tp_stage=state.get('tp_stage', 0)
        )
        return sig, reason, meta if isinstance(meta, dict) else {}

    elif strategy == 'DCA':
        sig, reason, extra = calculate_dca(
            df,
            dca_state=state.get('dca_state', 'SCANNING'),
            last_cross_direction=state.get('last_cross_direction', 'ABOVE')
        )
        return sig, reason, extra if isinstance(extra, dict) else {}

    elif strategy == 'NPR':
        result = calculate_npr(df)
        if isinstance(result, dict):
            sig = result.get('signal', 'HOLD')
            reason = result.get('reason', '')
            return sig, reason, result
        return 'HOLD', '', {}

    return 'HOLD', 'Unknown strategy', {}


# ══════════════════════════════════════════════════════════
# Position tracking
# ══════════════════════════════════════════════════════════

def _entry_fee(price, size):
    return price * size * FEE_RATE

def _exit_fee(price, size):
    return price * size * FEE_RATE

def _maker_fee(price, size):
    return price * size * MAKER_FEE_RATE


# ══════════════════════════════════════════════════════════
# 1m sub-bar resolution helpers
# ══════════════════════════════════════════════════════════

def _load_1m_subbars(pair, start_ts, end_ts):
    """Load raw 1m candles for the date range. Returns DataFrame or None on miss."""
    try:
        from candle_db import query as candle_db_query
        df_1m = candle_db_query(pair, 1, start_ts, end_ts)
        if df_1m is None or df_1m.empty:
            return None
        df_1m = df_1m.sort_values('start').reset_index(drop=True)
        # Pre-build numpy arrays for fast slicing in inner loop
        return {
            'starts': df_1m['start'].values,
            'opens':  df_1m['open'].values,
            'highs':  df_1m['high'].values,
            'lows':   df_1m['low'].values,
            'closes': df_1m['close'].values,
        }
    except Exception as e:
        log.warning(f"_load_1m_subbars failed for {pair}: {e}")
        return None


def _subbar_indices(subbars, bar_start, bar_end):
    """Return (start_idx, end_idx) for sub-bars within [bar_start, bar_end). Empty range returns (0,0)."""
    if subbars is None:
        return 0, 0
    starts = subbars['starts']
    if len(starts) == 0:
        return 0, 0
    # numpy searchsorted for O(log n) slicing
    import numpy as np
    s = int(np.searchsorted(starts, bar_start, side='left'))
    e = int(np.searchsorted(starts, bar_end, side='left'))
    return s, e


# ══════════════════════════════════════════════════════════
# DCA-specific backtest (full state machine simulation)
# ══════════════════════════════════════════════════════════

# Default tiered profit-taking (matches production DCA_PROFIT_TIERS)
DEFAULT_DCA_TIERS = [
    (3.0, 0.20), (5.0, 0.25), (7.5, 0.30),
    (10.0, 0.35), (15.0, 0.50), (20.0, 0.75),
]
DCA_CAUTIOUS_TIERS = [(2.0, 0.25), (3.0, 0.35), (5.0, 0.50)]
DCA_SCALP_TIERS = [(1.5, 1.0)]

DCA_MODE_CONFIG = {
    'NORMAL':   {'size_mult': 1.0,  'arm_thresh': -0.30, 'depth_cap': 6.0, 'tiers': DEFAULT_DCA_TIERS,  'circuit_break': 25},
    'CAUTIOUS': {'size_mult': 0.60, 'arm_thresh': -0.50, 'depth_cap': 3.0, 'tiers': DCA_CAUTIOUS_TIERS, 'circuit_break': 20},
    'SCALP':    {'size_mult': 0.30, 'arm_thresh': -1.00, 'depth_cap': 2.0, 'tiers': DCA_SCALP_TIERS,    'circuit_break': 15},
}


def _compute_trend_ema(df, tf_minutes=240, ema_length=50):
    """
    Resample 5m data to a higher timeframe and compute EMA.
    Returns a dict mapping each 5m bar's start timestamp to the previous-period EMA value
    (avoids look-ahead bias).

    tf_minutes: resampling timeframe in minutes (60=1h, 240=4h, 1440=daily)
    ema_length: EMA period on the resampled data
    """
    df_copy = df.copy()
    df_copy['_dt'] = pd.to_datetime(df_copy['start'], unit='s')

    # Resample to higher TF
    resampled = df_copy.set_index('_dt').resample(f'{tf_minutes}min').agg({
        'close': 'last', 'start': 'first'
    }).dropna()

    if len(resampled) < ema_length + 5:
        return {}

    resampled['ema'] = ta.ema(resampled['close'], length=ema_length)

    # Build lookup: for each resampled period, store (period_start_ts, ema_value)
    # Use PREVIOUS period's EMA to avoid look-ahead
    ema_values = resampled['ema'].values
    period_starts = resampled.index

    # Create a map from timestamp ranges to prev-period EMA
    ema_lookup = {}
    for idx in range(1, len(period_starts)):
        prev_ema = ema_values[idx - 1]
        if pd.isna(prev_ema):
            continue
        period_start = int(period_starts[idx].timestamp())
        period_end = int(period_starts[idx].timestamp()) + (tf_minutes * 60)
        ema_lookup[period_start] = (period_end, float(prev_ema))

    return ema_lookup


def _get_trend_mode(bar_time, close_px, ema_lookup, tf_minutes):
    """Determine adaptive defense mode from higher-TF EMA."""
    if not ema_lookup:
        return 'NORMAL'

    # Find which resampled period this bar falls into
    ema_val = None
    for period_start, (period_end, ema) in ema_lookup.items():
        if period_start <= bar_time < period_end:
            ema_val = ema
            break

    if ema_val is None or ema_val <= 0:
        # Fallback: use nearest prior period
        nearest = [ps for ps in ema_lookup if ps <= bar_time]
        if nearest:
            _, ema_val = ema_lookup[max(nearest)]
        else:
            return 'NORMAL'

    pct_below = max(0, (ema_val - close_px) / ema_val * 100)
    if pct_below <= 0:
        return 'NORMAL'
    elif pct_below <= 15:
        return 'CAUTIOUS'
    else:
        return 'SCALP'


def _score_tf_for_dca(pair, candidate_tf, end_ts, lookback_days, base_params, capital):
    """
    Run a mini DCA backtest on the candidate TF over the last `lookback_days`.
    Returns the total return percent (or None if data insufficient).
    Disables dynamic features in the mini-run to prevent recursion.
    Auto-scales lookback so higher TFs always get enough bars.
    """
    try:
        from candle_db import query as cdb_query
        # Scale lookback so we have at least WARMUP_BARS + 30 candles on this TF
        bars_per_day = 1440.0 / max(1, candidate_tf)
        min_days = int((WARMUP_BARS + 30) / bars_per_day) + 1
        effective_days = max(lookback_days, min_days)
        start_ts = end_ts - (effective_days * 86400)
        cand_df = cdb_query(pair, candidate_tf, start_ts, end_ts)
        if cand_df is None or len(cand_df) < WARMUP_BARS + 20:
            return None
        mini_params = dict(base_params)
        # Prevent recursion + reset state machinery
        mini_params['dynamic_tf_analysis'] = False
        mini_params['dynamic_depth'] = False  # score on baseline behavior, not dynamic
        # Run mini backtest
        result = _run_dca_backtest(pair, f'{candidate_tf}m', cand_df, capital, mini_params, None)
        if 'error' in result:
            return None
        return result.get('summary', {}).get('total_return_pct', 0)
    except Exception as e:
        log.warning(f"_score_tf_for_dca failed for {pair} {candidate_tf}m: {e}")
        return None


def _find_idx_at_or_after(df, target_ts):
    """Binary search for the first row where start >= target_ts."""
    starts = df['start'].values
    import numpy as np
    idx = int(np.searchsorted(starts, target_ts, side='left'))
    return idx if idx < len(starts) else None


def _score_tf_alignment(df, candidate_ema_lookup, end_bar_idx, lookback_bars):
    """
    Score how decisive a candidate TF's trend EMA filter has been over the last N bars.
    Higher score = more dominant signal (not flapping). Range: 0.0 - 1.0.

    Combines:
    - Dominance: how lopsided is above-vs-below-EMA distribution
    - Stability: 1 - (flips per bar)
    """
    if not candidate_ema_lookup:
        return 0.0
    start_idx = max(0, end_bar_idx - lookback_bars)
    above_count = 0
    below_count = 0
    flips = 0
    prev_above = None

    for j in range(start_idx, end_bar_idx + 1):
        bar_t = int(df.iloc[j]['start'])
        close_p = float(df.iloc[j]['close'])
        # Find which resampled period this bar falls in
        ema_val = None
        for period_start, (period_end, ema) in candidate_ema_lookup.items():
            if period_start <= bar_t < period_end:
                ema_val = ema
                break
        if ema_val is None or ema_val <= 0:
            continue
        is_above = close_p > ema_val
        if is_above:
            above_count += 1
        else:
            below_count += 1
        if prev_above is not None and is_above != prev_above:
            flips += 1
        prev_above = is_above

    total = above_count + below_count
    if total == 0:
        return 0.0
    dominance = max(above_count, below_count) / total
    flip_rate = flips / total
    stability = max(0.0, 1.0 - flip_rate * 10)  # scale flips up — even 10% flap is messy
    return (dominance * 0.6) + (stability * 0.4)


def _compute_crisis_score(drawdown_pct, exposure_ratio, mode, recovery_distance, cycles_to_recover):
    """
    Multi-factor crisis scoring engine. Returns (score 0-100, factor_breakdown dict).
    Higher score = more urgency to cut position and free capital.
    """
    # Factor 1: Drawdown severity (0-25)
    if drawdown_pct >= 25:    f1 = 25
    elif drawdown_pct >= 20:  f1 = 22
    elif drawdown_pct >= 15:  f1 = 18
    elif drawdown_pct >= 10:  f1 = 12
    elif drawdown_pct >= 5:   f1 = 5
    else:                     f1 = 0

    # Factor 2: Capital exposure (0-25)
    if exposure_ratio >= 0.85:   f2 = 25
    elif exposure_ratio >= 0.70: f2 = 22
    elif exposure_ratio >= 0.50: f2 = 18
    elif exposure_ratio >= 0.30: f2 = 12
    elif exposure_ratio >= 0.15: f2 = 5
    else:                        f2 = 0

    # Factor 3: Trend hostility (0-20)
    if mode == 'SCALP':      f3 = 20
    elif mode == 'CAUTIOUS':  f3 = 10
    else:                     f3 = 0

    # Factor 4: Recovery distance (0-15)
    if recovery_distance >= 20:   f4 = 15
    elif recovery_distance >= 10: f4 = 10
    elif recovery_distance >= 5:  f4 = 5
    else:                         f4 = 0

    # Factor 5: Opportunity cost (0-15) — low cycles_to_recover = cutting is cheap
    if cycles_to_recover <= 3:     f5 = 15
    elif cycles_to_recover <= 7:   f5 = 10
    elif cycles_to_recover <= 15:  f5 = 5
    else:                          f5 = 0

    score = f1 + f2 + f3 + f4 + f5
    factors = {'drawdown': f1, 'exposure': f2, 'trend': f3, 'distance': f4, 'opportunity': f5}
    return score, factors


def _run_dca_backtest(pair, timeframe, df, capital, params, progress_cb):
    """
    DCA backtest v3: Independent sub-cycles, WOUNDED state, dynamic degradation,
    ATR volatility sizing, and separated trade/risk logging.
    """
    n = len(df)
    if n < WARMUP_BARS + 10:
        return {'error': f'Need at least {WARMUP_BARS + 10} candles, got {n}'}

    # Config
    adaptive_defense = params.get('adaptive_defense', False)
    defense_tf = params.get('defense_tf', 240)
    defense_ema_len = params.get('defense_ema_len', 50)
    ema_lookup = _compute_trend_ema(df, tf_minutes=defense_tf, ema_length=defense_ema_len) if adaptive_defense else {}
    flat_tp = params.get('flat_tp_pct', 0)
    buy_pct = params.get('buy_pct', 5.0)

    # ── Feature 1: Dynamic depth threshold (dip-low SMA) ──
    dynamic_depth_enabled = adaptive_defense and params.get('dynamic_depth', False)
    dynamic_depth_window = int(params.get('dynamic_depth_window', 10))
    dynamic_depth_multiplier = float(params.get('dynamic_depth_multiplier', 0.85))
    dynamic_depth_floor = float(params.get('dynamic_depth_floor', -0.30))  # never shallower than this

    # Dip tracking state
    dip_in_progress = False
    current_dip_low = 0.0
    recent_dip_lows = []  # rolling window of dip bottoms

    # ── Feature 2: TF re-analysis after defensive action (Option A) ──
    # Switches the STRATEGY's bar TF (not the filter). Filter (e.g., daily 9 EMA) stays constant.
    dynamic_tf_enabled = adaptive_defense and params.get('dynamic_tf_analysis', False)
    tf_candidates = params.get('tf_candidates', [60, 240, 1440, 4320])  # 1h, 4h, 1d, 3d
    tf_lookback_days = int(params.get('tf_lookback_days', 30))
    tf_switch_threshold = float(params.get('tf_switch_threshold', 1.0))  # min % return improvement
    tf_dwell_bars = int(params.get('tf_dwell_bars', 50))

    # Determine starting TF in minutes
    if isinstance(timeframe, str):
        ts = timeframe.strip().lower()
        if ts.endswith('m'): starting_tf_min = int(ts.rstrip('m'))
        elif ts.endswith('h'): starting_tf_min = int(ts.rstrip('h')) * 60
        elif ts.endswith('d'): starting_tf_min = int(ts.rstrip('d')) * 1440
        else: starting_tf_min = 60
    else:
        starting_tf_min = int(timeframe)

    # Pre-compute candidate dataframes (using candle_db) when feature enabled
    candidate_dfs = {starting_tf_min: df}
    if dynamic_tf_enabled:
        try:
            from candle_db import query as cdb_query
            first_ts = int(df.iloc[0]['start'])
            last_ts = int(df.iloc[-1]['start']) + 86400
            for cand_tf in tf_candidates:
                if cand_tf == starting_tf_min:
                    continue
                cand_df = cdb_query(pair, cand_tf, first_ts, last_ts)
                if cand_df is not None and len(cand_df) >= WARMUP_BARS + 10:
                    candidate_dfs[cand_tf] = cand_df.sort_values('start').reset_index(drop=True)
        except Exception as e:
            log.warning(f"Failed to pre-load candidate TFs: {e}")

    current_tf = starting_tf_min
    current_df = df
    tf_locked_until_bar = 0
    tf_switch_log = []

    # Compute ATR baseline (200-bar EMA of ATR) for volatility sizing
    atr_series = ta.atr(df['high'], df['low'], df['close'], length=14)
    atr_baseline_series = ta.ema(atr_series, length=200)

    # ── Primary position (the "bag") ──
    cash = float(capital)
    bag_held = 0.0
    bag_avg = 0.0
    bag_cost = 0.0
    bag_buys = 0
    bag_entry_times = []  # store timestamps so they survive TF switches
    bag_highest_tier = 0.0
    bag_frozen = False  # True when crisis cut happened, bag just sits

    # ── Sub-cycle position (independent mini-cycles after crisis) ──
    sub_held = 0.0
    sub_avg = 0.0
    sub_cost = 0.0
    sub_entry_time = 0  # timestamp, not index

    # ── DCA state machine ──
    dca_state = 'SCANNING'  # SCANNING, ARMED, ACCUMULATING, WOUNDED
    last_cross = 'ABOVE'
    wounded_mode = False     # True after any crisis cut

    # ── Crisis scoring ──
    last_action_score = 0
    crisis_score = 0

    # ── Logging ──
    cycle_trades = []        # Normal TP exits
    risk_events = []         # Crisis cuts (capital preservation)
    completed_cycle_profits = []
    equity_curve = []
    total_bars = n - WARMUP_BARS

    # Re-compute atr_series for current_df (must update on TF switch)
    atr_series = ta.atr(current_df['high'], current_df['low'], current_df['close'], length=14)
    atr_baseline_series = ta.ema(atr_series, length=200)

    i = WARMUP_BARS
    n_current = len(current_df)
    while i < n_current:
        bar = current_df.iloc[i]
        close_px = float(bar['close'])
        bar_high = float(bar['high'])  # NEW: for intrabar tier exit detection
        bar_low = float(bar['low'])    # NEW: for intrabar sub-cycle stop detection
        bar_time = int(bar['start'])

        # Reset per-bar flags
        crisis_action_fired_this_bar = False

        if progress_cb and (i - WARMUP_BARS) % max(1, total_bars // 50) == 0:
            # Use current_df length — may have switched mid-run
            cur_total = max(1, len(current_df) - WARMUP_BARS)
            pct = int(max(0, min(100, (i - WARMUP_BARS) / cur_total * 100)))
            progress_cb('running', pct)

        # ── Trend mode ──
        mode = _get_trend_mode(bar_time, close_px, ema_lookup, defense_tf) if adaptive_defense else 'NORMAL'
        mode_cfg = DCA_MODE_CONFIG[mode]
        active_tiers = [(flat_tp, 1.0)] if flat_tp > 0 else mode_cfg['tiers']

        # ── ATR volatility multiplier (Idea F) ──
        cur_atr = atr_series.iloc[i] if i < len(atr_series) and not pd.isna(atr_series.iloc[i]) else close_px * 0.02
        baseline_atr = atr_baseline_series.iloc[i] if i < len(atr_baseline_series) and not pd.isna(atr_baseline_series.iloc[i]) else cur_atr
        vol_mult = max(0.1, min(1.5, baseline_atr / cur_atr if cur_atr > 0 else 1.0))

        # ══════════════════════════════════════════
        # FROZEN BAG: crisis scoring + tier exits
        # ══════════════════════════════════════════
        if bag_held > 0 and bag_avg > 0:
            bag_dd = ((bag_avg - close_px) / bag_avg) * 100
            bag_profit_pct = -bag_dd

            # ── Crisis scoring (continuous) ──
            if bag_dd > 0 and not bag_frozen:
                pos_val = bag_held * close_px
                total_cap = cash + pos_val + (sub_held * close_px)
                exp_ratio = pos_val / total_cap if total_cap > 0 else 1.0
                first_tier = active_tiers[0][0] if active_tiers else 3.0
                rec_dist = bag_dd + first_tier
                win_profs = [p for p in completed_cycle_profits if p > 0]
                avg_cyc_prof = sum(win_profs) / len(win_profs) if win_profs else capital * 0.02
                unrealized = abs((bag_avg - close_px) * bag_held)
                cyc_to_rec = unrealized / avg_cyc_prof if avg_cyc_prof > 0 else 999

                crisis_score, factors = _compute_crisis_score(bag_dd, exp_ratio, mode, rec_dist, cyc_to_rec)

                # Dynamic degradation (Idea C) — applied to entry sizing below
                # Score also drives cuts:
                if crisis_score > last_action_score:
                    cut_pct = 0.0
                    action_name = 'NONE'

                    if crisis_score >= 90:
                        cut_pct, action_name = 1.0, 'FULL_CUT'
                    elif crisis_score >= 75:
                        cut_pct, action_name = 0.75, 'HEAVY_CUT'
                    elif crisis_score >= 60:
                        cut_pct, action_name = 0.50, 'MOD_CUT'
                    elif crisis_score >= 45:
                        cut_pct, action_name = 0.25, 'LIGHT_CUT'

                    if cut_pct > 0:
                        sell_qty = bag_held * cut_pct
                        # Crisis cuts are market sells in live — taker fee + slippage
                        exit_px = close_px * (1 - SLIPPAGE_PCT)
                        fee = _exit_fee(exit_px, sell_qty)
                        pnl = (exit_px - bag_avg) * sell_qty
                        cash += exit_px * sell_qty - fee
                        f_str = ' '.join(f'{k[0]}={v}' for k, v in factors.items())
                        risk_events.append({
                            'entry_time': bag_entry_times[0] if bag_entry_times else bar_time,
                            'exit_time': bar_time, 'side': 'LONG',
                            'entry_price': bag_avg, 'exit_price': exit_px,
                            'size': sell_qty, 'pnl': round(pnl - fee, 4), 'fee': round(fee, 4),
                            'exit_reason': f'{action_name} (score={crisis_score} {f_str})',
                            'signal_type': f'DCA/{mode} ({bag_buys} buys)',
                            'trade_type': 'risk_event',
                            'price_at_cut': close_px
                        })
                        sold_frac = sell_qty / (bag_held) if bag_held > 0 else 1
                        bag_cost *= (1 - sold_frac)
                        bag_held -= sell_qty
                        last_action_score = crisis_score
                        crisis_action_fired_this_bar = True  # Feature 2 trigger

                        # Enter WOUNDED state — bag frozen, sub-cycles take over
                        wounded_mode = True
                        bag_frozen = True
                        dca_state = 'SCANNING'
                        last_cross = 'ABOVE'

                        if bag_held < 1e-10:
                            bag_held = 0.0
                            bag_avg = 0.0
                            bag_cost = 0.0
                            bag_frozen = False
                            bag_buys = 0
                            bag_entry_times = []
                            last_action_score = 0

            # ── Tier ladder re-arm: a ≥3% retrace re-arms the lower tiers ──
            if bag_held > 0 and bag_profit_pct <= -3.0 and bag_highest_tier > 0:
                bag_highest_tier = 0.0

            # ── Tier exits on bag (still active even when frozen) ──
            if bag_held > 0 and bag_profit_pct > 0:
                for tier_pct, sell_frac in active_tiers:
                    if tier_pct <= bag_highest_tier:
                        continue
                    # Use bar_high (intrabar) for tier hit detection — live limit orders fill at tier price
                    tier_price = bag_avg * (1 + tier_pct / 100)
                    bar_high_profit_pct = ((bar_high - bag_avg) / bag_avg) * 100 if bag_avg > 0 else 0
                    if bar_high_profit_pct >= tier_pct:
                        sell_qty = bag_held * sell_frac
                        if sell_qty <= 0:
                            continue
                        # Exit at the tier price (limit order fill), not bar close
                        exit_px = tier_price
                        fee = _maker_fee(exit_px, sell_qty)
                        tier_pnl = (exit_px - bag_avg) * sell_qty - fee
                        cash += exit_px * sell_qty - fee
                        sf = sell_qty / bag_held if bag_held > 0 else 1
                        bag_cost *= (1 - sf)
                        bag_held -= sell_qty
                        bag_highest_tier = tier_pct
                        cycle_trades.append({
                            'entry_time': bag_entry_times[0] if bag_entry_times else bar_time,
                            'exit_time': bar_time, 'side': 'LONG',
                            'entry_price': bag_avg, 'exit_price': exit_px,
                            'size': sell_qty, 'pnl': round(tier_pnl, 4), 'fee': round(fee, 4),
                            'exit_reason': f'TIER_{tier_pct}%',
                            'signal_type': f'DCA/{mode} ({bag_buys} buys)',
                            'trade_type': 'cycle_trade'
                        })

                # Bag fully exited via tiers — recovery complete
                if bag_held < 1e-10:
                    recent = [t['pnl'] for t in cycle_trades[-10:] if 'TIER' in t.get('exit_reason', '')]
                    if recent:
                        completed_cycle_profits.append(sum(recent))
                    bag_held = 0.0
                    bag_avg = 0.0
                    bag_cost = 0.0
                    bag_frozen = False
                    bag_buys = 0
                    bag_entry_times = []
                    bag_highest_tier = 0.0
                    last_action_score = 0

            # ── WOUNDED recovery check ──
            # Reset WOUNDED only when: EMA defense exits CAUTIOUS/SCALP AND score < 20
            if wounded_mode and mode == 'NORMAL' and crisis_score < 20:
                wounded_mode = False
                if bag_held < 1e-10:
                    bag_frozen = False

        # ══════════════════════════════════════════
        # SUB-CYCLE: independent mini-position (flat 3% TP)
        # ══════════════════════════════════════════
        if sub_held > 0:
            sub_tp = flat_tp if flat_tp > 0 else 3.0  # default 3% for sub-cycles
            sub_tp_price = sub_avg * (1 + sub_tp / 100)
            sub_stop_price = sub_avg * (1 - 5.0 / 100)
            # Check the stop BEFORE the TP on the same bar (conservative intrabar ordering)
            if bar_low <= sub_stop_price:
                # Sub-cycle stop: tight -5% stop (intrabar trigger).
                # Gap-through: if the bar opened below the stop, fill at the open.
                # Market sell in live — taker fee + slippage.
                exit_px = min(sub_stop_price, float(bar['open'])) * (1 - SLIPPAGE_PCT)
                fee = _exit_fee(exit_px, sub_held)
                pnl = (exit_px - sub_avg) * sub_held - fee
                cash += exit_px * sub_held - fee
                cycle_trades.append({
                    'entry_time': sub_entry_time,
                    'exit_time': bar_time, 'side': 'LONG',
                    'entry_price': sub_avg, 'exit_price': exit_px,
                    'size': sub_held, 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                    'exit_reason': 'SUB_STOP',
                    'signal_type': 'DCA/SUB-CYCLE',
                    'trade_type': 'cycle_trade'
                })
                sub_held = 0.0
                sub_avg = 0.0
                sub_cost = 0.0
            elif bar_high >= sub_tp_price:
                exit_px = sub_tp_price
                fee = _maker_fee(exit_px, sub_held)
                pnl = (exit_px - sub_avg) * sub_held - fee
                cash += exit_px * sub_held - fee
                completed_cycle_profits.append(pnl)
                cycle_trades.append({
                    'entry_time': sub_entry_time,
                    'exit_time': bar_time, 'side': 'LONG',
                    'entry_price': sub_avg, 'exit_price': exit_px,
                    'size': sub_held, 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                    'exit_reason': f'SUB_TP_{sub_tp}%',
                    'signal_type': 'DCA/SUB-CYCLE',
                    'trade_type': 'cycle_trade'
                })
                sub_held = 0.0
                sub_avg = 0.0
                sub_cost = 0.0

        # ══════════════════════════════════════════
        # FEATURE 2 (Option A): SWITCH STRATEGY BAR TF AFTER DEFENSIVE ACTION
        # The trend filter (e.g., daily 9 EMA) stays constant; only the bar TF the
        # strategy operates on may change. Run mini-backtests on each candidate TF
        # over the recent lookback window and switch to the best.
        # ══════════════════════════════════════════
        if dynamic_tf_enabled and crisis_action_fired_this_bar and i >= tf_locked_until_bar:
            current_score = _score_tf_for_dca(pair, current_tf, bar_time, tf_lookback_days, params, capital)
            best_tf = None
            best_score = current_score if current_score is not None else -999
            for cand_tf in tf_candidates:
                if cand_tf == current_tf:
                    continue
                if cand_tf not in candidate_dfs:
                    continue
                cand_score = _score_tf_for_dca(pair, cand_tf, bar_time, tf_lookback_days, params, capital)
                if cand_score is not None and cand_score > best_score + tf_switch_threshold:
                    best_score = cand_score
                    best_tf = cand_tf
            if best_tf is not None and best_tf != current_tf:
                # SWITCH bar TF: jump into the new df at the current timestamp
                new_df = candidate_dfs[best_tf]
                new_idx = _find_idx_at_or_after(new_df, bar_time)
                # Need just enough bars before for ROC calc (~50). Don't require full WARMUP_BARS.
                if new_idx is not None and new_idx >= 50 and new_idx < len(new_df):
                    old_tf = current_tf
                    current_tf = best_tf
                    current_df = new_df
                    n_current = len(current_df)
                    # Re-compute ATR series for the new TF
                    atr_series = ta.atr(current_df['high'], current_df['low'], current_df['close'], length=14)
                    atr_baseline_series = ta.ema(atr_series, length=200)
                    # Reset strategy state (signal machinery is TF-specific)
                    dca_state = 'SCANNING'
                    last_cross = 'ABOVE'
                    recent_dip_lows = []
                    current_dip_low = 0.0
                    dip_in_progress = False
                    # Position state (cash, bag) carries over — that's the user's actual position
                    i = new_idx
                    tf_locked_until_bar = i + tf_dwell_bars
                    tf_switch_log.append({
                        'bar': i, 'time': bar_time,
                        'from_tf': old_tf, 'to_tf': best_tf,
                        'score': round(best_score, 3)
                    })
                    continue  # restart loop on new TF

        # ══════════════════════════════════════════
        # SIGNAL EVALUATION + ENTRY
        # ══════════════════════════════════════════
        window = current_df.iloc[max(0, i - 300):i + 1].copy()

        # ── ARM threshold selection ──
        # WOUNDED: extreme; Dynamic depth: dip-low SMA (deeper-only); else: mode default
        if wounded_mode:
            arm_thresh = -1.50
        elif dynamic_depth_enabled and len(recent_dip_lows) >= 3:
            avg_low = sum(recent_dip_lows) / len(recent_dip_lows)
            dyn_thresh = avg_low * dynamic_depth_multiplier
            # min() with floor: more negative = deeper. floor=-0.30, dyn=-1.5 → min(-0.30,-1.5) = -1.5
            arm_thresh = min(dynamic_depth_floor, dyn_thresh)
        else:
            arm_thresh = mode_cfg['arm_thresh']

        try:
            if dca_state == 'PAUSED':
                sig, reason, extra = 'HOLD', 'Paused', {}
            else:
                sig, reason, extra = calculate_dca(window, dca_state=dca_state,
                    last_cross_direction=last_cross, arm_threshold=arm_thresh)
        except Exception:
            sig, reason, extra = 'HOLD', '', {}
        extra = extra if isinstance(extra, dict) else {}

        # ── Feature 1: Dip tracking — record local ROC bottoms ──
        if dynamic_depth_enabled:
            fast_roc_now = extra.get('fast_roc', 0)
            slow_roc_now = extra.get('slow_roc', 0)
            worst_roc = min(fast_roc_now, slow_roc_now)

            if not dip_in_progress and worst_roc < 0:
                dip_in_progress = True
                current_dip_low = worst_roc
            elif dip_in_progress:
                if worst_roc < current_dip_low:
                    current_dip_low = worst_roc
                # Dip ends when both ROCs return non-negative
                if fast_roc_now >= 0 and slow_roc_now >= 0:
                    recent_dip_lows.append(current_dip_low)
                    if len(recent_dip_lows) > dynamic_depth_window:
                        recent_dip_lows.pop(0)
                    dip_in_progress = False
                    current_dip_low = 0.0

        if dca_state == 'SCANNING':
            if sig == 'ARM':
                dca_state = 'ARMED'

        elif dca_state == 'ARMED':
            if sig == 'DISARM':
                dca_state = 'SCANNING'
            elif sig == 'BUY' and cash > 5.0:
                depth_mult = min(extra.get('depth_multiplier', 1.0), mode_cfg['depth_cap'])
                drawdown_mult = 1.0
                if bag_held > 0 and bag_avg > 0:
                    dd = ((bag_avg - close_px) / bag_avg) * 100
                    if dd >= 20: drawdown_mult = 0.25
                    elif dd >= 10: drawdown_mult = 0.50

                # Dynamic degradation (Idea C): scale by crisis score
                degrade_mult = max(0.05, 1.0 - (crisis_score / 45.0) ** 2) if crisis_score < 45 else 0.05

                buy_usd = cash * (buy_pct / 100) * depth_mult * drawdown_mult * mode_cfg['size_mult'] * vol_mult * degrade_mult
                buy_usd = min(buy_usd, cash * 0.50)
                if buy_usd >= 5.0:
                    qty = buy_usd / close_px
                    fee = _maker_fee(close_px, qty)

                    if wounded_mode or bag_frozen:
                        # ── Independent sub-cycle (Idea E) ──
                        if sub_held == 0:
                            sub_held = qty
                            sub_avg = close_px
                            sub_cost = close_px * qty
                            sub_entry_time = bar_time
                            cash -= (close_px * qty + fee)
                    else:
                        # ── Normal accumulation into bag ──
                        old_cost = bag_cost
                        new_cost = close_px * qty
                        bag_cost = old_cost + new_cost
                        bag_held += qty
                        bag_avg = bag_cost / bag_held if bag_held > 0 else close_px
                        cash -= (new_cost + fee)
                        bag_buys += 1
                        bag_entry_times.append(bar_time)

                    dca_state = 'ACCUMULATING'
                    last_cross = 'BELOW'
                else:
                    dca_state = 'ACCUMULATING' if (bag_held > 0 or sub_held > 0) else 'SCANNING'

        elif dca_state == 'ACCUMULATING':
            if last_cross == 'BELOW':
                fast_roc = extra.get('fast_roc', 0)
                slow_roc = extra.get('slow_roc', 0)
                if fast_roc >= 0 and slow_roc >= 0:
                    last_cross = 'ABOVE'
            elif last_cross == 'ABOVE':
                if sig == 'ARM':
                    dca_state = 'ARMED'
                    last_cross = 'BELOW'

        # ── Equity ──
        bag_val = bag_held * close_px if bag_held > 0 else 0
        sub_val = sub_held * close_px if sub_held > 0 else 0
        equity_curve.append({'time': bar_time, 'equity': round(cash + bag_val + sub_val, 2)})

        # Advance to next bar
        i += 1

    # ── Close open positions at end ──
    final_px = float(current_df.iloc[-1]['close'])
    final_time = int(current_df.iloc[-1]['start'])
    if bag_held > 0:
        fee = _exit_fee(final_px, bag_held)  # end-of-data close = market
        pnl = (final_px - bag_avg) * bag_held - fee
        cash += final_px * bag_held - fee
        cycle_trades.append({
            'entry_time': bag_entry_times[0] if bag_entry_times else final_time,
            'exit_time': final_time, 'side': 'LONG',
            'entry_price': bag_avg, 'exit_price': final_px,
            'size': bag_held, 'pnl': round(pnl, 4), 'fee': round(fee, 4),
            'exit_reason': 'END_OF_DATA', 'signal_type': f'DCA/{mode} ({bag_buys} buys)',
            'trade_type': 'cycle_trade'
        })
    if sub_held > 0:
        fee = _exit_fee(final_px, sub_held)  # end-of-data close = market
        pnl = (final_px - sub_avg) * sub_held - fee
        cash += final_px * sub_held - fee
        cycle_trades.append({
            'entry_time': sub_entry_time,
            'exit_time': final_time, 'side': 'LONG',
            'entry_price': sub_avg, 'exit_price': final_px,
            'size': sub_held, 'pnl': round(pnl, 4), 'fee': round(fee, 4),
            'exit_reason': 'END_OF_DATA', 'signal_type': 'DCA/SUB-CYCLE',
            'trade_type': 'cycle_trade'
        })

    # ── Compute stats separately ──
    all_trades = cycle_trades + risk_events
    summary = _compute_stats(all_trades, equity_curve, capital, timeframe)

    # Cycle-only stats (the real win rate)
    cycle_wins = [t['pnl'] for t in cycle_trades if t['pnl'] > 0]
    cycle_losses = [t['pnl'] for t in cycle_trades if t['pnl'] <= 0]
    summary['cycle_trades'] = len(cycle_trades)
    summary['cycle_win_rate'] = round(len(cycle_wins) / len(cycle_trades) * 100, 1) if cycle_trades else 0
    summary['risk_events'] = len(risk_events)
    summary['risk_total_pnl'] = round(sum(t['pnl'] for t in risk_events), 2)

    # Capital saved by risk events (how much further price dropped after each cut)
    total_saved = 0
    n_cur = len(current_df)
    for re in risk_events:
        cut_px = re['price_at_cut']
        # Find lowest price after this cut within 500 bars (in current_df, post-switch)
        cut_idx = next((j for j in range(WARMUP_BARS, n_cur) if int(current_df.iloc[j]['start']) >= re['exit_time']), n_cur - 1)
        future_low = current_df.iloc[cut_idx:min(cut_idx + 500, n_cur)]['low'].min()
        saved_per_unit = cut_px - future_low
        total_saved += saved_per_unit * re['size']
    summary['capital_saved_by_cuts'] = round(total_saved, 2)

    return {
        'equity_curve': equity_curve,
        'trades': all_trades,
        'cycle_trades': cycle_trades,
        'risk_events': risk_events,
        'summary': summary,
        'tf_switches': tf_switch_log if dynamic_tf_enabled else [],
        'final_recent_dip_lows': recent_dip_lows if dynamic_depth_enabled else [],
        'config': {'pair': pair, 'strategy': 'DCA', 'timeframe': timeframe,
                   'capital': capital, 'params': params, 'total_bars': n, 'warmup': WARMUP_BARS}
    }


# ══════════════════════════════════════════════════════════
# GRID Backtest — Dedicated simulation engine
# ══════════════════════════════════════════════════════════

def _run_grid_backtest(pair, timeframe, df, capital, params, progress_cb):
    """
    Grid bot backtest — mirrors grid_engine.py exactly:
    Grid follow/sliding, direction-aware ADX halts, per-fill tiered trailing stops,
    depth escalation, buy cancellation/redeployment, runner exits, recovery velocity,
    circuit breaker.
    """
    n = len(df)
    if n < WARMUP_BARS + 10:
        return {'error': f'Need at least {WARMUP_BARS + 10} candles, got {n}'}

    # --- Config ---
    step_pct = params.get('step_pct', 0.6) / 100.0
    mode = params.get('mode', 'LONG').upper()
    follow_enabled = params.get('follow', True)
    circuit_breaker_pct = params.get('circuit_breaker_pct', 6.0) / 100.0
    min_order_usd = params.get('min_order_usd', 5.0)
    is_dynamic = params.get('dynamic', False)
    min_step_pct = params.get('min_step_pct', 0.3)
    max_step_pct = params.get('max_step_pct', 3.0)

    # Pre-compute indicators
    atr_series = ta.atr(df['high'], df['low'], df['close'], length=14)
    adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
    adx_col = adx_df['ADX_14'] if adx_df is not None and 'ADX_14' in adx_df.columns else pd.Series([0.0] * n)
    sma5 = ta.sma(df['close'], 5)

    # Determine test TF in minutes (from string like "5m", "1h", "1d", or int)
    if isinstance(timeframe, str):
        tf_str = timeframe.strip().lower()
        if tf_str.endswith('m'):
            tf_min = int(tf_str.rstrip('m'))
        elif tf_str.endswith('h'):
            tf_min = int(tf_str.rstrip('h')) * 60
        elif tf_str.endswith('d'):
            tf_min = int(tf_str.rstrip('d')) * 1440
        else:
            tf_min = 5
    else:
        tf_min = int(timeframe)

    # Load 1m sub-bars for realistic intrabar price action (skip if test TF == 1m)
    subbars = None
    if tf_min > 1:
        first_ts = int(df.iloc[0]['start'])
        last_ts = int(df.iloc[-1]['start']) + (tf_min * 60)
        subbars = _load_1m_subbars(pair, first_ts, last_ts)
        if subbars is None:
            log.warning(f"[{pair}] No 1m sub-bar data — falling back to bar-based price action")

    # Pre-compute Bollinger Bands for dynamic mode
    bb_data = ta.bbands(df['close'], length=20, std=2)
    bb_upper_col = None
    bb_lower_col = None
    bb_width_series = pd.Series([0.0] * n)
    bb_width_avg_series = pd.Series([0.0] * n)
    if is_dynamic and bb_data is not None:
        u_cols = [c for c in bb_data.columns if 'BBU' in c]
        l_cols = [c for c in bb_data.columns if 'BBL' in c]
        if u_cols and l_cols:
            bb_upper_col = bb_data[u_cols[0]]
            bb_lower_col = bb_data[l_cols[0]]
            bb_width_series = bb_upper_col - bb_lower_col
            bb_width_avg_series = ta.sma(bb_width_series, 50)
            if bb_width_avg_series is None:
                bb_width_avg_series = bb_width_series

    # Initial grid setup
    first_close = float(df.iloc[WARMUP_BARS]['close'])
    step_usd = first_close * step_pct
    grid_count = max(2, int(capital / min_order_usd))
    chunk_usd = capital / grid_count

    # Auto-center grid bounds
    if 'lower_price' in params and 'upper_price' in params:
        lower_price = params['lower_price']
        upper_price = params['upper_price']
    else:
        if mode == 'LONG':
            lower_price = first_close - (grid_count * step_usd)
            upper_price = first_close
        elif mode == 'SHORT':
            lower_price = first_close
            upper_price = first_close + (grid_count * step_usd)
        else:
            half = grid_count // 2
            lower_price = first_close - (half * step_usd)
            upper_price = first_close + ((grid_count - half) * step_usd)

    # --- State ---
    cash = float(capital)
    total_asset = 0.0
    halted = False
    halt_mode = None  # 'FAVORABLE', 'ADVERSE', 'NEUTRAL'

    # Per-fill trailing stops (mirrors per_fill_trails)
    trails = []  # {fill_price, qty, hwm, base_trail, trail_mult, effective_trail, level_idx, fill_bar, has_sell}

    # Active grid orders (buy side only — sells tracked via trails)
    buy_levels = []  # prices where buy orders sit
    pending_sells = {}  # fill_price -> sell_price

    # Risk state
    cancelled_buy_levels = []
    recovery_timestamps = []
    depth_direction_history = []  # last N direction readings

    # V3 state
    is_gtfo_active = False
    gtfo_target_price = 0
    gtfo_armed_bar = -1
    gtfo_high_score_streak = 0
    quarantine_until = 0
    loss_history = []  # [(bar_time, loss_pct), ...] rolling 120s
    last_regime_label = ''

    # Pre-compute ATR SMA(50) for dynamic quarantine
    atr_sma_50_series = ta.sma(atr_series, 50)

    trades = []
    equity_curve = []

    # --- Helper: compute direction (matches compute_direction in grid_engine.py) ---
    def get_direction(bar_idx):
        if sma5 is None or bar_idx < 8:
            return 'CHOPPY'
        cur_sma = float(sma5.iloc[bar_idx]) if not pd.isna(sma5.iloc[bar_idx]) else 0
        prev_sma = float(sma5.iloc[bar_idx - 3]) if bar_idx >= 3 and not pd.isna(sma5.iloc[bar_idx - 3]) else cur_sma
        cur_px = float(df.iloc[bar_idx]['close'])
        if prev_sma <= 0:
            return 'CHOPPY'
        slope = (cur_sma - prev_sma) / prev_sma
        if cur_px > cur_sma and slope > 0:
            return 'RISING'
        elif cur_px < cur_sma and slope < 0:
            return 'FALLING'
        return 'CHOPPY'

    # --- Helper: trail distance by grid level (matches get_trail_distance) ---
    def get_trail_dist(level_idx, total_levels, step):
        if total_levels <= 1:
            return step * 1.0
        pos_from_top = (total_levels - 1) - level_idx
        third = total_levels / 3.0
        if pos_from_top < third:
            return step * 3.0
        elif pos_from_top < third * 2:
            return step * 2.0
        elif level_idx == 0:
            return step * 1.0
        else:
            return step * 1.5

    # --- Helper: adjust trail multipliers (matches adjust_trail_multipliers) ---
    def adjust_multipliers(h_mode, depth, velocity):
        total_levels = len(buy_levels) + len(trails) + 1
        for t in trails:
            m = 1.0
            if h_mode == 'FAVORABLE':
                m *= 1.5
            elif h_mode == 'ADVERSE':
                m *= 0.75
            if depth >= 6:
                m *= 0.75
            elif depth >= 4:
                if t['level_idx'] < total_levels * 0.3:
                    m *= 0.75
            if velocity >= 2.0 and t['level_idx'] < total_levels * 0.4:
                m *= 1.25
            t['trail_mult'] = round(m, 3)
            t['effective_trail'] = round(t['base_trail'] * m, 6)

    # --- Helper: recovery velocity ---
    def get_velocity(bar_time):
        recovery_window = max(300, 10 * tf_min * 60)  # spec: 10 candles of the test TF
        recent = [ts for ts in recovery_timestamps if bar_time - ts < recovery_window]
        return float(len(recent))

    # --- Helper: spacing guard ---
    def has_nearby(price, levels, min_gap):
        return any(abs(price - lvl) < min_gap for lvl in levels)

    # Build initial buy levels
    lvl = lower_price
    while lvl <= upper_price:
        if lvl < first_close * 0.999:
            if mode != 'SHORT':
                buy_levels.append(lvl)
        lvl += step_usd
    total_buy_levels = len(buy_levels)

    total_bars = n - WARMUP_BARS

    for bar_idx in range(WARMUP_BARS, n):
        if progress_cb and bar_idx % 500 == 0:
            progress_cb('Simulating grid...', int((bar_idx - WARMUP_BARS) / total_bars * 100))

        row = df.iloc[bar_idx]
        bar_time = int(row['start'])
        bar_high = float(row['high'])
        bar_low = float(row['low'])
        close_px = float(row['close'])
        # LOOKAHEAD FIX (BT-H1): all gating/sizing inputs (ATR/ADX/BB/regime/crisis)
        # come from the PREVIOUS bar — the live engine acts after a candle closes,
        # so decisions governing THIS bar's sub-bars can't use this bar's close.
        sig_idx = bar_idx - 1
        sig_close = float(df.iloc[sig_idx]['close'])
        atr = float(atr_series.iloc[sig_idx]) if not pd.isna(atr_series.iloc[sig_idx]) else sig_close * 0.01
        adx = float(adx_col.iloc[sig_idx]) if not pd.isna(adx_col.iloc[sig_idx]) else 0.0

        step_usd = sig_close * step_pct
        depth = len(trails)
        direction = get_direction(sig_idx)
        velocity = get_velocity(bar_time)
        suspend_buys = False

        # Dynamic mode: compute regime-aware step, trail, sizing each bar
        dyn_trail_dist = atr  # fallback
        regime = 'WIDE_RANGE'
        crisis = 0
        if is_dynamic:
            bb_w = float(bb_width_series.iloc[sig_idx]) if not pd.isna(bb_width_series.iloc[sig_idx]) else 0
            bb_wa = float(bb_width_avg_series.iloc[sig_idx]) if not pd.isna(bb_width_avg_series.iloc[sig_idx]) else bb_w
            vol_ratio = (bb_w / bb_wa) if bb_wa > 0 else 1.0
            regime = compute_regime(adx, bb_w, bb_wa, direction)

            # V3: depth-aware step and Kelly
            step_usd = compute_dynamic_step(atr, adx, bb_w, bb_wa, direction, sig_close,
                                            min_step_pct, max_step_pct, depth=depth)
            dyn_trail_dist = compute_dynamic_trail(atr, adx, direction, velocity)
            chunk_usd = compute_kelly_size(adx, direction, vol_ratio, step_usd, dyn_trail_dist,
                                           capital, min_order_usd, depth=depth)

            # Dynamic grid bounds from BB
            if bb_upper_col is not None and not pd.isna(bb_upper_col.iloc[sig_idx]):
                upper_price = float(bb_upper_col.iloc[sig_idx]) + (0.5 * atr)
                lower_price = float(bb_lower_col.iloc[sig_idx]) - (0.5 * atr)

            # V3: Update loss_history (rolling window) — prev close, same rationale
            unrealized_loss = sum(max(0, (t['fill_price'] - sig_close) * t['qty']) for t in trails)
            u_loss_pct = (unrealized_loss / capital * 100) if capital > 0 else 0
            loss_history.append((bar_time, u_loss_pct))
            loss_window = max(120, 2 * tf_min * 60)  # collapse to <=1 bar on 5m+ otherwise
            loss_history = [(t, lp) for t, lp in loss_history if bar_time - t <= loss_window]

            # V3: 5-factor crisis score with velocity-of-loss
            crisis = compute_grid_crisis_score(depth, u_loss_pct, regime, direction, velocity, loss_history)

            # ── V3 STICKY GTFO MODE ──
            if is_gtfo_active and trails:
                # Run GTFO cycle: resync target, check fill, time-decay nibble.
                # The fill test uses the PREVIOUS bar's target (this bar's crisis
                # score derives from its close — same-bar fill was lookahead).
                prior_target = gtfo_target_price
                avg_price = sum(t['qty'] * t['fill_price'] for t in trails) / sum(t['qty'] for t in trails)
                new_target = compute_gtfo_target(avg_price, crisis)

                gtfo_fillable = prior_target > 0 and bar_idx > gtfo_armed_bar and bar_high >= prior_target
                if gtfo_fillable:
                    new_target = prior_target  # fill at the resting order's price
                if gtfo_fillable:
                    # Full stack exit at target
                    total_qty = sum(t['qty'] for t in trails)
                    total_cost = sum(t['qty'] * t['fill_price'] for t in trails)
                    fee = _maker_fee(new_target, total_qty)
                    pnl = (new_target * total_qty) - total_cost - fee
                    cash += new_target * total_qty - fee
                    trades.append({
                        'entry_time': int(df.iloc[trails[0]['fill_bar']]['start']),
                        'exit_time': bar_time, 'side': 'LONG',
                        'entry_price': avg_price, 'exit_price': new_target,
                        'size': total_qty, 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                        'exit_reason': 'GTFO_FILL', 'signal_type': 'GRID'
                    })
                    total_asset = 0.0
                    trails = []
                    pending_sells = {}
                    buy_levels = []
                    is_gtfo_active = False
                    gtfo_target_price = 0
                    gtfo_high_score_streak = 0

                    # V3: dynamic quarantine
                    atr_sma_50 = float(atr_sma_50_series.iloc[bar_idx]) if not pd.isna(atr_sma_50_series.iloc[bar_idx]) else atr
                    quarantine_min = max(15, min(240, 60 * (atr / atr_sma_50) if atr_sma_50 > 0 else 60))
                    quarantine_until = bar_time + int(quarantine_min * 60)
                else:
                    # Time-decay nibble: score > 70 for 4+ cycles → sell 10% at market
                    if crisis > 70:
                        gtfo_high_score_streak += 1
                        if gtfo_high_score_streak >= 4:
                            total_qty = sum(t['qty'] for t in trails)
                            nibble_qty = total_qty * 0.10
                            nib_px = close_px * (1 - SLIPPAGE_PCT)
                            fee = _exit_fee(nib_px, nibble_qty)  # market sell in live
                            avg_for_pnl = avg_price
                            pnl = (nib_px - avg_for_pnl) * nibble_qty - fee
                            cash += nib_px * nibble_qty - fee
                            for t in trails:
                                t['qty'] *= 0.90
                            total_asset = max(0, total_asset - nibble_qty)
                            trades.append({
                                'entry_time': int(df.iloc[trails[0]['fill_bar']]['start']),
                                'exit_time': bar_time, 'side': 'LONG',
                                'entry_price': avg_for_pnl, 'exit_price': close_px,
                                'size': nibble_qty, 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                                'exit_reason': 'NIBBLE', 'signal_type': 'GRID'
                            })
                            gtfo_high_score_streak = 0
                    else:
                        gtfo_high_score_streak = 0
                    gtfo_target_price = new_target

                # Skip everything else while in GTFO
                portfolio = cash + total_asset * close_px
                equity_curve.append({'time': bar_time, 'equity': round(portfolio, 2)})
                continue

            # ── V3 QUARANTINE: don't redeploy until time elapsed AND ADX < 20 ──
            if quarantine_until > bar_time and adx >= 20:
                portfolio = cash + total_asset * close_px
                equity_curve.append({'time': bar_time, 'equity': round(portfolio, 2)})
                continue

            # ── V3 CRISIS ACTIONS ──
            if crisis >= 80 and trails:
                # Hard liquidation: 75% emergency dump
                n_exit = max(1, int(len(trails) * 0.75))
                sorted_trails = sorted(trails, key=lambda t: t['fill_price'])
                for t in sorted_trails[:n_exit]:
                    cut_px = close_px * (1 - SLIPPAGE_PCT)  # market sell in live
                    fee = _exit_fee(cut_px, t['qty'])
                    pnl = (cut_px - t['fill_price']) * t['qty'] - fee
                    cash += cut_px * t['qty'] - fee
                    total_asset -= t['qty']
                    trades.append({
                        'entry_time': int(df.iloc[t['fill_bar']]['start']),
                        'exit_time': bar_time, 'side': 'LONG',
                        'entry_price': t['fill_price'], 'exit_price': close_px,
                        'size': t['qty'], 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                        'exit_reason': 'CRISIS_CUT', 'signal_type': 'GRID'
                    })
                trails = sorted_trails[n_exit:]
                suspend_buys = True
            elif crisis >= 50 and trails:
                # V3: Enter Whole-Stack GTFO
                is_gtfo_active = True
                avg_price = sum(t['qty'] * t['fill_price'] for t in trails) / sum(t['qty'] for t in trails)
                gtfo_target_price = compute_gtfo_target(avg_price, crisis)
                pending_sells = {}
                buy_levels = []
                cancelled_buy_levels = []
                gtfo_high_score_streak = 0
                gtfo_armed_bar = bar_idx
                # Fill only from the NEXT bar: the target derives from THIS bar's
                # close-based crisis score, so its own high is lookahead.
                if False and bar_high >= gtfo_target_price:
                    total_qty = sum(t['qty'] for t in trails)
                    total_cost = sum(t['qty'] * t['fill_price'] for t in trails)
                    fee = _maker_fee(gtfo_target_price, total_qty)
                    pnl = (gtfo_target_price * total_qty) - total_cost - fee
                    cash += gtfo_target_price * total_qty - fee
                    trades.append({
                        'entry_time': int(df.iloc[trails[0]['fill_bar']]['start']),
                        'exit_time': bar_time, 'side': 'LONG',
                        'entry_price': avg_price, 'exit_price': gtfo_target_price,
                        'size': total_qty, 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                        'exit_reason': 'GTFO_FILL', 'signal_type': 'GRID'
                    })
                    total_asset = 0.0
                    trails = []
                    is_gtfo_active = False
                    atr_sma_50 = float(atr_sma_50_series.iloc[bar_idx]) if not pd.isna(atr_sma_50_series.iloc[bar_idx]) else atr
                    quarantine_min = max(15, min(240, 60 * (atr / atr_sma_50) if atr_sma_50 > 0 else 60))
                    quarantine_until = bar_time + int(quarantine_min * 60)
                portfolio = cash + total_asset * close_px
                equity_curve.append({'time': bar_time, 'equity': round(portfolio, 2)})
                continue
            elif crisis >= 40:
                buy_levels.sort()
                n_cancel = len(buy_levels) // 2
                cancelled_buy_levels.extend(buy_levels[:n_cancel])
                buy_levels = buy_levels[n_cancel:]
                suspend_buys = True

            # ── V3 WATERFALL CLAUSE ──
            if adx > 35 and bb_w > 0:
                bb_lower_now = float(bb_lower_col.iloc[bar_idx]) if bb_lower_col is not None and not pd.isna(bb_lower_col.iloc[bar_idx]) else 0
                if bb_lower_now > 0 and close_px < bb_lower_now:
                    suspend_buys = True

            # ── V3 SYNTHETIC RUNNER on STRONG_TREND+RISING transition ──
            current_label = regime + ('_RISING' if direction == 'RISING' else ('_FALLING' if direction == 'FALLING' else ''))
            if regime == 'STRONG_TREND' and direction == 'RISING' and last_regime_label != 'STRONG_TREND_RISING':
                if trails:
                    trails_sorted = sorted(trails, key=lambda t: t['fill_price'], reverse=True)
                    n_runners = max(1, int(len(trails_sorted) * 0.25))
                    runner_trail = 2.5 * atr
                    for t in trails_sorted[:n_runners]:
                        if t.get('is_runner'):
                            continue
                        # Cancel its pending sell
                        if t['fill_price'] in pending_sells:
                            del pending_sells[t['fill_price']]
                        t['effective_trail'] = runner_trail
                        t['base_trail'] = runner_trail
                        t['trail_mult'] = 1.0
                        t['is_runner'] = True
                suspend_buys = True
            last_regime_label = current_label

        min_gap = step_usd * 0.4

        # ── Circuit breaker ──
        if not halted and trails:
            total_loss = sum(max(0, (t['fill_price'] - close_px) * t['qty']) for t in trails)
            if capital > 0 and total_loss / capital >= circuit_breaker_pct:
                for t in trails:
                    cb_px = close_px * (1 - SLIPPAGE_PCT)  # market sell in live
                    fee = _exit_fee(cb_px, t['qty'])
                    pnl = (cb_px - t['fill_price']) * t['qty'] - fee
                    cash += cb_px * t['qty'] - fee
                    trades.append({
                        'entry_time': int(df.iloc[t['fill_bar']]['start']),
                        'exit_time': bar_time, 'side': 'LONG',
                        'entry_price': t['fill_price'], 'exit_price': cb_px,
                        'size': t['qty'], 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                        'exit_reason': 'CIRCUIT_BREAKER', 'signal_type': 'GRID'
                    })
                total_asset = 0.0
                trails = []
                pending_sells = {}
                buy_levels = []
                halted = True

        if halted:
            equity_curve.append({'time': bar_time, 'equity': round(cash, 2)})
            continue

        # ── ADX halt logic (direction-aware) ──
        is_halted = adx >= 25
        if is_halted:
            if direction == 'RISING':
                halt_mode = 'FAVORABLE'
            elif direction == 'FALLING':
                halt_mode = 'ADVERSE'
            else:
                halt_mode = 'NEUTRAL'
            # Cancel all open buys on halt
            if buy_levels:
                cancelled_buy_levels.extend(buy_levels)
                buy_levels = []
        else:
            halt_mode = None

        # ── Adjust trail multipliers based on halt/depth/velocity ──
        adjust_multipliers(halt_mode, depth, velocity)

        # ── 1m sub-bar walk for realistic intrabar price action ──
        if subbars is not None:
            sb_start, sb_end = _subbar_indices(subbars, bar_time, bar_time + (tf_min * 60))
            sb_iter = range(sb_start, sb_end) if sb_end > sb_start else range(0)
            sb_highs = subbars['highs']
            sb_lows = subbars['lows']
            sb_closes = subbars['closes']
            sb_starts = subbars['starts']
            sb_first_idx = sb_start
        else:
            # Fallback: single virtual sub-bar = the full test TF bar
            sb_iter = [0]
            sb_highs = [bar_high]
            sb_lows = [bar_low]
            sb_closes = [close_px]
            sb_starts = [bar_time]
            sb_first_idx = 0

        for sb_idx in sb_iter:
            sub_high = float(sb_highs[sb_idx])
            sub_low = float(sb_lows[sb_idx])
            sub_close = float(sb_closes[sb_idx])
            sub_time = int(sb_starts[sb_idx])

            # ── Per-fill trailing stops (intrabar) ──
            # Test against the trigger from the PREVIOUS hwm first, then ratchet:
            # lifting the stop with the same bar's high assumed high-before-low.
            surviving_trails = []
            for t in trails:
                trigger = t['hwm'] - t['effective_trail']
                if sub_low <= trigger:
                    # Gap-through + slippage: market sell in live, not a fill at trigger
                    sub_open_g = float(sb_opens[sb_idx]) if 'sb_opens' in dir() else trigger
                    exit_px = min(trigger, sub_open_g) * (1 - SLIPPAGE_PCT)
                    fee = _exit_fee(exit_px, t['qty'])
                    pnl = (exit_px - t['fill_price']) * t['qty'] - fee
                    cash += exit_px * t['qty'] - fee
                    total_asset -= t['qty']
                    trades.append({
                        'entry_time': int(df.iloc[t['fill_bar']]['start']),
                        'exit_time': sub_time, 'side': 'LONG',
                        'entry_price': t['fill_price'], 'exit_price': exit_px,
                        'size': t['qty'], 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                        'exit_reason': 'TRAILING_STOP', 'signal_type': 'GRID'
                    })
                    pending_sells.pop(t['fill_price'], None)
                else:
                    if sub_high > t['hwm']:
                        t['hwm'] = sub_high
                    surviving_trails.append(t)
            trails = surviving_trails
            depth = len(trails)

            # ── Depth escalation: cancel lowest buys (only re-check on first sub-bar) ──
            if sb_idx == sb_first_idx:
                if depth >= 6 and buy_levels:
                    cancelled_buy_levels.extend(buy_levels)
                    buy_levels = []
                elif depth >= 4 and direction == 'FALLING' and len(buy_levels) >= 2:
                    buy_levels.sort()
                    to_cancel = buy_levels[:2]
                    cancelled_buy_levels.extend(to_cancel)
                    buy_levels = buy_levels[2:]

                # Runner management
                if velocity >= 2.0:
                    for t in trails:
                        if t.get('has_sell', False):
                            profit_steps = (sub_close - t['fill_price']) / step_usd if step_usd > 0 else 0
                            if profit_steps >= 2.0:
                                pending_sells.pop(t['fill_price'], None)
                                t['has_sell'] = False

            # ── Sell fills (grid flips): price rose to a pending sell level ──
            for fill_px, sell_px in list(pending_sells.items()):
                if sub_high >= sell_px:
                    match = None
                    for t in trails:
                        if t['fill_price'] == fill_px:
                            match = t
                            break
                    if match:
                        fee = _maker_fee(sell_px, match['qty'])
                        pnl = (sell_px - match['fill_price']) * match['qty'] - fee
                        cash += sell_px * match['qty'] - fee
                        total_asset -= match['qty']
                        trades.append({
                            'entry_time': int(df.iloc[match['fill_bar']]['start']),
                            'exit_time': sub_time, 'side': 'LONG',
                            'entry_price': match['fill_price'], 'exit_price': sell_px,
                            'size': match['qty'], 'pnl': round(pnl, 4), 'fee': round(fee, 4),
                            'exit_reason': 'GRID_FLIP', 'signal_type': 'GRID'
                        })
                        trails.remove(match)
                        pending_sells.pop(fill_px, None)
                        recovery_timestamps.append(sub_time)
                        if len(recovery_timestamps) > 20:
                            recovery_timestamps[:] = recovery_timestamps[-20:]
                        if not is_halted:
                            new_buy = sell_px - step_usd
                            if new_buy > 0 and not has_nearby(new_buy, buy_levels, min_gap):
                                buy_levels.append(new_buy)
                        else:
                            cancelled_buy_levels.append(sell_px - step_usd)

            # ── Buy fills: price dipped to a buy level (intrabar) ──
            if not is_halted and not suspend_buys:
                filled = []
                for lvl in sorted(buy_levels):
                    if sub_low <= lvl and cash >= chunk_usd:
                        qty = (chunk_usd * 0.99) / lvl
                        fee = _maker_fee(lvl, qty)
                        cash -= (lvl * qty + fee)
                        total_asset += qty
                        level_idx = max(0, int((lvl - lower_price) / step_usd)) if step_usd > 0 else 0
                        total_levels = max(1, total_buy_levels)
                        base_trail = dyn_trail_dist if is_dynamic else get_trail_dist(level_idx, total_levels, step_usd)
                        trails.append({
                            'fill_price': lvl, 'qty': qty, 'hwm': sub_close,
                            'base_trail': base_trail, 'trail_mult': 1.0,
                            'effective_trail': base_trail,
                            'level_idx': level_idx, 'fill_bar': bar_idx, 'has_sell': True
                        })
                        if is_dynamic:
                            pending_sells[lvl] = compute_dynamic_sell_price(lvl, step_usd, regime, velocity)
                        else:
                            pending_sells[lvl] = lvl + step_usd
                        filled.append(lvl)
                for lvl in filled:
                    buy_levels.remove(lvl)
        # ── End of sub-bar loop ──

        # ── Buy redeployment (matches evaluate_buy_redeployment) ──
        depth = len(trails)
        if not suspend_buys and cancelled_buy_levels and depth <= 3 and not (direction == 'FALLING' and depth > 3):
            max_redeploy = min(len(cancelled_buy_levels), 3)
            redeployed = 0
            for i in range(max_redeploy):
                new_px = close_px - (step_usd * (i + 1))
                if new_px > 0 and not has_nearby(new_px, buy_levels, min_gap):
                    buy_levels.append(new_px)
                    redeployed += 1
            if redeployed:
                cancelled_buy_levels = cancelled_buy_levels[redeployed:]

        # ── Grid follow/sliding ──
        if follow_enabled and not is_halted and not suspend_buys and depth <= 3:
            grid_high = max(buy_levels) if buy_levels else upper_price
            grid_low = min(buy_levels) if buy_levels else lower_price

            if close_px > grid_high + step_usd and buy_levels:
                # Price above grid — recycle lowest buy to above grid
                buy_levels.sort()
                old_lvl = buy_levels[0]
                steps_above = max(1, int((close_px - grid_high) / step_usd))
                new_buy = grid_high + steps_above * step_usd
                if new_buy >= close_px:
                    new_buy -= step_usd
                if new_buy > 0 and new_buy < close_px and not has_nearby(new_buy, buy_levels, min_gap):
                    buy_levels.remove(old_lvl)
                    buy_levels.append(new_buy)
                    upper_price = max(upper_price, new_buy + step_usd)

            elif mode == 'LONG' and close_px < grid_low - step_usd and buy_levels:
                # Price below grid — recycle highest buy to below grid
                buy_levels.sort()
                old_lvl = buy_levels[-1]
                steps_below = max(1, int((grid_low - close_px) / step_usd))
                new_buy = grid_low - steps_below * step_usd
                if new_buy >= close_px:
                    new_buy -= step_usd
                if new_buy > 0 and not has_nearby(new_buy, buy_levels, min_gap):
                    buy_levels.remove(old_lvl)
                    buy_levels.append(new_buy)
                    lower_price = min(lower_price, new_buy)

        # ── Equity curve ──
        portfolio = cash + total_asset * close_px
        equity_curve.append({'time': bar_time, 'equity': round(portfolio, 2)})

    # ── Close remaining inventory at end ──
    final_px = float(df.iloc[-1]['close'])
    final_time = int(df.iloc[-1]['start'])
    for t in trails:
        fee = _exit_fee(final_px, t['qty'])  # end-of-data close = market
        pnl = (final_px - t['fill_price']) * t['qty'] - fee
        cash += final_px * t['qty'] - fee
        trades.append({
            'entry_time': int(df.iloc[t['fill_bar']]['start']),
            'exit_time': final_time, 'side': 'LONG',
            'entry_price': t['fill_price'], 'exit_price': final_px,
            'size': t['qty'], 'pnl': round(pnl, 4), 'fee': round(fee, 4),
            'exit_reason': 'END_OF_DATA', 'signal_type': 'GRID'
        })

    summary = _compute_stats(trades, equity_curve, capital, timeframe)
    grid_flips = [t for t in trades if t['exit_reason'] == 'GRID_FLIP']
    trail_exits = [t for t in trades if t['exit_reason'] == 'TRAILING_STOP']
    summary['grid_flips'] = len(grid_flips)
    summary['trail_exits'] = len(trail_exits)
    summary['circuit_breaker_hit'] = halted
    summary['grid_flip_pnl'] = round(sum(t['pnl'] for t in grid_flips), 2)
    summary['cycle_trades'] = len(grid_flips)
    summary['risk_events'] = len(trail_exits) + (1 if halted else 0)

    return {
        'equity_curve': equity_curve,
        'trades': trades,
        'summary': summary,
        'config': {'pair': pair, 'strategy': 'GRID', 'timeframe': timeframe,
                   'capital': capital, 'params': params, 'total_bars': n, 'warmup': WARMUP_BARS}
    }


# ══════════════════════════════════════════════════════════
# Core backtest loop
# ══════════════════════════════════════════════════════════

def run_backtest(pair, strategy, timeframe, df, capital, params=None, progress_cb=None):
    """
    Walk-forward backtest.

    Args:
        pair: e.g. "BTC-USD"
        strategy: e.g. "QUAD", "MOMENTUM"
        timeframe: e.g. "15m" (for stats annualization)
        df: DataFrame with columns: start, open, high, low, close, volume
        capital: starting capital in USD
        params: strategy-specific params dict
        progress_cb: callable(phase, pct) for progress updates

    Returns dict with keys: equity_curve, trades, summary, config
    """
    if params is None:
        params = {}

    # DCA and GRID use their own full simulation engines
    if strategy == 'DCA':
        return _run_dca_backtest(pair, timeframe, df, capital, params, progress_cb)
    if strategy == 'GRID':
        return _run_grid_backtest(pair, timeframe, df, capital, params, progress_cb)

    n = len(df)
    if n < WARMUP_BARS + 10:
        return {'error': f'Need at least {WARMUP_BARS + 10} candles, got {n}'}

    # Determine test TF in minutes
    if isinstance(timeframe, str):
        tf_str = timeframe.strip().lower()
        if tf_str.endswith('m'):
            tf_min = int(tf_str.rstrip('m'))
        elif tf_str.endswith('h'):
            tf_min = int(tf_str.rstrip('h')) * 60
        elif tf_str.endswith('d'):
            tf_min = int(tf_str.rstrip('d')) * 1440
        else:
            tf_min = 5
    else:
        tf_min = int(timeframe)

    # Load 1m sub-bars for realistic intrabar SL/TP detection
    subbars = None
    if tf_min > 1:
        first_ts = int(df.iloc[0]['start'])
        last_ts = int(df.iloc[-1]['start']) + (tf_min * 60)
        subbars = _load_1m_subbars(pair, first_ts, last_ts)
        if subbars is None:
            log.warning(f"[{pair}] No 1m sub-bar data — falling back to bar-based SL/TP")

    # State
    cash = float(capital)
    position = 'FLAT'
    entry_price = 0.0
    asset_held = 0.0
    stop_price = 0.0
    target_price = 0.0
    high_water_mark = 0.0
    entry_atr = 0.0
    entry_signal = ''
    entry_bar = 0

    # TRAP-specific state
    trap_state = {'position': 'FLAT', 'entry_stage': 0, 'avg_entry': 0.0, 'breakout_data': None, 'tp_stage': 0}
    # ORB-specific state
    orb_state = {'position': 'FLAT', 'entry_price': 0.0, 'orb_data': None, 'tp_stage': 0}
    # DCA-specific state
    dca_state = {'dca_state': 'SCANNING', 'last_cross_direction': 'ABOVE'}
    # MOMENTUM 3-phase trailing state
    momentum_fee_est = 0.0
    momentum_phase = 0
    # NPR state machine: event_stop / trailing after partial fill at +0.25 ATR
    npr_partial_filled = False
    npr_event_stop = 0.0
    npr_trail_dist = 0.0
    npr_event_power = 0.0
    npr_zone = 0
    npr_daily_loss = 0.0
    npr_last_loss_day = 0
    npr_max_loss_day = (params.get('max_loss_per_trade', capital * 0.01) * 3) if params else (capital * 0.03)
    npr_halted = False
    # Generic state ref for adapter
    state = {}

    equity_curve = []
    trades = []
    total_bars = n - WARMUP_BARS

    for i in range(WARMUP_BARS, n):
        bar = df.iloc[i]
        prev = df.iloc[i - 1]
        close_px = float(bar['close'])
        high_px = float(bar['high'])
        low_px = float(bar['low'])
        open_px = float(bar['open'])
        bar_time = int(bar['start'])

        # Progress
        if progress_cb and (i - WARMUP_BARS) % max(1, total_bars // 50) == 0:
            pct = int((i - WARMUP_BARS) / total_bars * 100)
            progress_cb('running', pct)

        # ── Check SL/TP intra-bar when in position (1m sub-bar walk for correct ordering) ──
        exited_this_bar = False

        if position == 'LONG' and asset_held > 0:
            # Build sub-bar list (1m if available, else fall back to test TF bar)
            if subbars is not None:
                sb_start, sb_end = _subbar_indices(subbars, bar_time, bar_time + (tf_min * 60))
                sb_iter = range(sb_start, sb_end)
                sb_highs = subbars['highs']
                sb_lows = subbars['lows']
                sb_opens = subbars['opens']
                sb_starts = subbars['starts']
            else:
                sb_iter = [0]
                sb_highs = [high_px]
                sb_lows = [low_px]
                sb_opens = [open_px]
                sb_starts = [bar_time]

            for sb_idx in sb_iter:
                sub_high = float(sb_highs[sb_idx])
                sub_low = float(sb_lows[sb_idx])
                sub_open = float(sb_opens[sb_idx])
                sub_time = int(sb_starts[sb_idx])

                # NOTE: the high-water mark is ratcheted at the END of the sub-bar
                # (after the stop test) — using the same bar's high to lift the stop
                # its own low then hits assumed high-before-low and inflated exits.

                # ── Per-strategy trailing stop recomputation (matches live executors) ──
                if strategy == 'MOMENTUM' and entry_atr > 0 and asset_held > 0:
                    # 3-phase stop: phase from the sub-bar OPEN (known before the bar
                    # trades; the high assumed the peak arrived before the stop test)
                    pnl_now = (sub_open - entry_price) * asset_held
                    if pnl_now >= momentum_fee_est * 2:
                        # Phase 3: 0.75x ATR trail, floor at entry + fee_per_unit
                        momentum_phase = 3
                        trail_stop = high_water_mark - (0.75 * entry_atr)
                        fee_per_unit = momentum_fee_est / asset_held if asset_held > 0 else 0
                        floor_stop = entry_price + fee_per_unit
                        stop_price = max(trail_stop, floor_stop)
                    elif pnl_now >= momentum_fee_est:
                        # Phase 2: 1.0x ATR trail, floor at entry - 0.5x ATR
                        momentum_phase = 2
                        trail_stop = high_water_mark - (1.0 * entry_atr)
                        floor_stop = entry_price - (0.5 * entry_atr)
                        stop_price = max(trail_stop, floor_stop)
                    else:
                        # Phase 1: 1.5x ATR trail
                        momentum_phase = 1
                        stop_price = high_water_mark - (1.5 * entry_atr)
                elif strategy == 'VWAP_MR' and entry_atr > 0:
                    # Continuous 1.5x ATR trail from HWM
                    stop_price = high_water_mark - (1.5 * entry_atr)
                elif strategy == 'SQUEEZE' and entry_atr > 0:
                    # Continuous 2.0x ATR trail from HWM
                    stop_price = high_water_mark - (2.0 * entry_atr)
                elif strategy == 'ORB' and orb_state.get('tp_stage', 0) >= 1 and entry_atr > 0:
                    # ATR trail after T1 hit: 1.5x ATR from HWM, floored at entry (breakeven)
                    trail_stop = high_water_mark - (1.5 * entry_atr)
                    stop_price = max(trail_stop, entry_price)
                elif strategy == 'TRAP' and trap_state.get('tp_stage', 0) >= 1:
                    # After T1 hit: stop moves to breakeven
                    stop_price = max(stop_price, entry_price)
                elif strategy == 'NPR':
                    # Activate breakeven + trail at +0.25x ATR profit
                    if not npr_partial_filled and entry_atr > 0 and sub_high >= (entry_price + 0.25 * entry_atr):
                        npr_partial_filled = True
                        # Trail distance based on event power and zone
                        if npr_event_power >= 2.0:
                            npr_trail_dist = 1.5 * entry_atr
                        elif npr_zone == 3:
                            npr_trail_dist = 0.75 * entry_atr
                        else:
                            npr_trail_dist = 1.0 * entry_atr
                        # Move stop to breakeven
                        stop_price = max(npr_event_stop, entry_price)
                    if npr_partial_filled and npr_trail_dist > 0:
                        # Trailing stop replaces event stop
                        trail_stop = high_water_mark - npr_trail_dist
                        stop_price = max(stop_price, trail_stop)

                # SL check first (intrabar). Gap-through: if the sub-bar opened
                # below the stop, fill at the open; always charge slippage on the
                # market-style exit (booking exactly the stop price was optimistic
                # on every losing exit).
                if stop_price > 0 and sub_low <= stop_price:
                    exit_px = min(stop_price, sub_open) * (1 - SLIPPAGE_PCT)
                    fee = _exit_fee(exit_px, asset_held)
                    pnl = (exit_px - entry_price) * asset_held - _entry_fee(entry_price, asset_held) - fee
                    cash += exit_px * asset_held - fee
                    # Strategy-specific exit reason for trailing strategies
                    if strategy == 'MOMENTUM':
                        sl_reason = 'STOP_LOSS' if momentum_phase == 1 else 'TRAILING_STOP'
                    elif strategy in ('VWAP_MR', 'SQUEEZE'):
                        sl_reason = 'TRAILING_STOP'
                    elif strategy == 'NPR':
                        sl_reason = 'TRAILING_STOP' if npr_partial_filled else 'EVENT_STOP'
                    elif strategy == 'ORB' and orb_state.get('tp_stage', 0) >= 1:
                        sl_reason = 'TRAILING_STOP'
                    else:
                        sl_reason = 'STOP_LOSS'
                    trades.append({
                        'entry_time': int(df.iloc[entry_bar]['start']),
                        'exit_time': sub_time,
                        'side': 'LONG',
                        'entry_price': entry_price,
                        'exit_price': exit_px,
                        'size': asset_held,
                        'pnl': round(pnl, 4),
                        'fee': round(_entry_fee(entry_price, asset_held) + fee, 4),
                        'exit_reason': sl_reason,
                        'signal_type': entry_signal
                    })
                    # NPR daily loss tracking
                    if strategy == 'NPR' and pnl < 0:
                        npr_daily_loss += abs(pnl)
                        if npr_daily_loss >= npr_max_loss_day:
                            npr_halted = True
                    # Reset per-strategy state
                    if strategy == 'TRAP':
                        trap_state.update({'position': 'FLAT', 'entry_stage': 0, 'avg_entry': 0.0, 'breakout_data': None, 'tp_stage': 0})
                    elif strategy == 'ORB':
                        orb_state.update({'position': 'FLAT', 'entry_price': 0.0, 'orb_data': None, 'tp_stage': 0})
                    elif strategy == 'NPR':
                        npr_partial_filled = False
                        npr_event_stop = 0.0
                        npr_trail_dist = 0.0
                    position = 'FLAT'
                    asset_held = 0.0
                    exited_this_bar = True
                    break

                # TP check
                if target_price > 0 and sub_high >= target_price:
                    exit_px = target_price
                    fee = _exit_fee(exit_px, asset_held)
                    pnl = (exit_px - entry_price) * asset_held - _entry_fee(entry_price, asset_held) - fee
                    cash += exit_px * asset_held - fee
                    trades.append({
                        'entry_time': int(df.iloc[entry_bar]['start']),
                        'exit_time': sub_time,
                        'side': 'LONG',
                        'entry_price': entry_price,
                        'exit_price': exit_px,
                        'size': asset_held,
                        'pnl': round(pnl, 4),
                        'fee': round(_entry_fee(entry_price, asset_held) + fee, 4),
                        'exit_reason': 'TAKE_PROFIT',
                        'signal_type': entry_signal
                    })
                    position = 'FLAT'
                    asset_held = 0.0
                    exited_this_bar = True
                    break

                # Ratchet the high-water mark AFTER this sub-bar's stop/TP tests —
                # the next sub-bar's trail uses it (no same-bar high-lifts-stop bias)
                if sub_high > high_water_mark:
                    high_water_mark = sub_high

        # ── Evaluate strategy signal ──
        if not exited_this_bar:
            window = df.iloc[max(0, i - 300):i + 1].copy()

            # Build state for adapter
            if strategy == 'TRAP':
                trap_state['position'] = position
                state = trap_state
            elif strategy == 'ORB':
                orb_state['position'] = position
                orb_state['entry_price'] = entry_price
                state = orb_state
            elif strategy == 'DCA':
                state = dca_state
            else:
                state = {'position': position}

            try:
                action, reason, meta = _adapt_signal(strategy, window, state, params)
            except Exception as e:
                action, reason, meta = 'HOLD', str(e), {}

            # ── DCA state transitions (ARM/DISARM) ──
            if strategy == 'DCA':
                if action == 'ARM':
                    dca_state['dca_state'] = 'ARMED'
                    dca_state['last_cross_direction'] = meta.get('last_cross_direction', dca_state.get('last_cross_direction', 'ABOVE'))
                elif action == 'DISARM':
                    dca_state['dca_state'] = 'SCANNING'

            # ── TRAP state updates ──
            if strategy == 'TRAP' and isinstance(meta, dict):
                if meta.get('breakout_data'):
                    trap_state['breakout_data'] = meta['breakout_data']

            # ── ORB state updates ──
            if strategy == 'ORB' and isinstance(meta, dict):
                if meta.get('range_high'):
                    orb_state['orb_data'] = meta

            # ── PARTIAL EXIT (T1: sell 50%, move stop to breakeven) — TRAP & ORB ──
            if action == 'PARTIAL_EXIT_LONG' and position == 'LONG' and asset_held > 0:
                sell_qty = asset_held * 0.5
                fee = _exit_fee(close_px, sell_qty)
                pnl = (close_px - entry_price) * sell_qty - fee
                cash += close_px * sell_qty - fee
                asset_held -= sell_qty
                if strategy == 'TRAP':
                    trap_state['tp_stage'] = 1
                elif strategy == 'ORB':
                    orb_state['tp_stage'] = 1
                # Move stop to breakeven
                stop_price = entry_price
                trades.append({
                    'entry_time': int(df.iloc[entry_bar]['start']),
                    'exit_time': bar_time,
                    'side': 'LONG',
                    'entry_price': entry_price,
                    'exit_price': close_px,
                    'size': sell_qty,
                    'pnl': round(pnl, 4),
                    'fee': round(fee, 4),
                    'exit_reason': 'TARGET_1',
                    'signal_type': entry_signal
                })

            # ── SELL signal while in position ──
            elif action in ('SELL', 'EXIT_LONG') and position == 'LONG' and asset_held > 0:
                exit_px = close_px
                fee = _exit_fee(exit_px, asset_held)
                pnl = (exit_px - entry_price) * asset_held - _entry_fee(entry_price, asset_held) - fee
                cash += exit_px * asset_held - fee
                exit_type = meta.get('exit_type', 'SIGNAL') if isinstance(meta, dict) else 'SIGNAL'
                trades.append({
                    'entry_time': int(df.iloc[entry_bar]['start']),
                    'exit_time': bar_time,
                    'side': 'LONG',
                    'entry_price': entry_price,
                    'exit_price': exit_px,
                    'size': asset_held,
                    'pnl': round(pnl, 4),
                    'fee': round(_entry_fee(entry_price, asset_held) + fee, 4),
                    'exit_reason': exit_type,
                    'signal_type': entry_signal
                })
                # NPR daily loss tracking
                if strategy == 'NPR' and pnl < 0:
                    npr_daily_loss += abs(pnl)
                    if npr_daily_loss >= npr_max_loss_day:
                        npr_halted = True
                position = 'FLAT'
                asset_held = 0.0
                if strategy == 'TRAP':
                    trap_state.update({'position': 'FLAT', 'entry_stage': 0, 'avg_entry': 0.0, 'breakout_data': None, 'tp_stage': 0})
                elif strategy == 'ORB':
                    orb_state.update({'position': 'FLAT', 'entry_price': 0.0, 'orb_data': None, 'tp_stage': 0})
                elif strategy == 'NPR':
                    npr_partial_filled = False
                    npr_event_stop = 0.0
                    npr_trail_dist = 0.0
                if strategy == 'DCA':
                    dca_state['dca_state'] = 'SCANNING'

            # ── BUY / entry signal while flat ──
            elif action in ('BUY', 'BREAKOUT_LONG', 'LONG') and position == 'FLAT' and cash > 5.0:
                # NPR daily halt + UTC midnight reset
                if strategy == 'NPR':
                    bar_day = bar_time // 86400
                    if bar_day != npr_last_loss_day:
                        npr_daily_loss = 0.0
                        npr_halted = False
                        npr_last_loss_day = bar_day
                    if npr_halted:
                        meta = {}
                        # skip entry while halted
                        portfolio_value = cash + (asset_held * close_px if asset_held > 0 else 0)
                        equity_curve.append({'time': bar_time, 'equity': round(portfolio_value, 2)})
                        continue
                meta = meta if isinstance(meta, dict) else {}

                # Confidence-based sizing (QUAD) or fixed allocation
                if strategy == 'QUAD':
                    confidence = meta.get('confidence', 0.5)
                    atr = meta.get('atr', close_px * 0.01)
                    sig_type = meta.get('signal_type', 'UNKNOWN')
                    allocation = cash * confidence * 0.99
                    sl_mult = _QUAD_SL_MULT.get(sig_type, 2.0)
                    tp_mult = _QUAD_TP_MULT.get(sig_type, 3.0)
                    stop_price = close_px - (sl_mult * atr)
                    target_price = close_px + (tp_mult * atr)
                    entry_signal = sig_type
                    entry_atr = atr
                elif strategy == 'TRAP':
                    # Stage 1: 10% capital (Velez)
                    allocation = cash * 0.10
                    atr = meta.get('atr', close_px * 0.01)
                    elephant_low = meta.get('low', close_px - 2.0 * atr)
                    stop_price = min(close_px - (2.0 * atr), elephant_low) if atr > 0 else 0
                    R = close_px - stop_price if stop_price > 0 else atr
                    target_price = close_px + (2.5 * R)  # T1 at 2.5R
                    entry_signal = 'BREAKOUT'
                    entry_atr = atr
                    trap_state.update({'position': 'LONG', 'entry_stage': 1, 'avg_entry': close_px, 'breakout_data': meta, 'tp_stage': 0})
                elif strategy == 'ORB':
                    # Risk-based sizing: 2% of capital / stop_distance
                    atr = meta.get('atr', close_px * 0.01)
                    stop_dist = meta.get('stop_distance', 1.5 * atr)
                    risk_usd = cash * 0.02
                    allocation = (risk_usd / stop_dist) * close_px if stop_dist > 0 else cash * 0.10
                    allocation = min(allocation, cash * 0.50)
                    stop_price = close_px - stop_dist
                    R = stop_dist if stop_dist > 0 else atr
                    target_price = close_px + (1.5 * R)  # T1 at 1.5R
                    entry_signal = 'ORB'
                    entry_atr = atr
                    orb_state.update({'position': 'LONG', 'entry_price': close_px, 'orb_data': meta, 'tp_stage': 0})
                elif strategy == 'DCA':
                    allocation = cash * 0.20  # DCA buys in tranches
                    atr = meta.get('atr', close_px * 0.01)
                    stop_price = close_px - (3.0 * atr) if atr > 0 else 0
                    target_price = close_px * 1.05  # 5% initial target
                    entry_signal = 'DCA_BUY'
                    entry_atr = atr
                    dca_state['dca_state'] = 'ACCUMULATING'
                elif strategy == 'MOMENTUM':
                    # Live: 99% capital via maker limit. 3-phase trailing stop, no TP.
                    allocation = cash * 0.99
                    atr = meta.get('atr', close_px * 0.01)
                    # Phase 1 stop: hwm - 1.5x ATR (set initial to entry - 1.5x ATR)
                    stop_price = close_px - (1.5 * atr) if atr > 0 else 0
                    target_price = 0  # no hard TP — trailing only
                    entry_signal = 'MOMENTUM'
                    entry_atr = atr
                elif strategy == 'VWAP_MR':
                    # Live: 95% capital market buy. Continuous 1.5x ATR trailing, exit on VWAP_TOUCH signal.
                    allocation = cash * 0.95
                    atr = meta.get('atr', close_px * 0.015)
                    stop_price = close_px - (1.5 * atr) if atr > 0 else 0
                    target_price = 0
                    entry_signal = 'VWAP_MR'
                    entry_atr = atr
                elif strategy == 'SQUEEZE':
                    # Live: 95% capital market buy. Continuous 2.0x ATR trailing, exit on EXIT_LONG signal.
                    allocation = cash * 0.95
                    atr = meta.get('atr', close_px * 0.015)
                    stop_price = close_px - (2.0 * atr) if atr > 0 else 0
                    target_price = 0
                    entry_signal = 'SQUEEZE'
                    entry_atr = atr
                elif strategy == 'NPR':
                    # Live: risk-based sizing, event_stop + trailing after partial fill at +0.25 ATR
                    atr = meta.get('atr', close_px * 0.015)
                    # NPR uses risk-based sizing: max_loss_per_trade / (stop_distance * mult)
                    max_loss_per_trade = params.get('max_loss_per_trade', cash * 0.01) if params else cash * 0.01
                    event_stop = meta.get('event_stop', close_px - 1.5 * atr)
                    stop_dist = abs(close_px - event_stop) if event_stop > 0 else 1.5 * atr
                    if stop_dist > 0:
                        position_size = max_loss_per_trade / stop_dist
                        allocation = min(position_size * close_px, cash * 0.95)
                    else:
                        allocation = cash * 0.05
                    stop_price = event_stop  # initially the event stop
                    target_price = 0  # no hard TP — event_stop or trailing
                    entry_signal = 'NPR'
                    entry_atr = atr
                else:
                    allocation = cash * 0.95
                    atr = meta.get('atr', close_px * 0.01)
                    stop_price = close_px - (2.0 * atr) if atr > 0 else 0
                    target_price = close_px + (3.0 * atr) if atr > 0 else 0
                    entry_signal = strategy
                    entry_atr = atr

                if allocation < 5.0:
                    pass  # skip tiny allocations
                else:
                    qty = allocation / close_px
                    fee = _entry_fee(close_px, qty)
                    cash -= (close_px * qty + fee)
                    asset_held = qty
                    entry_price = close_px
                    high_water_mark = close_px
                    entry_bar = i
                    position = 'LONG'

                    # Per-strategy state initialization
                    if strategy == 'MOMENTUM':
                        # fee_estimate used in 3-phase stop calculation
                        momentum_fee_est = _entry_fee(close_px, qty) + _exit_fee(close_px, qty)
                        momentum_phase = 1
                    elif strategy == 'NPR':
                        npr_partial_filled = False
                        npr_event_stop = stop_price  # event_stop set at entry
                        npr_trail_dist = 0.0
                        npr_event_power = meta.get('event_power', 1.0) if isinstance(meta, dict) else 1.0
                        npr_zone = meta.get('zone', 1) if isinstance(meta, dict) else 1

            # ── TRAP ADD_LONG: +10% on pullback (Velez 3-stage pyramiding) ──
            elif action == 'ADD_LONG' and strategy == 'TRAP' and position == 'LONG' and cash > 5.0:
                initial_capital = cash + (asset_held * close_px)
                add_alloc = min(initial_capital * 0.10, cash * 0.99)
                add_qty = add_alloc / close_px
                add_fee = _entry_fee(close_px, add_qty)
                old_cost = entry_price * asset_held
                new_cost = close_px * add_qty
                asset_held += add_qty
                entry_price = (old_cost + new_cost) / asset_held if asset_held > 0 else close_px
                cash -= (close_px * add_qty + add_fee)
                new_stage = trap_state.get('entry_stage', 1) + 1
                trap_state.update({'entry_stage': new_stage, 'avg_entry': entry_price})
                atr = meta.get('atr', close_px * 0.01)
                bo_data = trap_state.get('breakout_data', {})
                elephant_low = bo_data.get('low', entry_price - 2.0 * atr) if bo_data else entry_price - 2.0 * atr
                stop_price = min(entry_price - (2.0 * atr), elephant_low) if atr > 0 else 0
                R = entry_price - stop_price if stop_price > 0 else atr
                target_price = entry_price + (2.5 * R)

        # ── Record equity ──
        portfolio_value = cash + (asset_held * close_px if asset_held > 0 else 0)
        equity_curve.append({'time': bar_time, 'equity': round(portfolio_value, 2)})

    # Close any open position at the end
    if position == 'LONG' and asset_held > 0:
        final_px = float(df.iloc[-1]['close'])
        fee = _exit_fee(final_px, asset_held)
        pnl = (final_px - entry_price) * asset_held - _entry_fee(entry_price, asset_held) - fee
        cash += final_px * asset_held - fee
        trades.append({
            'entry_time': int(df.iloc[entry_bar]['start']),
            'exit_time': int(df.iloc[-1]['start']),
            'side': 'LONG',
            'entry_price': entry_price,
            'exit_price': final_px,
            'size': asset_held,
            'pnl': round(pnl, 4),
            'fee': round(_entry_fee(entry_price, asset_held) + fee, 4),
            'exit_reason': 'END_OF_DATA',
            'signal_type': entry_signal
        })

    summary = _compute_stats(trades, equity_curve, capital, timeframe)

    return {
        'equity_curve': equity_curve,
        'trades': trades,
        'summary': summary,
        'config': {
            'pair': pair, 'strategy': strategy, 'timeframe': timeframe,
            'capital': capital, 'params': params, 'total_bars': n,
            'warmup': WARMUP_BARS
        }
    }


# ══════════════════════════════════════════════════════════
# Statistics computation
# ══════════════════════════════════════════════════════════

TF_BARS_PER_YEAR = {
    '1m': 525600, '5m': 105120, '15m': 35040, '30m': 17520,
    '1h': 8760, '6h': 1460, '1d': 365
}

def _compute_stats(trades, equity_curve, capital, timeframe):
    stats = {}
    n_trades = len(trades)
    stats['total_trades'] = n_trades

    if n_trades == 0:
        stats.update({
            'total_return_usd': 0, 'total_return_pct': 0, 'win_rate': 0,
            'profit_factor': 0, 'max_drawdown_pct': 0, 'max_drawdown_usd': 0,
            'sharpe_ratio': 0, 'sortino_ratio': 0, 'avg_win': 0, 'avg_loss': 0,
            'largest_win': 0, 'largest_loss': 0, 'total_fees': 0,
            'avg_trade_bars': 0, 'exposure_pct': 0, 'buy_hold_return_pct': 0,
        })
        return stats

    pnls = [t['pnl'] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    final_equity = equity_curve[-1]['equity'] if equity_curve else capital
    stats['total_return_usd'] = round(final_equity - capital, 2)
    stats['total_return_pct'] = round((final_equity / capital - 1) * 100, 2)
    stats['win_rate'] = round(len(wins) / n_trades * 100, 1) if n_trades > 0 else 0
    stats['avg_win'] = round(sum(wins) / len(wins), 2) if wins else 0
    stats['avg_loss'] = round(sum(losses) / len(losses), 2) if losses else 0
    stats['largest_win'] = round(max(pnls), 2) if pnls else 0
    stats['largest_loss'] = round(min(pnls), 2) if pnls else 0
    stats['total_fees'] = round(sum(t['fee'] for t in trades), 2)

    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    stats['profit_factor'] = round(gross_wins / gross_losses, 2) if gross_losses > 0 else float('inf') if gross_wins > 0 else 0

    # Max drawdown
    peak = capital
    max_dd = 0
    max_dd_usd = 0
    for pt in equity_curve:
        eq = pt['equity']
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak if peak > 0 else 0
        dd_usd = peak - eq
        if dd > max_dd:
            max_dd = dd
        if dd_usd > max_dd_usd:
            max_dd_usd = dd_usd
    stats['max_drawdown_pct'] = round(max_dd * 100, 2)
    stats['max_drawdown_usd'] = round(max_dd_usd, 2)

    # Sharpe & Sortino
    if len(equity_curve) > 1:
        equities = [e['equity'] for e in equity_curve]
        returns = [(equities[i] - equities[i-1]) / equities[i-1] if equities[i-1] > 0 else 0
                    for i in range(1, len(equities))]
        mean_ret = np.mean(returns) if returns else 0
        std_ret = np.std(returns) if returns else 1
        bars_per_year = TF_BARS_PER_YEAR.get(timeframe)
        if bars_per_year is None:
            # API timeframes arrive as minute strings ('60m', '1440m', ...) that
            # never matched '1h'/'1d' keys — hourly Sharpe was inflated 2x, daily 6x
            m = re.fullmatch(r'(\d+)\s*[mM]', str(timeframe).strip())
            if m and int(m.group(1)) > 0:
                bars_per_year = 525600 / int(m.group(1))
            else:
                log.warning(f"Unknown timeframe '{timeframe}' for annualization — defaulting to 15m factor")
                bars_per_year = 35040
        ann_factor = math.sqrt(bars_per_year)
        stats['sharpe_ratio'] = round((mean_ret / std_ret * ann_factor) if std_ret > 0 else 0, 2)

        # Downside deviation over ALL returns (std of only the losses subtracts
        # the loss mean and inflates Sortino when losses are uniform)
        downside_dev = float(np.sqrt(np.mean([min(r, 0.0) ** 2 for r in returns])))
        stats['sortino_ratio'] = round((mean_ret / downside_dev * ann_factor) if downside_dev > 0 else 0, 2)
    else:
        stats['sharpe_ratio'] = 0
        stats['sortino_ratio'] = 0

    # Exposure time: sum of trade durations over the tested span (the old
    # equity != capital check read ~100% after the first trade forever)
    if equity_curve and len(equity_curve) >= 2:
        span_sec = max(1, equity_curve[-1]['time'] - equity_curve[0]['time'])
        in_trade_sec = sum(max(0, t.get('exit_time', 0) - t.get('entry_time', 0)) for t in trades)
        bars_in_trade = min(in_trade_sec, span_sec)  # overlapping fills clamp at 100%
        _exposure_pct_override = round(bars_in_trade / span_sec * 100, 1)
    else:
        _exposure_pct_override = 0.0
    stats['exposure_pct'] = _exposure_pct_override

    # Buy and hold comparison
    if len(equity_curve) > 0:
        # Not available here since we don't have the price series easily
        # Will be computed in the route if needed
        stats['buy_hold_return_pct'] = 0

    # Avg trade duration — durations are unix-second diffs; convert to bars
    if trades:
        durations = []
        for t in trades:
            if t.get('entry_time') and t.get('exit_time'):
                durations.append(t['exit_time'] - t['entry_time'])
        m = re.fullmatch(r'(\d+)\s*[mM]', str(timeframe).strip())
        tf_sec = int(m.group(1)) * 60 if m else {'1h': 3600, '6h': 21600, '1d': 86400}.get(timeframe, 900)
        avg_sec = float(np.mean(durations)) if durations else 0.0
        stats['avg_trade_duration_sec'] = round(avg_sec, 0)
        stats['avg_trade_bars'] = round(avg_sec / tf_sec, 1) if tf_sec > 0 else 0
    else:
        stats['avg_trade_bars'] = 0

    return stats
