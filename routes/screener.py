import time
import json
import os
import threading
import numpy as np
import pandas as pd
import pandas_ta as ta
from flask import Blueprint, jsonify, request
from shared import client, SCREENER_DATA
from logger import get_logger

log = get_logger('api')

screener_bp = Blueprint('screener', __name__)

# ==========================================
# PERSISTENT CONFIG
# ==========================================
CONFIG_FILE = "screener_config.json"

DEFAULT_WATCHLIST = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "AVAX-USD", "LINK-USD", "ADA-USD", "SHIB-USD"]

# Master registry: key -> (display_name, evaluator_function_name, timeframe_needed)
ALL_STRATEGIES = {
    "QUAD":     {"label": "QUAD",     "desc": "Strict pullback (4 stoch TFs + EMA)"},
    "SUPER":    {"label": "SUPER",    "desc": "Capitulation divergence after quad flush"},
    "GRID":     {"label": "GRID",     "desc": "Range trading (ADX regime gating)"},
    "ORB":      {"label": "ORB",      "desc": "60-min opening range breakout"},
    "TRAP":     {"label": "TRAP",     "desc": "SMA squeeze breakout"},
    "MOMENTUM": {"label": "MOM",      "desc": "Trend pullback (dual ROC + ADX)"},
    "DCA":      {"label": "DCA",      "desc": "Signal-gated accumulation (ROC cycle)"},
    "NPR":      {"label": "NPR",      "desc": "Price-action events (180/elephant/tail)"},
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {
        "watchlist": list(DEFAULT_WATCHLIST),
        "strategies": list(ALL_STRATEGIES.keys())
    }

def save_config(cfg):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(cfg, f, indent=2)

SCREENER_CONFIG = load_config()

CB_GRAN = {
    "5m":  ("FIVE_MINUTE",     300),
    "15m": ("FIFTEEN_MINUTE",  900),
    "1h":  ("ONE_HOUR",        3600),
    "1d":  ("ONE_DAY",         86400),
}

# ==========================================
# DATA HELPERS
# ==========================================
def fetch_candles(pair, gran_key, bars=300):
    cb_gran, sec = CB_GRAN[gran_key]
    end_ts = int(time.time())
    start_ts = end_ts - (bars * sec)
    res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
        "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
    })
    rows = [{
        'start': int(c['start']), 'time': int(c['start']),
        'open': float(c['open']), 'high': float(c['high']),
        'low': float(c['low']), 'close': float(c['close']),
        'volume': float(c.get('volume', 0))
    } for c in res.get('candles', [])]
    return pd.DataFrame(rows).sort_values('start').reset_index(drop=True)

def safe_val(v):
    try:
        f = float(v)
        return None if np.isnan(f) or np.isinf(f) else round(f, 4)
    except:
        return None

def safe_last(series):
    if series is None or series.dropna().empty:
        return None
    return safe_val(series.dropna().iloc[-1])

# ==========================================
# STRATEGY EVALUATORS
# Each returns: { status, reason, metrics }
#   status: "SIGNAL" | "SETUP" | "NEUTRAL" | "AVOID"
# ==========================================

def eval_quad(df):
    """QUAD: Strict pullback - macro/med stoch > 80, trigger stoch dips to 20, EMA touch."""
    m = {}
    if len(df) < 200:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    ema20 = ta.ema(c, 20)
    ema50 = ta.ema(c, 50)

    stoch_macro = ta.stoch(h, l, c, k=60, d=10, smooth_k=10)
    stoch_med   = ta.stoch(h, l, c, k=40, d=4,  smooth_k=4)
    stoch_trig  = ta.stoch(h, l, c, k=9,  d=3,  smooth_k=3)

    try:
        macro_d = float(stoch_macro.iloc[-1, 1])
        med_d   = float(stoch_med.iloc[-1, 1])
        trig_d  = float(stoch_trig.iloc[-1, 1])
        e20     = float(ema20.iloc[-1])
        e50     = float(ema50.iloc[-1])
        px      = float(c.iloc[-1])
        lo      = float(l.iloc[-1])
    except:
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    m = {"macro_d": round(macro_d, 1), "med_d": round(med_d, 1), "trig_d": round(trig_d, 1)}

    trend_ok    = px > e20 and px > e50
    macro_ok    = macro_d > 80
    med_ok      = med_d > 80
    trig_os     = trig_d <= 20
    ema_touch   = lo <= (e20 * 1.005)

    conditions_met = sum([trend_ok, macro_ok, med_ok, trig_os, ema_touch])

    if conditions_met == 5:
        return {"status": "SIGNAL", "reason": "All 5 conditions met", "metrics": m}
    elif conditions_met >= 3 and trend_ok:
        parts = []
        if not macro_ok: parts.append(f"Macro {macro_d:.0f}")
        if not med_ok:   parts.append(f"Med {med_d:.0f}")
        if not trig_os:  parts.append(f"Trig {trig_d:.0f}")
        if not ema_touch: parts.append("No EMA touch")
        return {"status": "SETUP", "reason": f"{conditions_met}/5: need " + ", ".join(parts), "metrics": m}
    elif not trend_ok:
        return {"status": "AVOID", "reason": "Below EMA trend filter", "metrics": m}
    else:
        return {"status": "NEUTRAL", "reason": f"{conditions_met}/5 conditions", "metrics": m}


def eval_quad_super(df):
    """QUAD_SUPER: Capitulation divergence after 4-stoch flush."""
    m = {}
    if len(df) < 200:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    stoch_macro = ta.stoch(h, l, c, k=60, d=10, smooth_k=10)
    stoch_med   = ta.stoch(h, l, c, k=40, d=4,  smooth_k=4)
    stoch_fast  = ta.stoch(h, l, c, k=14, d=3,  smooth_k=3)
    stoch_trig  = ta.stoch(h, l, c, k=9,  d=3,  smooth_k=3)

    try:
        macro_d = stoch_macro.iloc[:, 1]
        med_d   = stoch_med.iloc[:, 1]
        fast_d  = stoch_fast.iloc[:, 1]
        trig_d  = stoch_trig.iloc[:, 1]
    except:
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    curr_trig = float(trig_d.iloc[-1])
    curr_fast = float(fast_d.iloc[-1])
    m = {"trig_d": round(curr_trig, 1), "fast_d": round(curr_fast, 1)}

    flush_idx = None
    for i in range(2, min(16, len(df))):
        if (float(macro_d.iloc[-i]) < 20 and float(med_d.iloc[-i]) < 20 and
            float(fast_d.iloc[-i]) < 20 and float(trig_d.iloc[-i]) < 20):
            flush_idx = -i
            break

    if flush_idx is not None:
        anchor_low = float(l.iloc[flush_idx])
        curr_low = float(l.iloc[-1])
        price_lower = curr_low < anchor_low
        stoch_higher = curr_trig > 20 and curr_fast > 20
        curling = curr_trig > float(trig_d.iloc[-2]) and curr_fast > float(fast_d.iloc[-2])
        reversal = float(c.iloc[-1]) > float(df['open'].iloc[-1])

        m["flush_ago"] = abs(flush_idx)

        if price_lower and stoch_higher and curling and reversal:
            return {"status": "SIGNAL", "reason": f"Divergence confirmed (flush {abs(flush_idx)} bars ago)", "metrics": m}
        elif price_lower and stoch_higher:
            return {"status": "SETUP", "reason": "Divergence forming", "metrics": m}
        else:
            return {"status": "SETUP", "reason": f"Flush {abs(flush_idx)} bars ago", "metrics": m}

    all_low = all(v < 30 for v in [float(macro_d.iloc[-1]), float(med_d.iloc[-1]), curr_fast, curr_trig])
    if all_low:
        return {"status": "SETUP", "reason": "All stochs depressed", "metrics": m}

    return {"status": "NEUTRAL", "reason": "No recent quad flush", "metrics": m}


def eval_grid(df):
    """GRID: Ranging market (ADX < 25) is ideal."""
    m = {}
    if len(df) < 50:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    adx_df = ta.adx(h, l, c, 14)
    atr_s  = ta.atr(h, l, c, 14)

    try:
        adx = float(adx_df.iloc[-1, 0])
        atr = float(atr_s.iloc[-1])
        px  = float(c.iloc[-1])
    except:
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    atr_pct = (atr / px) * 100 if px > 0 else 0
    m = {"adx": round(adx, 1), "atr_pct": round(atr_pct, 2)}

    if adx < 20:
        return {"status": "SIGNAL", "reason": f"ADX {adx:.0f} \u2014 ideal range", "metrics": m}
    elif adx < 25:
        return {"status": "SETUP", "reason": f"ADX {adx:.0f} \u2014 borderline", "metrics": m}
    elif adx < 35:
        return {"status": "NEUTRAL", "reason": f"ADX {adx:.0f} \u2014 moderate trend", "metrics": m}
    else:
        return {"status": "AVOID", "reason": f"ADX {adx:.0f} \u2014 strong trend", "metrics": m}


def eval_orb(df_5m):
    """ORB: Opening range breakout window check."""
    m = {}
    if len(df_5m) < 60:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    df_5m = df_5m.copy()
    df_5m['datetime'] = pd.to_datetime(df_5m['start'], unit='s', utc=True)
    current_date = df_5m['datetime'].iloc[-1].date()
    curr_hour = df_5m['datetime'].iloc[-1].hour

    opening_range = df_5m[(df_5m['datetime'].dt.date == current_date) & (df_5m['datetime'].dt.hour == 0)]

    if len(opening_range) < 12:
        return {"status": "SETUP", "reason": "Range defining (00:00\u201301:00 UTC)", "metrics": m}

    range_high = float(opening_range['high'].max())
    range_low  = float(opening_range['low'].min())
    range_pct  = ((range_high - range_low) / range_low) * 100 if range_low > 0 else 0
    px = float(df_5m['close'].iloc[-1])

    m = {"range_h": round(range_high, 2), "range_l": round(range_low, 2), "range_pct": round(range_pct, 2)}

    hours_since = (df_5m['datetime'].iloc[-1] - opening_range['datetime'].iloc[0]).total_seconds() / 3600

    if hours_since > 6:
        return {"status": "NEUTRAL", "reason": "ORB window expired", "metrics": m}

    if curr_hour < 1:
        return {"status": "SETUP", "reason": "Range completing", "metrics": m}

    ema20 = ta.ema(df_5m['close'], 20)
    e20 = float(ema20.iloc[-1]) if ema20 is not None and not ema20.dropna().empty else px

    if px > range_high and px > e20:
        return {"status": "SIGNAL", "reason": f"Breakout above ${range_high:.2f}", "metrics": m}
    elif px < range_low and px < e20:
        return {"status": "SIGNAL", "reason": f"Breakdown below ${range_low:.2f}", "metrics": m}
    elif px > range_high * 0.998 or px < range_low * 1.002:
        return {"status": "SETUP", "reason": "Near range boundary", "metrics": m}
    else:
        return {"status": "NEUTRAL", "reason": f"Inside range ({range_pct:.1f}%)", "metrics": m}


def eval_trap(df):
    """TRAP: Converging SMAs + squeeze breakout."""
    m = {}
    if len(df) < 210:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    sma20  = ta.sma(c, 20)
    sma200 = ta.sma(c, 200)
    atr_s  = ta.atr(h, l, c, 14)
    vol_avg = ta.sma(df['volume'], 20)

    try:
        s20  = float(sma20.iloc[-1])
        s200 = float(sma200.iloc[-1])
        atr  = float(atr_s.iloc[-1])
        s20_start  = float(sma20.iloc[-21])
        s200_start = float(sma200.iloc[-21])
    except:
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    sma20_slope  = abs((s20 - s20_start) / s20_start) if s20_start > 0 else 999
    sma200_slope = abs((s200 - s200_start) / s200_start) if s200_start > 0 else 999
    sma_gap = abs(s20 - s200) / max(s20, s200) if max(s20, s200) > 0 else 999

    m = {
        "sma_gap_pct": round(sma_gap * 100, 2),
        "sma20_slope": round(sma20_slope * 100, 3),
        "sma200_slope": round(sma200_slope * 100, 3),
    }

    flat_20  = sma20_slope <= 0.003
    flat_200 = sma200_slope <= 0.0015
    converged = sma_gap <= 0.015

    if flat_20 and flat_200 and converged:
        curr = df.iloc[-1]
        curr_body = abs(float(curr['close']) - float(curr['open']))
        va = float(vol_avg.iloc[-1]) if vol_avg is not None and not pd.isna(vol_avg.iloc[-1]) else 0
        body_big = curr_body > atr
        vol_ok = float(curr['volume']) > (va * 1.5) if va > 0 else False

        if body_big and vol_ok:
            return {"status": "SIGNAL", "reason": "Power candle in squeeze", "metrics": m}
        else:
            return {"status": "SETUP", "reason": f"Squeeze active (gap {sma_gap*100:.1f}%)", "metrics": m}

    conditions_met = sum([flat_20, flat_200, converged])
    if conditions_met >= 2:
        return {"status": "SETUP", "reason": f"Converging ({conditions_met}/3)", "metrics": m}
    else:
        return {"status": "NEUTRAL", "reason": f"SMAs diverged ({sma_gap*100:.1f}%)", "metrics": m}


def eval_momentum(df):
    """MOMENTUM: Strong trend pullback - ADX >= 25, SMA20 > SMA200, dual ROC curl."""
    m = {}
    if len(df) < 210:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    sma20  = ta.sma(c, 20)
    sma200 = ta.sma(c, 200)
    adx_df = ta.adx(h, l, c, 14)
    roc5_raw = ta.roc(c, 5)
    roc14_raw = ta.roc(c, 14)
    roc5   = ta.sma(roc5_raw, 2) if roc5_raw is not None else None
    roc14  = ta.sma(roc14_raw, 2) if roc14_raw is not None else None

    try:
        adx   = float(adx_df.iloc[-1, 0])
        s20   = float(sma20.iloc[-1])
        s200  = float(sma200.iloc[-1])
        fast  = float(roc5.iloc[-1])
        slow  = float(roc14.iloc[-1])
        fast_p = float(roc5.iloc[-2])
        slow_p = float(roc14.iloc[-2])
    except:
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    m = {"adx": round(adx, 1), "fast_roc": round(fast, 2), "slow_roc": round(slow, 2)}

    trend_ok = s20 > s200
    adx_ok   = adx >= 25
    dip_ok   = fast <= -0.30 and slow <= -0.30
    curl_ok  = fast > fast_p and slow > slow_p

    if trend_ok and adx_ok and dip_ok and curl_ok:
        return {"status": "SIGNAL", "reason": "Dip + curl confirmed", "metrics": m}
    elif trend_ok and adx_ok and dip_ok:
        return {"status": "SETUP", "reason": "Dip detected, awaiting curl", "metrics": m}
    elif trend_ok and adx_ok:
        return {"status": "NEUTRAL", "reason": f"Trending, no dip (ROC {fast:.2f})", "metrics": m}
    elif not adx_ok:
        return {"status": "AVOID", "reason": f"ADX low ({adx:.0f})", "metrics": m}
    else:
        return {"status": "AVOID", "reason": "No uptrend", "metrics": m}


def eval_dca(df):
    """DCA: Dual ROC cross-below cycle - depth + curl."""
    m = {}
    if len(df) < 210:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    adx_df = ta.adx(h, l, c, 14)
    roc5_raw = ta.roc(c, 5)
    roc14_raw = ta.roc(c, 14)
    roc5   = ta.sma(roc5_raw, 3) if roc5_raw is not None else None
    roc14  = ta.sma(roc14_raw, 3) if roc14_raw is not None else None

    try:
        adx    = float(adx_df.iloc[-1, 0])
        fast   = float(roc5.iloc[-1])
        slow   = float(roc14.iloc[-1])
        fast_p = float(roc5.iloc[-2])
        slow_p = float(roc14.iloc[-2])
    except:
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    m = {"adx": round(adx, 1), "fast_roc": round(fast, 2), "slow_roc": round(slow, 2)}

    both_below = fast < 0 and slow < 0
    deep_enough = fast <= -0.30 and slow <= -0.30
    curling = fast > fast_p or slow > slow_p
    adx_ok = adx >= 10

    if both_below and deep_enough and curling and adx_ok:
        return {"status": "SIGNAL", "reason": "DCA buy zone", "metrics": m}
    elif both_below and deep_enough:
        return {"status": "SETUP", "reason": f"Deep dip, awaiting curl", "metrics": m}
    elif both_below:
        return {"status": "SETUP", "reason": f"ROCs below zero", "metrics": m}
    else:
        return {"status": "NEUTRAL", "reason": f"ROCs positive ({fast:.2f}/{slow:.2f})", "metrics": m}


def eval_npr(df):
    """NPR: Price-action events near converged flat SMAs."""
    m = {}
    if len(df) < 210:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    sma20  = ta.sma(c, 20)
    sma200 = ta.sma(c, 200)
    atr_s  = ta.atr(h, l, c, 14)

    try:
        s20  = float(sma20.iloc[-1])
        s200 = float(sma200.iloc[-1])
        atr  = float(atr_s.iloc[-1])
        px   = float(c.iloc[-1])
        s200_start = float(sma200.iloc[-21])
    except:
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    slope = abs((s200 - s200_start) / s200_start) if s200_start > 0 else 999
    gap   = abs(s20 - s200) / atr if atr > 0 else 999

    m = {"sma200_slope": round(slope * 100, 3), "ma_gap_atr": round(gap, 1)}

    flat_ok = slope <= 0.0015
    close_ok = gap <= 3.0

    if not flat_ok:
        return {"status": "AVOID", "reason": f"SMA200 not flat ({slope*100:.3f}%)", "metrics": m}
    if not close_ok:
        return {"status": "NEUTRAL", "reason": f"SMAs separated ({gap:.1f} ATR)", "metrics": m}

    if len(df) >= 2:
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        c_body = abs(float(curr['close']) - float(curr['open']))
        c_range = float(curr['high']) - float(curr['low'])

        bodies = df['close'].iloc[-21:-1].values - df['open'].iloc[-21:-1].values
        avg_body = float(pd.Series(abs(bodies)).mean()) if len(bodies) > 0 else c_body

        has_elephant = c_body >= 2.5 * avg_body and avg_body > 0
        has_tail = c_range > 0 and c_body > 0 and (c_body / c_range) <= 0.35
        has_180 = (float(prev['close']) < float(prev['open']) and float(curr['close']) > float(curr['open']) and
                   float(curr['high']) > float(prev['high'])) or \
                  (float(prev['close']) > float(prev['open']) and float(curr['close']) < float(curr['open']) and
                   float(curr['low']) < float(prev['low']))

        if has_180:
            return {"status": "SIGNAL", "reason": "180 bar in NPR zone", "metrics": m}
        elif has_elephant:
            return {"status": "SIGNAL", "reason": "Elephant bar in NPR zone", "metrics": m}
        elif has_tail:
            return {"status": "SETUP", "reason": "Tail bar detected", "metrics": m}

    return {"status": "SETUP", "reason": "In NPR zone, scanning", "metrics": m}


def eval_vwap_mr(df):
    """VWAP_MR: Mean reversion from VWAP lower band with RSI confirmation."""
    m = {}
    if len(df) < 100:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l, v = df['close'], df['high'], df['low'], df['volume']
    typical = (h + l + c) / 3
    cum_tv = (typical * v).cumsum()
    cum_v = v.cumsum()
    vwap = cum_tv / cum_v
    vwap_sq = ((typical - vwap) ** 2 * v).cumsum() / cum_v
    vwap_std = vwap_sq.apply(lambda x: x ** 0.5 if x > 0 else 0)
    rsi_s = ta.rsi(c, 14)

    try:
        px = float(c.iloc[-1])
        vw = float(vwap.iloc[-1])
        sd = float(vwap_std.iloc[-1])
        rsi = float(rsi_s.iloc[-1])
    except (IndexError, TypeError):
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    if sd <= 0:
        return {"status": "NEUTRAL", "reason": "VWAP std zero", "metrics": m}

    dev = (vw - px) / sd
    m = {"vwap": round(vw, 2), "deviation": round(dev, 2), "rsi": round(rsi, 1)}

    if px < vw - sd and rsi < 35:
        return {"status": "SIGNAL", "reason": f"Below VWAP -1σ, RSI {rsi:.0f}", "metrics": m}
    elif px < vw - (0.5 * sd) and rsi < 40:
        return {"status": "SETUP", "reason": f"Approaching lower band ({dev:.1f}σ)", "metrics": m}
    elif px > vw + sd:
        return {"status": "AVOID", "reason": f"Above VWAP +1σ, overextended", "metrics": m}
    return {"status": "NEUTRAL", "reason": f"Deviation {dev:.2f}σ, RSI {rsi:.0f}", "metrics": m}


def eval_squeeze(df):
    """SQUEEZE: Bollinger Bands inside Keltner Channels with momentum direction."""
    m = {}
    if len(df) < 210:
        return {"status": "NEUTRAL", "reason": "Not enough data", "metrics": m}

    c, h, l = df['close'], df['high'], df['low']
    bb = ta.bbands(c, length=20, std=2.0)
    kc = ta.kc(h, l, c, length=20, scalar=1.5)
    mom = ta.mom(c, length=12)

    try:
        bb_l, bb_u = float(bb.iloc[-1, 0]), float(bb.iloc[-1, 2])
        kc_l, kc_u = float(kc.iloc[-1, 0]), float(kc.iloc[-1, 2])
        bb_l_p, bb_u_p = float(bb.iloc[-2, 0]), float(bb.iloc[-2, 2])
        kc_l_p, kc_u_p = float(kc.iloc[-2, 0]), float(kc.iloc[-2, 2])
        cur_mom = float(mom.iloc[-1])
        prev_mom = float(mom.iloc[-2])
    except (IndexError, TypeError, KeyError):
        return {"status": "NEUTRAL", "reason": "Indicators warming up", "metrics": m}

    in_squeeze = bb_l > kc_l and bb_u < kc_u
    was_squeeze = bb_l_p > kc_l_p and bb_u_p < kc_u_p
    m = {"in_squeeze": in_squeeze, "momentum": round(cur_mom, 2)}

    if was_squeeze and not in_squeeze and cur_mom > 0:
        return {"status": "SIGNAL", "reason": f"Squeeze LONG release, momentum {cur_mom:.2f}", "metrics": m}
    elif was_squeeze and not in_squeeze and cur_mom < 0:
        return {"status": "SIGNAL", "reason": f"Squeeze SHORT release, momentum {cur_mom:.2f}", "metrics": m}
    elif in_squeeze:
        return {"status": "SETUP", "reason": f"In squeeze, momentum {cur_mom:.2f}", "metrics": m}
    return {"status": "NEUTRAL", "reason": f"No squeeze, momentum {cur_mom:.2f}", "metrics": m}


# ==========================================
# CORE METRICS
# ==========================================
def compute_core_metrics(df_daily, df_hourly):
    m = {}
    try:
        c = df_daily['close']
        h, l = df_daily['high'], df_daily['low']
        px = float(c.iloc[-1])
        m['price'] = px

        rsi = safe_last(ta.rsi(c, 14))
        m['rsi'] = rsi

        if len(df_hourly) > 30:
            adx_df = ta.adx(df_hourly['high'], df_hourly['low'], df_hourly['close'], 14)
            m['adx'] = safe_val(float(adx_df.iloc[-1, 0])) if adx_df is not None else None
        else:
            m['adx'] = None

        ema20 = safe_last(ta.ema(c, 20))
        ema50 = safe_last(ta.ema(c, 50))
        if ema20 and ema50:
            m['trend'] = "BULL" if ema20 > ema50 else "BEAR"
        else:
            m['trend'] = "\u2014"

        if len(c) >= 2:
            prev_px = float(c.iloc[-2])
            m['chg_24h'] = round(((px - prev_px) / prev_px) * 100, 2) if prev_px > 0 else 0
        else:
            m['chg_24h'] = 0

        atr = safe_last(ta.atr(h, l, c, 14))
        m['atr_pct'] = round((atr / px) * 100, 2) if atr and px > 0 else None

    except Exception as e:
        m['error'] = str(e)

    return m


# ==========================================
# BACKGROUND SCANNER THREAD
# ==========================================
EVAL_MAP = {
    "QUAD":     lambda dd, dh, d5: eval_quad(dd),
    "SUPER":    lambda dd, dh, d5: eval_quad_super(dd),
    "GRID":     lambda dd, dh, d5: eval_grid(dh),
    "ORB":      lambda dd, dh, d5: eval_orb(d5),
    "TRAP":     lambda dd, dh, d5: eval_trap(dh),
    "MOMENTUM": lambda dd, dh, d5: eval_momentum(dh),
    "DCA":      lambda dd, dh, d5: eval_dca(dd),
    "NPR":      lambda dd, dh, d5: eval_npr(dh),
    "VWAP_MR":  lambda dd, dh, d5: eval_vwap_mr(d5),
    "SQUEEZE":  lambda dd, dh, d5: eval_squeeze(dh),
}

def strategy_scanner():
    """Background thread: evaluate enabled strategies for every watchlist pair."""
    while True:
        try:
            cfg = SCREENER_CONFIG
            watchlist = cfg.get("watchlist", DEFAULT_WATCHLIST)
            enabled = cfg.get("strategies", list(ALL_STRATEGIES.keys()))
            results = []

            for pair in watchlist:
                row = {"pair": pair, "strategies": {}, "metrics": {}}
                try:
                    df_daily = fetch_candles(pair, "1d", 300)
                    time.sleep(0.3)
                    df_hourly = fetch_candles(pair, "1h", 300)
                    time.sleep(0.3)
                    df_5m = fetch_candles(pair, "5m", 200)
                    time.sleep(0.2)

                    # Inject live price
                    try:
                        p = client.get_product(product_id=pair)
                        live_px = float(p.price)
                        if not df_daily.empty:
                            df_daily.loc[df_daily.index[-1], 'close'] = live_px
                        if not df_hourly.empty:
                            df_hourly.loc[df_hourly.index[-1], 'close'] = live_px
                    except:
                        pass

                    row["metrics"] = compute_core_metrics(df_daily, df_hourly)

                    for name in enabled:
                        fn = EVAL_MAP.get(name)
                        if fn:
                            row["strategies"][name] = fn(df_daily, df_hourly, df_5m)

                    # Determine best-fit
                    best = None
                    for name in enabled:
                        s = row["strategies"].get(name, {})
                        if s.get("status") == "SIGNAL":
                            best = name
                            break
                    if not best:
                        for name in enabled:
                            s = row["strategies"].get(name, {})
                            if s.get("status") == "SETUP":
                                best = name
                                break
                    row["best"] = best

                except Exception as e:
                    row["error"] = str(e)
                    log.error("Screener error on %s: %s", pair, e)

                results.append(row)
                time.sleep(0.5)

            if results:
                SCREENER_DATA.clear()
                SCREENER_DATA.extend(results)
                log.info("Screener scan complete: %d pairs, %d strategies", len(results), len(enabled))

        except Exception as e:
            log.error("Screener thread error: %s", e)

        time.sleep(90)

threading.Thread(target=strategy_scanner, daemon=True).start()


# ==========================================
# API ENDPOINTS
# ==========================================
@screener_bp.route('/api/screener')
def get_screener():
    return jsonify(SCREENER_DATA)

@screener_bp.route('/api/screener/config')
def get_screener_config():
    """Return current watchlist, enabled strategies, and full strategy registry."""
    return jsonify({
        "watchlist": SCREENER_CONFIG.get("watchlist", []),
        "strategies": SCREENER_CONFIG.get("strategies", []),
        "all_strategies": ALL_STRATEGIES
    })

@screener_bp.route('/api/screener/watchlist', methods=['POST'])
def update_watchlist():
    """Add or remove a pair from the watchlist."""
    data = request.json
    action = data.get("action", "").upper()   # ADD or REMOVE
    pair = data.get("pair", "").upper().strip()

    if not pair:
        return jsonify({"success": False, "error": "No pair specified"})

    # Auto-append -USD if not present
    if not pair.endswith("-USD"):
        pair = pair + "-USD"

    wl = SCREENER_CONFIG.get("watchlist", list(DEFAULT_WATCHLIST))

    if action == "ADD":
        if pair in wl:
            return jsonify({"success": False, "error": f"{pair} already in watchlist"})
        # Validate the pair exists on Coinbase
        try:
            client.get_product(product_id=pair)
        except Exception as e:
            return jsonify({"success": False, "error": f"Invalid pair: {pair}"})
        wl.append(pair)
    elif action == "REMOVE":
        if pair not in wl:
            return jsonify({"success": False, "error": f"{pair} not in watchlist"})
        wl.remove(pair)
    else:
        return jsonify({"success": False, "error": "Action must be ADD or REMOVE"})

    SCREENER_CONFIG["watchlist"] = wl
    save_config(SCREENER_CONFIG)
    return jsonify({"success": True, "watchlist": wl})

@screener_bp.route('/api/screener/strategies', methods=['POST'])
def update_strategies():
    """Enable or disable a strategy from the screener."""
    data = request.json
    action = data.get("action", "").upper()   # ADD or REMOVE
    name = data.get("strategy", "").upper().strip()

    if not name:
        return jsonify({"success": False, "error": "No strategy specified"})

    if name not in ALL_STRATEGIES:
        return jsonify({"success": False, "error": f"Unknown strategy: {name}"})

    enabled = SCREENER_CONFIG.get("strategies", list(ALL_STRATEGIES.keys()))

    if action == "ADD":
        if name in enabled:
            return jsonify({"success": False, "error": f"{name} already enabled"})
        enabled.append(name)
    elif action == "REMOVE":
        if name not in enabled:
            return jsonify({"success": False, "error": f"{name} not enabled"})
        enabled.remove(name)
    else:
        return jsonify({"success": False, "error": "Action must be ADD or REMOVE"})

    SCREENER_CONFIG["strategies"] = enabled
    save_config(SCREENER_CONFIG)
    return jsonify({"success": True, "strategies": enabled})
