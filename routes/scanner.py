import time
import numpy as np
import pandas as pd
import pandas_ta as ta
from flask import Blueprint, jsonify
from shared import client

scanner_bp = Blueprint('scanner', __name__)

# ==========================================
# COINBASE GRANULARITY MAPPING
# ==========================================
CB_GRAN = {
    "15m": ("FIFTEEN_MINUTE", 900),
    "30m": ("THIRTY_MINUTE",  1800),
    "1h":  ("ONE_HOUR",       3600),
}

# ==========================================
# DATA FETCHING
# ==========================================
def fetch_ohlcv(pair, gran_key, bars=300):
    cb_gran, sec = CB_GRAN[gran_key]
    end_ts = int(time.time())
    start_ts = end_ts - (bars * sec)
    res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
        "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
    })
    rows = [{
        'time': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
        'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))
    } for c in res.get('candles', [])]
    return pd.DataFrame(rows).sort_values('time').reset_index(drop=True)

def aggregate_4h(df_1h):
    """Aggregate 1H candles into 4H candles."""
    if df_1h.empty:
        return df_1h
    df = df_1h.copy()
    df['grp'] = df['time'] // 14400
    return df.groupby('grp').agg({
        'time': 'first', 'open': 'first', 'high': 'max',
        'low': 'min', 'close': 'last', 'volume': 'sum'
    }).reset_index(drop=True)

# ==========================================
# HELPERS
# ==========================================
def safe(val):
    if val is None:
        return None
    try:
        v = float(val)
        return None if np.isnan(v) or np.isinf(v) else round(v, 6)
    except:
        return None

def safe_last(series):
    if series is None or series.dropna().empty:
        return None
    return safe(series.dropna().iloc[-1])

# ==========================================
# INDICATOR COMPUTATION (LATEST VALUES)
# ==========================================
def compute_latest(df):
    """Compute latest indicator values for one timeframe's dashboard row."""
    if len(df) < 30:
        return {'error': 'Not enough data'}

    c, h, l, o, v = df['close'], df['high'], df['low'], df['open'], df['volume']
    hl2 = (h + l) / 2
    ohlc4 = (o + h + l + c) / 4
    r = {}

    try:
        r['price'] = safe(c.iloc[-1])
        r['high'] = safe(h.iloc[-1])
        r['low'] = safe(l.iloc[-1])
        r['avg'] = safe((h.iloc[-1] + l.iloc[-1]) / 2)
    except: pass

    # --- EMAs ---
    try:
        r['ema20'] = safe_last(ta.ema(c, 20))
        r['ema50'] = safe_last(ta.ema(c, 50))
        r['ema200'] = safe_last(ta.ema(c, 200))
    except: pass

    # --- RSI ---
    try: r['rsi'] = safe_last(ta.rsi(c, 14))
    except: pass

    # --- Stochastic ---
    try:
        stoch = ta.stoch(h, l, c, k=14, d=3, smooth_k=3)
        if stoch is not None and not stoch.empty:
            r['stoch_k'] = safe(stoch.iloc[-1, 0])
            r['stoch_d'] = safe(stoch.iloc[-1, 1])
    except: pass

    # --- CCI ---
    try: r['cci'] = safe_last(ta.cci(h, l, c, 20))
    except: pass

    # --- MFI ---
    try: r['mfi'] = safe_last(ta.mfi(h, l, c, v, 14))
    except: pass

    # --- ADX ---
    try:
        adx_df = ta.adx(h, l, c, 14)
        if adx_df is not None and not adx_df.empty:
            r['adx'] = safe(adx_df.iloc[-1, 0])
    except: pass

    # --- ROC ---
    try: r['roc'] = safe_last(ta.roc(c, 9))
    except: pass

    # --- Momentum ---
    try:
        if len(c) > 10:
            r['momentum'] = safe(c.iloc[-1] - c.iloc[-11])
    except: pass

    # --- MACD ---
    try:
        macd_df = ta.macd(c, 12, 26, 9)
        if macd_df is not None and not macd_df.empty:
            r['macd'] = safe(macd_df.iloc[-1, 0])
            r['macd_signal'] = safe(macd_df.iloc[-1, 1])
            r['macd_hist'] = safe(macd_df.iloc[-1, 2])
    except: pass

    # --- Tar Baby (RMA of OHLC4, length 120) ---
    try:
        if len(df) >= 120:
            tar = ohlc4.ewm(alpha=1.0/120.0, adjust=False).mean()
            r['tar'] = safe(tar.iloc[-1])
    except: pass

    # --- ATR ---
    try: r['atr'] = safe_last(ta.atr(h, l, c, 14))
    except: pass

    # --- StdDev ---
    try: r['stdev'] = safe_last(ta.stdev(c, 20))
    except: pass

    # --- Z-Score ---
    try:
        if len(c) >= 100:
            sma100 = float(ta.sma(c, 100).iloc[-1])
            std100 = float(ta.stdev(c, 100).iloc[-1])
            if std100 > 0:
                r['z_score'] = safe((c.iloc[-1] - sma100) / std100)
    except: pass

    # --- Volume Metrics ---
    try:
        r['vol'] = safe(v.iloc[-1])
        r['vol_avg'] = safe_last(ta.sma(v, 20))
        r['delta'] = safe(v.iloc[-1] if c.iloc[-1] >= o.iloc[-1] else -v.iloc[-1])
    except: pass

    # --- Rolling VWAP ---
    try:
        vw_num = ta.sma(c * v, 20)
        vw_den = ta.sma(v, 20)
        if vw_num is not None and vw_den is not None:
            den = float(vw_den.iloc[-1])
            r['vwap'] = safe(float(vw_num.iloc[-1]) / den) if den > 0 else None
    except: pass

    # --- Derived States ---
    if r.get('ema20') is not None and r.get('ema50') is not None:
        r['ema_cross'] = 'BULL' if r['ema20'] > r['ema50'] else 'BEAR'
    if r.get('ema50') is not None and r.get('ema200') is not None:
        r['ema_golden'] = 'GOLD' if r['ema50'] > r['ema200'] else 'DEATH'
    if r.get('tar') is not None and r.get('price') is not None:
        r['tar_state'] = 'ABOVE' if r['price'] > r['tar'] else 'BELOW'

    return r

# ==========================================
# VOLUME CANDLE COLORING
# ==========================================
def compute_volume_colors(df, vol_len=20):
    vol_sma = ta.sma(df['volume'], vol_len)
    colors = []
    for i in range(len(df)):
        is_bull = df['close'].iloc[i] >= df['open'].iloc[i]
        avg = float(vol_sma.iloc[i]) if vol_sma is not None and i < len(vol_sma) and pd.notna(vol_sma.iloc[i]) else 0
        mult = (df['volume'].iloc[i] / avg) if avg > 0 else 0

        if is_bull:
            if mult >= 2.0:   c = '#00FF00'
            elif mult >= 1.5: c = '#00BFFF'
            elif mult >= 1.0: c = '#008000'
            else:             c = '#006400'
        else:
            if mult >= 2.0:   c = '#FFFF00'
            elif mult >= 1.5: c = '#FFA500'
            elif mult >= 1.0: c = '#FF0000'
            else:             c = '#8B0000'
        colors.append(c)
    return colors

# ==========================================
# OVERLAY SERIES (FULL ARRAYS FOR CHART)
# ==========================================
def compute_overlays(df):
    """Compute full series for all toggleable overlays on the base timeframe."""
    if len(df) < 50:
        return {}

    c, h, l, o, v = df['close'], df['high'], df['low'], df['open'], df['volume']
    hl2 = (h + l) / 2
    ohlc4 = (o + h + l + c) / 4
    times = df['time'].tolist()
    overlays = {}

    def to_series(series, name):
        if series is None:
            return
        data = []
        for i in range(len(series)):
            val = series.iloc[i] if hasattr(series, 'iloc') else series[i]
            if pd.notna(val) and i < len(times):
                data.append({'time': int(times[i]), 'value': round(float(val), 6)})
        if data:
            overlays[name] = data

    # --- EMAs ---
    try:
        to_series(ta.ema(c, 20), 'ema20')
        to_series(ta.ema(c, 50), 'ema50')
        to_series(ta.ema(c, 200), 'ema200')
    except: pass

    # --- Bollinger Bands ---
    try:
        bb_basis = ta.sma(c, 20)
        bb_dev = 2.0 * ta.stdev(c, 20)
        if bb_basis is not None and bb_dev is not None:
            to_series(bb_basis + bb_dev, 'bb_upper')
            to_series(bb_basis - bb_dev, 'bb_lower')
            to_series(bb_basis, 'bb_mid')
    except: pass

    # --- Keltner Channels ---
    try:
        kc_basis = ta.ema(c, 20)
        # True Range calculation
        prev_c = c.shift(1)
        tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
        kc_range = ta.ema(tr, 20) * 1.5
        if kc_basis is not None and kc_range is not None:
            to_series(kc_basis + kc_range, 'kc_upper')
            to_series(kc_basis - kc_range, 'kc_lower')
    except: pass

    # --- Tar Baby ---
    try:
        if len(df) >= 120:
            tar = ohlc4.ewm(alpha=1.0/120.0, adjust=False).mean()
            to_series(tar, 'tar')
    except: pass

    # --- ALMA ---
    try:
        to_series(ta.alma(c, length=50, sigma=6, distribution_offset=0.85), 'alma_fast')
        to_series(ta.alma(c, length=200, sigma=6, distribution_offset=0.85), 'alma_slow')
    except: pass

    # --- Ichimoku ---
    try:
        tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
        kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
        span_a = ((tenkan + kijun) / 2)
        span_b = ((h.rolling(52).max() + l.rolling(52).min()) / 2)
        to_series(tenkan, 'ichi_tenkan')
        to_series(kijun, 'ichi_kijun')
        # Span A and B are shifted forward 26 bars in Pine; we send them un-shifted
        # and let the frontend handle offset if needed
        to_series(span_a, 'ichi_span_a')
        to_series(span_b, 'ichi_span_b')
    except: pass

    # --- Gator (Williams Alligator) ---
    try:
        to_series(ta.sma(hl2, 13), 'gator_jaw')
        to_series(ta.sma(hl2, 8), 'gator_teeth')
        to_series(ta.sma(hl2, 5), 'gator_lips')
    except: pass

    return overlays

# ==========================================
# MASTER CONFLUENCE
# ==========================================
def compute_confluence(rows):
    bull_count, bear_count = 0, 0

    for r in rows:
        b, br = True, True

        rsi = r.get('rsi')
        if rsi is not None:
            if rsi <= 50: b = False
            if rsi >= 50: br = False

        mac = r.get('macd')
        if mac is not None:
            if mac <= 0: b = False
            if mac >= 0: br = False

        ts = r.get('tar_state')
        if ts:
            if ts == 'BELOW': b = False
            if ts == 'ABOVE': br = False

        ec = r.get('ema_cross')
        if ec:
            if ec == 'BEAR': b = False
            if ec == 'BULL': br = False

        if b: bull_count += 1
        if br: bear_count += 1

    signal = "STRONG BUY" if bull_count == 4 else "STRONG SELL" if bear_count == 4 else "NEUTRAL"
    return {'bull_count': bull_count, 'bear_count': bear_count, 'signal': signal}

# ==========================================
# MAIN SCANNER ENDPOINT
# ==========================================
@scanner_bp.route('/api/scanner/<pair>')
def get_scanner(pair):
    """
    On-demand scanner: 4 API calls to Coinbase (15m, 30m, 1h for 4h aggregation).
    No polling. Called only when user clicks the SCANNER button.
    """
    try:
        # Fetch candles for each timeframe
        df_15m = fetch_ohlcv(pair, "15m", 300)
        time.sleep(0.15)  # Gentle rate-limit spacing
        df_30m = fetch_ohlcv(pair, "30m", 300)
        time.sleep(0.15)
        df_1h  = fetch_ohlcv(pair, "1h",  300)
        df_4h  = aggregate_4h(df_1h)

        # Dashboard rows
        rows = []
        for label, df in [("15m", df_15m), ("30m", df_30m), ("1H", df_1h), ("4H", df_4h)]:
            r = compute_latest(df)
            r['label'] = label
            rows.append(r)

        # Volume-colored candles for base TF
        vol_colors = compute_volume_colors(df_15m)
        candles = []
        for i in range(len(df_15m)):
            row = df_15m.iloc[i]
            candles.append({
                'time': int(row['time']),
                'open':  round(float(row['open']), 6),
                'high':  round(float(row['high']), 6),
                'low':   round(float(row['low']), 6),
                'close': round(float(row['close']), 6),
                'volume': round(float(row['volume']), 2),
                'color': vol_colors[i]
            })

        # Overlay line series for base TF chart
        overlays = compute_overlays(df_15m)

        # Master confluence
        confluence = compute_confluence(rows)

        return jsonify({
            'candles': candles,
            'dashboard': rows,
            'overlays': overlays,
            'confluence': confluence
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(error=str(e))
