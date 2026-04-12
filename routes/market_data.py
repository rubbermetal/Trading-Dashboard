import time
import pandas as pd
import pandas_ta as ta
from flask import Blueprint, jsonify
from shared import client
from logger import get_logger

log = get_logger('api')

market_data_bp = Blueprint('market_data', __name__)

# ==========================================
# PRODUCT LIST CACHE
# ==========================================
_product_cache = {"data": None, "ts": 0}
CACHE_TTL = 3600  # Refresh product list once per hour

@market_data_bp.route('/api/products')
def get_products():
    """Returns all tradeable Coinbase products, grouped by type. Cached for 1 hour."""
    now = time.time()
    if _product_cache["data"] and (now - _product_cache["ts"]) < CACHE_TTL:
        return jsonify(_product_cache["data"])

    try:
        # Spot products
        res = client.get("/api/v3/brokerage/products", params={"limit": 5000})
        products = res.get('products', [])

        # Futures require a separate query — the default only returns SPOT
        try:
            fut_res = client.get("/api/v3/brokerage/products", params={"limit": 5000, "product_type": "FUTURE"})
            products += fut_res.get('products', [])
        except Exception as e:
            log.warning("Failed to fetch futures products: %s", e)

        spot, deriv = [], []
        for p in products:
            pid = p.get('product_id', '')
            status = p.get('status', '').upper()
            ptype = p.get('product_type', 'SPOT').upper()
            # Spot products use ONLINE status; futures have no status field
            if ptype == 'SPOT' and status != 'ONLINE':
                continue
            entry = {
                "id": pid,
                "base": p.get('base_currency_id', ''),
                "quote": p.get('quote_currency_id', ''),
                "price": float(p.get('price') or 0),
                "type": ptype
            }
            if ptype == 'SPOT':
                if entry['quote'] in ('USD', 'USDC'):
                    spot.append(entry)
            else:
                deriv.append(entry)

        spot.sort(key=lambda x: x['id'])
        deriv.sort(key=lambda x: x['id'])

        result = {"spot": spot, "derivatives": deriv}
        _product_cache["data"] = result
        _product_cache["ts"] = now
        return jsonify(result)
    except Exception as e:
        return jsonify(error=str(e))

@market_data_bp.route('/api/orderbook/<pair>')
def get_book(pair):
    try:
        res = client.get_product_book(product_id=pair, limit=50)
        bids = res.pricebook.bids
        asks = res.pricebook.asks

        if not bids or not asks:
            return jsonify(error="Empty book")

        total_bid_vol = sum(float(b.size) for b in bids)
        total_ask_vol = sum(float(a.size) for a in asks)
        total_vol = total_bid_vol + total_ask_vol

        bid_pct = (total_bid_vol / total_vol) * 100 if total_vol > 0 else 50
        ask_pct = (total_ask_vol / total_vol) * 100 if total_vol > 0 else 50

        biggest_bid = max(bids, key=lambda x: float(x.size))
        biggest_ask = max(asks, key=lambda x: float(x.size))

        return jsonify({
            "bids": [{"price": b.price, "size": b.size} for b in bids[:5]],
            "asks": [{"price": a.price, "size": a.size} for a in asks[:5]],
            "imbalance": {
                "bid_pct": round(bid_pct, 1), 
                "ask_pct": round(ask_pct, 1)
            },
            "walls": {
                "buy_wall_px": biggest_bid.price, 
                "buy_wall_size": biggest_bid.size,
                "sell_wall_px": biggest_ask.price, 
                "sell_wall_size": biggest_ask.size
            }
        })
    except Exception as e:
        return jsonify(error=str(e))

# ==========================================
# CANDLES ENDPOINT FOR LIGHTWEIGHT CHARTS
# ==========================================
GRANULARITY_MAP = {
    "1m":  {"cb": "ONE_MINUTE",      "seconds": 60},
    "5m":  {"cb": "FIVE_MINUTE",     "seconds": 300},
    "15m": {"cb": "FIFTEEN_MINUTE",  "seconds": 900},
    "30m": {"cb": "THIRTY_MINUTE",   "seconds": 1800},
    "1h":  {"cb": "ONE_HOUR",        "seconds": 3600},
    "6h":  {"cb": "SIX_HOURS",       "seconds": 21600},
    "1d":  {"cb": "ONE_DAY",         "seconds": 86400},
}

@market_data_bp.route('/api/candles/<pair>/<granularity>')
def get_candles(pair, granularity):
    """Returns OHLCV candles for Lightweight Charts. Max 300 per Coinbase limit."""
    from flask import request
    g = GRANULARITY_MAP.get(granularity)
    if not g:
        return jsonify(error=f"Invalid granularity. Use: {', '.join(GRANULARITY_MAP.keys())}")

    try:
        limit = min(int(request.args.get('limit', 300)), 300)
        end_ts = int(time.time())
        start_ts = end_ts - (limit * g['seconds'])

        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": g['cb']
        })
        candles = res.get('candles', [])

        parsed = sorted([{
            "time": int(c['start']),
            "open": float(c['open']),
            "high": float(c['high']),
            "low": float(c['low']),
            "close": float(c['close']),
            "volume": float(c.get('volume', 0))
        } for c in candles], key=lambda x: x['time'])

        return jsonify(parsed)
    except Exception as e:
        return jsonify(error=str(e))


@market_data_bp.route('/api/chart_candles/<pair>/<granularity>')
def get_chart_candles_endpoint(pair, granularity):
    """
    DB-first chart candles. Reads historical bars from candles.db (1m store,
    aggregated on the fly to the requested TF) then fills in the recent tail
    from Coinbase so the last few bars are live.
    Falls back to a pure Coinbase fetch if the pair has no DB coverage.
    """
    from flask import request
    from candle_db import get_chart_candles, get_last_timestamp, fetch_and_store_1m

    g = GRANULARITY_MAP.get(granularity)
    if not g:
        return jsonify(error=f"Invalid granularity. Use: {', '.join(GRANULARITY_MAP.keys())}")

    try:
        limit = max(10, min(int(request.args.get('limit', 1500)), 5000))
        tf_minutes = g['seconds'] // 60
        now_ts = int(time.time())
        tf_seconds = g['seconds']

        # Top up the 1m store if it's more than one TF period stale
        last_1m = get_last_timestamp(pair)
        if last_1m > 0 and (now_ts - last_1m) > tf_seconds:
            try:
                fetch_and_store_1m(pair, last_1m, now_ts)
            except Exception as e:
                log.warning(f"[{pair}] chart tail top-up failed: {e}")

        df = get_chart_candles(pair, tf_minutes, limit)

        if df.empty:
            # No DB coverage — straight Coinbase fallback (bounded to 300)
            fallback_limit = min(limit, 300)
            start_ts = now_ts - (fallback_limit * tf_seconds)
            res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
                "start": str(start_ts), "end": str(now_ts), "granularity": g['cb']
            })
            parsed = sorted([{
                "time": int(c['start']),
                "open": float(c['open']),
                "high": float(c['high']),
                "low": float(c['low']),
                "close": float(c['close']),
                "volume": float(c.get('volume', 0))
            } for c in res.get('candles', [])], key=lambda x: x['time'])
            return jsonify(parsed)

        parsed = [{
            "time": int(row.start),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
            "volume": float(row.volume)
        } for row in df.itertuples(index=False)]

        return jsonify(parsed)
    except Exception as e:
        log.error(f"[{pair}] chart_candles error: {e}")
        return jsonify(error=str(e))

@market_data_bp.route('/api/chart_indicators/<pair>/<granularity>')
def get_chart_indicators(pair, granularity):
    """
    Deep-history oscillator indicators computed from candles.db.
    Query with ?set=rsi,adx,roc,macd,stoch — any subset.
    Returns {indicator_key: [{time, value}, ...]} — multiple keys per indicator where relevant.
    """
    from flask import request
    from candle_db import get_chart_candles, get_last_timestamp, fetch_and_store_1m

    g = GRANULARITY_MAP.get(granularity)
    if not g:
        return jsonify(error=f"Invalid granularity")

    raw_set = (request.args.get('set') or '').strip()
    if not raw_set:
        return jsonify({})
    wanted = {s.strip().lower() for s in raw_set.split(',') if s.strip()}
    if not wanted:
        return jsonify({})

    try:
        limit = max(50, min(int(request.args.get('limit', 1500)), 5000))
        tf_minutes = g['seconds'] // 60
        now_ts = int(time.time())

        last_1m = get_last_timestamp(pair)
        if last_1m > 0 and (now_ts - last_1m) > g['seconds']:
            try:
                fetch_and_store_1m(pair, last_1m, now_ts)
            except Exception as e:
                log.warning(f"[{pair}] indicator tail top-up failed: {e}")

        df = get_chart_candles(pair, tf_minutes, limit)
        if df.empty:
            return jsonify({})

        c = df['close']
        h = df['high']
        l = df['low']
        times = df['start'].tolist()

        def to_series(series):
            data = []
            for i in range(len(series)):
                val = series.iloc[i] if hasattr(series, 'iloc') else series[i]
                if pd.notna(val) and i < len(times):
                    data.append({'time': int(times[i]), 'value': round(float(val), 6)})
            return data

        out = {}

        if 'rsi' in wanted:
            try:
                out['rsi'] = to_series(ta.rsi(c, length=14))
            except Exception as e:
                log.warning(f"[{pair}] RSI compute failed: {e}")

        if 'adx' in wanted:
            try:
                adx_df = ta.adx(h, l, c, length=14)
                adx_col = next((col for col in adx_df.columns if col.startswith('ADX')), None)
                if adx_col:
                    out['adx'] = to_series(adx_df[adx_col])
            except Exception as e:
                log.warning(f"[{pair}] ADX compute failed: {e}")

        if 'roc' in wanted:
            try:
                out['roc_fast'] = to_series(ta.roc(c, length=7))
                out['roc_slow'] = to_series(ta.roc(c, length=21))
            except Exception as e:
                log.warning(f"[{pair}] ROC compute failed: {e}")

        if 'macd' in wanted:
            try:
                macd_df = ta.macd(c, fast=12, slow=26, signal=9)
                macd_col = next((col for col in macd_df.columns if col.startswith('MACD_')), None)
                signal_col = next((col for col in macd_df.columns if col.startswith('MACDs_')), None)
                hist_col = next((col for col in macd_df.columns if col.startswith('MACDh_')), None)
                if macd_col: out['macd'] = to_series(macd_df[macd_col])
                if signal_col: out['macd_signal'] = to_series(macd_df[signal_col])
                if hist_col: out['macd_hist'] = to_series(macd_df[hist_col])
            except Exception as e:
                log.warning(f"[{pair}] MACD compute failed: {e}")

        if 'stoch' in wanted:
            try:
                stoch_df = ta.stoch(h, l, c, k=14, d=3)
                k_col = next((col for col in stoch_df.columns if col.startswith('STOCHk')), None)
                d_col = next((col for col in stoch_df.columns if col.startswith('STOCHd')), None)
                if k_col: out['stoch_k'] = to_series(stoch_df[k_col])
                if d_col: out['stoch_d'] = to_series(stoch_df[d_col])
            except Exception as e:
                log.warning(f"[{pair}] STOCH compute failed: {e}")

        return jsonify(out)
    except Exception as e:
        log.error(f"[{pair}] chart_indicators error: {e}")
        return jsonify(error=str(e))


@market_data_bp.route('/api/chart_overlays/<pair>/<granularity>')
def get_chart_overlays(pair, granularity):
    """
    Deep-history overlays (EMA20/50/200, BB, KC, Ichimoku, Alligator, Tar, ALMA)
    computed over the same candle window the chart is showing (DB-backed, up to 1500 bars).
    Mirrors scanner.compute_overlays but uses candles.db instead of a 300-bar Coinbase fetch.
    """
    from flask import request
    from candle_db import get_chart_candles, get_last_timestamp, fetch_and_store_1m
    from routes.scanner import compute_overlays

    g = GRANULARITY_MAP.get(granularity)
    if not g:
        return jsonify(error=f"Invalid granularity. Use: {', '.join(GRANULARITY_MAP.keys())}")

    try:
        limit = max(50, min(int(request.args.get('limit', 1500)), 5000))
        tf_minutes = g['seconds'] // 60
        now_ts = int(time.time())

        # Top up the DB tail so overlays include the latest bar
        last_1m = get_last_timestamp(pair)
        if last_1m > 0 and (now_ts - last_1m) > g['seconds']:
            try:
                fetch_and_store_1m(pair, last_1m, now_ts)
            except Exception as e:
                log.warning(f"[{pair}] overlay tail top-up failed: {e}")

        df = get_chart_candles(pair, tf_minutes, limit)
        if df.empty:
            return jsonify({})

        # compute_overlays expects a 'time' column (not 'start')
        df = df.rename(columns={'start': 'time'})
        overlays = compute_overlays(df)
        return jsonify(overlays)
    except Exception as e:
        log.error(f"[{pair}] chart_overlays error: {e}")
        return jsonify(error=str(e))


@market_data_bp.route('/api/volume_profile/<pair>/<granularity>')
def get_volume_profile(pair, granularity):
    """
    Returns volume binned by price level for a volume profile overlay.
    Uses the local candle DB (deep history) with a Coinbase fallback.
    ?limit=N controls how many bars to bin (default 1500, max 5000).
    """
    from flask import request
    from candle_db import get_chart_candles, get_last_timestamp, fetch_and_store_1m

    g = GRANULARITY_MAP.get(granularity)
    if not g:
        return jsonify(error="Invalid granularity")
    try:
        limit = max(50, min(int(request.args.get('limit', 1500)), 5000))
        tf_minutes = g['seconds'] // 60
        now_ts = int(time.time())

        # Top up the DB tail so the profile includes the latest bar
        last_1m = get_last_timestamp(pair)
        if last_1m > 0 and (now_ts - last_1m) > g['seconds']:
            try:
                fetch_and_store_1m(pair, last_1m, now_ts)
            except Exception as e:
                log.warning(f"[{pair}] volume_profile tail top-up failed: {e}")

        df = get_chart_candles(pair, tf_minutes, limit)
        if df.empty:
            # Fallback: direct Coinbase (capped at 300 bars by API)
            end_ts = now_ts
            start_ts = end_ts - (300 * g['seconds'])
            res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
                "start": str(start_ts), "end": str(end_ts), "granularity": g['cb']
            })
            parsed = sorted([{
                'open': float(c['open']), 'high': float(c['high']),
                'low': float(c['low']), 'close': float(c['close']),
                'volume': float(c.get('volume', 0))
            } for c in res.get('candles', [])], key=lambda x: x.get('time', 0))
            if not parsed:
                return jsonify([])
            df = pd.DataFrame(parsed)

        if df.empty:
            return jsonify([])

        # Determine price range and bin count
        price_min = float(df['low'].min())
        price_max = float(df['high'].max())
        price_range = price_max - price_min
        if price_range <= 0:
            return jsonify([])

        num_bins = 40
        bin_size = price_range / num_bins

        # Accumulate volume into bins, split by buy/sell (up-close vs down-close)
        bins = [{'price': price_min + (i + 0.5) * bin_size, 'buy_vol': 0.0, 'sell_vol': 0.0} for i in range(num_bins)]
        for row in df.itertuples(index=False):
            vol = float(row.volume)
            mid = (float(row.high) + float(row.low)) / 2
            idx = min(int((mid - price_min) / bin_size), num_bins - 1)
            if idx < 0:
                idx = 0
            if float(row.close) >= float(row.open):
                bins[idx]['buy_vol'] += vol
            else:
                bins[idx]['sell_vol'] += vol

        # Find max volume for normalization
        max_vol = max(b['buy_vol'] + b['sell_vol'] for b in bins) or 1.0

        result = []
        for b in bins:
            total = b['buy_vol'] + b['sell_vol']
            if total > 0:
                result.append({
                    'price': round(b['price'], 6),
                    'total': round(total, 2),
                    'buy_pct': round(b['buy_vol'] / total * 100, 1),
                    'strength': round(total / max_vol, 3),  # 0-1 normalized
                })
        return jsonify(result)
    except Exception as e:
        log.error(f"[{pair}] volume_profile error: {e}")
        return jsonify(error=str(e))

# ==========================================
# BOT INDICATOR SERIES FOR CHART OVERLAYS
# ==========================================
def _safe_series(series, times):
    """Convert a pandas Series to [{time, value}] for Lightweight Charts."""
    if series is None:
        return []
    data = []
    for i in range(len(series)):
        val = series.iloc[i] if hasattr(series, 'iloc') else series[i]
        if pd.notna(val) and i < len(times):
            data.append({'time': int(times[i]), 'value': round(float(val), 6)})
    return data

@market_data_bp.route('/api/bot_indicators/<pair>/<granularity>/<strategy>')
def get_bot_indicators(pair, granularity, strategy):
    """
    Returns strategy-specific indicator series for bot chart overlays.
    
    Response format:
    {
        "overlays": { "name": [{time, value}, ...] },     // price-scale lines
        "oscillators": {
            "pane_name": {
                "series": { "name": {data, color, width} },
                "range": [min, max],       // y-axis range hint
                "levels": [20, 80]         // horizontal reference lines
            }
        }
    }
    """
    from flask import request
    from candle_db import get_chart_candles, get_last_timestamp, fetch_and_store_1m

    g = GRANULARITY_MAP.get(granularity)
    if not g:
        return jsonify(error="Invalid granularity")

    try:
        limit = max(50, min(int(request.args.get('limit', 1500)), 5000))
        tf_minutes = g['seconds'] // 60
        now_ts = int(time.time())

        # Top up the DB tail so indicators include the latest bar
        last_1m = get_last_timestamp(pair)
        if last_1m > 0 and (now_ts - last_1m) > g['seconds']:
            try:
                fetch_and_store_1m(pair, last_1m, now_ts)
            except Exception as e:
                log.warning(f"[{pair}] bot_indicators tail top-up failed: {e}")

        df = get_chart_candles(pair, tf_minutes, limit)

        if df.empty:
            # Fallback to Coinbase direct if the DB has no coverage for this pair
            end_ts = now_ts
            start_ts = end_ts - (300 * g['seconds'])
            res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
                "start": str(start_ts), "end": str(end_ts), "granularity": g['cb']
            })
            candles = res.get('candles', [])
            if len(candles) < 50:
                return jsonify(error="Not enough candle data")
            parsed = sorted([{
                'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
                'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))
            } for c in candles], key=lambda x: x['start'])
            df = pd.DataFrame(parsed)
    except Exception as e:
        return jsonify(error=str(e))

    if len(df) < 50:
        return jsonify(error="Not enough candle data")

    c, h, l, o, v = df['close'], df['high'], df['low'], df['open'], df['volume']
    times = df['start'].tolist()
    
    overlays = {}
    oscillators = {}
    strategy = strategy.upper()
    
    # ==========================================
    # MOMENTUM / DCA: SMA(20), SMA(200) + dual ROC + ADX
    # ==========================================
    if strategy in ('MOMENTUM', 'DCA'):
        # Overlays
        sma20 = ta.sma(c, 20)
        sma200 = ta.sma(c, 200)
        if sma20 is not None:
            overlays['SMA 20'] = {'data': _safe_series(sma20, times), 'color': '#FFD700', 'width': 1}
        if sma200 is not None:
            overlays['SMA 200'] = {'data': _safe_series(sma200, times), 'color': '#4169E1', 'width': 2}
        
        # ROC pane
        roc5_raw = ta.roc(c, 5)
        roc5_smooth = ta.sma(roc5_raw, 3) if roc5_raw is not None else None
        roc14_raw = ta.roc(c, 14)
        roc14_smooth = ta.sma(roc14_raw, 3) if roc14_raw is not None else None
        
        roc_series = {}
        if roc5_smooth is not None:
            roc_series['Fast ROC'] = {'data': _safe_series(roc5_smooth, times), 'color': '#00d578', 'width': 1}
        if roc14_smooth is not None:
            roc_series['Slow ROC'] = {'data': _safe_series(roc14_smooth, times), 'color': '#ff4b5c', 'width': 1}
        
        if roc_series:
            oscillators['ROC'] = {
                'series': roc_series,
                'levels': [{'value': -0.50, 'color': '#ffc107', 'label': '-0.50'}, {'value': 0, 'color': '#555', 'label': '0'}],
            }
        
        # ADX pane
        adx_df = ta.adx(h, l, c, 14)
        if adx_df is not None and not adx_df.empty:
            adx_series = adx_df.iloc[:, 0]
            oscillators['ADX'] = {
                'series': {
                    'ADX': {'data': _safe_series(adx_series, times), 'color': '#66fcf1', 'width': 2}
                },
                'levels': [
                    {'value': 20, 'color': '#ffc107', 'label': '20'},
                    {'value': 25, 'color': '#ff4b5c', 'label': '25'}
                ],
            }
    
    # ==========================================
    # GRID: SMA(5) + ADX
    # ==========================================
    elif strategy == 'GRID':
        sma5 = ta.sma(c, 5)
        if sma5 is not None:
            overlays['SMA 5'] = {'data': _safe_series(sma5, times), 'color': '#FFD700', 'width': 1}
        
        adx_df = ta.adx(h, l, c, 14)
        if adx_df is not None and not adx_df.empty:
            oscillators['ADX'] = {
                'series': {
                    'ADX': {'data': _safe_series(adx_df.iloc[:, 0], times), 'color': '#66fcf1', 'width': 2}
                },
                'levels': [{'value': 25, 'color': '#ff4b5c', 'label': '25'}],
            }
    
    # ==========================================
    # QUAD / QUAD_SUPER: EMAs + Stochastics
    # ==========================================
    elif strategy in ('QUAD', 'QUAD_SUPER'):
        ema20 = ta.ema(c, 20)
        ema50 = ta.ema(c, 50)
        ema200 = ta.ema(c, 200)
        if ema20 is not None:
            overlays['EMA 20'] = {'data': _safe_series(ema20, times), 'color': '#FFD700', 'width': 1}
        if ema50 is not None:
            overlays['EMA 50'] = {'data': _safe_series(ema50, times), 'color': '#FFA500', 'width': 1}
        if ema200 is not None:
            overlays['EMA 200'] = {'data': _safe_series(ema200, times), 'color': '#4169E1', 'width': 2}
        
        # Stochastics pane
        stoch_series = {}
        try:
            stoch_macro = ta.stoch(h, l, c, k=60, d=10, smooth_k=10)
            stoch_med = ta.stoch(h, l, c, k=40, d=4, smooth_k=4)
            stoch_fast = ta.stoch(h, l, c, k=14, d=3, smooth_k=3)
            stoch_trig = ta.stoch(h, l, c, k=9, d=3, smooth_k=3)
            
            if stoch_macro is not None and not stoch_macro.empty:
                stoch_series['Macro %D'] = {'data': _safe_series(stoch_macro.iloc[:, 1], times), 'color': '#4169E1', 'width': 2}
            if stoch_med is not None and not stoch_med.empty:
                stoch_series['Med %D'] = {'data': _safe_series(stoch_med.iloc[:, 1], times), 'color': '#FFA500', 'width': 1}
            if stoch_fast is not None and not stoch_fast.empty:
                stoch_series['Fast %D'] = {'data': _safe_series(stoch_fast.iloc[:, 1], times), 'color': '#00d578', 'width': 1}
            if stoch_trig is not None and not stoch_trig.empty:
                stoch_series['Trig %D'] = {'data': _safe_series(stoch_trig.iloc[:, 1], times), 'color': '#ff4b5c', 'width': 1}
        except:
            pass
        
        if stoch_series:
            oscillators['Stochastic'] = {
                'series': stoch_series,
                'levels': [
                    {'value': 20, 'color': '#00d578', 'label': '20'},
                    {'value': 80, 'color': '#ff4b5c', 'label': '80'}
                ],
            }
    
    # ==========================================
    # TRAP: SMA(20), SMA(200) + ATR + Volume ratio
    # ==========================================
    elif strategy == 'TRAP':
        sma20 = ta.sma(c, 20)
        sma200 = ta.sma(c, 200)
        if sma20 is not None:
            overlays['SMA 20'] = {'data': _safe_series(sma20, times), 'color': '#FFD700', 'width': 1}
        if sma200 is not None:
            overlays['SMA 200'] = {'data': _safe_series(sma200, times), 'color': '#4169E1', 'width': 2}
        
        # ATR pane
        atr = ta.atr(h, l, c, 14)
        if atr is not None:
            oscillators['ATR'] = {
                'series': {
                    'ATR(14)': {'data': _safe_series(atr, times), 'color': '#ffc107', 'width': 1}
                },
                'levels': [],
            }
        
        # Volume ratio pane
        vol_avg = ta.sma(v, 20)
        if vol_avg is not None:
            vol_ratio = v / vol_avg
            vol_ratio = vol_ratio.fillna(0)
            oscillators['Vol Ratio'] = {
                'series': {
                    'Vol/Avg': {'data': _safe_series(vol_ratio, times), 'color': '#66fcf1', 'width': 1}
                },
                'levels': [{'value': 1.5, 'color': '#ffc107', 'label': '1.5x'}],
            }
    
    # ==========================================
    # ORB: EMA(20), VWAP + opening range
    # ==========================================
    elif strategy == 'ORB':
        ema20 = ta.ema(c, 20)
        if ema20 is not None:
            overlays['EMA 20'] = {'data': _safe_series(ema20, times), 'color': '#FFD700', 'width': 1}
        
        # VWAP
        typical = (h + l + c) / 3
        vwap = (typical * v).cumsum() / v.cumsum()
        if vwap is not None:
            overlays['VWAP'] = {'data': _safe_series(vwap, times), 'color': '#9B59B6', 'width': 1}
    
    return jsonify({
        'overlays': overlays,
        'oscillators': oscillators,
    })
