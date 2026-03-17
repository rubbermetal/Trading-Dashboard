import time
from flask import Blueprint, jsonify
from shared import client

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
        res = client.get("/api/v3/brokerage/products", params={"limit": 5000})
        products = res.get('products', [])

        spot, deriv = [], []
        for p in products:
            pid = p.get('product_id', '')
            status = p.get('status', '').upper()
            if status != 'ONLINE':
                continue
            ptype = p.get('product_type', 'SPOT').upper()
            entry = {
                "id": pid,
                "base": p.get('base_currency_id', ''),
                "quote": p.get('quote_currency_id', ''),
                "price": float(p.get('price', 0)),
                "type": ptype
            }
            if ptype == 'SPOT':
                # Only include USD and USDC quote pairs
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
        # Fetch a deeper book (top 50 levels) instead of just 5
        res = client.get_product_book(product_id=pair, limit=50)
        
        # The Coinbase Python SDK stores these inside pricebook.bids / pricebook.asks
        bids = res.pricebook.bids
        asks = res.pricebook.asks

        if not bids or not asks:
            return jsonify(error="Empty book")

        # 1. Calculate Imbalance (Buying Pressure vs Selling Pressure)
        total_bid_vol = sum(float(b.size) for b in bids)
        total_ask_vol = sum(float(a.size) for a in asks)
        total_vol = total_bid_vol + total_ask_vol

        bid_pct = (total_bid_vol / total_vol) * 100 if total_vol > 0 else 50
        ask_pct = (total_ask_vol / total_vol) * 100 if total_vol > 0 else 50

        # 2. Find the "Whale Walls" (The single largest order in the top 50 levels)
        biggest_bid = max(bids, key=lambda x: float(x.size))
        biggest_ask = max(asks, key=lambda x: float(x.size))

        return jsonify({
            "bids": [{"price": b.price, "size": b.size} for b in bids[:5]], # Keep top 5 for micro-view
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
    g = GRANULARITY_MAP.get(granularity)
    if not g:
        return jsonify(error=f"Invalid granularity. Use: {', '.join(GRANULARITY_MAP.keys())}")
    
    try:
        end_ts = int(time.time())
        start_ts = end_ts - (300 * g['seconds'])
        
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": g['cb']
        })
        candles = res.get('candles', [])
        
        # Coinbase returns newest-first; Lightweight Charts needs oldest-first
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