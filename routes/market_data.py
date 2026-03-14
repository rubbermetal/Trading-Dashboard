import time
from flask import Blueprint, jsonify
from shared import client

market_data_bp = Blueprint('market_data', __name__)

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