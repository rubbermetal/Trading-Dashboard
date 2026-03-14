import uuid, time, threading
import pandas as pd
import pandas_ta as ta
from flask import Blueprint, jsonify, request
from shared import client, TRAILING_STOPS

trading_bp = Blueprint('trading', __name__)

def background_watcher():
    """Checks active trailing stops every 10 seconds."""
    while True:
        try:
            for pair, data in list(TRAILING_STOPS.items()):
                p = client.get_product(product_id=pair)
                cur_px = float(p.price)
                if cur_px > data['highest_price']:
                    TRAILING_STOPS[pair]['highest_price'] = cur_px
                
                trigger_px = data['highest_price'] * (1 - data['trail_pct'])
                if cur_px <= trigger_px:
                    client.market_order_sell(client_order_id=str(uuid.uuid4()), product_id=pair, base_size=data['size'])
                    del TRAILING_STOPS[pair]
        except Exception as e: pass
        time.sleep(10)

threading.Thread(target=background_watcher, daemon=True).start()

@trading_bp.route('/api/search/<symbol>')
def search(symbol):
    s = symbol.upper()
    for v in [f"{s}-USD", f"{s}-PERP", f"{s}-USDC", s]:
        try:
            p = client.get_product(product_id=v)
            return jsonify({"success": True, "symbol": p.product_id, "price": float(p.price), "type": p.product_type})
        except: continue
    return jsonify({"success": False, "error": "Not Found"})

@trading_bp.route('/api/trade', methods=['POST'])
def trade():
    d = request.json
    try:
        oid = str(uuid.uuid4())
        if d.get('action') == 'CLOSE':
            side = 'SELL' if d.get('side') == 'BUY' or d.get('type') == 'SPOT' else 'BUY'
            client.market_order_sell(client_order_id=oid, product_id=d['pair'], base_size=d['size'])
            return jsonify({"success": True, "message": "Position Closed"})

        side, pair, o_type, amt = d['side'], d['pair'], d['order_type'], str(d['amount'])
        if o_type == 'MARKET':
            if side == 'BUY': client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=amt)
            else: client.market_order_sell(client_order_id=oid, product_id=pair, base_size=amt)
        else:
            px = str(d['limit_price'])
            if side == 'BUY': client.limit_order_buy(client_order_id=oid, product_id=pair, base_size=amt, limit_price=px)
            else: client.limit_order_sell(client_order_id=oid, product_id=pair, base_size=amt, limit_price=px)
        return jsonify({"success": True, "message": "Order Submitted"})
    except Exception as e: return jsonify({"success": False, "error": str(e)})

@trading_bp.route('/api/tpsl', methods=['POST'])
def handle_tpsl():
    d = request.json
    try:
        pair, size, tp_price, sl_price = d['pair'], str(d['size']), d.get('tp_price'), d.get('sl_price')
        side = 'SELL' if d.get('side') == 'BUY' or d.get('type') == 'SPOT' else 'BUY'
        msgs = []
        
        if tp_price:
            tp_oid = str(uuid.uuid4())
            if side == 'SELL': client.limit_order_sell(client_order_id=tp_oid, product_id=pair, base_size=size, limit_price=str(tp_price))
            else: client.limit_order_buy(client_order_id=tp_oid, product_id=pair, base_size=size, limit_price=str(tp_price))
            msgs.append("TP Set")
            
        if sl_price:
            sl_oid = str(uuid.uuid4())
            direction = 'STOP_DIRECTION_STOP_DOWN' if side == 'SELL' else 'STOP_DIRECTION_STOP_UP'
            if side == 'SELL': client.stop_limit_order_sell(client_order_id=sl_oid, product_id=pair, base_size=size, limit_price=str(sl_price), stop_price=str(sl_price), stop_direction=direction)
            else: client.stop_limit_order_buy(client_order_id=sl_oid, product_id=pair, base_size=size, limit_price=str(sl_price), stop_price=str(sl_price), stop_direction=direction)
            msgs.append("SL Set")
            
        if not msgs: return jsonify({"success": False, "error": "No prices provided."})
        return jsonify({"success": True, "message": " & ".join(msgs) + " Successfully!"})
    except Exception as e: return jsonify({"success": False, "error": str(e)})

@trading_bp.route('/api/trail', methods=['POST'])
def set_trail():
    d = request.json
    try:
        TRAILING_STOPS[d['pair']] = {"side": "SELL", "trail_pct": float(d['pct'])/100, "highest_price": float(d['cur_px']), "size": d['size']}
        return jsonify(success=True, message="Trailing Stop Activated")
    except Exception as e: return jsonify(success=False, error=str(e))

@trading_bp.route('/api/indicators/<pair>')
def get_indicators(pair):
    try:
        end_ts = int(time.time())
        start_ts = end_ts - (150 * 3600) 
        try:
            res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": "ONE_HOUR"})
            candle_data = res.get('candles', [])
        except: return jsonify({"rsi": "API Err", "trend": "API Err", "macd": "API Err", "debug": "Fetch failed"})
            
        if not candle_data or len(candle_data) < 15: return jsonify({"rsi": "No Data", "trend": "No Data", "macd": "No Data", "debug": "Not enough candles."})
            
        parsed = [{'start': c.get('start'), 'close': float(c.get('close', 0))} for c in candle_data]
        df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)
        
        rsi_val = ta.rsi(df['close'], length=14).dropna().iloc[-1]
        sma_val = ta.sma(df['close'], length=50).dropna().iloc[-1]
        macd_df = ta.macd(df['close'])
        
        last_px = df['close'].iloc[-1]
        
        rsi_status = "Bullish" if rsi_val < 30 else "Bearish" if rsi_val > 70 else "Neutral"
        trend_status = "Bullish" if last_px > sma_val else "Bearish"
        macd_status = "Bullish" if macd_df is not None and macd_df.iloc[:, 0].dropna().iloc[-1] > macd_df.iloc[:, 2].dropna().iloc[-1] else "Bearish"
            
        return jsonify({"rsi": rsi_status, "trend": trend_status, "macd": macd_status, "debug": ""})
    except Exception as e: return jsonify({"rsi": "Err", "trend": "Err", "macd": "Err", "debug": str(e)})
