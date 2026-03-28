import uuid, time, threading
from decimal import Decimal, ROUND_DOWN
import pandas as pd
import pandas_ta as ta
from flask import Blueprint, jsonify, request
from shared import client, TRAILING_STOPS, BRACKET_ORDERS, TWAP_ORDERS, SNIPER_ORDERS
from bot_utils import snap_to_increment

trading_bp = Blueprint('trading', __name__)

# ==========================================
# BACKGROUND WATCHERS
# ==========================================
def background_watcher():
    """Monitors trailing stops and bracket orders every 10 seconds."""
    while True:
        try:
            # --- Trailing Stops ---
            for pair, data in list(TRAILING_STOPS.items()):
                try:
                    p = client.get_product(product_id=pair)
                    cur_px = float(p.price)
                    side = data.get('side', 'SELL')

                    if side == 'SELL':
                        # Long trailing stop — tracks highest price, triggers on drop
                        if cur_px > data['highest_price']:
                            TRAILING_STOPS[pair]['highest_price'] = cur_px
                        trigger_px = data['highest_price'] * (1 - data['trail_pct'])
                        if cur_px <= trigger_px:
                            client.market_order_sell(client_order_id=str(uuid.uuid4()), product_id=pair, base_size=data['size'])
                            del TRAILING_STOPS[pair]
                            print(f"[TRAIL] {pair} SELL triggered @ {cur_px:.2f} (peak {data['highest_price']:.2f})")
                    else:
                        # Short trailing stop — tracks lowest price, triggers on rise
                        if cur_px < data.get('lowest_price', cur_px):
                            TRAILING_STOPS[pair]['lowest_price'] = cur_px
                        trigger_px = data.get('lowest_price', cur_px) * (1 + data['trail_pct'])
                        if cur_px >= trigger_px:
                            client.market_order_buy(client_order_id=str(uuid.uuid4()), product_id=pair, quote_size=str(float(data['size']) * cur_px))
                            del TRAILING_STOPS[pair]
                            print(f"[TRAIL] {pair} BUY triggered @ {cur_px:.2f} (trough {data.get('lowest_price', 0):.2f})")
                except Exception as e:
                    print(f"[TRAIL] Error checking {pair}: {e}")

            # --- TWAP Orders ---
            for twap_id, tw in list(TWAP_ORDERS.items()):
                try:
                    if tw['status'] != 'RUNNING':
                        continue
                    now = time.time()
                    if now < tw.get('next_slice_at', 0):
                        continue

                    pair = tw['pair']
                    product = client.get_product(product_id=pair)
                    cur_px = float(product.price)
                    quote_inc = product.quote_increment
                    base_inc = product.base_increment
                    slice_usd = tw['total_usd'] / tw['slices']
                    remaining_slices = tw['slices'] - tw['filled_slices']

                    if remaining_slices <= 0:
                        tw['status'] = 'COMPLETED'
                        print(f"[TWAP] {twap_id} completed: {tw['filled_slices']} slices filled")
                        continue

                    # Place maker limit at best bid/ask
                    book = client.get_product_book(product_id=pair, limit=1)
                    oid = str(uuid.uuid4())

                    if tw['side'] == 'BUY':
                        limit_px = float(book.pricebook.bids[0].price)
                        str_price = snap_to_increment(limit_px, quote_inc)
                        base_qty = slice_usd / limit_px
                        str_qty = snap_to_increment(base_qty, base_inc)
                        if float(str_qty) > 0:
                            client.limit_order_gtc_buy(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True)
                    else:
                        limit_px = float(book.pricebook.asks[0].price)
                        str_price = snap_to_increment(limit_px, quote_inc)
                        str_qty = snap_to_increment(slice_usd / limit_px, base_inc)
                        if float(str_qty) > 0:
                            client.limit_order_gtc_sell(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True)

                    tw['filled_slices'] = tw.get('filled_slices', 0) + 1
                    tw['next_slice_at'] = now + tw['interval_sec']
                    tw['last_price'] = cur_px
                    print(f"[TWAP] {pair} slice {tw['filled_slices']}/{tw['slices']} @ ${str_price} ({str_qty})")
                except Exception as e:
                    print(f"[TWAP] Error on {twap_id}: {e}")

            # --- Sniper Orders ---
            for snip_id, sn in list(SNIPER_ORDERS.items()):
                try:
                    if sn['status'] != 'WATCHING':
                        continue
                    pair = sn['pair']
                    product = client.get_product(product_id=pair)
                    cur_px = float(product.price)
                    quote_inc = product.quote_increment
                    base_inc = product.base_increment
                    trigger = sn['trigger_price']

                    triggered = False
                    if sn['direction'] == 'BELOW' and cur_px <= trigger:
                        triggered = True
                    elif sn['direction'] == 'ABOVE' and cur_px >= trigger:
                        triggered = True

                    if triggered:
                        book = client.get_product_book(product_id=pair, limit=1)
                        oid = str(uuid.uuid4())
                        if sn['side'] == 'BUY':
                            limit_px = float(book.pricebook.bids[0].price)
                            str_price = snap_to_increment(limit_px, quote_inc)
                            base_qty = float(sn['amount']) / limit_px
                            str_qty = snap_to_increment(base_qty, base_inc)
                            client.limit_order_gtc_buy(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True)
                        else:
                            limit_px = float(book.pricebook.asks[0].price)
                            str_price = snap_to_increment(limit_px, quote_inc)
                            str_qty = snap_to_increment(float(sn['amount']), base_inc)
                            client.limit_order_gtc_sell(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True)

                        sn['status'] = 'TRIGGERED'
                        sn['triggered_at'] = cur_px
                        print(f"[SNIPER] {pair} triggered @ ${cur_px:.2f} (target ${trigger:.2f}), {sn['side']} placed @ ${str_price}")
                except Exception as e:
                    print(f"[SNIPER] Error on {snip_id}: {e}")

            # --- Bracket Orders (software OCO) ---
            for pair, bkt in list(BRACKET_ORDERS.items()):
                try:
                    p = client.get_product(product_id=pair)
                    cur_px = float(p.price)
                    side = bkt.get('side', 'BUY')  # side of the original entry
                    hit = None

                    if side == 'BUY':
                        # Long bracket: TP above, SL below
                        if bkt.get('tp_price') and cur_px >= bkt['tp_price']:
                            hit = 'TP'
                        elif bkt.get('sl_price') and cur_px <= bkt['sl_price']:
                            hit = 'SL'
                    else:
                        # Short bracket: TP below, SL above
                        if bkt.get('tp_price') and cur_px <= bkt['tp_price']:
                            hit = 'TP'
                        elif bkt.get('sl_price') and cur_px >= bkt['sl_price']:
                            hit = 'SL'

                    if hit:
                        exit_side = 'SELL' if side == 'BUY' else 'BUY'
                        oid = str(uuid.uuid4())
                        if exit_side == 'SELL':
                            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=bkt['size'])
                        else:
                            client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(float(bkt['size']) * cur_px))
                        pnl = (cur_px - bkt['entry_price']) * float(bkt['size']) if side == 'BUY' else (bkt['entry_price'] - cur_px) * float(bkt['size'])
                        print(f"[BRACKET] {pair} {hit} hit @ {cur_px:.2f} | entry {bkt['entry_price']:.2f} | PnL ${pnl:.2f}")
                        del BRACKET_ORDERS[pair]
                except Exception as e:
                    print(f"[BRACKET] Error checking {pair}: {e}")

        except Exception as e:
            print(f"[WATCHER] Top-level error: {e}")
        time.sleep(10)

threading.Thread(target=background_watcher, daemon=True).start()

# ==========================================
# SEARCH
# ==========================================
@trading_bp.route('/api/search/<symbol>')
def search(symbol):
    s = symbol.upper()
    for v in [f"{s}-USD", f"{s}-PERP", f"{s}-USDC", s]:
        try:
            p = client.get_product(product_id=v)
            return jsonify({"success": True, "symbol": p.product_id, "price": float(p.price), "type": p.product_type})
        except: continue
    return jsonify({"success": False, "error": "Not Found"})

# ==========================================
# TRADE EXECUTION
# ==========================================
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
        elif o_type == 'MAKER_LIMIT':
            product = client.get_product(product_id=pair)
            quote_inc = product.quote_increment
            base_inc = product.base_increment
            book = client.get_product_book(product_id=pair, limit=1)
            best_bid = float(book.pricebook.bids[0].price)
            best_ask = float(book.pricebook.asks[0].price)

            if side == 'BUY':
                limit_px = best_bid
                str_price = snap_to_increment(limit_px, quote_inc)
                base_qty = float(amt) / limit_px
                str_qty = snap_to_increment(base_qty, base_inc)
                client.limit_order_buy(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price)
            else:
                limit_px = best_ask
                str_price = snap_to_increment(limit_px, quote_inc)
                str_qty = snap_to_increment(float(amt), base_inc)
                client.limit_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price)
            return jsonify({"success": True, "message": f"Maker limit {side} @ {str_price}"})
        else:
            px = str(d['limit_price'])
            if side == 'BUY': client.limit_order_buy(client_order_id=oid, product_id=pair, base_size=amt, limit_price=px)
            else: client.limit_order_sell(client_order_id=oid, product_id=pair, base_size=amt, limit_price=px)
        return jsonify({"success": True, "message": "Order Submitted"})
    except Exception as e: return jsonify({"success": False, "error": str(e)})

# ==========================================
# TP / SL
# ==========================================
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

# ==========================================
# BRACKET ORDER (software OCO: TP + SL on entry)
# ==========================================
@trading_bp.route('/api/bracket', methods=['POST'])
def set_bracket():
    """Attach a TP and/or SL to a position. Monitored software-side as OCO."""
    d = request.json
    try:
        pair = d['pair']
        size = str(d['size'])
        side = d.get('side', 'BUY')
        entry_price = float(d['entry_price'])
        tp_price = float(d['tp_price']) if d.get('tp_price') else None
        sl_price = float(d['sl_price']) if d.get('sl_price') else None

        if not tp_price and not sl_price:
            return jsonify(success=False, error="Set at least TP or SL.")

        BRACKET_ORDERS[pair] = {
            'size': size,
            'side': side,
            'entry_price': entry_price,
            'tp_price': tp_price,
            'sl_price': sl_price,
            'created': time.time()
        }

        parts = []
        if tp_price: parts.append(f"TP @ ${tp_price:,.2f}")
        if sl_price: parts.append(f"SL @ ${sl_price:,.2f}")
        return jsonify(success=True, message=f"Bracket set: {' / '.join(parts)}")
    except Exception as e:
        return jsonify(success=False, error=str(e))

# ==========================================
# TRAILING STOP
# ==========================================
@trading_bp.route('/api/trail', methods=['POST'])
def set_trail():
    d = request.json
    try:
        pair = d['pair']
        side = d.get('side', 'SELL')
        cur_px = float(d['cur_px'])
        entry = {
            'side': side,
            'trail_pct': float(d['pct']) / 100,
            'size': d['size'],
            'entry_price': cur_px
        }
        if side == 'SELL':
            entry['highest_price'] = cur_px
        else:
            entry['lowest_price'] = cur_px

        TRAILING_STOPS[pair] = entry
        return jsonify(success=True, message=f"Trailing stop ({d['pct']}%) activated for {pair}")
    except Exception as e:
        return jsonify(success=False, error=str(e))

# ==========================================
# ACTIVE PROTECTIONS STATUS & CANCEL
# ==========================================
@trading_bp.route('/api/protections')
def get_protections():
    """Return all active trailing stops and bracket orders for the UI."""
    items = []

    for pair, data in TRAILING_STOPS.items():
        side = data.get('side', 'SELL')
        if side == 'SELL':
            peak = data.get('highest_price', 0)
            trigger = peak * (1 - data['trail_pct'])
        else:
            trough = data.get('lowest_price', 0)
            trigger = trough * (1 + data['trail_pct'])
        items.append({
            'type': 'TRAIL',
            'pair': pair,
            'side': side,
            'size': data['size'],
            'pct': round(data['trail_pct'] * 100, 2),
            'peak': data.get('highest_price', data.get('lowest_price', 0)),
            'trigger': round(trigger, 2),
            'entry_price': data.get('entry_price', 0)
        })

    for pair, data in BRACKET_ORDERS.items():
        items.append({
            'type': 'BRACKET',
            'pair': pair,
            'side': data['side'],
            'size': data['size'],
            'entry_price': data['entry_price'],
            'tp_price': data.get('tp_price'),
            'sl_price': data.get('sl_price')
        })

    return jsonify(items)

@trading_bp.route('/api/protections/cancel', methods=['POST'])
def cancel_protection():
    d = request.json
    pair = d.get('pair', '')
    ptype = d.get('type', '')

    if ptype == 'TRAIL' and pair in TRAILING_STOPS:
        del TRAILING_STOPS[pair]
        return jsonify(success=True, message=f"Trailing stop cancelled for {pair}")
    elif ptype == 'BRACKET' and pair in BRACKET_ORDERS:
        del BRACKET_ORDERS[pair]
        return jsonify(success=True, message=f"Bracket order cancelled for {pair}")
    return jsonify(success=False, error="Protection not found.")

@trading_bp.route('/api/protections/breakeven', methods=['POST'])
def move_to_breakeven():
    """Move a bracket order's SL to entry price (break-even)."""
    d = request.json
    pair = d.get('pair', '')
    if pair in BRACKET_ORDERS:
        BRACKET_ORDERS[pair]['sl_price'] = BRACKET_ORDERS[pair]['entry_price']
        return jsonify(success=True, message=f"SL moved to break-even @ ${BRACKET_ORDERS[pair]['entry_price']:,.2f}")
    return jsonify(success=False, error="No bracket order found for this pair.")

# ==========================================
# TWAP ORDERS
# ==========================================
@trading_bp.route('/api/twap', methods=['POST'])
def create_twap():
    """Create a TWAP order: split total_usd into N slices over duration minutes."""
    d = request.json
    try:
        pair = d['pair']
        side = d.get('side', 'BUY')
        total_usd = float(d['total_usd'])
        slices = int(d.get('slices', 5))
        duration_min = float(d.get('duration_min', 30))

        if slices < 2: return jsonify(success=False, error="Need at least 2 slices.")
        if total_usd <= 0: return jsonify(success=False, error="Amount must be positive.")

        interval_sec = (duration_min * 60) / slices
        twap_id = str(uuid.uuid4())[:8]

        TWAP_ORDERS[twap_id] = {
            'pair': pair,
            'side': side,
            'total_usd': total_usd,
            'slices': slices,
            'interval_sec': interval_sec,
            'filled_slices': 0,
            'status': 'RUNNING',
            'next_slice_at': time.time(),  # first slice immediately
            'created': time.time(),
            'last_price': 0,
        }

        return jsonify(success=True, message=f"TWAP started: {slices} slices of ${total_usd/slices:.2f} over {duration_min:.0f}min", id=twap_id)
    except Exception as e:
        return jsonify(success=False, error=str(e))

@trading_bp.route('/api/twap/cancel', methods=['POST'])
def cancel_twap():
    d = request.json
    twap_id = d.get('id', '')
    if twap_id in TWAP_ORDERS:
        TWAP_ORDERS[twap_id]['status'] = 'CANCELLED'
        return jsonify(success=True, message=f"TWAP {twap_id} cancelled ({TWAP_ORDERS[twap_id]['filled_slices']} slices already filled)")
    return jsonify(success=False, error="TWAP order not found.")

# ==========================================
# SNIPER ENTRY
# ==========================================
@trading_bp.route('/api/sniper', methods=['POST'])
def create_sniper():
    """Watch for a price level and auto-fire a maker limit when hit."""
    d = request.json
    try:
        pair = d['pair']
        side = d.get('side', 'BUY')
        trigger_price = float(d['trigger_price'])
        amount = d['amount']  # USD for buys, units for sells
        direction = d.get('direction', 'BELOW')  # BELOW = buy the dip, ABOVE = sell the rip

        snip_id = str(uuid.uuid4())[:8]
        SNIPER_ORDERS[snip_id] = {
            'pair': pair,
            'side': side,
            'trigger_price': trigger_price,
            'amount': amount,
            'direction': direction,
            'status': 'WATCHING',
            'created': time.time(),
        }

        return jsonify(success=True, message=f"Sniper set: {side} {pair} when price {'<=' if direction == 'BELOW' else '>='} ${trigger_price:,.2f}", id=snip_id)
    except Exception as e:
        return jsonify(success=False, error=str(e))

@trading_bp.route('/api/sniper/cancel', methods=['POST'])
def cancel_sniper():
    d = request.json
    snip_id = d.get('id', '')
    if snip_id in SNIPER_ORDERS:
        SNIPER_ORDERS[snip_id]['status'] = 'CANCELLED'
        return jsonify(success=True, message=f"Sniper {snip_id} cancelled")
    return jsonify(success=False, error="Sniper order not found.")

# ==========================================
# SCALED LIMIT ORDERS
# ==========================================
@trading_bp.route('/api/scaled', methods=['POST'])
def create_scaled():
    """Place a ladder of limit orders across a price range."""
    d = request.json
    try:
        pair = d['pair']
        side = d.get('side', 'BUY')
        price_from = float(d['price_from'])
        price_to = float(d['price_to'])
        num_orders = int(d.get('num_orders', 5))
        total_usd = float(d['total_usd'])

        if num_orders < 2: return jsonify(success=False, error="Need at least 2 orders.")
        if price_from == price_to: return jsonify(success=False, error="Price range must span a range.")

        product = client.get_product(product_id=pair)
        quote_inc = product.quote_increment
        base_inc = product.base_increment
        cur_px = float(product.price)

        step = (price_to - price_from) / (num_orders - 1)
        slice_usd = total_usd / num_orders
        placed = 0
        errors = []

        for i in range(num_orders):
            px = price_from + step * i
            str_price = snap_to_increment(px, quote_inc)
            base_qty = slice_usd / px if px > 0 else 0
            str_qty = snap_to_increment(base_qty, base_inc)

            if float(str_qty) <= 0:
                continue

            try:
                oid = str(uuid.uuid4())
                if side == 'BUY':
                    client.limit_order_gtc_buy(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price)
                else:
                    client.limit_order_gtc_sell(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price)
                placed += 1
            except Exception as e:
                errors.append(f"${str_price}: {e}")

        msg = f"Placed {placed}/{num_orders} orders from ${price_from:,.2f} to ${price_to:,.2f}"
        if errors:
            msg += f" ({len(errors)} failed)"
        return jsonify(success=True, message=msg, placed=placed, errors=errors)
    except Exception as e:
        return jsonify(success=False, error=str(e))

# ==========================================
# ADVANCED ORDERS STATUS
# ==========================================
@trading_bp.route('/api/advanced_orders')
def get_advanced_orders():
    """Return all active TWAP and sniper orders."""
    items = []
    for tid, tw in TWAP_ORDERS.items():
        if tw['status'] in ('RUNNING',):
            items.append({
                'type': 'TWAP', 'id': tid, 'pair': tw['pair'], 'side': tw['side'],
                'progress': f"{tw['filled_slices']}/{tw['slices']}",
                'total_usd': tw['total_usd'], 'status': tw['status'],
            })
    for sid, sn in SNIPER_ORDERS.items():
        if sn['status'] == 'WATCHING':
            items.append({
                'type': 'SNIPER', 'id': sid, 'pair': sn['pair'], 'side': sn['side'],
                'trigger': sn['trigger_price'], 'direction': sn['direction'],
                'amount': sn['amount'], 'status': sn['status'],
            })
    return jsonify(items)

# ==========================================
# INDICATORS
# ==========================================
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
