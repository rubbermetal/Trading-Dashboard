import uuid, time, threading
from decimal import Decimal, ROUND_DOWN
import pandas as pd
import pandas_ta as ta
from flask import Blueprint, jsonify, request
from shared import client, TRAILING_STOPS, BRACKET_ORDERS, TWAP_ORDERS, SNIPER_ORDERS, MANUAL_POSITIONS
from bot_utils import snap_to_increment, get_bot_tf, TF_MAP, get_contract_multiplier, is_derivative, record_trade, save_bots, poll_market_fill, extract_fee
from validators import validate_trade, validate_bracket, validate_trail, rate_limit
from logger import get_logger

log = get_logger('trading')
from notifier import notify_bracket_hit, notify_sniper, notify_twap_complete

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
                            log.info(f"[{pair}] TRAIL SELL triggered @ {cur_px:.2f} (peak {data['highest_price']:.2f})")
                    else:
                        # Short trailing stop — tracks lowest price, triggers on rise
                        if cur_px < data.get('lowest_price', cur_px):
                            TRAILING_STOPS[pair]['lowest_price'] = cur_px
                        trigger_px = data.get('lowest_price', cur_px) * (1 + data['trail_pct'])
                        if cur_px >= trigger_px:
                            client.market_order_buy(client_order_id=str(uuid.uuid4()), product_id=pair, quote_size=str(float(data['size']) * cur_px))
                            del TRAILING_STOPS[pair]
                            log.info(f"[{pair}] TRAIL BUY triggered @ {cur_px:.2f} (trough {data.get('lowest_price', 0):.2f})")
                except Exception as e:
                    log.error(f"[{pair}] Trail check error: {e}")

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
                        log.info(f"TWAP {twap_id} completed: {tw['filled_slices']} slices filled")
                        notify_twap_complete(pair, tw['side'], tw['total_usd'], tw['filled_slices'])
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
                    log.info(f"[{pair}] TWAP slice {tw['filled_slices']}/{tw['slices']} @ ${str_price} ({str_qty})")
                except Exception as e:
                    log.error(f"TWAP {twap_id} error: {e}")

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
                        log.info(f"[{pair}] Sniper triggered @ ${cur_px:.2f} (target ${trigger:.2f}), {sn['side']} placed @ ${str_price}")
                        notify_sniper(pair, sn['side'], cur_px)
                except Exception as e:
                    log.error(f"Sniper {snip_id} error: {e}")

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
                        log.info(f"[{pair}] Bracket {hit} hit @ {cur_px:.2f} | entry {bkt['entry_price']:.2f} | PnL ${pnl:.2f}")
                        notify_bracket_hit(pair, hit, cur_px, pnl)
                        del BRACKET_ORDERS[pair]
                except Exception as e:
                    log.error(f"[{pair}] Bracket check error: {e}")

        except Exception as e:
            log.error(f"Watcher top-level error: {e}")
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
@rate_limit
def trade():
    d = request.json
    if not d:
        return jsonify(success=False, error="Request body required"), 400
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
@rate_limit
def set_bracket():
    """Attach a TP and/or SL to a position. Monitored software-side as OCO."""
    d = request.json
    valid, err = validate_bracket(d)
    if not valid:
        return jsonify(success=False, error=err), 400
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
@rate_limit
def set_trail():
    d = request.json
    valid, err = validate_trail(d)
    if not valid:
        return jsonify(success=False, error=err), 400
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
# NOTIFICATION CONFIG
# ==========================================
@trading_bp.route('/api/notify/config', methods=['GET'])
def get_notify_config():
    from notifier import get_config
    return jsonify(get_config())

@trading_bp.route('/api/notify/config', methods=['POST'])
def set_notify_config():
    from notifier import update_config
    d = request.json
    config = update_config(d)
    return jsonify(success=True, config=config)

@trading_bp.route('/api/notify/test', methods=['POST'])
def test_notify():
    from notifier import notify
    notify("Test Notification", "If you see this, notifications are working!", priority="default", tags=["white_check_mark"])
    return jsonify(success=True, message="Test notification sent")

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


# ==========================================
# STRATEGY-MANAGED MANUAL POSITIONS
# ==========================================
from strategies import (
    calculate_quad_rotation, calculate_orb,
    calculate_trap, calculate_momentum, calculate_npr
)
from bot_executors import momentum_get_stop_price, npr_get_stop_and_trail

SUPPORTED_EXIT_STRATEGIES = {
    'QUAD':       'ATR SL/TP + counter-trend + sequential bear rotation',
    'QUAD_SUPER': 'ATR SL/TP + counter-trend + sequential bear rotation',
    'ORB':        'Price crosses below midpoint',
    'TRAP':       'R-multiple TP (2.5R/4.0R) + 2x ATR / elephant bar stop',
    'MOMENTUM':   '3-phase trailing stop (ATR-based)',
    'NPR':        'Event stop + trailing stop',
    'VWAP_MR':    'VWAP touch or 1.5x ATR trail',
    'SQUEEZE':    'Momentum reversal or 2x ATR trail',
}

_manual_pos_file = 'manual_positions.json'
_manual_lock = threading.Lock()


def _save_manual_positions():
    import json
    with _manual_lock:
        with open(_manual_pos_file, 'w') as f:
            json.dump(MANUAL_POSITIONS, f)


def _load_manual_positions():
    import json, os
    if os.path.exists(_manual_pos_file):
        try:
            with open(_manual_pos_file, 'r') as f:
                MANUAL_POSITIONS.update(json.load(f))
        except Exception:
            pass


@trading_bp.route('/api/manual/strategies', methods=['GET'])
def get_exit_strategies():
    return jsonify(SUPPORTED_EXIT_STRATEGIES)


@trading_bp.route('/api/manual/enter', methods=['POST'])
@rate_limit
def manual_enter():
    """Enter a manual position with a strategy managing the exit."""
    d = request.json
    if not d:
        return jsonify(success=False, error="Request body required"), 400

    pair = d.get('pair', '').upper()
    amount = float(d.get('amount', 0))
    order_type = d.get('order_type', 'MARKET').upper()
    strategy = d.get('strategy', '').upper()

    if not pair or amount <= 0:
        return jsonify(success=False, error="pair and positive amount required"), 400
    if strategy not in SUPPORTED_EXIT_STRATEGIES:
        return jsonify(success=False, error=f"Invalid strategy. Use: {', '.join(SUPPORTED_EXIT_STRATEGIES)}"), 400

    try:
        oid = str(uuid.uuid4())
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
        mult = get_contract_multiplier(pair)

        if order_type == 'MARKET':
            client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(amount))
        elif order_type == 'MAKER':
            book = client.get_product_book(product_id=pair, limit=1)
            best_bid = float(book.pricebook.bids[0].price)
            str_price = snap_to_increment(best_bid, quote_inc)
            base_qty = amount / best_bid
            str_qty = snap_to_increment(base_qty, base_inc)
            client.limit_order_buy(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price)
        else:
            return jsonify(success=False, error="order_type must be MARKET or MAKER"), 400

        # Poll for fill
        fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=3, delay=1.0)
        entry_px = fill_px if fill_px else cur_px
        size = fill_sz if fill_sz else amount / cur_px

        pos_id = str(uuid.uuid4())[:8]

        # Build strategy-specific state
        bot_state = {}
        if strategy == 'MOMENTUM':
            # Fetch ATR for trailing stop
            end_ts = int(time.time())
            start_ts = end_ts - (300 * 300)
            try:
                res = client.get(f"/api/v3/brokerage/products/{pair}/candles",
                                 params={"start": str(start_ts), "end": str(end_ts), "granularity": "FIVE_MINUTE"})
                candles = res.get('candles', [])
                if len(candles) >= 50:
                    parsed = [{'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close'])} for c in candles]
                    cdf = pd.DataFrame(parsed)
                    atr_val = float(ta.atr(cdf['high'], cdf['low'], cdf['close'], 14).iloc[-1])
                    bot_state['entry_atr'] = atr_val
                    bot_state['fee_estimate'] = entry_px * size * mult * 0.005
            except Exception:
                bot_state['entry_atr'] = entry_px * 0.015  # fallback 1.5%
                bot_state['fee_estimate'] = entry_px * size * mult * 0.005
            bot_state['high_water_mark'] = entry_px
            bot_state['stop_phase'] = 1
        elif strategy == 'TRAP':
            bot_state['avg_entry'] = entry_px
            bot_state['entry_stage'] = 3  # treat as fully entered (Velez 3-stage)
            bot_state['tp_stage'] = 0
            # Fetch ATR for stop calculation
            try:
                end_ts = int(time.time())
                start_ts = end_ts - (300 * 300)
                res = client.get(f"/api/v3/brokerage/products/{pair}/candles",
                                 params={"start": str(start_ts), "end": str(end_ts), "granularity": "FIFTEEN_MINUTE"})
                candles = res.get('candles', [])
                if len(candles) >= 20:
                    parsed = [{'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close'])} for c in candles]
                    cdf = pd.DataFrame(parsed)
                    bot_state['breakout_atr'] = float(ta.atr(cdf['high'], cdf['low'], cdf['close'], 14).iloc[-1])
            except Exception:
                bot_state['breakout_atr'] = entry_px * 0.015
        elif strategy == 'NPR':
            bot_state['npr_state'] = 'IN_POSITION'
            bot_state['event_stop'] = entry_px * 0.97  # default 3% stop
            bot_state['high_water_mark'] = entry_px
            bot_state['trail_distance'] = entry_px * 0.02  # 2% trail

        MANUAL_POSITIONS[pos_id] = {
            'pair': pair,
            'side': 'LONG',
            'entry_price': entry_px,
            'size': size,
            'strategy': strategy,
            'status': 'ACTIVE',
            'base_inc': base_inc,
            'quote_inc': quote_inc,
            'bot_state': bot_state,
            'created_at': time.time(),
            'entry_oid': oid,
        }
        _save_manual_positions()
        log.info(f"[{pair}] Manual position opened: {strategy} exit, {size:.8f} @ ${entry_px:.2f}")
        return jsonify(success=True, pos_id=pos_id, entry_price=entry_px, size=size)

    except Exception as e:
        log.error(f"Manual enter failed: {e}")
        return jsonify(success=False, error=str(e))


@trading_bp.route('/api/manual/close/<pos_id>', methods=['POST'])
@rate_limit
def manual_close(pos_id):
    """Close a manual position immediately."""
    pos = MANUAL_POSITIONS.get(pos_id)
    if not pos or pos['status'] != 'ACTIVE':
        return jsonify(success=False, error="Position not found or already closed"), 404

    try:
        oid = str(uuid.uuid4())
        pair = pos['pair']
        str_qty = snap_to_increment(pos['size'], pos['base_inc'])
        client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

        fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
        exit_px = fill_px if fill_px else float(client.get_product(product_id=pair).price)
        mult = get_contract_multiplier(pair)
        pnl = (exit_px - pos['entry_price']) * pos['size'] * mult

        pos['status'] = 'CLOSED'
        pos['exit_price'] = exit_px
        pos['pnl'] = round(pnl, 4)
        pos['closed_at'] = time.time()
        pos['exit_reason'] = 'MANUAL_CLOSE'
        _save_manual_positions()
        log.info(f"[{pair}] Manual position closed: PnL ${pnl:.2f}")
        return jsonify(success=True, exit_price=exit_px, pnl=round(pnl, 4))
    except Exception as e:
        log.error(f"Manual close failed: {e}")
        return jsonify(success=False, error=str(e))


@trading_bp.route('/api/manual/strategy/<pos_id>', methods=['POST'])
def manual_switch_strategy(pos_id):
    """Switch the exit strategy on an active position."""
    pos = MANUAL_POSITIONS.get(pos_id)
    if not pos or pos['status'] != 'ACTIVE':
        return jsonify(success=False, error="Position not found or not active"), 404

    d = request.json or {}
    new_strat = d.get('strategy', '').upper()
    if new_strat not in SUPPORTED_EXIT_STRATEGIES:
        return jsonify(success=False, error=f"Invalid strategy. Use: {', '.join(SUPPORTED_EXIT_STRATEGIES)}"), 400

    old_strat = pos['strategy']
    pos['strategy'] = new_strat
    pos['bot_state'] = {}  # reset strategy state

    # Re-initialize for new strategy
    cur_px = pos['entry_price']  # approximate
    if new_strat == 'MOMENTUM':
        pos['bot_state']['entry_atr'] = cur_px * 0.015
        pos['bot_state']['fee_estimate'] = cur_px * pos['size'] * 0.005
        pos['bot_state']['high_water_mark'] = cur_px
        pos['bot_state']['stop_phase'] = 1
    elif new_strat == 'TRAP':
        pos['bot_state']['avg_entry'] = pos['entry_price']
        pos['bot_state']['entry_stage'] = 3
        pos['bot_state']['tp_stage'] = 0
        pos['bot_state']['breakout_atr'] = cur_px * 0.015
    elif new_strat == 'NPR':
        pos['bot_state']['npr_state'] = 'IN_POSITION'
        pos['bot_state']['event_stop'] = cur_px * 0.97
        pos['bot_state']['high_water_mark'] = cur_px
        pos['bot_state']['trail_distance'] = cur_px * 0.02

    _save_manual_positions()
    log.info(f"[{pos['pair']}] Strategy switched: {old_strat} -> {new_strat}")
    return jsonify(success=True, message=f"Exit strategy changed to {new_strat}")


@trading_bp.route('/api/manual/positions', methods=['GET'])
def get_manual_positions():
    """Return all manual positions with live PnL."""
    result = []
    for pos_id, pos in MANUAL_POSITIONS.items():
        entry = dict(pos)
        entry['pos_id'] = pos_id
        # Live PnL for active positions
        if pos['status'] == 'ACTIVE':
            try:
                cur_px = float(client.get_product(product_id=pos['pair']).price)
                mult = get_contract_multiplier(pos['pair'])
                entry['current_price'] = cur_px
                entry['unrealized_pnl'] = round((cur_px - pos['entry_price']) * pos['size'] * mult, 4)
                entry['pnl_pct'] = round(((cur_px - pos['entry_price']) / pos['entry_price']) * 100, 2)
                # Strategy status
                bs = pos.get('bot_state', {})
                if pos['strategy'] == 'MOMENTUM':
                    phase = bs.get('stop_phase', 1)
                    hwm = bs.get('high_water_mark', pos['entry_price'])
                    entry['strategy_status'] = f"Phase {phase}, HWM ${hwm:.0f}"
                elif pos['strategy'] == 'TRAP':
                    tp_stg = bs.get('tp_stage', 0)
                    stage = bs.get('entry_stage', 0)
                    tp_label = "T1 hit, trailing" if tp_stg >= 1 else "T1: 2.5R"
                    entry['strategy_status'] = f"Stage {stage}/3, {tp_label}"
                elif pos['strategy'] == 'NPR':
                    stop = bs.get('event_stop', 0)
                    entry['strategy_status'] = f"Stop ${stop:.0f}"
                else:
                    entry['strategy_status'] = 'Monitoring'
            except Exception:
                entry['current_price'] = 0
                entry['unrealized_pnl'] = 0
                entry['strategy_status'] = 'Price fetch error'
        # Remove bot_state from response (internal)
        entry.pop('bot_state', None)
        result.append(entry)
    return jsonify(result)


def _manual_position_evaluator():
    """Background thread: evaluates exit conditions for all active manual positions every 15s."""
    _load_manual_positions()
    while True:
        time.sleep(15)
        for pos_id, pos in list(MANUAL_POSITIONS.items()):
            if pos.get('status') != 'ACTIVE':
                continue
            pair = pos['pair']
            strategy = pos['strategy']
            bs = pos.get('bot_state', {})
            try:
                p_info = client.get_product(product_id=pair)
                cur_px = float(p_info.price)
                mult = get_contract_multiplier(pair)
            except Exception:
                continue

            should_exit = False
            exit_reason = ''

            try:
                if strategy in ('QUAD', 'QUAD_SUPER'):
                    # Fetch candles and check exit signal
                    end_ts = int(time.time())
                    start_ts = end_ts - (300 * 900)
                    res = client.get(f"/api/v3/brokerage/products/{pair}/candles",
                                     params={"start": str(start_ts), "end": str(end_ts), "granularity": "FIFTEEN_MINUTE"})
                    candles = res.get('candles', [])
                    if len(candles) >= 200:
                        parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
                                   'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
                                  for c in candles]
                        df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)
                        signal, reason, _meta = calculate_quad_rotation(df)
                        if signal == 'SELL':
                            should_exit = True
                            exit_reason = f'STRATEGY_EXIT ({reason})'

                elif strategy == 'ORB':
                    end_ts = int(time.time())
                    start_ts = end_ts - (300 * 300)
                    res = client.get(f"/api/v3/brokerage/products/{pair}/candles",
                                     params={"start": str(start_ts), "end": str(end_ts), "granularity": "FIVE_MINUTE"})
                    candles = res.get('candles', [])
                    if len(candles) >= 50:
                        parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
                                   'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
                                  for c in candles]
                        df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)
                        orb_data = bs.get('orb_data', None)
                        tp_stg = bs.get('tp_stage', 0)
                        signal, reason, _ = calculate_orb(df, pos_side='LONG',
                                                          entry_price=pos['entry_price'],
                                                          orb_data=orb_data, tp_stage=tp_stg)
                        if signal in ('EXIT_LONG', 'PARTIAL_EXIT_LONG'):
                            should_exit = True
                            exit_reason = f'STRATEGY_EXIT ({reason})'

                elif strategy == 'TRAP':
                    avg_entry = bs.get('avg_entry', pos['entry_price'])
                    atr = bs.get('breakout_atr', pos['entry_price'] * 0.015)
                    tp_stg = bs.get('tp_stage', 0)
                    # Velez: stop = 2x ATR (simplified for manual — no elephant bar data)
                    sl_price = avg_entry - (2.0 * atr)
                    if tp_stg >= 1:
                        sl_price = max(sl_price, avg_entry)  # breakeven after T1
                    R = avg_entry - sl_price if sl_price < avg_entry else atr
                    r_mult = (cur_px - avg_entry) / R if R > 0 else 0
                    sl_hit = cur_px <= sl_price
                    if tp_stg == 0 and r_mult >= 2.5:
                        should_exit = True
                        exit_reason = f'TARGET_1 (+{r_mult:.1f}R)'
                    elif tp_stg >= 1 and r_mult >= 4.0:
                        should_exit = True
                        exit_reason = f'TARGET_2 (+{r_mult:.1f}R)'
                    elif sl_hit:
                        should_exit = True
                        exit_reason = 'STOP_LOSS (2x ATR)'

                elif strategy == 'MOMENTUM':
                    hwm = bs.get('high_water_mark', pos['entry_price'])
                    if cur_px > hwm:
                        bs['high_water_mark'] = cur_px
                    # Build a mini-bot dict for momentum_get_stop_price
                    mock_bot = {
                        'entry_price': pos['entry_price'],
                        'high_water_mark': bs.get('high_water_mark', pos['entry_price']),
                        'entry_atr': bs.get('entry_atr', pos['entry_price'] * 0.015),
                        'fee_estimate': bs.get('fee_estimate', 0),
                        'allocated_usd': pos['entry_price'] * pos['size'] * mult,
                    }
                    stop_px, phase = momentum_get_stop_price(mock_bot, cur_px)
                    bs['stop_phase'] = phase
                    if stop_px > 0 and cur_px <= stop_px:
                        should_exit = True
                        exit_reason = f'STOP_LOSS (Phase {phase})' if phase == 1 else f'TRAILING_STOP (Phase {phase})'

                elif strategy == 'NPR':
                    event_stop = bs.get('event_stop', 0)
                    hwm = bs.get('high_water_mark', pos['entry_price'])
                    trail = bs.get('trail_distance', pos['entry_price'] * 0.02)
                    if cur_px > hwm:
                        bs['high_water_mark'] = cur_px
                    trail_stop = bs['high_water_mark'] - trail
                    if event_stop > 0 and cur_px <= event_stop:
                        should_exit = True
                        exit_reason = 'EVENT_STOP'
                    elif cur_px <= trail_stop:
                        should_exit = True
                        exit_reason = 'TRAILING_STOP'

            except Exception as e:
                log.debug(f"[{pair}] Manual eval error ({strategy}): {e}")
                continue

            if should_exit:
                try:
                    oid = str(uuid.uuid4())
                    str_qty = snap_to_increment(pos['size'], pos['base_inc'])
                    client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                    fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
                    exit_px = fill_px if fill_px else cur_px
                    pnl = (exit_px - pos['entry_price']) * pos['size'] * mult
                    pos['status'] = 'CLOSED'
                    pos['exit_price'] = exit_px
                    pos['pnl'] = round(pnl, 4)
                    pos['closed_at'] = time.time()
                    pos['exit_reason'] = exit_reason
                    _save_manual_positions()
                    log.info(f"[{pair}] Manual {strategy} EXIT ({exit_reason}): PnL ${pnl:.2f}")
                except Exception as e:
                    log.error(f"[{pair}] Manual exit sell failed: {e}")


def start_manual_evaluator():
    threading.Thread(target=_manual_position_evaluator, daemon=True).start()
