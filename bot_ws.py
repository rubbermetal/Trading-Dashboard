# bot_ws.py
import os
import json
import time
import uuid
import threading
from coinbase.websocket import WSClient

from shared import ACTIVE_BOTS, client
from bot_utils import (
    is_derivative, get_contract_multiplier, 
    record_trade, save_bots, snap_to_increment
)

from grid_engine import (
    place_grid_sell, place_grid_buy, 
    activate_trail, deactivate_trail_by_sell,
    cancel_order_safe, has_order_nearby, find_safe_price
)

from bot_executors import momentum_get_stop_price

_processed_fill_oids = set()

# FIX #4: Track subscribed pairs so we can resubscribe when new bots are added
_subscribed_pairs = set()
_ws_client_ref = None
_ws_lock = threading.Lock()

def _check_new_pairs():
    """Checks if any new bot pairs need WS subscription. Called periodically."""
    global _ws_client_ref, _subscribed_pairs
    if _ws_client_ref is None:
        return
    
    current_pairs = set(
        bot.get('pair') for bot in ACTIVE_BOTS.values()
        if bot.get('pair') and bot.get('strategy') in ('GRID', 'MOMENTUM', 'DCA') and bot.get('status') == 'RUNNING'
    )
    # Always include baseline pairs
    current_pairs.update(["BTC-USD", "ETH-USD"])
    
    new_pairs = current_pairs - _subscribed_pairs
    if new_pairs:
        try:
            new_list = list(new_pairs)
            with _ws_lock:
                _ws_client_ref.subscribe(product_ids=new_list, channels=["user"])
                _ws_client_ref.subscribe(product_ids=new_list, channels=["ticker"])
                _subscribed_pairs.update(new_pairs)
            print(f"[WS ENGINE] Subscribed to {len(new_list)} new pairs: {new_list}")
        except Exception as e:
            print(f"[WS ENGINE] Resubscribe error: {e}")

def process_price_tick(pair, cur_px):
    """Evaluates trailing stops asynchronously with network-level millisecond latency."""
    changes_made = False
    
    for bot_id, bot in list(ACTIVE_BOTS.items()):
        if bot.get('strategy') != 'GRID' or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair: continue
        
        risk = bot.get('settings', {}).get('risk', {})
        trails = risk.get('per_fill_trails', [])
        if not trails:
            continue

        active_grids = bot.get('settings', {}).get('active_grids', [])
        base_inc = bot['settings'].get('base_inc', '0.00000001')
        mult = get_contract_multiplier(pair)
        triggered = []

        # 1. Update High-Water Marks & Check Triggers Instantly
        for t in trails:
            if cur_px > t['high_water_mark']: 
                t['high_water_mark'] = cur_px
                changes_made = True 
                
            effective = t.get('effective_trail', t.get('base_trail_distance', 0)) 
            trigger_price = t['high_water_mark'] - effective
            
            if cur_px <= trigger_price:
                triggered.append(t)

        # 2. Execute Market Sells at Network Speed
        for t in triggered:
            qty = t['quantity']
            effective = t.get('effective_trail', t.get('base_trail_distance', 0))
            print(f"[WS RISK ENGINE | {pair}] TRAIL STOP HIT: HWM={t['high_water_mark']:.2f} trail={effective:.2f} exit@{cur_px:.2f}")
            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(qty, base_inc)
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

                record_trade(bot, t['fill_price'], cur_px, qty, 'LONG', 'TRAILING_STOP', pair, mult)
                bot['asset_held'] -= qty
                bot['current_usd'] += qty * cur_px * 0.995

                for g in list(active_grids):
                    if ((t.get('sell_oid') and g.get('oid') == t['sell_oid']) or
                        (t.get('sell_cb_oid') and g.get('cb_oid') == t['sell_cb_oid'])):
                        cancel_order_safe(g) 
                        active_grids.remove(g)
                        break
                        
            except Exception as e:
                print(f"[WS RISK ENGINE | {pair}] Sell failed: {e}")
                continue
                
            trails.remove(t)
            changes_made = True

        if triggered:
            risk['depth_score'] = len(trails)
            risk['risk_current'] = round(sum(t.get('effective_trail', t.get('base_trail_distance', 0)) * t['quantity'] for t in trails), 4)

    # ==========================================
    # MOMENTUM BOTS: 3-Phase Trailing Stop (WS Primary Path)
    # ==========================================
    for bot_id, bot in list(ACTIVE_BOTS.items()):
        if bot.get('strategy') != 'MOMENTUM' or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair: continue
        if bot.get('position_side') != 'LONG' or bot.get('asset_held', 0) <= 0: continue

        # Update high water mark
        hwm = bot.get('high_water_mark', cur_px)
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
            changes_made = True

        stop_px, phase = momentum_get_stop_price(bot, cur_px)
        bot['stop_phase'] = phase

        if stop_px > 0 and cur_px <= stop_px:
            exit_reason = 'STOP_LOSS' if phase == 1 else 'TRAILING_STOP'
            held = abs(bot['asset_held'])
            mult = get_contract_multiplier(pair)
            print(f"[WS MOMENTUM | {pair}] Phase {phase} STOP HIT: price {cur_px:.2f} <= stop {stop_px:.2f}")

            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(held, bot.get('settings', {}).get('base_inc', '0.00000001'))
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

                record_trade(bot, bot['entry_price'], cur_px, held, 'LONG', exit_reason, pair, mult)

                profit = (cur_px - bot['entry_price']) * held * mult
                bot['current_usd'] = bot['allocated_usd'] + (profit * 0.995)
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                for key in ['entry_atr', 'high_water_mark', 'stop_phase', 'fee_estimate',
                            'pending_order_oid', 'pending_order_time', 'signal_retries']:
                    bot.pop(key, None)
                changes_made = True
                print(f"[WS MOMENTUM | {pair}] EXIT ({exit_reason}): PnL ${profit:.2f}")
            except Exception as e:
                print(f"[WS MOMENTUM | {pair}] Sell failed: {e}")

    # ==========================================
    # DCA BOTS: Live profit % update (display only, no stop execution)
    # ==========================================
    for bot_id, bot in list(ACTIVE_BOTS.items()):
        if bot.get('strategy') != 'DCA' or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair: continue
        if bot.get('asset_held', 0) <= 0 or bot.get('avg_entry', 0) <= 0: continue

        avg_entry = bot['avg_entry']
        profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
        bot['live_profit_pct'] = round(profit_pct, 2)
        changes_made = True

    if changes_made:
        save_bots()

def process_grid_fill(order_id, filled_size, filled_value, status, pair):
    """Processes real-time WS fills, calculates increments, and flips the grid order."""
    if order_id in _processed_fill_oids:
        return
    
    changes_made = False
    for bot_id, bot in list(ACTIVE_BOTS.items()):
        if bot.get('strategy') != 'GRID' or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair: continue
        
        settings = bot.get('settings', {})
        active_grids = settings.get('active_grids', [])
        
        for i, grid in enumerate(active_grids):
            matched = (grid.get('oid') == order_id) or (grid.get('cb_oid') == order_id)
            if matched and status == 'FILLED':
                _processed_fill_oids.add(order_id)
                if grid.get('oid'): _processed_fill_oids.add(grid['oid'])
                if grid.get('cb_oid'): _processed_fill_oids.add(grid['cb_oid'])
                if len(_processed_fill_oids) > 500:
                    _processed_fill_oids.clear()
                changes_made = True
                
                base_inc = settings.get('base_inc', '0.00000001')
                quote_inc = settings.get('quote_inc', '0.01')
                step_size = settings.get('step_size')
                chunk_usd = settings.get('chunk_size', 0)
                mult = get_contract_multiplier(pair)
                deriv_flag = is_derivative(pair)
                is_halted = settings.get('halted', False)
                min_gap = step_size * 0.4 if step_size else 0
                
                print(f"[WS ENGINE] Fill detected: {grid['side']} at {grid['price']:.2f} (order {order_id[:8]}...)")
                
                if grid['side'] == 'BUY':
                    new_price = grid['price'] + step_size
                    # FIX #1: Properly handle None from find_safe_price
                    if min_gap > 0 and has_order_nearby(new_price, active_grids, min_gap):
                        safe_px = find_safe_price(new_price, active_grids, min_gap, direction='up')
                        if safe_px is None:
                            print(f"[WS ENGINE] No safe sell price near {new_price:.2f}. Holding with trail only.")
                            bot['asset_held'] += float(filled_size)
                            bot['current_usd'] -= float(filled_value)
                            risk = bot['settings'].setdefault('risk', {})
                            total_levels = risk.get('total_buy_levels', 10)
                            level_idx = grid.get('level_idx', total_levels // 2)
                            activate_trail(bot, grid['price'], float(filled_size), level_idx, total_levels, step_size)
                            # Remove filled buy from tracking (it's consumed)
                            try: active_grids.pop(i)
                            except: pass
                            break
                        print(f"[WS ENGINE] Sell nudged {new_price:.2f} -> {safe_px:.2f} (spacing guard)")
                        new_price = safe_px

                    new_grid = place_grid_sell(pair, new_price, filled_size, base_inc, quote_inc, deriv_flag, mult)
                    
                    if new_grid:
                        active_grids[i] = new_grid
                        bot['asset_held'] += float(filled_size)
                        bot['current_usd'] -= float(filled_value)
                    else:
                        print(f"[WS GRID FLIP] Sell placement failed at {new_price:.2f}")
                        bot['asset_held'] += float(filled_size)
                        bot['current_usd'] -= float(filled_value)

                    risk = bot['settings'].setdefault('risk', {})
                    total_levels = risk.get('total_buy_levels', 10)
                    level_idx = grid.get('level_idx', total_levels // 2)
                    activate_trail(bot, grid['price'], float(filled_size), level_idx, total_levels, step_size, sell_grid=new_grid if new_grid else None)
                    
                elif grid['side'] == 'SELL':
                    buy_price = grid['price'] - step_size
                    sell_price = grid['price']
                    record_trade(bot, buy_price, sell_price, filled_size, 'LONG', 'GRID_FLIP', pair, mult)
                    deactivate_trail_by_sell(bot, sell_oid=grid.get('oid'), sell_cb_oid=grid.get('cb_oid'))
                    
                    if is_halted:
                        try: active_grids.pop(i)
                        except: pass
                        bot['asset_held'] -= float(filled_size)
                        bot['current_usd'] += float(filled_value)
                        print(f"[WS ENGINE] SELL filled during halt. No new BUY.")
                    else:
                        new_price = grid['price'] - step_size
                        # FIX #1: Properly handle None from find_safe_price
                        if min_gap > 0 and has_order_nearby(new_price, active_grids, min_gap):
                            safe_px = find_safe_price(new_price, active_grids, min_gap, direction='down')
                            if safe_px is None:
                                print(f"[WS ENGINE] No safe buy price near {new_price:.2f}. Level queued for redeployment.")
                                bot['asset_held'] -= float(filled_size)
                                bot['current_usd'] += float(filled_value)
                                risk = bot['settings'].setdefault('risk', {})
                                risk.setdefault('cancelled_buy_levels', []).append(new_price)
                                try: active_grids.pop(i)
                                except: pass
                                break
                            print(f"[WS ENGINE] Buy nudged {new_price:.2f} -> {safe_px:.2f} (spacing guard)")
                            new_price = safe_px

                        new_grid = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
                        
                        if new_grid:
                            active_grids[i] = new_grid
                            bot['asset_held'] -= float(filled_size)
                            bot['current_usd'] += float(filled_value)
                        else:
                            print(f"[WS GRID FLIP] Buy placement failed at {new_price:.2f}")
                            bot['asset_held'] -= float(filled_size)
                            bot['current_usd'] += float(filled_value)
                    
    if changes_made:
        save_bots()

def _ws_daemon():
    """Background Daemon for listening to User Channel WS execution events."""
    global _ws_client_ref, _subscribed_pairs
    try:
        def on_message(msg):
            try:
                data = json.loads(msg) if isinstance(msg, str) else msg
                for event in data.get('events', []):
                    if event.get('type') == 'update':
                        for order in event.get('orders', []):
                            if order.get('status') == 'FILLED':
                                client_oid = order.get('client_order_id', '')
                                server_oid = order.get('order_id', '')
                                f_size = float(order.get('cumulative_quantity', 0))
                                f_val = float(order.get('total_value_after_fees', 0))
                                if f_val == 0: f_val = f_size * float(order.get('avg_price', 0))
                                pair = order.get('product_id')
                                if client_oid: process_grid_fill(client_oid, f_size, f_val, 'FILLED', pair)
                                if server_oid: process_grid_fill(server_oid, f_size, f_val, 'FILLED', pair)
                    elif event.get('type') == 'ticker':
                        for tick in event.get('tickers', []):
                            pair = tick.get('product_id')
                            price = float(tick.get('price', 0))
                            if price > 0:
                                process_price_tick(pair, price)
            except Exception as e:
                pass # Suppress noisy WS parsing errors

        api_key = os.getenv('COINBASE_API_KEY_NAME', '')
        api_secret = os.getenv('COINBASE_API_PRIVATE_KEY', '')
        
        if api_key and api_secret:
            active_pairs = list(set([bot.get('pair') for bot in ACTIVE_BOTS.values() if bot.get('pair')] + ["BTC-USD", "ETH-USD"]))
            ws_client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message)
            ws_client.open()
            ws_client.subscribe(product_ids=active_pairs, channels=["user"])
            ws_client.subscribe(product_ids=active_pairs, channels=["ticker"])
            
            # FIX #4: Store reference and track subscribed pairs for dynamic resubscription
            with _ws_lock:
                _ws_client_ref = ws_client
                _subscribed_pairs = set(active_pairs)
            
            print(f"[WS ENGINE] Subscribed to {len(active_pairs)} pairs: {active_pairs}")
            # open() runs WS in background thread; keep this daemon alive
            # Periodically check for new pairs that need subscription
            while True:
                time.sleep(30)
                _check_new_pairs()
        else:
            print("[WS ENGINE] Missing API Keys in Env. WS Tracking Disabled.")
    except Exception as e:
        print(f"[WS ENGINE] Init error: {e}")

def start_ws_engine():
    threading.Thread(target=_ws_daemon, daemon=True).start()
