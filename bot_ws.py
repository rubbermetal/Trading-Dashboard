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
    record_trade, save_bots, snap_to_increment,
    extract_fee, poll_market_fill
)

from grid_engine import (
    place_grid_sell, place_grid_buy,
    activate_trail, deactivate_trail_by_sell,
    cancel_order_safe, has_order_nearby, find_safe_price
)

from bot_executors import momentum_get_stop_price, npr_get_stop_and_trail
from logger import get_logger

log = get_logger('ws_engine')

_processed_fill_oids = set()

# FIX #4: Track subscribed pairs so we can resubscribe when new bots are added
_subscribed_pairs = set()
_ws_client_ref = None
_ws_lock = threading.Lock()
_ws_connected = False
_last_message_ts = 0.0

def ws_is_connected():
    return _ws_connected

def _check_new_pairs():
    """Checks if any new bot pairs need WS subscription. Called periodically."""
    global _ws_client_ref, _subscribed_pairs
    if _ws_client_ref is None:
        return

    current_pairs = set(
        bot.get('pair') for bot in ACTIVE_BOTS.values()
        if bot.get('pair') and bot.get('strategy') in ('GRID', 'MOMENTUM', 'DCA', 'NPR') and bot.get('status') == 'RUNNING'
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
            log.info(f"Subscribed to {len(new_list)} new pairs: {new_list}")
        except Exception as e:
            log.error(f"Resubscribe error: {e}")

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
            log.warning(f"[{pair}] TRAIL STOP HIT: HWM={t['high_water_mark']:.2f} trail={effective:.2f} exit@{cur_px:.2f}")
            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(qty, base_inc)
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * qty * mult * 0.0025)

                record_trade(bot, t['fill_price'], actual_exit, qty, 'LONG', 'TRAILING_STOP', pair, mult, actual_fee=actual_fee)
                bot['asset_held'] -= qty
                bot['current_usd'] += (qty * actual_exit * mult) - actual_fee

                for g in list(active_grids):
                    if ((t.get('sell_oid') and g.get('oid') == t['sell_oid']) or
                        (t.get('sell_cb_oid') and g.get('cb_oid') == t['sell_cb_oid'])):
                        cancel_order_safe(g)
                        active_grids.remove(g)
                        break

            except Exception as e:
                log.error(f"[{pair}] Trail sell failed: {e}")
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
            log.warning(f"[{pair}] MOMENTUM Phase {phase} STOP HIT: price {cur_px:.2f} <= stop {stop_px:.2f}")

            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(held, bot.get('base_inc', bot.get('settings', {}).get('base_inc', '0.00000001')))
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

                record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

                profit = (actual_exit - bot['entry_price']) * held * mult
                bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                for key in ['entry_atr', 'high_water_mark', 'stop_phase', 'fee_estimate',
                            'pending_order_oid', 'pending_order_time', 'signal_retries']:
                    bot.pop(key, None)
                changes_made = True
                log.info(f"[{pair}] MOMENTUM EXIT ({exit_reason}): PnL ${profit:.2f}")
            except Exception as e:
                log.error(f"[{pair}] MOMENTUM sell failed: {e}")

    # ==========================================
    # VWAP_MR / SQUEEZE BOTS: ATR trailing stop (WS path)
    # ==========================================
    for bot_id, bot in list(ACTIVE_BOTS.items()):
        strat = bot.get('strategy', '')
        if strat not in ('VWAP_MR', 'SQUEEZE') or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair or bot.get('position_side') != 'LONG': continue
        held = bot.get('asset_held', 0)
        if held <= 0: continue

        hwm = bot.get('high_water_mark', cur_px)
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
            changes_made = True
        entry_atr = bot.get('entry_atr', cur_px * 0.015)
        trail_mult = 1.5 if strat == 'VWAP_MR' else 2.0
        stop_px = bot['high_water_mark'] - (trail_mult * entry_atr)

        if cur_px <= stop_px:
            mult = get_contract_multiplier(pair)
            log.warning(f"[{pair}] {strat} TRAIL STOP: price {cur_px:.2f} <= stop {stop_px:.2f}")
            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(held, bot.get('base_inc', bot.get('settings', {}).get('base_inc', '0.00000001')))
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)
                record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', 'TRAILING_STOP', pair, mult, actual_fee=actual_fee)
                profit = (actual_exit - bot['entry_price']) * held * mult
                bot['current_usd'] += (held * actual_exit * mult) - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                for k in ['entry_price', 'high_water_mark', 'entry_atr']:
                    bot.pop(k, None)
                changes_made = True
                log.info(f"[{pair}] {strat} EXIT (TRAILING_STOP): PnL ${profit:.2f}")
            except Exception as e:
                log.error(f"[{pair}] {strat} trail sell failed: {e}")

    # ==========================================
    # NPR BOTS: Event stop + trailing stop
    # ==========================================
    for bot_id, bot in list(ACTIVE_BOTS.items()):
        if bot.get('strategy') != 'NPR' or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair or bot.get('npr_state') != 'IN_POSITION': continue
        if bot.get('position_side') == 'FLAT': continue
        should_exit, exit_reason = npr_get_stop_and_trail(bot, cur_px)
        if should_exit:
            side = bot['position_side']
            held, entry_px = bot.get('asset_held', 0), bot.get('entry_price', cur_px)
            mult = get_contract_multiplier(pair)
            try:
                p_info = client.get_product(product_id=pair)
                qi = str(getattr(p_info, 'quote_increment', '0.01'))
                bi = str(getattr(p_info, 'base_increment', '0.00000001'))
                exit_oid = str(uuid.uuid4())
                if side == 'LONG':
                    sp = snap_to_increment(cur_px - float(qi), qi)
                    sq = snap_to_increment(held, bi)
                    client.limit_order_gtc_sell(client_order_id=exit_oid, product_id=pair, base_size=sq, limit_price=sp, post_only=True)
                else:
                    sp = snap_to_increment(cur_px + float(qi), qi)
                    sq = snap_to_increment(held, bi)
                    client.limit_order_gtc_buy(client_order_id=exit_oid, product_id=pair, base_size=sq, limit_price=sp, post_only=True)

                fill_px, fill_sz, fill_fee = poll_market_fill(exit_oid, pair, retries=2, delay=0.5)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

                pnl = (actual_exit - entry_px) * held * mult if side == 'LONG' else (entry_px - actual_exit) * held * mult
                record_trade(bot, entry_px, actual_exit, held, side, exit_reason, pair, mult, actual_fee=actual_fee)
                if pnl < 0: bot['daily_loss'] = bot.get('daily_loss', 0) + abs(pnl)
                gross_proceeds = held * actual_exit * mult
                bot['current_usd'] += gross_proceeds - actual_fee
                bot['asset_held'] = 0.0; bot['position_side'] = 'FLAT'; bot['npr_state'] = 'SCANNING'
                for k in ['event_stop','event_type','event_direction','event_bar_data','high_water_mark','low_water_mark','trail_distance','partial_filled','atr_at_entry']:
                    bot.pop(k, None)
                changes_made = True
                log.info(f"[{pair}] NPR EXIT ({exit_reason}): PnL ${pnl:.2f}")
            except Exception as e:
                log.error(f"[{pair}] NPR exit failed: {e}")

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

                log.info(f"[{pair}] Fill detected: {grid['side']} at {grid['price']:.2f} (order {order_id[:8]}...)")

                # V3: if bot is in GTFO mode, don't reorder (it's exiting, not trading)
                ws_risk_check = bot.get('settings', {}).get('risk', {})
                if ws_risk_check.get('is_gtfo_active'):
                    log.debug(f"[{pair}] WS fill ignored: GTFO mode active")
                    continue

                if grid['side'] == 'BUY':
                    # Dynamic mode: regime-aware sell target
                    if bot.get('settings', {}).get('dynamic', False):
                        from grid_engine import compute_dynamic_sell_price
                        ws_risk = bot['settings'].get('risk', {})
                        new_price = compute_dynamic_sell_price(grid['price'], step_size,
                                                               ws_risk.get('regime', 'WIDE_RANGE'),
                                                               ws_risk.get('recovery_velocity', 0))
                    else:
                        new_price = grid['price'] + step_size
                    if min_gap > 0 and has_order_nearby(new_price, active_grids, min_gap):
                        safe_px = find_safe_price(new_price, active_grids, min_gap, direction='up')
                        if safe_px is None:
                            log.warning(f"[{pair}] No safe sell price near {new_price:.2f}. Holding with trail only.")
                            bot['asset_held'] += float(filled_size)
                            bot['current_usd'] -= float(filled_value)
                            risk = bot['settings'].setdefault('risk', {})
                            total_levels = risk.get('total_buy_levels', 10)
                            level_idx = grid.get('level_idx', total_levels // 2)
                            activate_trail(bot, grid['price'], float(filled_size), level_idx, total_levels, step_size)
                            try: active_grids.pop(i)
                            except IndexError: pass
                            break
                        log.debug(f"[{pair}] Sell nudged {new_price:.2f} -> {safe_px:.2f} (spacing guard)")
                        new_price = safe_px

                    new_grid = place_grid_sell(pair, new_price, filled_size, base_inc, quote_inc, deriv_flag, mult)

                    if new_grid:
                        active_grids[i] = new_grid
                        bot['asset_held'] += float(filled_size)
                        bot['current_usd'] -= float(filled_value)
                    else:
                        log.error(f"[{pair}] Sell placement failed at {new_price:.2f}")
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
                        except IndexError: pass
                        bot['asset_held'] -= float(filled_size)
                        bot['current_usd'] += float(filled_value)
                        log.info(f"[{pair}] SELL filled during halt. No new BUY.")
                    else:
                        new_price = grid['price'] - step_size
                        if min_gap > 0 and has_order_nearby(new_price, active_grids, min_gap):
                            safe_px = find_safe_price(new_price, active_grids, min_gap, direction='down')
                            if safe_px is None:
                                log.warning(f"[{pair}] No safe buy price near {new_price:.2f}. Level queued for redeployment.")
                                bot['asset_held'] -= float(filled_size)
                                bot['current_usd'] += float(filled_value)
                                risk = bot['settings'].setdefault('risk', {})
                                risk.setdefault('cancelled_buy_levels', []).append(new_price)
                                try: active_grids.pop(i)
                                except IndexError: pass
                                break
                            log.debug(f"[{pair}] Buy nudged {new_price:.2f} -> {safe_px:.2f} (spacing guard)")
                            new_price = safe_px

                        new_grid = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)

                        if new_grid:
                            active_grids[i] = new_grid
                            bot['asset_held'] -= float(filled_size)
                            bot['current_usd'] += float(filled_value)
                        else:
                            log.error(f"[{pair}] Buy placement failed at {new_price:.2f}")
                            bot['asset_held'] -= float(filled_size)
                            bot['current_usd'] += float(filled_value)

    if changes_made:
        save_bots()

def _ws_daemon():
    """Background Daemon for listening to User Channel WS execution events."""
    global _ws_client_ref, _subscribed_pairs, _ws_connected, _last_message_ts

    backoff = 5
    max_backoff = 60

    while True:
        try:
            def on_message(msg):
                global _last_message_ts
                _last_message_ts = time.time()
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
                except (KeyError, ValueError, TypeError) as e:
                    log.debug(f"WS parse error: {e}")

            api_key = os.getenv('COINBASE_API_KEY_NAME', '')
            api_secret = os.getenv('COINBASE_API_PRIVATE_KEY', '')

            if not (api_key and api_secret):
                log.error("Missing API Keys in Env. WS Tracking Disabled.")
                return

            active_pairs = list(set([bot.get('pair') for bot in ACTIVE_BOTS.values() if bot.get('pair')] + ["BTC-USD", "ETH-USD"]))
            ws_client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message)
            ws_client.open()
            ws_client.subscribe(product_ids=active_pairs, channels=["user"])
            ws_client.subscribe(product_ids=active_pairs, channels=["ticker"])

            with _ws_lock:
                _ws_client_ref = ws_client
                _subscribed_pairs = set(active_pairs)

            _ws_connected = True
            _last_message_ts = time.time()
            backoff = 5  # Reset backoff on successful connect
            log.info(f"Connected. Subscribed to {len(active_pairs)} pairs: {active_pairs}")

            while True:
                time.sleep(30)
                _check_new_pairs()
                # Heartbeat watchdog: reconnect if no message in 120s
                if time.time() - _last_message_ts > 120:
                    log.warning("No WS messages in 120s, forcing reconnect")
                    _ws_connected = False
                    try:
                        ws_client.close()
                    except Exception:
                        pass
                    break

        except Exception as e:
            _ws_connected = False
            log.error(f"WS connection error: {e}. Reconnecting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

def start_ws_engine():
    threading.Thread(target=_ws_daemon, daemon=True).start()
