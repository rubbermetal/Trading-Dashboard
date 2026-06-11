# grid_engine.py
import time
import uuid
import pandas as pd
import pandas_ta as ta
from collections import deque
from datetime import datetime, timezone

from shared import client, ACTIVE_BOTS
from bot_utils import (
    get_bot_tf, is_derivative, get_contract_multiplier,
    snap_to_increment, record_trade, save_bots,
    order_success, order_error, poll_market_fill, extract_fee
)
from logger import get_logger

log = get_logger('grid_engine')

# GRID-H5: assumed maker fee (percent per side). Matches the 0.0025 estimate
# used in record_trade. Override per bot via settings['maker_fee_pct'].
DEFAULT_MAKER_FEE_PCT = 0.25

def get_fee_floor_pct(settings):
    """Minimum profitable grid step in percent: round-trip maker fees + 0.05% margin."""
    maker_fee_pct = settings.get('maker_fee_pct', DEFAULT_MAKER_FEE_PCT)
    return (2 * maker_fee_pct) + 0.05

# GRID-M2: bounded dedupe eviction. The old `clear()` at 500 entries wiped
# seconds-old dedupe history; instead evict the oldest half at ~2000 entries.
_OID_EVICT_AT = 2000
_grid_oid_order = deque()

def _remember_processed_oids(processed_set, oids):
    """Record processed order ids with insertion order so the oldest half can
    be evicted when the set grows, instead of wiping everything."""
    for oid in oids:
        if oid and oid not in processed_set:
            processed_set.add(oid)
            _grid_oid_order.append(oid)
    if len(processed_set) > _OID_EVICT_AT:
        evict = len(processed_set) // 2
        while evict > 0 and _grid_oid_order:
            processed_set.discard(_grid_oid_order.popleft())
            evict -= 1

# ==========================================
# GRID PNL CALCULATOR
# ==========================================
def calculate_grid_pnl(bot, live_px):
    """
    Calculates unrealized PnL for a GRID bot.
    
    GRID bots hold fractional inventory across multiple fill levels.
    PnL = (current_usd + inventory_value) - allocated_usd
    
    For per-fill detail, uses the trailing stop state which tracks
    each fill's entry price and quantity.
    """
    mult = get_contract_multiplier(bot['pair'])
    held = abs(bot.get('asset_held', 0))
    
    # Inventory value at current market price
    inventory_value = held * live_px * mult
    
    # Total bot value = idle cash + inventory at market
    total_value = bot.get('current_usd', 0) + inventory_value
    
    # PnL vs allocated capital (deposits - withdrawals already reflected in allocated_usd)
    pnl = total_value - bot.get('allocated_usd', 0)
    
    return round(pnl, 4), round(total_value, 4)

# ==========================================
# ORDER SPACING GUARD
# ==========================================
def has_order_nearby(price, active_grids, min_gap):
    """
    Returns True if any existing grid order is within min_gap of the target price.
    Prevents orders from being placed cents apart due to market-relative vs
    grid-relative anchor drift between follow/flip/redeployment paths.
    """
    for g in active_grids:
        if abs(g['price'] - price) < min_gap:
            return True
    return False

def find_safe_price(price, active_grids, min_gap, direction='up'):
    """
    Nudges a price until it's at least min_gap from all existing orders.
    direction: 'up' nudges higher (for sells), 'down' nudges lower (for buys).
    Returns adjusted price, or None if can't find safe price within 5 attempts.
    """
    for _ in range(5):
        if not has_order_nearby(price, active_grids, min_gap):
            return price
        if direction == 'up':
            price += min_gap
        else:
            price -= min_gap
    return None

def derive_level_idx(price, settings, step_size, total_levels):
    """GRID-M6: derive a grid level index from a price's position within the
    configured grid range (nearest original level), instead of a faked constant."""
    lower = settings.get('lower_price')
    if not lower or step_size <= 0 or total_levels <= 0:
        return max(0, total_levels // 2)
    idx = int(round((price - lower) / step_size))
    return max(0, min(total_levels - 1, idx))

# ==========================================
# GRID HELPERS
# ==========================================
def cancel_all_pair_orders(pair):
    cancelled = 0
    try:
        open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "OPEN",
            "product_id": pair,
            "limit": 100
        })
        open_orders = open_res.get('orders', [])
        if not open_orders: return 0

        real_ids = [o['order_id'] for o in open_orders if o.get('order_id')]
        if not real_ids: return 0

        for i in range(0, len(real_ids), 10):
            batch = real_ids[i:i+10]
            try:
                res = client.cancel_orders(order_ids=batch)
                results = res.get('results', []) if isinstance(res, dict) else getattr(res, 'results', [])
                for r in results:
                    r_dict = r if isinstance(r, dict) else r.__dict__ if hasattr(r, '__dict__') else {}
                    if r_dict.get('success', False): cancelled += 1
            except Exception as e:
                log.error(f"[{pair}] Batch cancel error: {e}")
            time.sleep(0.2)
        log.info(f"[{pair}] cancel_all_pair_orders: {cancelled}/{len(real_ids)} cancelled")
    except Exception as e:
        log.error(f"[{pair}] cancel_all_pair_orders fetch error: {e}")
    return cancelled

def cancel_order_safe(grid_entry):
    cb_oid = grid_entry.get('cb_oid', '')
    client_oid = grid_entry.get('oid', '')

    if cb_oid:
        try:
            res = client.cancel_orders(order_ids=[cb_oid])
            results = res.get('results', []) if isinstance(res, dict) else getattr(res, 'results', [])
            if results:
                r = results[0] if isinstance(results[0], dict) else results[0]
                success = r.get('success', False) if isinstance(r, dict) else getattr(r, 'success', False)
                if success: return True
        except Exception as e:
            log.warning(f"Cancel by cb_oid failed for {cb_oid}: {e}")

    if client_oid:
        try:
            open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "OPEN", "limit": 50
            })
            for o in open_res.get('orders', []):
                if o.get('client_order_id') == client_oid:
                    real_id = o.get('order_id')
                    if real_id:
                        res = client.cancel_orders(order_ids=[real_id])
                        results = res.get('results', []) if isinstance(res, dict) else getattr(res, 'results', [])
                        if results:
                            r = results[0] if isinstance(results[0], dict) else results[0]
                            return r.get('success', False) if isinstance(r, dict) else getattr(r, 'success', False)
        except Exception as e:
            log.warning(f"Cancel by client_oid lookup failed for {client_oid}: {e}")

    return False

def place_grid_buy(pair, price, chunk_usd, base_inc, quote_inc, deriv_flag, mult):
    if deriv_flag:
        grid_qty = int(chunk_usd / (price * mult))
        if grid_qty < 1: return None
    else:
        grid_qty = float(chunk_usd * 0.99) / price

    str_price = snap_to_increment(price, quote_inc)
    str_qty = snap_to_increment(grid_qty, base_inc)
    if float(str_qty) <= 0: return None

    oid = str(uuid.uuid4())
    try:
        api_res = client.limit_order_gtc_buy(
            client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True
        )
        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
            cb_oid = ''
            if isinstance(api_res, dict):
                cb_oid = api_res.get('order_id', '') or api_res.get('success_response', {}).get('order_id', '')
            else:
                cb_oid = getattr(api_res, 'order_id', '') or getattr(getattr(api_res, 'success_response', None), 'order_id', '')
            return {"price": float(str_price), "side": "BUY", "oid": oid, "cb_oid": cb_oid}
        else:
            log.error(f"BUY rejected at {str_price}: {fail_reason}")
    except Exception as e:
        log.error(f"BUY exception at {str_price}: {e}")
    return None

def place_grid_sell(pair, price, qty_or_chunk, base_inc, quote_inc, deriv_flag, mult, use_chunk=False, chunk_usd=0):
    if use_chunk:
        if deriv_flag:
            grid_qty = int(chunk_usd / (price * mult))
            if grid_qty < 1: return None
        else:
            grid_qty = float(chunk_usd * 0.99) / price
    else:
        grid_qty = qty_or_chunk

    str_price = snap_to_increment(price, quote_inc)
    str_qty = snap_to_increment(grid_qty, base_inc)
    if float(str_qty) <= 0: return None

    oid = str(uuid.uuid4())
    try:
        api_res = client.limit_order_gtc_sell(
            client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True
        )
        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
            cb_oid = ''
            if isinstance(api_res, dict):
                cb_oid = api_res.get('order_id', '') or api_res.get('success_response', {}).get('order_id', '')
            else:
                cb_oid = getattr(api_res, 'order_id', '') or getattr(getattr(api_res, 'success_response', None), 'order_id', '')
            return {"price": float(str_price), "side": "SELL", "oid": oid, "cb_oid": cb_oid}
        else:
            log.error(f"SELL rejected at {str_price}: {fail_reason}")
    except Exception as e:
        log.error(f"SELL exception at {str_price}: {e}")
    return None

# ==========================================
# REST-BASED FILL CHECKER (15s FALLBACK)
# ==========================================
def grid_check_fills(bot_id, bot, pair):
    from bot_ws import _processed_fill_oids
    
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])
    if not active_grids: return

    step_size = settings.get('step_size', 0)
    chunk_usd = settings.get('chunk_size', 0)
    base_inc = settings.get('base_inc', '0.00000001')
    quote_inc = settings.get('quote_inc', '0.01')
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)
    is_halted = settings.get('halted', False)

    if step_size <= 0: return

    # Minimum gap = 40% of step size to prevent near-duplicate orders
    min_gap = step_size * 0.4

    try:
        order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "FILLED", "product_id": pair, "limit": 50
        })
        filled_orders = order_data.get('orders', [])
    except Exception as e:
        log.error(f"[{pair}] Fill check API error: {e}")
        return
    
    filled_map = {}
    for o in filled_orders:
        coid = o.get('client_order_id', '')
        oid = o.get('order_id', '')
        if coid: filled_map[coid] = o
        if oid: filled_map[oid] = o

    changes_made = False

    for i, grid in enumerate(list(active_grids)):
        grid_oid = grid.get('oid', '')
        grid_cb_oid = grid.get('cb_oid', '')
        
        if grid_oid in _processed_fill_oids or grid_cb_oid in _processed_fill_oids:
            continue
        
        filled_match = filled_map.get(grid_oid) or filled_map.get(grid_cb_oid)
        if not filled_match: continue
        
        # GRID-M2: bounded eviction instead of clear(); oids are un-marked below
        # if processing raises, so the fill is retried instead of lost forever.
        fill_server_id = filled_match.get('order_id', '')
        oids_marked = [o for o in (grid_oid, grid_cb_oid, fill_server_id) if o]
        _remember_processed_oids(_processed_fill_oids, oids_marked)

        filled_size = float(filled_match.get('filled_size', 0))
        avg_price = float(filled_match.get('average_filled_price', grid['price']))
        filled_value = filled_size * avg_price

        if filled_size <= 0: continue

        log.info(f"[{pair}] Fill detected: {grid['side']} at {grid['price']:.2f}")

        try:
            _process_one_grid_fill(bot, pair, grid, active_grids, filled_size, avg_price, filled_value,
                                   step_size, chunk_usd, base_inc, quote_inc, deriv_flag, mult,
                                   is_halted, min_gap, settings)
            changes_made = True
        except Exception as e:
            for o in oids_marked:
                _processed_fill_oids.discard(o)
            log.error(f"[{pair}] Fill processing failed for {grid.get('side')} @ {grid.get('price')}: {e}. Will retry next cycle.")
            continue

    if changes_made:
        save_bots()

def _process_one_grid_fill(bot, pair, grid, active_grids, filled_size, avg_price, filled_value,
                           step_size, chunk_usd, base_inc, quote_inc, deriv_flag, mult,
                           is_halted, min_gap, settings):
        if grid['side'] == 'BUY':
            risk = bot['settings'].setdefault('risk', {})
            # Dynamic mode: use regime-aware sell target
            if bot.get('settings', {}).get('dynamic', False):
                regime = risk.get('regime', 'WIDE_RANGE')
                velocity = risk.get('recovery_velocity', 0)
                new_price = compute_dynamic_sell_price(grid['price'], step_size, regime, velocity)
            else:
                new_price = grid['price'] + step_size
            # SPACING GUARD: nudge sell price up if too close to existing order
            safe_price = find_safe_price(new_price, active_grids, min_gap, direction='up')
            if safe_price is None:
                # FIX #1: No safe price found — skip placement, hold inventory with trail only
                log.warning(f"[{pair}] No safe sell price near {new_price:.2f}. Holding with trail only.")
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value

                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels, step_size)
                return
            if safe_price != new_price:
                log.debug(f"[{pair}] Sell nudged {new_price:.2f} -> {safe_price:.2f} (spacing guard)")
            new_price = safe_price

            new_grid = place_grid_sell(pair, new_price, filled_size, base_inc, quote_inc, deriv_flag, mult)
            
            if new_grid:
                try:
                    idx = active_grids.index(grid)
                    active_grids[idx] = new_grid
                except ValueError: active_grids.append(new_grid)
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value

                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                # GRID-M1: store actual buy fill price for flip PnL
                new_grid['entry_price'] = avg_price
                # GRID-M6: propagate level index through the flip
                new_grid['level_idx'] = level_idx
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels, step_size, sell_grid=new_grid)
                log.info(f"[{pair}] BUY filled -> SELL at {new_price:.2f} (trail active, depth={risk.get('depth_score', 0)})")
            else:
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value

                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels, step_size)
                log.warning(f"[{pair}] BUY filled, SELL flip failed. Trail active, inventory held.")

        elif grid['side'] == 'SELL':
            # GRID-M1: use the actual buy fill price stored on the flip sell when available
            buy_price = grid.get('entry_price', grid['price'] - step_size)
            record_trade(bot, buy_price, grid['price'], filled_size, 'LONG', 'GRID_FLIP', pair, mult)
            deactivate_trail_by_sell(bot, sell_oid=grid.get('oid'), sell_cb_oid=grid.get('cb_oid'))

            if is_halted:
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] -= filled_size
                bot['current_usd'] += filled_value
                log.info(f"[{pair}] SELL filled during halt. Depth now={bot['settings'].get('risk', {}).get('depth_score', 0)}")
            else:
                new_price = grid['price'] - step_size
                # SPACING GUARD: nudge buy price down if too close to existing order
                safe_price = find_safe_price(new_price, active_grids, min_gap, direction='down')
                if safe_price is None:
                    # FIX #1: No safe price — record the level for redeployment later
                    log.warning(f"[{pair}] No safe buy price near {new_price:.2f}. Level queued for redeployment.")
                    try: active_grids.remove(grid)
                    except ValueError: pass
                    bot['asset_held'] -= filled_size
                    bot['current_usd'] += filled_value
                    risk = bot['settings'].setdefault('risk', {})
                    risk.setdefault('cancelled_buy_levels', []).append(new_price)
                    return
                if safe_price != new_price:
                    log.debug(f"[{pair}] Buy nudged {new_price:.2f} -> {safe_price:.2f} (spacing guard)")
                new_price = safe_price

                new_grid = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)

                if new_grid:
                    # GRID-M6: keep the original level index through the flip
                    if 'level_idx' in grid:
                        new_grid['level_idx'] = grid['level_idx']
                    else:
                        total_levels = bot['settings'].setdefault('risk', {}).get('total_buy_levels', 10)
                        new_grid['level_idx'] = derive_level_idx(new_price, settings, step_size, total_levels)
                    try:
                        idx = active_grids.index(grid)
                        active_grids[idx] = new_grid
                    except ValueError: active_grids.append(new_grid)
                    bot['asset_held'] -= filled_size
                    bot['current_usd'] += filled_value
                    log.info(f"[{pair}] Flipped SELL -> BUY at {new_price:.2f}")
                else:
                    try: active_grids.remove(grid)
                    except ValueError: pass
                    bot['asset_held'] -= filled_size
                    bot['current_usd'] += filled_value

def grid_emergency_halt(bot_id, bot, pair, cur_px, reason, halt_mode='NEUTRAL'):
    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})
    active_grids = settings.get('active_grids', [])
    base_inc = settings.get('base_inc', '0.00000001')
    quote_inc = settings.get('quote_inc', '0.01')
    step_size = settings.get('step_size', 0)
    mult = get_contract_multiplier(pair)
    deriv_flag = is_derivative(pair)

    log.warning(f"[{pair}] HALT: {reason}")

    # FIX #3: Spec says ALL halt modes cancel buys as precaution (including FAVORABLE)
    buy_grids = [g for g in active_grids if g['side'] == 'BUY']
    cancelled = 0
    cancelled_prices = risk.setdefault('cancelled_buy_levels', [])

    for g in list(buy_grids):
        if cancel_order_safe(g):
            cancelled_prices.append(g['price'])
            if g in active_grids: active_grids.remove(g)
            cancelled += 1
    sell_grids = [g for g in active_grids if g['side'] == 'SELL']

    if halt_mode == 'FAVORABLE':
        log.warning(f"[{pair}] FAVORABLE: Cancelled {cancelled} buys (precautionary). Keeping {len(sell_grids)} sells. Widened trails.")
    else:
        log.warning(f"[{pair}] {halt_mode}: Cancelled {cancelled} buys. Keeping {len(sell_grids)} sells.")

    held = abs(bot.get('asset_held', 0))
    if held > 0 and not any(g['side'] == 'SELL' for g in active_grids) and step_size > 0:
        exit_px = cur_px + (step_size * 0.5)
        # SPACING GUARD on exit sell
        min_gap = step_size * 0.4
        safe_px = find_safe_price(exit_px, active_grids, min_gap, direction='up')
        if safe_px:
            exit_px = safe_px
        else:
            log.warning(f"[{pair}] No safe exit sell price found near {exit_px:.2f}. Trail-only exit.")
        if safe_px:
            exit_grid = place_grid_sell(pair, exit_px, held, base_inc, quote_inc, deriv_flag, mult)
            if exit_grid:
                active_grids.append(exit_grid)
                log.info(f"[{pair}] Placed exit SELL at {exit_px:.2f}")

    bot['settings']['halted'] = True
    bot['settings']['halted_reason'] = reason
    bot['settings']['halted_at'] = datetime.now(timezone.utc).isoformat()
    risk['halt_mode'] = halt_mode
    risk['halt_trigger_price'] = cur_px
    risk['direction_streak'] = 0
    risk['last_streak_direction'] = risk.get('direction', 'CHOPPY')
    save_bots()

def grid_follow(bot_id, bot, pair, cur_px, df):
    """
    Slides the grid window to follow price by recycling stale orders.
    
    Key design rules:
    - New orders are placed at GRID-ALIGNED prices (step multiples from existing levels)
      not at market-relative prices, preventing drift between follow and fill-flip anchors.
    - Orders are NEVER removed from tracking unless a replacement is successfully placed.
    - Max 1 recycle per cycle to prevent churn.
    - Orders placed less than 120 seconds ago are never recycled (cooldown).
    """
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])
    step_size = settings.get('step_size', 0)
    chunk_usd = settings.get('chunk_size', 0)
    base_inc = settings.get('base_inc', '0.00000001')
    quote_inc = settings.get('quote_inc', '0.01')
    mode = settings.get('mode', 'LONG').upper()
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)
    
    if not active_grids or step_size <= 0 or chunk_usd <= 0: return
    
    buy_grids = [g for g in active_grids if g['side'] == 'BUY']
    sell_grids = [g for g in active_grids if g['side'] == 'SELL']
    
    all_prices = [g['price'] for g in active_grids]
    if not all_prices: return
    
    grid_low = min(all_prices)
    grid_high = max(all_prices)
    
    # Only trigger follow if price is more than 1 full step beyond the grid edge
    if not (cur_px > grid_high + step_size and mode != 'SHORT') and \
       not (cur_px < grid_low - step_size and mode == 'LONG'):
        return

    # Minimum gap between any two orders
    min_gap = step_size * 0.4
    now = time.time()
    cooldown = 120  # seconds before an order can be recycled

    # --- PRICE ABOVE GRID (LONG or BOTH mode): move lowest buy up ---
    if cur_px > grid_high + step_size and mode != 'SHORT':
        # Find the single lowest buy that's old enough to recycle
        candidates = sorted(buy_grids, key=lambda g: g['price'])
        old_grid = None
        for c in candidates:
            placed_at = c.get('placed_at', 0)
            if now - placed_at >= cooldown:
                old_grid = c
                break
        
        if not old_grid:
            return  # All orders are too fresh to recycle
        
        # Calculate new price: grid-aligned, one step below cur_px (O(1))
        import math
        steps_above = max(1, math.floor((cur_px - grid_high) / step_size))
        new_buy_px = grid_high + steps_above * step_size
        if new_buy_px + step_size < cur_px:
            new_buy_px += step_size
        
        # Ensure it's actually below current price (it's a buy)
        if new_buy_px >= cur_px:
            new_buy_px -= step_size
        
        # Check spacing
        if has_order_nearby(new_buy_px, active_grids, min_gap):
            safe_px = find_safe_price(new_buy_px, active_grids, min_gap, direction='down')
            if not safe_px:
                return  # No safe price -- do nothing, keep all orders
            new_buy_px = safe_px
        
        # Only recycle if the new position is meaningfully better (at least 0.5 step closer to price)
        if (cur_px - new_buy_px) >= (cur_px - old_grid['price']) - (step_size * 0.5):
            return  # Not worth recycling
        
        if cancel_order_safe(old_grid):
            new_grid = place_grid_buy(pair, new_buy_px, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
            if new_grid:
                new_grid['placed_at'] = now
                active_grids.remove(old_grid)
                active_grids.append(new_grid)
                # Update bounds
                remaining_prices = [g['price'] for g in active_grids]
                if remaining_prices:
                    settings['lower_price'] = min(remaining_prices)
                    settings['upper_price'] = max(remaining_prices)
                save_bots()
                log.info(f"[{pair}] Follow recycled BUY {old_grid['price']:.2f} -> {new_buy_px:.2f}")
            else:
                # FIX #2: Placement failed after cancel — queue level for redeployment
                log.error(f"[{pair}] Follow recycle placement failed at {new_buy_px:.2f}. Queued for redeployment.")
                active_grids.remove(old_grid)
                risk = bot['settings'].setdefault('risk', {})
                risk.setdefault('cancelled_buy_levels', []).append(old_grid['price'])
                save_bots()

    # --- PRICE BELOW GRID (LONG mode): move highest buy down ---
    elif cur_px < grid_low - step_size and mode == 'LONG':
        candidates = sorted(buy_grids, key=lambda g: -g['price'])
        old_grid = None
        for c in candidates:
            placed_at = c.get('placed_at', 0)
            if now - placed_at >= cooldown:
                old_grid = c
                break
        
        if not old_grid:
            return
        
        # Grid-aligned: one step above cur_px, below grid_low (O(1))
        import math
        steps_below = max(1, math.floor((grid_low - cur_px) / step_size))
        new_buy_px = grid_low - steps_below * step_size
        if new_buy_px - step_size > cur_px:
            new_buy_px -= step_size
        
        if new_buy_px >= cur_px:
            new_buy_px -= step_size

        if has_order_nearby(new_buy_px, active_grids, min_gap):
            safe_px = find_safe_price(new_buy_px, active_grids, min_gap, direction='down')
            if not safe_px:
                return
            new_buy_px = safe_px
        
        if (old_grid['price'] - new_buy_px) < step_size * 0.5:
            return  # Not worth it

        if cancel_order_safe(old_grid):
            new_grid = place_grid_buy(pair, new_buy_px, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
            if new_grid:
                new_grid['placed_at'] = now
                active_grids.remove(old_grid)
                active_grids.append(new_grid)
                remaining_prices = [g['price'] for g in active_grids]
                if remaining_prices:
                    settings['lower_price'] = min(remaining_prices)
                    settings['upper_price'] = max(remaining_prices)
                save_bots()
                log.info(f"[{pair}] Followed DOWN: BUY {old_grid['price']:.2f} -> {new_buy_px:.2f}")
            else:
                # FIX #2: Placement failed after cancel — queue level for redeployment
                log.error(f"[{pair}] Follow-down placement failed at {new_buy_px:.2f}. Queued for redeployment.")
                active_grids.remove(old_grid)
                risk = bot['settings'].setdefault('risk', {})
                risk.setdefault('cancelled_buy_levels', []).append(old_grid['price'])
                save_bots()

# ==========================================
# GRID RISK ENGINE
# ==========================================
def compute_direction(df):
    if len(df) < 8: return "CHOPPY"
    try:
        sma5 = ta.sma(df['close'], 5)
        if sma5 is None or sma5.dropna().empty: return "CHOPPY"
        cur_sma = float(sma5.iloc[-1])
        prev_sma = float(sma5.iloc[-4]) if len(sma5) >= 4 else cur_sma
        cur_px = float(df['close'].iloc[-1])

        if prev_sma <= 0: return "CHOPPY"
        slope = (cur_sma - prev_sma) / prev_sma

        if cur_px > cur_sma and slope > 0: return "RISING"
        elif cur_px < cur_sma and slope < 0: return "FALLING"
        return "CHOPPY"
    except (ValueError, TypeError, IndexError) as e:
        log.debug(f"compute_direction failed: {e}")
        return "CHOPPY"

def get_trail_distance(level_index, total_levels, step_size):
    if total_levels <= 1: return step_size * 1.0
    pos_from_top = (total_levels - 1) - level_index
    third = total_levels / 3.0
    if pos_from_top < third: return step_size * 3.0
    elif pos_from_top < third * 2: return step_size * 2.0
    elif level_index == 0: return step_size * 1.0
    else: return step_size * 1.5

def calculate_max_loss(buy_levels, step_size, chunk_usd):
    total = len(buy_levels)
    if total == 0 or step_size <= 0: return 0.0
    max_loss = 0.0
    for i, lvl_px in enumerate(buy_levels):
        if lvl_px <= 0: continue
        trail_dist = get_trail_distance(i, total, step_size)
        qty = (chunk_usd * 0.99) / lvl_px
        max_loss += trail_dist * qty
    return max_loss

def init_risk_state(settings, buy_levels, step_size, chunk_usd, cur_px):
    total = len(buy_levels)
    max_loss = calculate_max_loss(buy_levels, step_size, chunk_usd)
    cb_price = 0.0
    if total > 0:
        lowest_trail = get_trail_distance(0, total, step_size)
        cb_price = buy_levels[0] - lowest_trail

    # GRID-L3: update in place — do NOT wholesale-replace the risk dict, which
    # erased stateful keys (regime, quarantine_until, loss_history, atr_sma_50,
    # dyn_trail, per_fill_trails, ...) set earlier in the same cycle.
    risk = settings.setdefault('risk', {})
    defaults = {
        "depth_score": 0, "direction": "CHOPPY", "halt_mode": None,
        "per_fill_trails": [], "cancelled_buy_levels": [],
        "recovery_timestamps": [], "recovery_velocity": 0.0,
    }
    for k, v in defaults.items():
        risk.setdefault(k, v)
    # Geometry-derived values are always refreshed for the new grid
    risk['risk_current'] = round(max_loss, 4)
    risk['risk_max'] = round(max_loss, 4)
    risk['circuit_breaker_price'] = round(cb_price, 2)
    risk['total_buy_levels'] = total
    return max_loss

def activate_trail(bot, fill_price, quantity, level_index, total_levels, step_size, sell_grid=None):
    risk = bot['settings'].setdefault('risk', {})
    trails = risk.setdefault('per_fill_trails', [])
    # Dynamic mode: use ATR-based trail instead of tiered step-based
    if bot.get('settings', {}).get('dynamic', False) and risk.get('dyn_trail', 0) > 0:
        base_dist = risk['dyn_trail']
    else:
        base_dist = get_trail_distance(level_index, total_levels, step_size)

    trail_entry = {
        "fill_id": str(uuid.uuid4())[:8],
        "fill_price": round(fill_price, 6),
        "quantity": quantity,
        "high_water_mark": fill_price,
        "base_trail_distance": round(base_dist, 6),
        "trail_multiplier": 1.0,
        "effective_trail": round(base_dist, 6),
        "level_index": level_index,
        "sell_oid": sell_grid.get('oid', '') if sell_grid else '',
        "sell_cb_oid": sell_grid.get('cb_oid', '') if sell_grid else '',
    }
    trails.append(trail_entry)
    risk['depth_score'] = len(trails)
    return trail_entry

def deactivate_trail_by_sell(bot, sell_oid=None, sell_cb_oid=None):
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])

    for i, t in enumerate(trails):
        matched = False
        if sell_oid and t.get('sell_oid') == sell_oid: matched = True
        if sell_cb_oid and t.get('sell_cb_oid') == sell_cb_oid: matched = True
        if matched:
            trails.pop(i)
            timestamps = risk.setdefault('recovery_timestamps', [])
            timestamps.append(time.time())
            if len(timestamps) > 20: risk['recovery_timestamps'] = timestamps[-20:]
            break
    risk['depth_score'] = len(trails)

def adjust_trail_multipliers(bot, halt_mode, depth):
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])
    total_levels = risk.get('total_buy_levels', 10)
    velocity = risk.get('recovery_velocity', 0)

    for t in trails:
        m = 1.0
        if halt_mode == 'FAVORABLE': m *= 1.5
        elif halt_mode == 'ADVERSE': m *= 0.75

        if depth >= 6: m *= 0.75
        elif depth >= 4:
            if t.get('level_index', 0) < total_levels * 0.3: m *= 0.75

        if velocity >= 2.0 and t.get('level_index', 0) < total_levels * 0.4: m *= 1.25
        t['trail_multiplier'] = round(m, 3)
        t['effective_trail'] = round(t['base_trail_distance'] * m, 6)

def compute_recovery_velocity(risk, bot=None):
    timestamps = risk.get('recovery_timestamps', [])
    now = time.time()
    # GRID-M8: spec window is "10 candles" of the bot's grid timeframe,
    # floored at 300s so 30s/1m grids keep the original behavior.
    window = 300
    if bot is not None:
        try:
            _, tf_sec = get_bot_tf(bot)
            window = max(300, 10 * tf_sec)
        except Exception:
            window = 300
    recent = [ts for ts in timestamps if now - ts < window]
    velocity = float(len(recent))
    risk['recovery_velocity'] = velocity
    return velocity

def evaluate_depth_escalation(bot, pair, direction, cur_px):
    risk = bot['settings'].get('risk', {})
    depth = risk.get('depth_score', 0)
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])

    if depth < 4: return

    buy_grids = sorted([g for g in active_grids if g['side'] == 'BUY'], key=lambda g: g['price'])
    if not buy_grids: return

    cancelled_prices = risk.setdefault('cancelled_buy_levels', [])

    if depth >= 6:
        count = 0
        for g in list(buy_grids):
            if cancel_order_safe(g):
                cancelled_prices.append(g['price'])
                if g in active_grids: active_grids.remove(g)
                count += 1
        if count:
            log.warning(f"[{pair}] CRITICAL depth={depth}: Cancelled {count} open buys")
            save_bots()

    elif depth >= 4 and direction == 'FALLING':
        to_cancel = buy_grids[:min(2, len(buy_grids))]
        count = 0
        for g in to_cancel:
            if cancel_order_safe(g):
                cancelled_prices.append(g['price'])
                if g in active_grids: active_grids.remove(g)
                count += 1
        if count:
            log.warning(f"[{pair}] ELEVATED depth={depth} FALLING: Cancelled {count} lowest buys")
            save_bots()

def evaluate_buy_redeployment(bot, pair, direction, cur_px, step_size, base_inc, quote_inc, deriv_flag, mult, chunk_usd):
    risk = bot['settings'].get('risk', {})
    depth = risk.get('depth_score', 0)
    cancelled = risk.get('cancelled_buy_levels', [])
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])

    if not cancelled or step_size <= 0 or chunk_usd <= 0: return
    # GRID-M4: spec §5 — redeploy only while RISING/CHOPPY, never while FALLING
    if direction == 'FALLING': return
    if depth > 3: return

    total_levels = risk.get('total_buy_levels', 10)
    deployed = 0
    max_per_cycle = min(len(cancelled), 3)
    min_gap = step_size * 0.4

    # GRID-H2: respect remaining capital — open buys already commit chunk_usd each
    open_buy_count = sum(1 for g in active_grids if g.get('side') == 'BUY')
    available_usd = bot.get('current_usd', 0) - (open_buy_count * chunk_usd)

    for i in range(max_per_cycle):
        if available_usd < chunk_usd:
            log.debug(f"[{pair}] Redeployment stopped: insufficient capital (${available_usd:.2f} < ${chunk_usd:.2f})")
            break
        new_price = cur_px - (step_size * (i + 1))
        # SPACING GUARD: nudge down if too close to any existing order
        safe_px = find_safe_price(new_price, active_grids, min_gap, direction='down')
        if not safe_px:
            log.debug(f"[{pair}] Redeployment skipped at ~{new_price:.2f}: no safe price")
            continue
        new_price = safe_px

        g = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
        if g:
            # GRID-M6: derive index from price position in the grid, not a constant
            g['level_idx'] = derive_level_idx(new_price, settings, step_size, total_levels)
            active_grids.append(g)
            deployed += 1
            available_usd -= chunk_usd

    if deployed:
        risk['cancelled_buy_levels'] = cancelled[deployed:]
        save_bots()
        log.info(f"[{pair}] Redeployed {deployed} buys. {len(cancelled) - deployed} still cancelled.")

def check_circuit_breaker(bot, cur_px, pair):
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])
    settings = bot.get('settings', {})
    if not trails: return False

    total_loss = 0.0
    for t in trails:
        loss = (t['fill_price'] - cur_px) * t['quantity']
        if loss > 0: total_loss += loss

    allocated = bot.get('allocated_usd', 0)
    if allocated <= 0: return False

    loss_pct = total_loss / allocated
    cb_threshold = 0.06

    if loss_pct >= cb_threshold:
        # GRID-H3: rate-limit retries so a failing sell doesn't hammer the API
        now_ts = time.time()
        if now_ts - risk.get('cb_last_attempt', 0) < 30:
            return False
        risk['cb_last_attempt'] = now_ts

        log.warning(f"[{pair}] CIRCUIT BREAKER TRIGGERED: ${total_loss:.2f} = {loss_pct*100:.1f}% of ${allocated:.2f}")
        cancel_all_pair_orders(pair)
        time.sleep(0.3)

        held = abs(bot.get('asset_held', 0))
        if held > 0.000001:
            # GRID-H3: only clear protection state after the liquidation sell
            # actually succeeds. On failure keep trails so the CB re-fires.
            try:
                base_inc = settings.get('base_inc', '0.00000001')
                str_qty = snap_to_increment(held, base_inc)
                oid = str(uuid.uuid4())
                res = client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
            except Exception as e:
                log.error(f"[{pair}] Circuit breaker sell failed: {e}. Keeping trails, retrying in 30s.")
                save_bots()
                return False
            if not order_success(res):
                log.error(f"[{pair}] Circuit breaker sell rejected: {order_error(res)}. Keeping trails, retrying in 30s.")
                save_bots()
                return False

            fill_px, fill_qty, fill_fee = poll_market_fill(oid, pair)
            exit_px = fill_px if fill_px else cur_px
            exit_qty = fill_qty if fill_qty else held
            mult = get_contract_multiplier(pair)
            total_trail_qty = sum(t['quantity'] for t in trails) or 1.0
            for t in trails:
                fee_share = (fill_fee * t['quantity'] / total_trail_qty) if fill_fee is not None else None
                record_trade(bot, t['fill_price'], exit_px, t['quantity'], 'LONG', 'CIRCUIT_BREAKER', pair, mult, actual_fee=fee_share)
            bot['asset_held'] = 0.0
            proceeds = exit_qty * exit_px
            bot['current_usd'] += (proceeds - fill_fee) if fill_fee is not None else proceeds * 0.995

        risk['per_fill_trails'] = []
        risk['depth_score'] = 0
        risk['cancelled_buy_levels'] = []
        settings['active_grids'] = []
        settings['halted'] = True
        settings['halted_reason'] = f"CIRCUIT BREAKER: {loss_pct*100:.1f}% loss exceeded {cb_threshold*100:.0f}% threshold"
        settings['halted_at'] = datetime.now(timezone.utc).isoformat()
        risk['halt_mode'] = 'ADVERSE'
        save_bots()
        return True
    return False

def manage_runner_exits(bot, pair, cur_px):
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])
    velocity = risk.get('recovery_velocity', 0)
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])
    step_size = settings.get('step_size', 0)

    if velocity < 2.0 or step_size <= 0: return

    converted = 0
    for t in trails:
        profit_steps = (cur_px - t['fill_price']) / step_size if step_size > 0 else 0
        if profit_steps >= 2.0 and t.get('sell_oid'):
            for j, g in enumerate(list(active_grids)):
                if ((t['sell_oid'] and g.get('oid') == t['sell_oid']) or
                    (t.get('sell_cb_oid') and g.get('cb_oid') == t.get('sell_cb_oid'))):
                    if not cancel_order_safe(g):
                        log.warning(f"[{pair}] RUNNER convert: cancel failed (likely filled) — keeping tracked")
                        break
                    active_grids.remove(g)
                    t['sell_oid'] = ''
                    t['sell_cb_oid'] = ''
                    converted += 1
                    log.info(f"[{pair}] RUNNER: fill@{t['fill_price']:.2f} now +{profit_steps:.1f} steps. Sell cancelled, trailing only.")
                    break
    if converted: save_bots()

def check_trailing_stops(bot, cur_px, pair):
    """
    REST-cycle fallback for trailing stop evaluation (every 15s).
    The WS ticker in bot_ws.py checks on every tick, but if WS is down
    this ensures trails still get evaluated. This is the function that
    was completely missing -- trails were being activated and adjusted
    but never actually checked against price in the REST cycle.
    """
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])
    if not trails:
        return False

    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])
    mult = get_contract_multiplier(pair)
    base_inc = settings.get('base_inc', '0.00000001')
    triggered = []

    for t in trails:
        # Update high water mark
        if cur_px > t['high_water_mark']:
            t['high_water_mark'] = cur_px

        effective = t.get('effective_trail', t.get('base_trail_distance', 0))
        trigger_price = t['high_water_mark'] - effective

        if cur_px <= trigger_price:
            triggered.append(t)

    for t in triggered:
        qty = t['quantity']
        effective = t.get('effective_trail', t.get('base_trail_distance', 0))
        if time.time() - t.get('last_exit_attempt', 0) < 30:
            continue  # rate-limit retries after a failed sell
        t['last_exit_attempt'] = time.time()
        log.info(f"[{pair}] TRAILING STOP: fill@{t['fill_price']:.2f} "
              f"HWM={t['high_water_mark']:.2f} trail={effective:.2f} exit@{cur_px:.2f}")
        try:
            if bot.get('paper'):
                record_trade(bot, t['fill_price'], cur_px, qty, 'LONG', 'TRAILING_STOP', pair, mult)
                bot['asset_held'] = max(0.0, bot.get('asset_held', 0) - qty)
                bot['current_usd'] += qty * cur_px * 0.995
                for g in list(active_grids):
                    if ((t.get('sell_oid') and g.get('oid') == t['sell_oid']) or
                        (t.get('sell_cb_oid') and g.get('cb_oid') == t['sell_cb_oid'])):
                        active_grids.remove(g)
                        break
                trails.remove(t)
                continue

            # Cancel the resting grid sell FIRST and verify; if it filled in the race
            # window the inventory is already gone — market-selling again double-sells.
            cancelled_ok = True
            for g in list(active_grids):
                if ((t.get('sell_oid') and g.get('oid') == t['sell_oid']) or
                    (t.get('sell_cb_oid') and g.get('cb_oid') == t['sell_cb_oid'])):
                    if cancel_order_safe(g):
                        active_grids.remove(g)
                    else:
                        cancelled_ok = False
                        log.warning(f"[{pair}] Trail stop: grid sell cancel failed (likely filled) — skipping market sell")
                    break
            if not cancelled_ok:
                continue

            oid = str(uuid.uuid4())
            str_qty = snap_to_increment(qty, base_inc)
            res = client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
            if not order_success(res):
                log.error(f"[{pair}] Trail stop sell rejected: {order_error(res)} — will retry")
                continue

            record_trade(bot, t['fill_price'], cur_px, qty, 'LONG', 'TRAILING_STOP', pair, mult)
            bot['asset_held'] = max(0.0, bot.get('asset_held', 0) - qty)
            bot['current_usd'] += qty * cur_px * 0.995
        except Exception as e:
            log.error(f"[{pair}] Trail stop sell failed: {e}")
            continue

        trails.remove(t)

    if triggered:
        risk['depth_score'] = len(trails)
        risk['risk_current'] = round(sum(
            t.get('effective_trail', t.get('base_trail_distance', 0)) * t['quantity'] for t in trails
        ), 4)
        save_bots()
        return True
    return False

def _paper_grid_execute(bot_id, bot, pair):
    """Paper trading GRID: simulate fills when price crosses grid levels, no real orders."""
    settings = bot.get('settings', {})
    try:
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
    except Exception as e:
        log.error(f"[{pair}] Paper grid price fetch error: {e}")
        return

    # Initialize paper grids on first run
    if 'paper_grids' not in settings:
        lower = settings.get('lower_price', cur_px * 0.95)
        upper = settings.get('upper_price', cur_px * 1.05)
        step_pct = settings.get('step_pct', 0.6) / 100.0
        step = cur_px * step_pct
        mode = settings.get('mode', 'LONG').upper()

        levels = []
        lvl = lower
        while lvl <= upper:
            if lvl < cur_px * 0.999:
                levels.append({'price': round(lvl, 6), 'side': 'BUY', 'filled': False})
            elif lvl > cur_px * 1.001 and mode != 'LONG':
                levels.append({'price': round(lvl, 6), 'side': 'SELL', 'filled': False})
            lvl += step

        total_orders = len(levels)
        if total_orders == 0:
            return
        settings['paper_grids'] = levels
        settings['paper_chunk_usd'] = bot['current_usd'] / total_orders
        settings['paper_step'] = step
        save_bots()
        log.info(f"[{pair}] Paper grid initialized {len(levels)} virtual levels, ${settings['paper_chunk_usd']:.2f}/level")
        return

    grids = settings['paper_grids']
    chunk_usd = settings.get('paper_chunk_usd', 5)
    step = settings.get('paper_step', cur_px * 0.006)
    changes = False
    sim_fee_rate = 0.004

    for g in grids:
        if g['filled']:
            continue
        if g['side'] == 'BUY' and cur_px <= g['price']:
            # Simulate buy fill
            qty = chunk_usd / g['price']
            fee = chunk_usd * sim_fee_rate
            bot['asset_held'] = bot.get('asset_held', 0) + qty
            bot['current_usd'] -= (chunk_usd + fee)
            g['filled'] = True
            # Place virtual sell one step above
            sell_px = round(g['price'] + step, 6)
            grids.append({'price': sell_px, 'side': 'SELL', 'filled': False, 'qty': qty})
            changes = True
            log.info(f"[{pair}] Paper BUY filled @ ${g['price']:.4f} -> SELL @ ${sell_px:.4f}")
        elif g['side'] == 'SELL' and cur_px >= g['price']:
            # Simulate sell fill — never sell more than tracked holdings (pre-seeded
            # sell levels without qty previously credited cash for phantom inventory)
            qty = min(g.get('qty', chunk_usd / g['price']), bot.get('asset_held', 0))
            if qty <= 0:
                g['filled'] = True
                continue
            proceeds = qty * g['price']
            fee = proceeds * sim_fee_rate
            bot['asset_held'] = max(0, bot.get('asset_held', 0) - qty)
            bot['current_usd'] += (proceeds - fee)
            g['filled'] = True
            # Place virtual buy one step below
            buy_px = round(g['price'] - step, 6)
            grids.append({'price': buy_px, 'side': 'BUY', 'filled': False})
            changes = True
            pnl = (g['price'] - (g['price'] - step)) * qty - (fee * 2)
            log.info(f"[{pair}] Paper SELL filled @ ${g['price']:.4f} -> BUY @ ${buy_px:.4f} (flip PnL ~${pnl:.4f})")

    if changes:
        # Clean up filled entries to prevent unbounded growth
        settings['paper_grids'] = [g for g in grids if not g['filled']]
        save_bots()


# ==========================================
# GRID V2: DYNAMIC DECISION TREE ENGINE
# ==========================================

def compute_regime(adx, bb_width, bb_width_avg, direction):
    """Classify market regime from continuous indicators."""
    if adx >= 30:
        return 'STRONG_TREND'
    elif adx >= 20 and direction == 'FALLING':
        return 'MILD_DOWNTREND'
    elif adx >= 20 and direction == 'RISING':
        return 'MILD_UPTREND'
    elif bb_width_avg > 0 and bb_width < bb_width_avg:
        return 'TIGHT_RANGE'
    else:
        return 'WIDE_RANGE'


def compute_dynamic_step(atr, adx, bb_width, bb_width_avg, direction, price, min_step_pct, max_step_pct, depth=0):
    """ATR-based step with continuous scaling from ADX and BB width.
    V3: depth-aware exponential widening in FALLING markets (anti-waterfall)."""
    if atr <= 0 or price <= 0:
        return price * min_step_pct / 100.0

    # Trend factor: scales 0.5 → 1.5 as ADX goes 10 → 40
    trend_factor = max(0.5, min(2.0, 0.5 + (adx / 40.0)))

    # Volatility factor: BB_width relative to its own average
    vol_ratio = (bb_width / bb_width_avg) if bb_width_avg > 0 else 1.0
    vol_factor = max(0.5, min(2.0, vol_ratio))

    # Combined: weighted blend
    step_mult = (trend_factor * 0.6) + (vol_factor * 0.4)

    # Direction bias
    if direction == 'FALLING':
        step_mult += 0.25
    elif direction == 'RISING':
        step_mult -= 0.1

    step_mult = max(0.3, min(2.5, step_mult))

    step = atr * step_mult

    # V3: Anti-waterfall — widen subsequent buy spacing in falling markets
    if direction == 'FALLING' and depth > 0:
        step *= (1.2 ** depth)

    floor = price * min_step_pct / 100.0
    ceiling = price * max_step_pct / 100.0
    return max(floor, min(ceiling, step))


def compute_dynamic_trail(atr, adx, direction, velocity):
    """ATR-based trail distance with continuous scaling."""
    if atr <= 0:
        return 0

    # Base: inverse of trend strength
    trail_m = max(0.4, min(1.0, 1.0 - (adx / 80.0)))

    # Direction adjustment
    if direction == 'RISING':
        trail_m += 0.3
    elif direction == 'FALLING':
        trail_m -= 0.15

    # Velocity adjustment
    if velocity >= 2.0:
        trail_m += 0.2

    trail_m = max(0.4, min(1.5, trail_m))
    return atr * trail_m


def compute_kelly_size(adx, direction, vol_ratio, step, trail, allocated_usd, min_order_usd, depth=0):
    """Kelly position sizing with V3 cluster-risk adjustment.
    Base fraction reduced from 0.25 to 0.10. Depth penalty: 1/(1+depth*0.5).
    Result: first dip heavy, tenth dip nibble."""
    if trail <= 0 or step <= 0 or allocated_usd <= 0:
        return min_order_usd

    # Continuous win probability
    win_prob = 0.80 - (adx / 100.0)
    if direction == 'RISING':
        win_prob += 0.05
    elif direction == 'FALLING':
        win_prob -= 0.10
    if vol_ratio < 0.8:
        win_prob += 0.05
    win_prob = max(0.20, min(0.80, win_prob))

    # Kelly fraction: f* = (bp - q) / b where b = reward/risk, p = win_prob, q = 1-p
    b = step / trail
    if b <= 0:
        return min_order_usd
    kelly = (b * win_prob - (1 - win_prob)) / b
    if kelly <= 0:
        return 0.0  # negative edge: skip placement (was min_order_usd, i.e. trade anyway)

    # V3: cluster-risk penalty — each open fill reduces sizing
    cluster_penalty = 1.0 / (1.0 + (depth * 0.5))

    # V3: base fraction dropped from 0.25 to 0.10 (less aggressive)
    fraction = kelly * 0.10 * cluster_penalty
    order_usd = allocated_usd * fraction
    return max(min_order_usd, min(order_usd, allocated_usd * 0.10))


def compute_grid_crisis_score(depth, unrealized_loss_pct, regime, direction, velocity, loss_history=None):
    """5-factor crisis scoring for grid (0-120). Higher = more urgent to cut.
    V3 adds Factor 5: Velocity of Loss (catches flash crashes the 5-min window misses)."""
    # Factor 1: Inventory depth (max 30)
    if depth >= 8: f1 = 30
    elif depth >= 6: f1 = 25
    elif depth >= 4: f1 = 15
    elif depth >= 2: f1 = 5
    else: f1 = 0

    # Factor 2: Unrealized loss % (max 30)
    if unrealized_loss_pct >= 5.0: f2 = 30
    elif unrealized_loss_pct >= 3.0: f2 = 20
    elif unrealized_loss_pct >= 1.0: f2 = 10
    else: f2 = 0

    # Factor 3: Regime hostility (max 25)
    if regime == 'STRONG_TREND' and direction == 'FALLING': f3 = 25
    elif regime == 'MILD_DOWNTREND': f3 = 15
    elif regime == 'STRONG_TREND' and direction == 'RISING': f3 = 10
    elif regime == 'WIDE_RANGE': f3 = 5
    else: f3 = 0  # TIGHT_RANGE

    # Factor 4: Recovery probability (max 15)
    if velocity == 0 and depth >= 4: f4 = 15
    elif velocity < 1.0 and depth >= 3: f4 = 10
    elif velocity < 2.0: f4 = 5
    else: f4 = 0

    # V3 Factor 5: Velocity of Loss (max 20) — catches flash crashes
    f5 = 0
    if loss_history and len(loss_history) >= 2:
        # Find oldest entry within last 60 seconds
        now_ts = loss_history[-1][0]
        recent = [(ts, lp) for ts, lp in loss_history if now_ts - ts <= 60]
        if len(recent) >= 2:
            delta = recent[-1][1] - recent[0][1]
            if delta >= 2.0:
                f5 = 20

    return f1 + f2 + f3 + f4 + f5


def compute_dynamic_sell_price(fill_price, step, regime, velocity):
    """Dynamic profit target based on regime."""
    if regime == 'TIGHT_RANGE' and velocity >= 1.0:
        return fill_price + (1.5 * step)
    elif regime == 'MILD_UPTREND':
        return fill_price + (2.0 * step)
    else:
        return fill_price + step


def compute_bb_indicators(df):
    """Compute Bollinger Band width and its SMA(50) average."""
    bb = ta.bbands(df['close'], length=20, std=2)
    if bb is None:
        return 0, 0, 0, 0
    upper_col = [c for c in bb.columns if 'BBU' in c]
    lower_col = [c for c in bb.columns if 'BBL' in c]
    if not upper_col or not lower_col:
        return 0, 0, 0, 0
    bb_upper = float(bb[upper_col[0]].iloc[-1]) if not pd.isna(bb[upper_col[0]].iloc[-1]) else 0
    bb_lower = float(bb[lower_col[0]].iloc[-1]) if not pd.isna(bb[lower_col[0]].iloc[-1]) else 0
    bb_width = bb_upper - bb_lower

    width_series = bb[upper_col[0]] - bb[lower_col[0]]
    bb_width_avg = float(ta.sma(width_series, 50).iloc[-1]) if len(width_series) >= 50 and not pd.isna(ta.sma(width_series, 50).iloc[-1]) else bb_width

    return bb_upper, bb_lower, bb_width, bb_width_avg


# ==========================================
# GRID V3: GTFO + STICKY STATE + RUNNERS
# ==========================================

def compute_gtfo_target(weighted_avg, score):
    """Gravity-adjusted GTFO target. Score 50 = +0.2% buffer; score 79 = 0% buffer (true breakeven)."""
    gravity_mult = max(0, 1 - (score - 50) / 30)
    profit_buffer = 0.002 * gravity_mult
    return weighted_avg * (1 + profit_buffer)


def compute_quarantine_minutes(current_atr, atr_sma_50):
    """Dynamic quarantine: 60 minutes scaled by ATR ratio, clamped [15, 240]."""
    if atr_sma_50 <= 0:
        return 60
    ratio = current_atr / atr_sma_50
    minutes = 60 * ratio
    return max(15, min(240, minutes))


def compute_weighted_avg(trails):
    """Compute weighted average entry price across all per-fill trails."""
    if not trails:
        return 0
    total_qty = sum(t.get('quantity', 0) for t in trails)
    if total_qty == 0:
        return 0
    total_cost = sum(t.get('quantity', 0) * t.get('fill_price', 0) for t in trails)
    return total_cost / total_qty


def enter_gtfo_mode(bot, pair, current_px, base_inc, quote_inc, mult, deriv_flag):
    """Hot-swap order book to GTFO state: cancel all, compute weighted avg, place single exit limit."""
    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})
    trails = risk.get('per_fill_trails', [])
    if not trails:
        return False

    # 1. Cancel all open orders (atomic)
    try:
        cancel_all_pair_orders(pair)
        time.sleep(0.5)
    except Exception as e:
        log.error(f"[{pair}] GTFO cancel failed: {e}")
    settings['active_grids'] = []

    # 2. Compute weighted average from inventory
    avg_price = compute_weighted_avg(trails)
    if avg_price <= 0:
        log.warning(f"[{pair}] GTFO abort: weighted avg = 0")
        return False

    # 3. Compute gravity-adjusted target
    score = risk.get('crisis_score', 50)
    target = compute_gtfo_target(avg_price, score)

    # 4. Round target DOWN to quote increment (safe-side)
    quote_inc_f = float(quote_inc) if quote_inc else 0.01
    target = (int(target / quote_inc_f)) * quote_inc_f

    # 5. Place ONE limit sell for full inventory
    total_qty = sum(t.get('quantity', 0) for t in trails)
    str_qty = snap_to_increment(total_qty, base_inc)

    try:
        oid = str(uuid.uuid4())
        if current_px >= target:
            # Already past target — execute market sell immediately, WITH accounting
            res = client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
            if not order_success(res):
                log.error(f"[{pair}] GTFO market sell rejected: {order_error(res)}")
                return False
            fill_px, fill_qty, fill_fee = poll_market_fill(oid, pair)
            exit_px = fill_px if fill_px else current_px
            exit_qty = fill_qty if fill_qty else float(str_qty)
            fee = fill_fee if fill_fee is not None else exit_px * exit_qty * 0.0025
            record_trade(bot, avg_price, exit_px, exit_qty, 'LONG', 'GTFO', pair, mult, actual_fee=fee)
            bot['asset_held'] = max(0.0, bot.get('asset_held', 0) - exit_qty)
            bot['current_usd'] += (exit_qty * exit_px * mult) - fee
            risk['per_fill_trails'] = []
            risk['is_gtfo_active'] = True  # run_gtfo_cycle completes quarantine next cycle
            risk['gtfo_target_price'] = 0
            risk['gtfo_high_score_streak'] = 0
            save_bots()
            log.info(f"[{pair}] GTFO: market exit at {exit_px:.4f} (target {target:.4f} already breached)")
            return True
        # Place post-only limit (snap, not raw float math — off-increment rejections)
        str_target = snap_to_increment(target, quote_inc)
        res = client.limit_order_gtc_sell(
            client_order_id=oid, product_id=pair,
            base_size=str_qty, limit_price=str_target, post_only=True
        )
        if not order_success(res):
            log.error(f"[{pair}] GTFO limit sell rejected: {order_error(res)}")
            return False
        log.info(f"[{pair}] GTFO ENTERED: limit sell {str_qty} @ {str_target} (avg={avg_price:.4f}, score={score})")

        risk['is_gtfo_active'] = True
        risk['gtfo_target_price'] = float(str_target)
        risk['gtfo_order_id'] = oid
        risk['gtfo_order_qty'] = float(str_qty)
        risk['gtfo_avg_entry'] = avg_price
        risk['gtfo_high_score_streak'] = 0
        save_bots()
        return True
    except Exception as e:
        log.error(f"[{pair}] GTFO order placement failed: {e}")
        return False


def run_gtfo_cycle(bot, pair, current_px, current_atr, score, base_inc, quote_inc, mult, deriv_flag):
    """Per-cycle GTFO management: resync target, check fill, time-decay nibble."""
    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})
    trails = risk.get('per_fill_trails', [])

    # GRID-C2: the GTFO exit order is tracked only here (active_grids is empty and
    # the WS handler skips GTFO fills), so ITS fill must be accounted here or the
    # bot re-sells inventory it no longer holds.
    gtfo_oid = risk.get('gtfo_order_id')
    if gtfo_oid:
        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch",
                                    params={"product_id": pair, "limit": 50})
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == gtfo_oid and o.get('status') == 'FILLED':
                    fsz = float(o.get('filled_size', 0) or 0)
                    fpx = float(o.get('average_filled_price', current_px) or current_px)
                    fee = extract_fee(o)
                    if fee is None:
                        fee = fpx * fsz * mult * 0.0006
                    if fsz > 0:
                        entry = risk.get('gtfo_avg_entry') or compute_weighted_avg(trails) or fpx
                        record_trade(bot, entry, fpx, fsz, 'LONG', 'GTFO', pair, mult, actual_fee=fee)
                        bot['asset_held'] = max(0.0, bot.get('asset_held', 0) - fsz)
                        bot['current_usd'] += (fsz * fpx * mult) - fee
                        risk['per_fill_trails'] = []
                        risk.pop('gtfo_order_id', None)
                        trails = []
                        log.info(f"[{pair}] GTFO exit FILLED: {fsz} @ {fpx:.4f}")
                        save_bots()
                    break
        except Exception as e:
            log.error(f"[{pair}] GTFO fill check error: {e}")

    # Check if inventory is empty (order filled externally or via WS)
    if not trails or bot.get('asset_held', 0) <= 0:
        # Exit complete — start quarantine
        risk['is_gtfo_active'] = False
        risk['gtfo_target_price'] = 0
        risk.pop('gtfo_order_id', None)
        risk['gtfo_high_score_streak'] = 0

        atr_sma_50 = risk.get('atr_sma_50', current_atr)
        quarantine_min = compute_quarantine_minutes(current_atr, atr_sma_50)
        risk['quarantine_until'] = int(time.time()) + int(quarantine_min * 60)
        log.info(f"[{pair}] GTFO COMPLETE. Quarantine: {quarantine_min:.0f} min")
        save_bots()
        return

    # Recompute target with current score
    avg_price = compute_weighted_avg(trails)
    new_target = compute_gtfo_target(avg_price, score)
    quote_inc_f = float(quote_inc) if quote_inc else 0.01
    new_target = (int(new_target / quote_inc_f)) * quote_inc_f
    old_target = risk.get('gtfo_target_price', 0)

    # Resync if drift > 0.05%
    if old_target > 0 and abs(new_target - old_target) / old_target > 0.0005:
        try:
            cancel_all_pair_orders(pair)
            time.sleep(0.3)
            # Never sell more than the bot's tracked holdings (a filled GTFO order
            # previously left phantom inventory here and re-sold it every resync)
            total_qty = min(sum(t.get('quantity', 0) for t in trails),
                            max(0.0, bot.get('asset_held', 0)))
            str_qty = snap_to_increment(total_qty, base_inc)
            if float(str_qty) <= 0:
                return
            oid = str(uuid.uuid4())
            if current_px >= new_target:
                res = client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                if not order_success(res):
                    log.error(f"[{pair}] GTFO resync market sell rejected: {order_error(res)}")
                    return
                fill_px, fill_qty, fill_fee = poll_market_fill(oid, pair)
                exit_px = fill_px if fill_px else current_px
                exit_qty = fill_qty if fill_qty else float(str_qty)
                fee = fill_fee if fill_fee is not None else exit_px * exit_qty * 0.0025
                entry = risk.get('gtfo_avg_entry') or compute_weighted_avg(trails) or exit_px
                record_trade(bot, entry, exit_px, exit_qty, 'LONG', 'GTFO', pair, mult, actual_fee=fee)
                bot['asset_held'] = max(0.0, bot.get('asset_held', 0) - exit_qty)
                bot['current_usd'] += (exit_qty * exit_px * mult) - fee
                risk['per_fill_trails'] = []
                risk.pop('gtfo_order_id', None)
                log.info(f"[{pair}] GTFO market exit at {exit_px:.4f} (resynced target breached)")
            else:
                str_target = snap_to_increment(new_target, quote_inc)
                res = client.limit_order_gtc_sell(
                    client_order_id=oid, product_id=pair,
                    base_size=str_qty, limit_price=str_target, post_only=True
                )
                if not order_success(res):
                    log.error(f"[{pair}] GTFO resync limit rejected: {order_error(res)}")
                    return
                log.info(f"[{pair}] GTFO RESYNC: target {old_target:.4f} -> {new_target:.4f} (score={score})")
                risk['gtfo_target_price'] = float(str_target)
                risk['gtfo_order_id'] = oid
                risk['gtfo_order_qty'] = float(str_qty)
            save_bots()
        except Exception as e:
            log.error(f"[{pair}] GTFO resync failed: {e}")

    # Time-decay nibble: if score > 70 for 4+ cycles, sell 10% at market
    if score > 70:
        risk['gtfo_high_score_streak'] = risk.get('gtfo_high_score_streak', 0) + 1
        if risk['gtfo_high_score_streak'] >= 4:
            try:
                total_qty = sum(t.get('quantity', 0) for t in trails)
                nibble_qty = total_qty * 0.10
                str_qty = snap_to_increment(nibble_qty, base_inc)
                if float(str_qty) > 0:
                    cancel_all_pair_orders(pair)
                    time.sleep(0.3)
                    oid = str(uuid.uuid4())
                    client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                    # Reduce trails proportionally
                    for t in trails:
                        t['quantity'] *= 0.90
                    bot['asset_held'] = max(0, bot.get('asset_held', 0) - nibble_qty)
                    bot['current_usd'] += current_px * nibble_qty * 0.995
                    record_trade(bot, compute_weighted_avg(trails), current_px, nibble_qty, 'LONG', 'NIBBLE', pair, mult)
                    risk['gtfo_high_score_streak'] = 0
                    log.warning(f"[{pair}] TIME-DECAY NIBBLE: sold 10% ({nibble_qty:.6f}) at {current_px:.4f}")
                    save_bots()
            except Exception as e:
                log.error(f"[{pair}] Nibble failed: {e}")
    else:
        risk['gtfo_high_score_streak'] = 0


def convert_to_runners(bot, pair, current_atr, top_pct=0.25):
    """Synthetic Runner pivot: convert top 25% of inventory to wide-trail runners during STRONG_TREND+RISING."""
    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})
    trails = risk.get('per_fill_trails', [])
    if not trails or current_atr <= 0:
        return

    # Sort by fill_price descending
    trails_sorted = sorted(trails, key=lambda t: t['fill_price'], reverse=True)
    n_runners = max(1, int(len(trails_sorted) * top_pct))

    runner_trail = 2.5 * current_atr
    converted = 0
    for t in trails_sorted[:n_runners]:
        if t.get('is_runner'):
            continue  # already converted
        # Cancel its grid sell order if any
        sell_oid = t.get('sell_oid', '')
        if sell_oid:
            try:
                # Find and cancel matching grid order
                for g in list(settings.get('active_grids', [])):
                    if g.get('oid') == sell_oid and g.get('side') == 'SELL':
                        if not cancel_order_safe(g):
                            log.warning(f"[{pair}] Runner: sell cancel failed (likely filled) — keeping tracked")
                            break
                        try: settings['active_grids'].remove(g)
                        except ValueError: pass
                        break
            except Exception as e:
                log.warning(f"[{pair}] Runner cancel failed: {e}")
        t['sell_oid'] = ''
        t['sell_cb_oid'] = ''
        t['effective_trail'] = round(runner_trail, 6)
        t['base_trail_distance'] = round(runner_trail, 6)
        t['trail_multiplier'] = 1.0
        t['is_runner'] = True
        converted += 1

    if converted:
        log.info(f"[{pair}] SYNTHETIC RUNNER: converted top {converted} fills to {runner_trail:.4f} ATR-trails")
        save_bots()


def execute_grid_bot(bot_id, bot, pair):
    if bot.get('paper'):
        _paper_grid_execute(bot_id, bot, pair)
        return

    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})

    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (100 * tf_sec)
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = getattr(p_info, 'base_increment', '0.00000001')
        quote_inc = getattr(p_info, 'quote_increment', '0.01')
    except Exception as e:
        log.error(f"[{pair}] Data fetch error: {e}")
        return

    if len(candles) < 50: return

    parsed = [{'start': int(c['start']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close']), 'volume': float(c['volume'])} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    direction = compute_direction(df)
    risk['direction'] = direction

    try:
        adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
        atr_series = ta.atr(df['high'], df['low'], df['close'], length=14)
        curr_adx = float(adx_df.iloc[-1, 0]) if adx_df is not None and not adx_df.empty else 0.0
        curr_atr = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else 0.0
    except (ValueError, TypeError, IndexError) as e:
        log.debug(f"[{pair}] ADX/ATR computation failed: {e}")
        curr_adx, curr_atr = 0.0, 0.0

    # --- Grid v2/v3 dynamic calculations (when dynamic: True) ---
    is_dynamic = settings.get('dynamic', False)
    suspend_buys = False  # V3: set when GTFO active, crisis ≥50, or waterfall

    if is_dynamic:
        bb_upper, bb_lower, bb_width, bb_width_avg = compute_bb_indicators(df)
        regime = compute_regime(curr_adx, bb_width, bb_width_avg, direction)
        vol_ratio = (bb_width / bb_width_avg) if bb_width_avg > 0 else 1.0
        velocity = risk.get('recovery_velocity', 0)
        depth = len(risk.get('per_fill_trails', []))

        # V3: Compute ATR SMA(50) for dynamic quarantine
        try:
            atr_series_full = ta.atr(df['high'], df['low'], df['close'], length=14)
            atr_sma_50 = float(ta.sma(atr_series_full, 50).iloc[-1]) if len(atr_series_full) >= 50 and not pd.isna(ta.sma(atr_series_full, 50).iloc[-1]) else curr_atr
        except Exception:
            atr_sma_50 = curr_atr
        risk['atr_sma_50'] = atr_sma_50

        # V3: Update loss_history (rolling 120s window) for Velocity-of-Loss
        unrealized_loss = sum(max(0, (t['fill_price'] - cur_px) * t['quantity']) for t in risk.get('per_fill_trails', []))
        allocated = bot.get('allocated_usd', 1)
        unrealized_loss_pct = (unrealized_loss / allocated * 100) if allocated > 0 else 0
        now_ts = int(time.time())
        loss_history = risk.get('loss_history', [])
        loss_history.append((now_ts, unrealized_loss_pct))
        loss_history = [(t, lp) for t, lp in loss_history if now_ts - t <= 120]
        risk['loss_history'] = loss_history

        # Crisis scoring (now 5-factor with velocity-of-loss)
        crisis_score = compute_grid_crisis_score(depth, unrealized_loss_pct, regime, direction, velocity, loss_history)

        # Dynamic step floor must at least cover round-trip maker fees + margin —
        # a 0.3% flip at 0.25%/side maker fees is a guaranteed net loss.
        min_step_pct = max(settings.get('min_step_pct', 0.3), get_fee_floor_pct(settings))
        max_step_pct = settings.get('max_step_pct', 3.0)

        # V3: depth-aware step (exponential widening on FALLING)
        dyn_step = compute_dynamic_step(curr_atr, curr_adx, bb_width, bb_width_avg, direction, cur_px,
                                        min_step_pct, max_step_pct, depth=depth)
        dyn_trail = compute_dynamic_trail(curr_atr, curr_adx, direction, velocity)

        # V3: depth-aware Kelly (cluster-risk penalty)
        dyn_order_usd = compute_kelly_size(curr_adx, direction, vol_ratio, dyn_step, dyn_trail,
                                           bot.get('allocated_usd', 0), settings.get('min_order_usd', 5),
                                           depth=depth)

        # Store in risk dict for visibility
        risk['regime'] = regime
        risk['dyn_step'] = round(dyn_step, 2)
        risk['dyn_trail'] = round(dyn_trail, 2)
        risk['dyn_order_usd'] = round(dyn_order_usd, 2)
        risk['crisis_score'] = crisis_score
        risk['bb_upper'] = round(bb_upper, 2)
        risk['bb_lower'] = round(bb_lower, 2)
        risk['kelly_win_prob'] = round(max(0.20, min(0.80, 0.80 - (curr_adx / 100.0))), 3)

        # Override step_size for all downstream code
        settings['step_size'] = dyn_step

        deriv_flag_now = is_derivative(pair)
        mult_now = get_contract_multiplier(pair)

        # ── V3 STICKY STATE GUARD: GTFO mode ──
        if risk.get('is_gtfo_active'):
            run_gtfo_cycle(bot, pair, cur_px, curr_atr, crisis_score, base_inc, quote_inc, mult_now, deriv_flag_now)
            return  # Skip all deploy/redeploy logic while exiting

        # ── V3 QUARANTINE CHECK: don't redeploy until time elapsed AND ADX < 20 ──
        # (previously `until > now AND adx >= 20`, which let any ADX dip bypass the
        # time quarantine entirely — redeploying straight back into the decline)
        q_until = risk.get('quarantine_until', 0)
        if q_until > 0:
            if now_ts < q_until or curr_adx >= 20:
                log.debug(f"[{pair}] Quarantined until {q_until} (ADX={curr_adx:.1f}, need <20 after expiry)")
                return
            risk['quarantine_until'] = 0  # both conditions met — quarantine lifted

        # ── V3 CRISIS TRIGGER: enter GTFO at score ≥ 50 ──
        if crisis_score >= 80 and risk.get('per_fill_trails'):
            # Hard liquidation: 75% emergency dump. Sort DESCENDING — exit the
            # highest (deepest-underwater) entries first; ascending kept the worst
            # 25% and maximized remaining drawdown.
            trails_to_exit = sorted(risk['per_fill_trails'], key=lambda t: t['fill_price'], reverse=True)
            n_exit = max(1, int(len(trails_to_exit) * 0.75))
            exited = []
            for t in trails_to_exit[:n_exit]:
                try:
                    oid = str(uuid.uuid4())
                    str_qty = snap_to_increment(t['quantity'], base_inc)
                    res = client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                    if not order_success(res):
                        log.error(f"[{pair}] Crisis exit rejected: {order_error(res)} — keeping trail")
                        continue
                    record_trade(bot, t['fill_price'], cur_px, t['quantity'], 'LONG', 'CRISIS_CUT', pair, mult_now)
                    bot['asset_held'] = max(0, bot.get('asset_held', 0) - t['quantity'])
                    bot['current_usd'] += cur_px * t['quantity'] * 0.995
                    exited.append(t)
                except Exception as e:
                    log.error(f"[{pair}] Crisis exit failed: {e}")
            risk['per_fill_trails'] = [t for t in risk['per_fill_trails'] if t not in exited]
            risk['depth_score'] = len(risk['per_fill_trails'])
            log.warning(f"[{pair}] CRISIS SCORE {crisis_score}: Hard liquidation, exited {len(exited)} fills")
            save_bots()
            suspend_buys = True
        elif crisis_score >= 50 and risk.get('per_fill_trails'):
            # V3: Enter Whole-Stack GTFO mode
            entered = enter_gtfo_mode(bot, pair, cur_px, base_inc, quote_inc, mult_now, deriv_flag_now)
            if entered:
                return
            suspend_buys = True
        elif crisis_score >= 40:
            # Cancel bottom 50% buys (lighter touch). The cancel must be VERIFIED on
            # the exchange first — previously the orders were only dropped from
            # tracking and kept filling in the crash with nobody accounting for them.
            buy_grids = sorted([g for g in settings.get('active_grids', []) if g.get('side') == 'BUY'], key=lambda g: g['price'])
            n_cancel = len(buy_grids) // 2
            for g in buy_grids[:n_cancel]:
                if cancel_order_safe(g):
                    risk.setdefault('cancelled_buy_levels', []).append(g['price'])
                    settings['active_grids'].remove(g)
                else:
                    log.warning(f"[{pair}] Crisis buy-cancel failed at {g['price']:.2f} (likely filled) — keeping tracked for fill processing")
            suspend_buys = True

        # ── V3 WATERFALL CLAUSE: zero-sizing during volatility expansion ──
        if curr_adx > 35 and bb_lower > 0 and cur_px < bb_lower:
            suspend_buys = True
            log.debug(f"[{pair}] Waterfall clause: ADX={curr_adx:.1f} > 35, price below lower BB")

        # ── V3 SYNTHETIC RUNNER: convert top inventory on STRONG_TREND+RISING transition ──
        last_regime = risk.get('last_regime', '')
        if regime == 'STRONG_TREND' and direction == 'RISING' and last_regime != 'STRONG_TREND_RISING':
            convert_to_runners(bot, pair, curr_atr, top_pct=0.25)
            risk['last_regime'] = 'STRONG_TREND_RISING'
            suspend_buys = True
        else:
            risk['last_regime'] = regime + ('_RISING' if direction == 'RISING' else ('_FALLING' if direction == 'FALLING' else ''))

        # Store suspend_buys flag for downstream deploy logic
        risk['suspend_buys'] = suspend_buys

    active_grids = settings.get('active_grids', [])
    has_grids = active_grids and len(active_grids) > 0

    if has_grids:
        grid_check_fills(bot_id, bot, pair)
        active_grids = settings.get('active_grids', [])
        has_grids = active_grids and len(active_grids) > 0

    is_halted = settings.get('halted', False)
    lower = settings.get('lower_price')

    trails = risk.get('per_fill_trails', [])
    depth = len(trails)
    risk['depth_score'] = depth

    if check_circuit_breaker(bot, cur_px, pair): return

    # --- Trailing stop check (REST fallback, runs even if WS is down) ---
    if check_trailing_stops(bot, cur_px, pair):
        # Re-read state after potential sells
        active_grids = settings.get('active_grids', [])
        has_grids = active_grids and len(active_grids) > 0
        trails = risk.get('per_fill_trails', [])
        depth = len(trails)
        risk['depth_score'] = depth

    if has_grids and not is_halted:
        should_halt = False
        halt_reason = ''
        halt_mode = 'NEUTRAL'

        if curr_adx >= 25:
            should_halt = True
            if direction == 'RISING': halt_mode = 'FAVORABLE'
            elif direction == 'FALLING': halt_mode = 'ADVERSE'
            halt_reason = f"ADX={curr_adx:.1f} >= 25. Mode: {halt_mode}. Direction: {direction}."

        if not should_halt and lower and curr_atr > 0:
            tail_level = lower - (2.0 * curr_atr)
            if cur_px < tail_level:
                should_halt = True
                halt_mode = 'ADVERSE'
                halt_reason = f"Tail risk: price {cur_px:.2f} < {tail_level:.2f} (floor - 2*ATR)"

        if should_halt:
            held = abs(bot.get('asset_held', 0))
            
            # DORMANT BYPASS: no position, just abort grid
            if depth == 0 and held < 0.000001:
                log.warning(f"[{pair}] Aborting deployed grid (0 depth). Reverting to DORMANT. Reason: {halt_reason}")
                cancel_all_pair_orders(pair)
                settings.pop('active_grids', None)
                settings.pop('step_size', None)
                settings.pop('chunk_size', None)
                save_bots()
                return

            risk['halt_mode'] = halt_mode
            grid_emergency_halt(bot_id, bot, pair, cur_px, halt_reason, halt_mode)
            adjust_trail_multipliers(bot, halt_mode, depth)
            save_bots()
            return

    if is_halted:
        halt_mode = risk.get('halt_mode', 'NEUTRAL')
        halt_trigger_px = risk.get('halt_trigger_price', cur_px)
        streak = risk.get('direction_streak', 0)
        last_dir = risk.get('last_streak_direction', 'CHOPPY')

        if direction == last_dir and direction != 'CHOPPY': streak += 1
        elif direction != last_dir: streak = 1 if direction != 'CHOPPY' else 0
        risk['direction_streak'] = streak
        risk['last_streak_direction'] = direction

        new_mode = halt_mode

        if halt_mode == 'ADVERSE':
            if cur_px > halt_trigger_px:
                new_mode = 'NEUTRAL'
                risk['direction_streak'] = 0
                log.info(f"[{pair}] Deadband ADVERSE -> NEUTRAL: price {cur_px:.2f} > trigger {halt_trigger_px:.2f}")
        elif halt_mode == 'NEUTRAL':
            if direction == 'RISING' and streak >= 3:
                new_mode = 'FAVORABLE'
                risk['direction_streak'] = 0
                log.info(f"[{pair}] Deadband NEUTRAL -> FAVORABLE: 3 consecutive RISING")
            elif cur_px < halt_trigger_px or (direction == 'FALLING' and streak >= 2):
                new_mode = 'ADVERSE'
                risk['direction_streak'] = 0
                reason = f"price {cur_px:.2f} < trigger {halt_trigger_px:.2f}" if cur_px < halt_trigger_px else "2 consecutive FALLING"
                log.warning(f"[{pair}] Deadband NEUTRAL -> ADVERSE: {reason}")
        elif halt_mode == 'FAVORABLE':
            if direction == 'FALLING' and streak >= 3:
                new_mode = 'ADVERSE'
                risk['direction_streak'] = 0
                risk['halt_trigger_price'] = cur_px
                log.warning(f"[{pair}] Deadband FAVORABLE -> ADVERSE: 3 consecutive FALLING (fast path)")
            elif direction == 'FALLING' and streak >= 2:
                new_mode = 'NEUTRAL'
                risk['direction_streak'] = 0
                log.info(f"[{pair}] Deadband FAVORABLE -> NEUTRAL: 2 consecutive FALLING")

        halt_mode = new_mode
        risk['halt_mode'] = halt_mode

        adjust_trail_multipliers(bot, halt_mode, depth)

        if halt_mode in ('ADVERSE', 'NEUTRAL'):
            buy_grids = [g for g in (active_grids or []) if g['side'] == 'BUY']
            if buy_grids:
                cancelled_prices = risk.setdefault('cancelled_buy_levels', [])
                count = 0
                for g in list(buy_grids):
                    if cancel_order_safe(g):
                        cancelled_prices.append(g['price'])
                        if g in active_grids: active_grids.remove(g)
                        count += 1
                if count: log.warning(f"[{pair}] {halt_mode}: Cancelled {count} buys")

        if has_grids: grid_check_fills(bot_id, bot, pair)

        active_grids = settings.get('active_grids', [])
        trails = risk.get('per_fill_trails', [])
        depth = len(trails)
        risk['depth_score'] = depth
        held = abs(bot.get('asset_held', 0))

        # --- DUST CHECK: treat trivial inventory as zero ---
        # Floating-point residuals from fill math can leave dust (e.g. 0.000003)
        # that blocks halt clearing forever. If inventory value < $0.10, sweep it.
        dust_usd = held * cur_px * get_contract_multiplier(pair)
        if held > 0 and dust_usd < 0.10:
            log.debug(f"[{pair}] Sweeping dust: {held:.8f} units (${dust_usd:.4f}). Zeroing out.")
            bot['asset_held'] = 0.0
            held = 0.0
            # Clear any orphan trails that might reference this dust
            if depth > 0:
                risk['per_fill_trails'] = []
                depth = 0
                risk['depth_score'] = 0

        if depth == 0 and held < 0.000001:
            if curr_adx < 25:
                settings.pop('halted', None); settings.pop('halted_reason', None); settings.pop('halted_at', None)
                risk['halt_mode'] = None; risk['cancelled_buy_levels'] = []
                settings.pop('active_grids', None); settings.pop('step_size', None); settings.pop('chunk_size', None)
                save_bots()
                log.info(f"[{pair}] Halt cleared. ADX={curr_adx:.1f}. Ready to redeploy.")
            else:
                log.debug(f"[{pair}] Waiting for ADX < 25 to clear halt (current ADX={curr_adx:.1f})")
        elif held > 0.000001 and depth == 0:
            remaining = settings.get('active_grids', [])
            has_active_sell = any(g['side'] == 'SELL' for g in remaining)

            if not has_active_sell:
                # Try limit sell first
                step_size = settings.get('step_size', cur_px * 0.006)
                exit_px = cur_px + (step_size * 0.5)
                deriv_flag = is_derivative(pair)
                mult = get_contract_multiplier(pair)
                min_gap = step_size * 0.4
                safe_px = find_safe_price(exit_px, remaining, min_gap, direction='up')
                if safe_px: exit_px = safe_px

                exit_grid = place_grid_sell(pair, exit_px, held, base_inc, quote_inc, deriv_flag, mult)
                if exit_grid:
                    remaining.append(exit_grid)
                    save_bots()
                    log.info(f"[{pair}] Re-placed exit SELL at {exit_px:.2f}")
                elif curr_adx < 25:
                    # Limit sell failed AND ADX is clear — market sell to unblock
                    log.warning(f"[{pair}] Limit sell failed. ADX={curr_adx:.1f} < 25. Market-selling {held:.8f} to clear halt.")
                    try:
                        oid = str(uuid.uuid4())
                        str_qty = snap_to_increment(held, base_inc)
                        client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                        mult = get_contract_multiplier(pair)
                        record_trade(bot, cur_px, cur_px, held, 'LONG', 'HALT_CLEAR', pair, mult)
                        bot['current_usd'] += held * cur_px * 0.995
                        bot['asset_held'] = 0.0
                        save_bots()
                    except Exception as e:
                        log.error(f"[{pair}] Market sell for halt clear failed: {e}")
            else:
                log.debug(f"[{pair}] Waiting: held={held:.8f} depth={depth} sells_active={has_active_sell} ADX={curr_adx:.1f}")
        else:
            log.debug(f"[{pair}] Waiting: held={held:.8f} depth={depth} ADX={curr_adx:.1f} mode={halt_mode}")

        save_bots()
        return

    compute_recovery_velocity(risk, bot)
    adjust_trail_multipliers(bot, None, depth)

    if depth >= 4: evaluate_depth_escalation(bot, pair, direction, cur_px)
    if risk.get('recovery_velocity', 0) >= 2.0: manage_runner_exits(bot, pair, cur_px)

    risk['risk_current'] = round(sum(t.get('effective_trail', t.get('base_trail_distance', 0)) * t['quantity'] for t in risk.get('per_fill_trails', [])), 4) if risk.get('per_fill_trails') else risk.get('risk_max', 0)

    step_size = settings.get('step_size', 0)
    chunk_usd = settings.get('chunk_size', 0)
    deriv_flag = is_derivative(pair)
    mult_val = get_contract_multiplier(pair)

    # V3: respect suspend_buys flag from dynamic block (GTFO/crisis/waterfall)
    suspend_buys_v3 = risk.get('suspend_buys', False)
    if risk.get('cancelled_buy_levels') and not suspend_buys_v3:
        evaluate_buy_redeployment(bot, pair, direction, cur_px, step_size, base_inc, quote_inc, deriv_flag, mult_val, chunk_usd)

    if has_grids:
        if settings.get('follow', False) and not suspend_buys_v3:
            if depth > 3: log.warning(f"[{pair}] Follow BLOCKED: depth={depth} > 3")
            else: grid_follow(bot_id, bot, pair, cur_px, df)
        return

    if curr_adx >= 25:
        log.debug(f"[{pair}] DORMANT: ADX={curr_adx:.1f} >= 25. Waiting to deploy.")
        return

    # V3: don't deploy fresh grid while crisis/GTFO/waterfall conditions block buys
    if suspend_buys_v3:
        log.debug(f"[{pair}] Fresh deploy blocked: suspend_buys (crisis/waterfall/runner)")
        return

    orphans_killed = cancel_all_pair_orders(pair)
    if orphans_killed > 0:
        log.info(f"[{pair}] Swept {orphans_killed} orphan orders.")
        time.sleep(0.5)

    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)
    upper = settings.get('upper_price')
    grid_count = settings.get('grid_count')
    mode = settings.get('mode', 'LONG').upper()
    step_pct = settings.get('step_pct', 0.6) / 100.0

    if not lower or not upper or upper <= lower:
        log.error(f"[{pair}] Invalid floor/ceiling: {lower}/{upper}. Reconfigure.")
        return

    step = cur_px * step_pct

    # Dynamic mode: override step, bounds, and chunk size
    if is_dynamic:
        step = risk.get('dyn_step', step)
        # Bollinger-based grid range
        dyn_bb_upper = risk.get('bb_upper', 0)
        dyn_bb_lower = risk.get('bb_lower', 0)
        if dyn_bb_upper > 0 and dyn_bb_lower > 0:
            lower = dyn_bb_lower - (0.5 * curr_atr)
            upper = dyn_bb_upper + (0.5 * curr_atr)
            settings['lower_price'] = lower
            settings['upper_price'] = upper

    new_grids = []

    settings['base_inc'] = str(base_inc)
    settings['quote_inc'] = str(quote_inc)

    buy_levels, sell_levels = [], []
    level = lower
    while level <= upper:
        if level < cur_px * 0.999: buy_levels.append(level)
        elif level > cur_px * 1.001: sell_levels.append(level)
        level += step

    if mode == 'LONG': sell_levels = []
    elif mode == 'SHORT':
        buy_levels = []
        if not deriv_flag: log.warning(f"[{pair}] SHORT mode on spot requires inventory.")

    total_orders = len(buy_levels) + len(sell_levels)
    if total_orders == 0:
        log.info(f"[{pair}] No valid levels in old range. Auto-recentering on {cur_px:.2f}")
        grid_count = settings.get('grid_count', max(2, int(bot['current_usd'] / settings.get('min_order_usd', 5))))
        if mode == 'LONG':
            lower = cur_px - (grid_count * step)
            upper = cur_px
        elif mode == 'SHORT':
            lower = cur_px
            upper = cur_px + (grid_count * step)
        else:
            half = grid_count // 2
            lower = cur_px - (half * step)
            upper = cur_px + ((grid_count - half) * step)
        settings['lower_price'] = lower
        settings['upper_price'] = upper
        buy_levels, sell_levels = [], []
        level = lower
        while level <= upper:
            if level < cur_px * 0.999: buy_levels.append(level)
            elif level > cur_px * 1.001: sell_levels.append(level)
            level += step
        if mode == 'LONG': sell_levels = []
        elif mode == 'SHORT': buy_levels = []
        total_orders = len(buy_levels) + len(sell_levels)
        if total_orders == 0:
            log.warning(f"[{pair}] Still no valid levels after recenter. Waiting.")
            return
        log.info(f"[{pair}] Recentered: {lower:.2f} - {upper:.2f}, {total_orders} levels")

    chunk_size_usd = bot['current_usd'] / total_orders
    # Dynamic mode: Kelly-based order sizing — capped so the SUM across all levels
    # can never exceed available capital (10%/order x 15 levels = 150% before)
    if is_dynamic and risk.get('dyn_order_usd', 0) > 0:
        chunk_size_usd = min(risk['dyn_order_usd'], bot['current_usd'] / total_orders)

    # GRID-H5: warn when the configured step can't cover round-trip maker fees
    fee_floor = get_fee_floor_pct(settings)
    step_pct_actual = (step / cur_px) * 100 if cur_px > 0 else 0
    if step_pct_actual > 0 and step_pct_actual < fee_floor:
        log.warning(f"[{pair}] Grid step {step_pct_actual:.2f}% is BELOW the fee floor "
                    f"{fee_floor:.2f}% (2x maker fee + margin) — every flip loses money. "
                    f"Set settings['maker_fee_pct'] to your real fee tier or widen the step.")

    max_loss = init_risk_state(settings, buy_levels, step, chunk_size_usd, cur_px)
    alloc = bot.get('allocated_usd', 0) or 0
    pct_str = f" ({max_loss/alloc*100:.1f}% of capital)" if alloc > 0 else ""
    log.info(f"[{pair}] Max loss envelope: ${max_loss:.2f}{pct_str}")

    for idx, price in enumerate(buy_levels):
        g = place_grid_buy(pair, price, chunk_size_usd, base_inc, quote_inc, deriv_flag, mult)
        if g:
            g['level_idx'] = idx
            new_grids.append(g)

    if sell_levels:
        if deriv_flag:
            for price in sell_levels:
                g = place_grid_sell(pair, price, 0, base_inc, quote_inc, deriv_flag, mult, use_chunk=True, chunk_usd=chunk_size_usd)
                if g: new_grids.append(g)
        else:
            total_sell_qty = sum(float(chunk_size_usd * 0.99) / p for p in sell_levels)
            total_sell_cost = total_sell_qty * cur_px
            if total_sell_cost <= bot['current_usd'] * 0.95:
                try:
                    buy_oid = str(uuid.uuid4())
                    res = client.market_order_buy(client_order_id=buy_oid, product_id=pair, quote_size=str(round(total_sell_cost * 0.99, 2)))
                    if not order_success(res):
                        log.error(f"[{pair}] Inventory buy rejected: {order_error(res)} — skipping sell side")
                        raise RuntimeError('seed buy rejected')
                    fill_px, fill_qty, fill_fee = poll_market_fill(buy_oid, pair)
                    seed_px = fill_px if fill_px else cur_px
                    seed_qty = fill_qty if fill_qty else total_sell_qty
                    seed_fee = fill_fee if fill_fee is not None else total_sell_cost * 0.0025
                    bot['asset_held'] += seed_qty
                    bot['current_usd'] -= (seed_qty * seed_px + seed_fee)
                    log.info(f"[{pair}] Market bought {seed_qty:.6f} inventory for sell grid @ {seed_px:.4f}")
                    time.sleep(1)
                    risk_state = settings.setdefault('risk', {})
                    total_buy_levels = risk_state.get('total_buy_levels', len(buy_levels) or 10)
                    per_level_qty = seed_qty / len(sell_levels) if sell_levels else 0
                    for price in sell_levels:
                        qty = float(chunk_size_usd * 0.99) / price
                        g = place_grid_sell(pair, price, qty, base_inc, quote_inc, deriv_flag, mult)
                        if g:
                            g['entry_price'] = seed_px  # honest flip PnL for seed inventory
                            new_grids.append(g)
                            # GRID-H4: seed inventory must be trailed and visible to the
                            # circuit breaker like every other held fill
                            activate_trail(bot, seed_px, per_level_qty, 0, total_buy_levels, step, sell_grid=g)
                except RuntimeError:
                    pass
                except Exception as e:
                    log.error(f"[{pair}] Inventory buy failed: {e}")
            else:
                log.warning(f"[{pair}] Insufficient capital for sell inventory. Skipping sell side.")

    if new_grids:
        bot['settings']['active_grids'] = new_grids
        bot['settings']['step_size'] = step
        bot['settings']['chunk_size'] = chunk_size_usd
        save_bots()
        log.info(f"[{pair}] Deployed {len(new_grids)} levels ({mode} mode, {step_pct*100:.1f}% step, follow={'ON' if settings.get('follow') else 'OFF'})")
