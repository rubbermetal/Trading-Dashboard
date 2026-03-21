# grid_engine.py
import time
import uuid
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone

from shared import client, ACTIVE_BOTS
from bot_utils import (
    get_bot_tf, is_derivative, get_contract_multiplier, 
    snap_to_increment, record_trade, save_bots
)

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
                print(f"[GRID] Batch cancel error: {e}")
            time.sleep(0.2)
        print(f"[GRID] cancel_all_pair_orders({pair}): {cancelled}/{len(real_ids)} cancelled")
    except Exception as e:
        print(f"[GRID] cancel_all_pair_orders({pair}) fetch error: {e}")
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
            print(f"[GRID] Cancel by cb_oid failed for {cb_oid}: {e}")

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
            print(f"[GRID] Cancel by client_oid lookup failed for {client_oid}: {e}")

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
            print(f"[GRID] BUY rejected at {str_price}: {fail_reason}")
    except Exception as e:
        print(f"[GRID] BUY exception at {str_price}: {e}")
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
            print(f"[GRID] SELL rejected at {str_price}: {fail_reason}")
    except Exception as e:
        print(f"[GRID] SELL exception at {str_price}: {e}")
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
        print(f"[GRID REST | {pair}] Fill check API error: {e}")
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
        
        if grid_oid: _processed_fill_oids.add(grid_oid)
        if grid_cb_oid: _processed_fill_oids.add(grid_cb_oid)
        fill_server_id = filled_match.get('order_id', '')
        if fill_server_id: _processed_fill_oids.add(fill_server_id)
        if len(_processed_fill_oids) > 500: _processed_fill_oids.clear()

        filled_size = float(filled_match.get('filled_size', 0))
        avg_price = float(filled_match.get('average_filled_price', grid['price']))
        filled_value = filled_size * avg_price
        
        if filled_size <= 0: continue

        print(f"[GRID REST | {pair}] Fill detected: {grid['side']} at {grid['price']:.2f}")

        if grid['side'] == 'BUY':
            new_price = grid['price'] + step_size
            # SPACING GUARD: nudge sell price up if too close to existing order
            safe_price = find_safe_price(new_price, active_grids, min_gap, direction='up')
            if safe_price is None:
                # FIX #1: No safe price found — skip placement, hold inventory with trail only
                print(f"[GRID REST | {pair}] No safe sell price near {new_price:.2f}. Holding with trail only.")
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value
                changes_made = True

                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels, step_size)
                continue
            if safe_price != new_price:
                print(f"[GRID REST | {pair}] Sell nudged {new_price:.2f} -> {safe_price:.2f} (spacing guard)")
            new_price = safe_price

            new_grid = place_grid_sell(pair, new_price, filled_size, base_inc, quote_inc, deriv_flag, mult)
            
            if new_grid:
                try:
                    idx = active_grids.index(grid)
                    active_grids[idx] = new_grid
                except ValueError: active_grids.append(new_grid)
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value
                changes_made = True

                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels, step_size, sell_grid=new_grid)
                print(f"[GRID REST | {pair}] BUY filled -> SELL at {new_price:.2f} (trail active, depth={risk.get('depth_score', 0)})")
            else:
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value
                changes_made = True

                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels, step_size)
                print(f"[GRID REST | {pair}] BUY filled, SELL flip failed. Trail active, inventory held.")

        elif grid['side'] == 'SELL':
            buy_price = grid['price'] - step_size
            record_trade(bot, buy_price, grid['price'], filled_size, 'LONG', 'GRID_FLIP', pair, mult)
            deactivate_trail_by_sell(bot, sell_oid=grid.get('oid'), sell_cb_oid=grid.get('cb_oid'))

            if is_halted:
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] -= filled_size
                bot['current_usd'] += filled_value
                changes_made = True
                print(f"[GRID REST | {pair}] SELL filled during halt. Depth now={bot['settings'].get('risk', {}).get('depth_score', 0)}")
            else:
                new_price = grid['price'] - step_size
                # SPACING GUARD: nudge buy price down if too close to existing order
                safe_price = find_safe_price(new_price, active_grids, min_gap, direction='down')
                if safe_price is None:
                    # FIX #1: No safe price — record the level for redeployment later
                    print(f"[GRID REST | {pair}] No safe buy price near {new_price:.2f}. Level queued for redeployment.")
                    try: active_grids.remove(grid)
                    except ValueError: pass
                    bot['asset_held'] -= filled_size
                    bot['current_usd'] += filled_value
                    risk = bot['settings'].setdefault('risk', {})
                    risk.setdefault('cancelled_buy_levels', []).append(new_price)
                    changes_made = True
                    continue
                if safe_price != new_price:
                    print(f"[GRID REST | {pair}] Buy nudged {new_price:.2f} -> {safe_price:.2f} (spacing guard)")
                new_price = safe_price

                new_grid = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
                
                if new_grid:
                    try:
                        idx = active_grids.index(grid)
                        active_grids[idx] = new_grid
                    except ValueError: active_grids.append(new_grid)
                    bot['asset_held'] -= filled_size
                    bot['current_usd'] += filled_value
                    changes_made = True
                    print(f"[GRID REST | {pair}] Flipped SELL -> BUY at {new_price:.2f}")
                else:
                    try: active_grids.remove(grid)
                    except ValueError: pass
                    bot['asset_held'] -= filled_size
                    bot['current_usd'] += filled_value
                    changes_made = True

    if changes_made:
        save_bots()

def grid_emergency_halt(bot_id, bot, pair, cur_px, reason, halt_mode='NEUTRAL'):
    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})
    active_grids = settings.get('active_grids', [])
    base_inc = settings.get('base_inc', '0.00000001')
    quote_inc = settings.get('quote_inc', '0.01')
    step_size = settings.get('step_size', 0)
    mult = get_contract_multiplier(pair)
    deriv_flag = is_derivative(pair)

    print(f"[GRID HALT | {pair}] {reason}")

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
        print(f"[GRID HALT | {pair}] FAVORABLE: Cancelled {cancelled} buys (precautionary). Keeping {len(sell_grids)} sells. Widened trails.")
    else:
        print(f"[GRID HALT | {pair}] {halt_mode}: Cancelled {cancelled} buys. Keeping {len(sell_grids)} sells.")

    held = abs(bot.get('asset_held', 0))
    if held > 0 and not any(g['side'] == 'SELL' for g in active_grids) and step_size > 0:
        exit_px = cur_px + (step_size * 0.5)
        # SPACING GUARD on exit sell
        min_gap = step_size * 0.4
        safe_px = find_safe_price(exit_px, active_grids, min_gap, direction='up')
        if safe_px:
            exit_px = safe_px
        else:
            print(f"[GRID HALT | {pair}] No safe exit sell price found near {exit_px:.2f}. Trail-only exit.")
        if safe_px:
            exit_grid = place_grid_sell(pair, exit_px, held, base_inc, quote_inc, deriv_flag, mult)
            if exit_grid:
                active_grids.append(exit_grid)
                print(f"[GRID HALT | {pair}] Placed exit SELL at {exit_px:.2f}")

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
        
        # Calculate new price: grid-aligned, one step above current highest
        new_buy_px = grid_high + step_size
        # If that's still below price, step up until we're just below cur_px
        while new_buy_px + step_size < cur_px:
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
                print(f"[GRID FOLLOW | {pair}] Recycled BUY {old_grid['price']:.2f} -> {new_buy_px:.2f}")
            else:
                # FIX #2: Placement failed after cancel — queue level for redeployment
                print(f"[GRID FOLLOW | {pair}] Recycle placement failed at {new_buy_px:.2f}. Queued for redeployment.")
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
        
        # Grid-aligned: one step below current lowest
        new_buy_px = grid_low - step_size
        while new_buy_px - step_size > cur_px:
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
                print(f"[GRID FOLLOW | {pair}] Followed DOWN: BUY {old_grid['price']:.2f} -> {new_buy_px:.2f}")
            else:
                # FIX #2: Placement failed after cancel — queue level for redeployment
                print(f"[GRID FOLLOW | {pair}] Follow-down placement failed at {new_buy_px:.2f}. Queued for redeployment.")
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
    except:
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

    settings['risk'] = {
        "depth_score": 0, "direction": "CHOPPY", "halt_mode": None,
        "risk_current": round(max_loss, 4), "risk_max": round(max_loss, 4),
        "circuit_breaker_price": round(cb_price, 2), "per_fill_trails": [],
        "cancelled_buy_levels": [], "recovery_timestamps": [],
        "recovery_velocity": 0.0, "total_buy_levels": total,
    }
    return max_loss

def activate_trail(bot, fill_price, quantity, level_index, total_levels, step_size, sell_grid=None):
    risk = bot['settings'].setdefault('risk', {})
    trails = risk.setdefault('per_fill_trails', [])
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

def compute_recovery_velocity(risk):
    timestamps = risk.get('recovery_timestamps', [])
    now = time.time()
    recent = [ts for ts in timestamps if now - ts < 300]
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
            print(f"[RISK ENGINE | {pair}] CRITICAL depth={depth}: Cancelled {count} open buys")
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
            print(f"[RISK ENGINE | {pair}] ELEVATED depth={depth} FALLING: Cancelled {count} lowest buys")
            save_bots()

def evaluate_buy_redeployment(bot, pair, direction, cur_px, step_size, base_inc, quote_inc, deriv_flag, mult, chunk_usd):
    risk = bot['settings'].get('risk', {})
    depth = risk.get('depth_score', 0)
    cancelled = risk.get('cancelled_buy_levels', [])
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])

    if not cancelled or step_size <= 0 or chunk_usd <= 0: return
    if direction == 'FALLING' and depth > 3: return
    if depth > 3: return

    total_levels = risk.get('total_buy_levels', 10)
    deployed = 0
    max_per_cycle = min(len(cancelled), 3)
    min_gap = step_size * 0.4

    for i in range(max_per_cycle):
        new_price = cur_px - (step_size * (i + 1))
        # SPACING GUARD: nudge down if too close to any existing order
        safe_px = find_safe_price(new_price, active_grids, min_gap, direction='down')
        if not safe_px:
            print(f"[RISK ENGINE | {pair}] Redeployment skipped at ~{new_price:.2f}: no safe price")
            continue
        new_price = safe_px

        g = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
        if g:
            g['level_idx'] = max(0, total_levels - 1)
            active_grids.append(g)
            deployed += 1

    if deployed:
        risk['cancelled_buy_levels'] = cancelled[deployed:]
        save_bots()
        print(f"[RISK ENGINE | {pair}] Redeployed {deployed} buys. {len(cancelled) - deployed} still cancelled.")

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
        print(f"[CIRCUIT BREAKER | {pair}] TRIGGERED: ${total_loss:.2f} = {loss_pct*100:.1f}% of ${allocated:.2f}")
        cancel_all_pair_orders(pair)
        time.sleep(0.3)

        held = abs(bot.get('asset_held', 0))
        if held > 0.000001:
            try:
                base_inc = settings.get('base_inc', '0.00000001')
                str_qty = snap_to_increment(held, base_inc)
                oid = str(uuid.uuid4())
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                mult = get_contract_multiplier(pair)
                for t in trails: record_trade(bot, t['fill_price'], cur_px, t['quantity'], 'LONG', 'CIRCUIT_BREAKER', pair, mult)
                bot['asset_held'] = 0.0
                bot['current_usd'] += held * cur_px * 0.995
            except Exception as e:
                print(f"[CIRCUIT BREAKER | {pair}] Sell failed: {e}")

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
                    cancel_order_safe(g)
                    active_grids.remove(g)
                    t['sell_oid'] = ''
                    t['sell_cb_oid'] = ''
                    converted += 1
                    print(f"[RISK ENGINE | {pair}] RUNNER: fill@{t['fill_price']:.2f} now +{profit_steps:.1f} steps. Sell cancelled, trailing only.")
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
        print(f"[RISK ENGINE | {pair}] TRAILING STOP: fill@{t['fill_price']:.2f} "
              f"HWM={t['high_water_mark']:.2f} trail={effective:.2f} exit@{cur_px:.2f}")
        try:
            oid = str(uuid.uuid4())
            str_qty = snap_to_increment(qty, base_inc)
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

            record_trade(bot, t['fill_price'], cur_px, qty, 'LONG', 'TRAILING_STOP', pair, mult)
            bot['asset_held'] -= qty
            bot['current_usd'] += qty * cur_px * 0.995

            # Cancel the corresponding grid sell order
            for g in list(active_grids):
                if ((t.get('sell_oid') and g.get('oid') == t['sell_oid']) or
                    (t.get('sell_cb_oid') and g.get('cb_oid') == t['sell_cb_oid'])):
                    cancel_order_safe(g)
                    active_grids.remove(g)
                    break
        except Exception as e:
            print(f"[RISK ENGINE | {pair}] Trail stop sell failed: {e}")
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

def execute_grid_bot(bot_id, bot, pair):
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
        print(f"[GRID BOT | {pair}] Data fetch error: {e}")
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
    except:
        curr_adx, curr_atr = 0.0, 0.0

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
                print(f"[GRID BOT | {pair}] Aborting deployed grid (0 depth). Reverting to DORMANT. Reason: {halt_reason}")
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
                print(f"[DEADBAND | {pair}] ADVERSE -> NEUTRAL: price {cur_px:.2f} > trigger {halt_trigger_px:.2f}")
        elif halt_mode == 'NEUTRAL':
            if direction == 'RISING' and streak >= 3:
                new_mode = 'FAVORABLE'
                risk['direction_streak'] = 0
                print(f"[DEADBAND | {pair}] NEUTRAL -> FAVORABLE: 3 consecutive RISING")
            elif cur_px < halt_trigger_px or (direction == 'FALLING' and streak >= 2):
                new_mode = 'ADVERSE'
                risk['direction_streak'] = 0
                reason = f"price {cur_px:.2f} < trigger {halt_trigger_px:.2f}" if cur_px < halt_trigger_px else "2 consecutive FALLING"
                print(f"[DEADBAND | {pair}] NEUTRAL -> ADVERSE: {reason}")
        elif halt_mode == 'FAVORABLE':
            if direction == 'FALLING' and streak >= 3:
                new_mode = 'ADVERSE'
                risk['direction_streak'] = 0
                risk['halt_trigger_price'] = cur_px
                print(f"[DEADBAND | {pair}] FAVORABLE -> ADVERSE: 3 consecutive FALLING (fast path)")
            elif direction == 'FALLING' and streak >= 2:
                new_mode = 'NEUTRAL'
                risk['direction_streak'] = 0
                print(f"[DEADBAND | {pair}] FAVORABLE -> NEUTRAL: 2 consecutive FALLING")

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
                if count: print(f"[GRID HALT | {pair}] {halt_mode}: Cancelled {count} buys")

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
            print(f"[GRID HALT | {pair}] Sweeping dust: {held:.8f} units (${dust_usd:.4f}). Zeroing out.")
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
                print(f"[GRID BOT | {pair}] Halt cleared. ADX={curr_adx:.1f}. Ready to redeploy.")
            else:
                print(f"[GRID HALT | {pair}] Waiting for ADX < 25 to clear halt (current ADX={curr_adx:.1f})")
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
                    print(f"[GRID HALT | {pair}] Re-placed exit SELL at {exit_px:.2f}")
                elif curr_adx < 25:
                    # Limit sell failed AND ADX is clear — market sell to unblock
                    print(f"[GRID HALT | {pair}] Limit sell failed. ADX={curr_adx:.1f} < 25. Market-selling {held:.8f} to clear halt.")
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
                        print(f"[GRID HALT | {pair}] Market sell for halt clear failed: {e}")
            else:
                print(f"[GRID HALT | {pair}] Waiting: held={held:.8f} depth={depth} sells_active={has_active_sell} ADX={curr_adx:.1f}")
        else:
            print(f"[GRID HALT | {pair}] Waiting: held={held:.8f} depth={depth} ADX={curr_adx:.1f} mode={halt_mode}")

        save_bots()
        return

    compute_recovery_velocity(risk)
    adjust_trail_multipliers(bot, None, depth)

    if depth >= 4: evaluate_depth_escalation(bot, pair, direction, cur_px)
    if risk.get('recovery_velocity', 0) >= 2.0: manage_runner_exits(bot, pair, cur_px)

    risk['risk_current'] = round(sum(t.get('effective_trail', t.get('base_trail_distance', 0)) * t['quantity'] for t in risk.get('per_fill_trails', [])), 4) if risk.get('per_fill_trails') else risk.get('risk_max', 0)

    step_size = settings.get('step_size', 0)
    chunk_usd = settings.get('chunk_size', 0)
    deriv_flag = is_derivative(pair)
    mult_val = get_contract_multiplier(pair)

    if risk.get('cancelled_buy_levels'):
        evaluate_buy_redeployment(bot, pair, direction, cur_px, step_size, base_inc, quote_inc, deriv_flag, mult_val, chunk_usd)

    if has_grids:
        if settings.get('follow', False):
            if depth > 3: print(f"[RISK ENGINE | {pair}] Follow BLOCKED: depth={depth} > 3")
            else: grid_follow(bot_id, bot, pair, cur_px, df)
        return

    if curr_adx >= 25:
        print(f"[GRID BOT | {pair}] DORMANT: ADX={curr_adx:.1f} >= 25. Waiting to deploy.")
        return

    orphans_killed = cancel_all_pair_orders(pair)
    if orphans_killed > 0:
        print(f"[GRID INIT | {pair}] Swept {orphans_killed} orphan orders.")
        time.sleep(0.5)

    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)
    upper = settings.get('upper_price')
    grid_count = settings.get('grid_count')
    mode = settings.get('mode', 'LONG').upper()
    step_pct = settings.get('step_pct', 0.6) / 100.0

    if not lower or not upper or upper <= lower:
        print(f"[GRID BOT | {pair}] Invalid floor/ceiling: {lower}/{upper}. Reconfigure.")
        return

    step = cur_px * step_pct
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
        if not deriv_flag: print(f"[GRID BOT | {pair}] WARNING: SHORT mode on spot requires inventory.")

    total_orders = len(buy_levels) + len(sell_levels)
    if total_orders == 0:
        print(f"[GRID BOT | {pair}] No valid levels in old range. Auto-recentering on {cur_px:.2f}")
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
            print(f"[GRID BOT | {pair}] Still no valid levels after recenter. Waiting.")
            return
        print(f"[GRID BOT | {pair}] Recentered: {lower:.2f} - {upper:.2f}, {total_orders} levels")

    chunk_size_usd = bot['current_usd'] / total_orders

    max_loss = init_risk_state(settings, buy_levels, step, chunk_size_usd, cur_px)
    print(f"[GRID INIT | {pair}] Max loss envelope: ${max_loss:.2f} ({max_loss/bot['allocated_usd']*100:.1f}% of capital)")

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
                    client.market_order_buy(client_order_id=buy_oid, product_id=pair, quote_size=str(round(total_sell_cost * 0.99, 2)))
                    bot['asset_held'] += total_sell_qty
                    bot['current_usd'] -= total_sell_cost
                    print(f"[GRID INIT] Market bought {total_sell_qty:.6f} inventory for sell grid")
                    time.sleep(1)
                    for price in sell_levels:
                        qty = float(chunk_size_usd * 0.99) / price
                        g = place_grid_sell(pair, price, qty, base_inc, quote_inc, deriv_flag, mult)
                        if g: new_grids.append(g)
                except Exception as e:
                    print(f"[GRID INIT] Inventory buy failed: {e}")
            else:
                print(f"[GRID INIT] Insufficient capital for sell inventory. Skipping sell side.")

    if new_grids:
        bot['settings']['active_grids'] = new_grids
        bot['settings']['step_size'] = step
        bot['settings']['chunk_size'] = chunk_size_usd
        save_bots()
        print(f"[GRID BOT] Deployed {len(new_grids)} levels ({mode} mode, {step_pct*100:.1f}% step, follow={'ON' if settings.get('follow') else 'OFF'})")
