import time
import uuid
import pandas as pd
from shared import client, ACTIVE_BOTS
from notifier import notify_bot_entry, notify_bot_exit, notify_drawdown
from strategies import calculate_quad_rotation, calculate_quad_super, calculate_orb, calculate_trap, calculate_momentum, calculate_dca, calculate_npr, NPR_CONFIG, _compute_zone
from bot_utils import (
    get_bot_tf, is_derivative, get_contract_multiplier, 
    snap_to_increment, record_trade, save_bots,
    extract_fee, poll_market_fill
)


def paper_fill_buy(bot, pair, qty, price, mult):
    """Simulate a buy fill for paper trading bots."""
    filled_size = float(qty)
    fill_px = float(price)
    old_held = bot.get('asset_held', 0)
    old_cost = bot.get('total_cost', 0)
    new_cost = fill_px * filled_size * mult
    total_held = old_held + filled_size
    new_avg = (old_cost + new_cost) / (total_held * mult) if total_held > 0 else fill_px

    sim_fee = new_cost * 0.004  # simulate maker fee
    bot['asset_held'] = total_held
    bot['total_cost'] = old_cost + new_cost
    bot['avg_entry'] = new_avg
    bot['entry_price'] = new_avg
    bot['current_usd'] -= (new_cost + sim_fee)
    bot['position_side'] = 'LONG'
    bot['total_buys'] = bot.get('total_buys', 0) + 1
    bot['buy_count_this_cycle'] = bot.get('buy_count_this_cycle', 0) + 1
    bot['dca_state'] = 'ACCUMULATING'
    bot['last_cross_direction'] = 'BELOW'
    bot.pop('pending_buy_oid', None)
    bot.pop('pending_buy_time', None)
    bot.pop('buy_retries', None)
    save_bots()
    print(f"[PAPER DCA | {pair}] BUY: {filled_size:.8f} at ${fill_px:.2f}. Avg entry ${new_avg:.2f}")
    notify_bot_entry(pair, 'DCA (PAPER)', fill_px, filled_size)


def paper_fill_sell(bot, pair, tier_pct, qty, price, mult):
    """Simulate a sell fill for paper trading bots."""
    filled_size = float(qty)
    fill_px = float(price)
    sim_fee = filled_size * fill_px * mult * 0.004

    bot['asset_held'] = max(0, bot.get('asset_held', 0) - filled_size)
    old_held = bot.get('asset_held', 0) + filled_size
    if old_held > 0:
        sold_fraction = filled_size / old_held
        bot['total_cost'] = max(0, bot.get('total_cost', 0) * (1 - sold_fraction))
    gross = filled_size * fill_px * mult
    bot['current_usd'] += gross - sim_fee
    bot['highest_tier_sold'] = max(bot.get('highest_tier_sold', 0), tier_pct)
    save_bots()
    pnl = (fill_px - bot.get('avg_entry', fill_px)) * filled_size * mult
    print(f"[PAPER DCA | {pair}] SELL tier {tier_pct}%: {filled_size:.8f} at ${fill_px:.2f} PnL ${pnl:.4f}")
    notify_bot_exit(pair, 'DCA (PAPER)', fill_px, pnl, f'Tier {tier_pct}%')
    # Don't call record_trade — paper trades must not pollute permanent stats


# ==========================================
# STRATEGY EXECUTORS (NON-GRID)
# ==========================================

def execute_orb(bot_id, bot, pair):
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (288 * tf_sec) 
    
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
        candles = res.get('candles', [])
    except Exception as e:
        return
    
    if len(candles) < 50: return 
    
    # FIX #5: Include 'open' field for defensive completeness
    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close']), 'volume': float(c['volume'])} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)
    
    pos_side = bot.get('position_side', 'FLAT')
    signal, reason = calculate_orb(df, pos_side)
    current_px = float(df.iloc[-1]['close'])
    
    trail_pct = 0.015 
    activation_pct = 0.03 
    
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)
    
    if pos_side == 'LONG':
        profit_pct = (current_px - bot['entry_price']) / bot['entry_price']
        if profit_pct >= activation_pct and not bot.get('trail_active', False):
            bot['trail_active'] = True
            bot['high_water_mark'] = current_px
            print(f"[ORB BOT] {pair} Trailing Stop ACTIVATED at +{profit_pct*100:.2f}% profit!")
            save_bots()
            
        if bot.get('trail_active', False):
            high_mark = bot.get('high_water_mark', current_px)
            if current_px > high_mark:
                bot['high_water_mark'] = current_px
                save_bots()
                
            if current_px <= bot['high_water_mark'] * (1 - trail_pct):
                signal = 'EXIT_LONG'
                reason = f"Trailing Stop Hit! Secured profit (1.5% pullback from high)."

    elif pos_side == 'SHORT':
        profit_pct = (bot['entry_price'] - current_px) / bot['entry_price']
        if profit_pct >= activation_pct and not bot.get('trail_active', False):
            bot['trail_active'] = True
            bot['low_water_mark'] = current_px
            print(f"[ORB BOT] {pair} Trailing Stop ACTIVATED at +{profit_pct*100:.2f}% profit!")
            save_bots()
            
        if bot.get('trail_active', False):
            low_mark = bot.get('low_water_mark', current_px)
            if current_px < low_mark:
                bot['low_water_mark'] = current_px
                save_bots()
                
            if current_px >= bot['low_water_mark'] * (1 + trail_pct):
                signal = 'EXIT_SHORT'
                reason = f"Trailing Stop Hit! Secured profit (1.5% pullback from low)."

    if signal == 'LONG' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        print(f"[ORB BOT] {pair} LONG TRIGGERED: {reason}")
        
        if deriv_flag:
            qty = int((bot['current_usd'] * 0.99) / (current_px * mult))
            if qty < 1:
                print(f"[ORB BOT] {pair} Insufficient capital for 1 derivative contract.")
                return
        else:
            qty = round((bot['current_usd'] * 0.99) / current_px, 6)
            
        try:
            oid = str(uuid.uuid4())
            if deriv_flag:
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
            else:
                client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(bot['current_usd'] * 0.99))
                
            bot['asset_held'] = qty
            bot['current_usd'] = 0.0
            bot['position_side'] = 'LONG'
            bot['entry_price'] = current_px
            save_bots()
        except Exception as e: print(f"Order failed: {e}")
        
    elif signal == 'SHORT' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        print(f"[ORB BOT] {pair} SHORT TRIGGERED: {reason}")
        
        if deriv_flag:
            qty = int((bot['current_usd'] * 0.99) / (current_px * mult))
            if qty < 1:
                print(f"[ORB BOT] {pair} Insufficient capital for 1 derivative contract.")
                return
        else:
            print(f"[ORB BOT] WARNING: Cannot naturally short Spot pair {pair}. Skipping execution.")
            return
            
        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(qty)) 
            bot['asset_held'] = -qty 
            bot['current_usd'] = 0.0
            bot['position_side'] = 'SHORT'
            bot['entry_price'] = current_px
            save_bots()
        except Exception as e: print(f"Order failed: {e}")
        
    elif signal == 'EXIT_LONG' and pos_side == 'LONG':
        print(f"[ORB BOT] {pair} EXIT LONG: {reason}")
        exit_reason = 'TRAILING_STOP' if 'Trailing' in reason else ('STOP_LOSS' if 'Stop' in reason else 'SIGNAL')
        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(abs(bot['asset_held'])))
            
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            held = abs(bot['asset_held'])
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)
            
            profit = (actual_exit - bot['entry_price']) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot.pop('high_water_mark', None)
            bot.pop('trail_active', None)
            save_bots()
        except Exception as e: print(f"Exit failed: {e}")
        
    elif signal == 'EXIT_SHORT' and pos_side == 'SHORT':
        print(f"[ORB BOT] {pair} EXIT SHORT: {reason}")
        exit_reason = 'TRAILING_STOP' if 'Trailing' in reason else ('STOP_LOSS' if 'Stop' in reason else 'SIGNAL')
        try:
            oid = str(uuid.uuid4())
            client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(abs(bot['asset_held'])))
            
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            held = abs(bot['asset_held'])
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['entry_price'], actual_exit, held, 'SHORT', exit_reason, pair, mult, actual_fee=actual_fee)
            
            profit = (bot['entry_price'] - actual_exit) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot.pop('low_water_mark', None)
            bot.pop('trail_active', None)
            save_bots()
        except Exception as e: print(f"Exit failed: {e}")

def execute_quad(bot_id, bot, pair, mode='STANDARD'):
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (250 * tf_sec) 
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
        candles = res.get('candles', [])
    except Exception as e: return
    if len(candles) < 200: return 
    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close'])} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)
    
    if mode == 'STANDARD': signal, reason = calculate_quad_rotation(df)
    else: signal, reason = calculate_quad_super(df)
    
    current_px = float(df.iloc[-1]['close'])
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)
    
    if signal == 'BUY' and bot['current_usd'] > 5.0: 
        if deriv_flag:
            qty = int((bot['current_usd'] * 0.99) / (current_px * mult))
            if qty < 1: return
        else:
            qty = round((bot['current_usd'] * 0.99) / current_px, 6) 
            
        try:
            oid = str(uuid.uuid4())
            if deriv_flag:
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
            else:
                client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(bot['current_usd'] * 0.99))
                
            bot['asset_held'] += qty
            bot['current_usd'] = 0.0
            bot['entry_price'] = current_px
            bot['position_side'] = 'LONG'
            save_bots()
        except Exception as e: print(f"Order failed: {e}")
        
    elif signal == 'SELL' and bot['asset_held'] > 0:
        try:
            oid = str(uuid.uuid4())
            held = bot['asset_held']
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(held))
            
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            entry_px = bot.get('entry_price', current_px)
            record_trade(bot, entry_px, actual_exit, held, 'LONG', 'SIGNAL', pair, mult, actual_fee=actual_fee)
            
            profit = (actual_exit - entry_px) * held * mult
            bot['current_usd'] += profit - actual_fee + (entry_px * held * mult)
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            save_bots()
        except Exception as e: print(f"Order failed: {e}")

def execute_trap(bot_id, bot, pair):
    """
    TRAP: Consolidation Squeeze -> Momentum Breakout
    Two-stage entry (25% breakout, 75% pullback add).
    TP 2.5%, SL 1x ATR.
    """
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (250 * tf_sec)
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
    except Exception as e:
        print(f"[TRAP BOT | {pair}] Candle fetch error: {e}")
        return
    if len(candles) < 210: return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    pos_side = bot.get('position_side', 'FLAT')
    entry_stage = bot.get('entry_stage', 0)
    avg_entry = bot.get('avg_entry', 0.0)
    breakout_data = bot.get('breakout_data', None)
    current_px = float(df.iloc[-1]['close'])

    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)

    signal, reason, bo_data = calculate_trap(df, pos_side, entry_stage, avg_entry, breakout_data)
    print(f"[TRAP BOT | {pair}] {signal}: {reason}")

    # --- BREAKOUT ENTRY (25% of capital) ---
    if signal == 'BREAKOUT_LONG' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        alloc = bot['current_usd'] * 0.25
        if deriv_flag:
            qty = int((alloc * 0.99) / (current_px * mult))
            if qty < 1:
                return
        else:
            qty = round((alloc * 0.99) / current_px, 6)

        try:
            oid = str(uuid.uuid4())
            if deriv_flag:
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
            else:
                client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(round(alloc * 0.99, 2)))

            bot['asset_held'] = qty
            bot['current_usd'] -= alloc
            bot['position_side'] = 'LONG'
            bot['entry_price'] = current_px
            bot['avg_entry'] = current_px
            bot['entry_stage'] = 1
            bot['breakout_data'] = bo_data
            save_bots()
            print(f"[TRAP BOT | {pair}] LONG STAGE 1: 25% at {current_px:.2f}")
        except Exception as e:
            print(f"[TRAP BOT | {pair}] Stage 1 order failed: {e}")

    elif signal == 'BREAKOUT_SHORT' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        if not deriv_flag:
            return

        alloc = bot['current_usd'] * 0.25
        qty = int((alloc * 0.99) / (current_px * mult))
        if qty < 1:
            return

        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(qty))

            bot['asset_held'] = -qty
            bot['current_usd'] -= alloc
            bot['position_side'] = 'SHORT'
            bot['entry_price'] = current_px
            bot['avg_entry'] = current_px
            bot['entry_stage'] = 1
            bot['breakout_data'] = bo_data
            save_bots()
            print(f"[TRAP BOT | {pair}] SHORT STAGE 1: 25% at {current_px:.2f}")
        except Exception as e:
            print(f"[TRAP BOT | {pair}] Stage 1 order failed: {e}")

    # --- ADD TO POSITION (75% remaining) ---
    elif signal == 'ADD_LONG' and pos_side == 'LONG' and entry_stage == 1 and bot['current_usd'] > 5.0:
        alloc = bot['current_usd'] * 0.99
        if deriv_flag:
            qty = int((alloc * 0.99) / (current_px * mult))
            if qty < 1: return
        else:
            qty = round((alloc * 0.99) / current_px, 6)

        try:
            oid = str(uuid.uuid4())
            if deriv_flag:
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
            else:
                client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(round(alloc * 0.99, 2)))

            old_size = abs(bot['asset_held'])
            old_cost = old_size * bot['avg_entry']
            new_cost = qty * current_px
            total_size = old_size + qty
            bot['avg_entry'] = (old_cost + new_cost) / total_size if total_size > 0 else current_px

            bot['asset_held'] += qty
            bot['current_usd'] = 0.0
            bot['entry_stage'] = 2
            save_bots()
            print(f"[TRAP BOT | {pair}] LONG STAGE 2: +75% at {current_px:.2f}, avg {bot['avg_entry']:.2f}")
        except Exception as e:
            print(f"[TRAP BOT | {pair}] Stage 2 order failed: {e}")

    elif signal == 'ADD_SHORT' and pos_side == 'SHORT' and entry_stage == 1 and bot['current_usd'] > 5.0:
        if not deriv_flag: return

        alloc = bot['current_usd'] * 0.99
        qty = int((alloc * 0.99) / (current_px * mult))
        if qty < 1: return

        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(qty))

            old_size = abs(bot['asset_held'])
            old_cost = old_size * bot['avg_entry']
            new_cost = qty * current_px
            total_size = old_size + qty
            bot['avg_entry'] = (old_cost + new_cost) / total_size if total_size > 0 else current_px

            bot['asset_held'] -= qty
            bot['current_usd'] = 0.0
            bot['entry_stage'] = 2
            save_bots()
            print(f"[TRAP BOT | {pair}] SHORT STAGE 2: +75% at {current_px:.2f}, avg {bot['avg_entry']:.2f}")
        except Exception as e:
            print(f"[TRAP BOT | {pair}] Stage 2 order failed: {e}")

    # --- EXITS ---
    elif signal == 'EXIT_LONG' and pos_side == 'LONG' and bot['asset_held'] > 0:
        exit_reason = 'STOP_LOSS' if 'STOP' in reason.upper() else 'SIGNAL'
        try:
            oid = str(uuid.uuid4())
            held = abs(bot['asset_held'])
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(held))

            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['avg_entry'], actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

            profit = (actual_exit - bot['avg_entry']) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot['entry_stage'] = 0
            bot['avg_entry'] = 0.0
            bot.pop('breakout_data', None)
            save_bots()
            print(f"[TRAP BOT | {pair}] EXIT LONG: {reason}")
        except Exception as e:
            print(f"[TRAP BOT | {pair}] Exit failed: {e}")

    elif signal == 'EXIT_SHORT' and pos_side == 'SHORT' and bot['asset_held'] < 0:
        exit_reason = 'STOP_LOSS' if 'STOP' in reason.upper() else 'SIGNAL'
        try:
            oid = str(uuid.uuid4())
            held = abs(bot['asset_held'])
            client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(held))

            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['avg_entry'], actual_exit, held, 'SHORT', exit_reason, pair, mult, actual_fee=actual_fee)

            profit = (bot['avg_entry'] - actual_exit) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot['entry_stage'] = 0
            bot['avg_entry'] = 0.0
            bot.pop('breakout_data', None)
            save_bots()
            print(f"[TRAP BOT | {pair}] EXIT SHORT: {reason}")
        except Exception as e:
            print(f"[TRAP BOT | {pair}] Exit failed: {e}")

# ==========================================
# MOMENTUM EXECUTOR
# ==========================================

def momentum_get_stop_price(bot, cur_px):
    """
    Calculates the current stop price based on the three-phase trailing stop.
    Returns (stop_price, phase) tuple.
    Used by both REST cycle and WS tick handler for identical logic.
    """
    entry_px = bot.get('entry_price', 0)
    entry_atr = bot.get('entry_atr', 0)
    hwm = bot.get('high_water_mark', cur_px)
    fee_est = bot.get('fee_estimate', 0)
    held = abs(bot.get('asset_held', 0))
    mult = get_contract_multiplier(bot.get('pair', ''))

    if entry_px <= 0 or entry_atr <= 0 or held <= 0:
        return 0.0, 0

    pnl = (cur_px - entry_px) * held * mult
    fee_per_unit = fee_est / held if held > 0 else 0

    if pnl >= fee_est * 2:
        # Phase 3: LOCKED PROFIT — trail 0.75x ATR, floor at breakeven + fees
        phase = 3
        trail_stop = hwm - (0.75 * entry_atr)
        floor_stop = entry_px + fee_per_unit
        stop_px = max(trail_stop, floor_stop)
    elif pnl >= fee_est:
        # Phase 2: TIGHTENED — trail 1.0x ATR, floor at entry - 0.5x ATR
        phase = 2
        trail_stop = hwm - (1.0 * entry_atr)
        floor_stop = entry_px - (0.5 * entry_atr)
        stop_px = max(trail_stop, floor_stop)
    else:
        # Phase 1: INITIAL — trail 1.5x ATR
        phase = 1
        stop_px = hwm - (1.5 * entry_atr)

    return round(stop_px, 6), phase

def execute_momentum(bot_id, bot, pair):
    """
    MOMENTUM: Trend-pullback reversal with maker limit entry and 3-phase trailing stop.
    
    State machine:
    - FLAT + no pending: scan for signal, place maker limit on BUY
    - FLAT + pending: check fill, re-evaluate on timeout (90s, max 3 retries)
    - LONG: evaluate trailing stop (REST fallback; WS is primary via bot_ws.py)
    """
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = getattr(p_info, 'base_increment', '0.00000001')
        quote_inc = getattr(p_info, 'quote_increment', '0.01')
    except Exception as e:
        print(f"[MOMENTUM | {pair}] Data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    pos_side = bot.get('position_side', 'FLAT')
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)

    # ==========================================
    # STATE: LONG — Trailing Stop (REST fallback)
    # ==========================================
    if pos_side == 'LONG' and bot.get('asset_held', 0) > 0:
        # Update high water mark
        hwm = bot.get('high_water_mark', cur_px)
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
            hwm = cur_px

        stop_px, phase = momentum_get_stop_price(bot, cur_px)
        bot['stop_phase'] = phase

        if stop_px > 0 and cur_px <= stop_px:
            # STOP TRIGGERED
            exit_reason = 'STOP_LOSS' if phase == 1 else 'TRAILING_STOP'
            held = abs(bot['asset_held'])
            print(f"[MOMENTUM | {pair}] Phase {phase} stop triggered: price {cur_px:.2f} <= stop {stop_px:.2f}")

            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(held, base_inc)
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

                record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

                profit = (actual_exit - bot['entry_price']) * held * mult
                bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                # Clear momentum state
                for key in ['entry_atr', 'high_water_mark', 'stop_phase', 'fee_estimate',
                            'pending_order_oid', 'pending_order_time', 'signal_retries']:
                    bot.pop(key, None)
                save_bots()
                print(f"[MOMENTUM | {pair}] EXIT ({exit_reason}): PnL ${profit:.2f}")
            except Exception as e:
                print(f"[MOMENTUM | {pair}] Exit sell failed: {e}")
        else:
            save_bots()
        return

    # ==========================================
    # STATE: FLAT + PENDING ORDER — Check fill / timeout
    # ==========================================
    pending_oid = bot.get('pending_order_oid')
    if pos_side == 'FLAT' and pending_oid:
        pending_time = bot.get('pending_order_time', 0)
        elapsed = time.time() - pending_time
        retries = bot.get('signal_retries', 0)

        # Check if order filled
        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            filled = False
            mom_order_obj = None
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_oid:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    mom_order_obj = o
                    break
        except Exception as e:
            print(f"[MOMENTUM | {pair}] Fill check error: {e}")
            return

        if filled and filled_size > 0:
            # --- ORDER FILLED: Transition to LONG ---
            import pandas_ta as pta
            atr_series = pta.atr(df['high'], df['low'], df['close'], 14)
            entry_atr = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else cur_px * 0.01

            real_fee = extract_fee(mom_order_obj)
            gross_cost = avg_fill_px * filled_size * mult
            if real_fee is None:
                real_fee = gross_cost * 0.0025

            bot['asset_held'] = filled_size
            bot['current_usd'] -= (gross_cost + real_fee)
            bot['position_side'] = 'LONG'
            bot['entry_price'] = avg_fill_px
            bot['entry_atr'] = entry_atr
            bot['high_water_mark'] = avg_fill_px
            bot['stop_phase'] = 1
            bot['fee_estimate'] = real_fee
            bot.pop('pending_order_oid', None)
            bot.pop('pending_order_time', None)
            bot.pop('signal_retries', None)
            save_bots()
            print(f"[MOMENTUM | {pair}] FILLED at {avg_fill_px:.2f}. ATR={entry_atr:.2f}. Phase 1 stop at {avg_fill_px - 1.5*entry_atr:.2f}")
            return

        # Check timeout
        if elapsed >= 90:
            # Cancel the stale order
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == pending_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                            print(f"[MOMENTUM | {pair}] Cancelled stale limit order ({elapsed:.0f}s)")
                        break
            except Exception as e:
                print(f"[MOMENTUM | {pair}] Cancel error: {e}")

            # Re-evaluate signal
            signal, reason, sig_atr = calculate_momentum(df)

            if signal == 'BUY' and retries < 3:
                # Re-place at current best price
                limit_px = cur_px - float(quote_inc)
                str_price = snap_to_increment(limit_px, quote_inc)

                if deriv_flag:
                    qty = int((bot['current_usd'] * 0.99) / (limit_px * mult))
                    if qty < 1:
                        bot.pop('pending_order_oid', None)
                        bot.pop('pending_order_time', None)
                        bot.pop('signal_retries', None)
                        save_bots()
                        return
                    str_qty = str(qty)
                else:
                    qty = float(bot['current_usd'] * 0.99) / limit_px
                    str_qty = snap_to_increment(qty, base_inc)

                try:
                    new_oid = str(uuid.uuid4())
                    client.limit_order_gtc_buy(
                        client_order_id=new_oid, product_id=pair,
                        base_size=str_qty, limit_price=str_price, post_only=True
                    )
                    bot['pending_order_oid'] = new_oid
                    bot['pending_order_time'] = time.time()
                    bot['signal_retries'] = retries + 1
                    save_bots()
                    print(f"[MOMENTUM | {pair}] Re-placed limit buy at {str_price} (retry {retries + 1}/3)")
                except Exception as e:
                    print(f"[MOMENTUM | {pair}] Re-place failed: {e}")
                    bot.pop('pending_order_oid', None)
                    bot.pop('pending_order_time', None)
                    bot.pop('signal_retries', None)
                    save_bots()
            else:
                # Signal died or max retries — abandon
                reason_str = f"signal lost ({reason})" if signal != 'BUY' else f"max retries ({retries})"
                print(f"[MOMENTUM | {pair}] Abandoned entry: {reason_str}")
                bot.pop('pending_order_oid', None)
                bot.pop('pending_order_time', None)
                bot.pop('signal_retries', None)
                save_bots()
        return

    # ==========================================
    # STATE: FLAT — Scan for signal
    # ==========================================
    if pos_side != 'FLAT' or bot['current_usd'] <= 5.0:
        return

    signal, reason, sig_atr = calculate_momentum(df)

    if signal != 'BUY':
        return

    print(f"[MOMENTUM | {pair}] SIGNAL: {reason}")

    # Place maker limit buy 1 tick below current price
    limit_px = cur_px - float(quote_inc)
    str_price = snap_to_increment(limit_px, quote_inc)

    if deriv_flag:
        qty = int((bot['current_usd'] * 0.99) / (limit_px * mult))
        if qty < 1:
            print(f"[MOMENTUM | {pair}] Insufficient capital for 1 derivative contract.")
            return
        str_qty = str(qty)
    else:
        qty = float(bot['current_usd'] * 0.99) / limit_px
        str_qty = snap_to_increment(qty, base_inc)

    if float(str_qty) <= 0:
        return

    try:
        oid = str(uuid.uuid4())
        api_res = client.limit_order_gtc_buy(
            client_order_id=oid, product_id=pair,
            base_size=str_qty, limit_price=str_price, post_only=True
        )
        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))

        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
            bot['pending_order_oid'] = oid
            bot['pending_order_time'] = time.time()
            bot['signal_retries'] = 0
            save_bots()
            print(f"[MOMENTUM | {pair}] Limit BUY placed at {str_price} (post_only). Waiting for fill...")
        else:
            print(f"[MOMENTUM | {pair}] Limit order rejected: {fail_reason}")
    except Exception as e:
        print(f"[MOMENTUM | {pair}] Order placement failed: {e}")

# ==========================================
# DCA EXECUTOR
# ==========================================

DCA_PROFIT_TIERS = [
    (3.0,  0.20),  # 3.0% profit → sell 20% of remaining (net ~2.2% after fees)
    (5.0,  0.25),  # 5.0% → 25%
    (7.5,  0.30),  # 7.5% → 30%
    (10.0, 0.35),  # 10.0% → 35%
    (15.0, 0.50),  # 15.0% → 50%
    (20.0, 0.75),  # 20.0%+ → 75%, remainder rides as moonbag
]

def _dca_cancel_all_sells(bot, pair):
    """Cancel all pending DCA sell orders. Called when a new buy fills (avg entry changed)."""
    pending = bot.get('pending_sells', [])
    if not pending:
        return
    cancelled = 0
    for sell in list(pending):
        sell_oid = sell.get('oid', '')
        if sell_oid:
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == sell_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                            cancelled += 1
                        break
            except Exception as e:
                print(f"[DCA | {pair}] Cancel sell error: {e}")
    bot['pending_sells'] = []
    bot['highest_tier_sold'] = 0
    if cancelled:
        print(f"[DCA | {pair}] Cancelled {cancelled} pending sells (avg entry changed)")
    save_bots()

def _dca_check_sell_fills(bot, pair):
    """Check if any pending DCA sell orders have filled."""
    pending = bot.get('pending_sells', [])
    if not pending:
        return

    # Track processed OIDs to prevent re-processing the same fill
    processed = set(bot.get('_processed_sell_oids', []))

    mult = get_contract_multiplier(pair)
    try:
        order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "FILLED", "product_id": pair, "limit": 20
        })
        filled_orders = {o.get('client_order_id'): o for o in order_data.get('orders', []) if o.get('client_order_id')}
    except Exception as e:
        print(f"[DCA | {pair}] Sell fill check error: {e}")
        return

    new_pending = []
    changes = False
    for sell in pending:
        oid = sell.get('oid', '')
        if oid in processed:
            # Already processed this fill, skip entirely
            continue
        match = filled_orders.get(oid)
        if match:
            filled_size = float(match.get('filled_size', 0))
            avg_px = float(match.get('average_filled_price', sell.get('price', 0)))
            if filled_size > 0:
                # Mark as processed FIRST, before anything else
                processed.add(oid)
                bot['_processed_sell_oids'] = list(processed)[-50:]  # keep last 50

                bot['asset_held'] = max(0, bot.get('asset_held', 0) - filled_size)
                # Reduce total_cost proportionally to the fraction sold
                old_held = bot.get('asset_held', 0) + filled_size  # what held WAS before the line above
                if old_held > 0:
                    sold_fraction = filled_size / old_held
                    bot['total_cost'] = max(0, bot.get('total_cost', 0) * (1 - sold_fraction))
                real_fee = extract_fee(match)
                gross_proceeds = filled_size * avg_px * mult
                if real_fee is not None:
                    bot['current_usd'] += gross_proceeds - real_fee
                else:
                    real_fee = gross_proceeds * 0.0025  # fallback
                    bot['current_usd'] += gross_proceeds - real_fee
                tier_pct = sell.get('tier', 0)
                bot['highest_tier_sold'] = max(bot.get('highest_tier_sold', 0), tier_pct)
                changes = True
                pnl_val = (avg_px - bot.get('avg_entry', avg_px)) * filled_size * mult
                print(f"[DCA | {pair}] Tier {tier_pct}% sell FILLED: {filled_size:.8f} at ${avg_px:.2f} (fee=${real_fee:.4f})")
                notify_bot_exit(pair, 'DCA', avg_px, pnl_val, f'Tier {tier_pct}%')

                # Record trade AFTER state is fully updated and sell is NOT in new_pending
                try:
                    record_trade(bot, bot.get('avg_entry', avg_px), avg_px, filled_size, 'LONG', 'DCA_TIER', pair, mult, actual_fee=real_fee)
                except Exception as e:
                    print(f"[DCA | {pair}] record_trade error: {e}")
                continue  # Don't add to new_pending
        new_pending.append(sell)

    if changes:
        bot['pending_sells'] = new_pending
        held_remaining = bot.get('asset_held', 0)
        min_tradeable = float(bot.get('base_min_size', 0))

        # Cycle is done when remaining position is below minimum tradeable size
        # (all meaningful tiers have been sold, only untradeable dust or nothing left)
        cycle_done = (held_remaining < min_tradeable) and len(new_pending) == 0

        if cycle_done:
            if held_remaining > 0:
                # Dust remains — normalize total_cost to just the dust's actual value
                # so it doesn't inflate avg_entry on the next buy cycle
                dust_cost = held_remaining * bot.get('avg_entry', 0)
                bot['total_cost'] = dust_cost
                print(f"[DCA | {pair}] Tiers scaled out. {held_remaining:.8f} remains at avg ${bot.get('avg_entry',0):.2f}. Cycling to SCANNING.")
                bot['position_side'] = 'LONG'
            else:
                # Truly empty (asset_held == 0 exactly)
                print(f"[DCA | {pair}] Position fully closed. Resetting to SCANNING.")
                bot['position_side'] = 'FLAT'
                bot['avg_entry'] = 0
                bot['total_cost'] = 0
            # Trading cycle state resets independently of position
            bot['total_buys'] = 0
            bot['buy_count_this_cycle'] = 0
            bot['highest_tier_sold'] = 0
            bot['pending_sells'] = []
            bot['dca_state'] = 'SCANNING'
            bot['last_cross_direction'] = 'ABOVE'
        save_bots()

def _dca_manage_stale_sells(bot, pair, cur_px):
    """Cancel pending sells only when profit drops to zero or below (underwater).
    Leaves sells on the book as long as position is still profitable.
    Also requires a minimum 10-minute pending time to avoid churn."""
    pending = bot.get('pending_sells', [])
    avg_entry = bot.get('avg_entry', 0)
    if not pending or avg_entry <= 0:
        return

    profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
    now = time.time()
    changed = False

    for sell in list(pending):
        tier_pct = sell.get('tier', 0)
        placed_at = sell.get('placed_at', 0)
        pending_secs = now - placed_at if placed_at > 0 else 9999

        # Only cancel if profit has gone to zero or below AND sell has been
        # pending for at least 10 minutes (avoid thrashing on fresh placements)
        if profit_pct <= 0 and pending_secs >= 600:
            sell_oid = sell.get('oid', '')
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == sell_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                        break
            except Exception as e:
                print(f"[DCA | {pair}] Stale sell cancel error: {e}")
            pending.remove(sell)
            changed = True
            print(f"[DCA | {pair}] Cancelled stale {tier_pct}% sell (profit now {profit_pct:.2f}%, underwater)")

    if changed:
        bot['pending_sells'] = pending
        save_bots()

def execute_dca(bot_id, bot, pair):
    """
    DCA: Signal-gated accumulation with tiered profit-taking.
    All orders maker (post_only). Weighted avg entry tracking.
    """
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
        base_min = float(getattr(p_info, 'base_min_size', '0.00001'))
        if base_min * cur_px < 0.25:
            base_min = 0.25 / cur_px
    except Exception as e:
        print(f"[DCA | {pair}] Data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    # Cache product info
    bot['base_min_size'] = base_min
    bot['base_increment'] = base_inc
    bot['quote_increment'] = quote_inc

    dca_state = bot.get('dca_state', 'SCANNING')
    last_cross = bot.get('last_cross_direction', 'ABOVE')
    avg_entry = bot.get('avg_entry', 0)
    held = bot.get('asset_held', 0)
    mult = get_contract_multiplier(pair)
    deriv_flag = is_derivative(pair)

    # ==========================================
    # STEP 1: DRAWDOWN MANAGEMENT (graduated response)
    # ==========================================
    # -10% to -20%: half-size buys (keep accumulating, reduce exposure)
    # -20% to -25%: quarter-size buys (minimal, still averaging down)
    # Beyond -25%: full pause (catastrophic, something is fundamentally broken)
    drawdown_pct = 0
    drawdown_mult = 1.0
    if held >= base_min and avg_entry > 0:
        drawdown_pct = ((avg_entry - cur_px) / avg_entry) * 100
        if dca_state != 'PAUSED' and drawdown_pct >= 25:
            bot['dca_state'] = 'PAUSED'
            bot['paused_at'] = time.time()
            print(f"[DCA | {pair}] PAUSED: {drawdown_pct:.1f}% drawdown — catastrophic threshold")
            notify_drawdown(pair, 'DCA', drawdown_pct)
            save_bots()
        elif dca_state == 'PAUSED' and drawdown_pct < 22:  # 3% hysteresis
            bot['dca_state'] = 'ACCUMULATING'
            bot['paused_at'] = 0
            print(f"[DCA | {pair}] UN-PAUSED: drawdown recovered to {drawdown_pct:.1f}%")
            save_bots()
        elif drawdown_pct >= 20:
            drawdown_mult = 0.25  # quarter-size buys
        elif drawdown_pct >= 10:
            drawdown_mult = 0.50  # half-size buys
        dca_state = bot.get('dca_state', 'SCANNING')

    # ==========================================
    # STEP 2: CHECK PENDING BUY FILL
    # ==========================================
    pending_buy = bot.get('pending_buy_oid')
    if pending_buy and dca_state == 'BUYING':
        pending_time = bot.get('pending_buy_time', 0)
        elapsed = time.time() - pending_time
        retries = bot.get('buy_retries', 0)

        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            filled = False
            buy_order_obj = None
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_buy:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    buy_order_obj = o
                    break
        except Exception as e:
            print(f"[DCA | {pair}] Buy fill check error: {e}")
            return

        if filled and filled_size > 0:
            # --- BUY FILLED ---
            old_held = bot.get('asset_held', 0)
            old_cost = bot.get('total_cost', 0)
            new_cost = avg_fill_px * filled_size * mult
            total_held = old_held + filled_size
            new_avg = (old_cost + new_cost) / (total_held * mult) if total_held > 0 else avg_fill_px

            buy_fee = extract_fee(buy_order_obj) if buy_order_obj else None
            if buy_fee is None:
                buy_fee = new_cost * 0.0025  # fallback

            bot['asset_held'] = total_held
            bot['total_cost'] = old_cost + new_cost
            bot['avg_entry'] = new_avg
            bot['entry_price'] = new_avg
            bot['current_usd'] -= (new_cost + buy_fee)
            bot['position_side'] = 'LONG'
            bot['total_buys'] = bot.get('total_buys', 0) + 1
            bot['buy_count_this_cycle'] = bot.get('buy_count_this_cycle', 0) + 1
            bot['dca_state'] = 'ACCUMULATING'
            bot['last_cross_direction'] = 'BELOW'  # just bought on a cross-below, need cross-above to re-arm
            bot.pop('pending_buy_oid', None)
            bot.pop('pending_buy_time', None)
            bot.pop('buy_retries', None)

            # Cancel all pending sells — avg entry just changed
            _dca_cancel_all_sells(bot, pair)

            save_bots()
            print(f"[DCA | {pair}] BUY FILLED: {filled_size:.8f} at ${avg_fill_px:.2f}. Avg entry now ${new_avg:.2f}. Total buys: {bot['total_buys']}")
            notify_bot_entry(pair, 'DCA', avg_fill_px, filled_size)
            return

        if elapsed >= 90:
            # Cancel stale buy
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == pending_buy:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                        break
            except Exception as e:
                print(f"[DCA | {pair}] Cancel buy error: {e}")

            # Re-evaluate
            signal, reason, data = calculate_dca(df, 'ARMED', last_cross)
            if signal == 'BUY' and retries < 3:
                limit_px = cur_px - float(quote_inc)
                str_price = snap_to_increment(limit_px, quote_inc)
                depth_mult = data.get('depth_multiplier', 1.0)
                buy_qty = base_min * depth_mult * drawdown_mult
                if deriv_flag:
                    buy_qty = max(1, int(buy_qty))
                str_qty = snap_to_increment(buy_qty, base_inc)

                buy_usd = float(str_qty) * limit_px * mult
                if buy_usd > bot['current_usd'] * 0.99 or float(str_qty) <= 0:
                    bot['dca_state'] = 'ACCUMULATING' if held > 0 else 'SCANNING'
                    bot.pop('pending_buy_oid', None)
                    bot.pop('pending_buy_time', None)
                    bot.pop('buy_retries', None)
                    save_bots()
                    return

                try:
                    oid = str(uuid.uuid4())
                    client.limit_order_gtc_buy(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True)
                    bot['pending_buy_oid'] = oid
                    bot['pending_buy_time'] = time.time()
                    bot['buy_retries'] = retries + 1
                    save_bots()
                    print(f"[DCA | {pair}] Re-placed buy at ${str_price} (retry {retries+1}/3)")
                except Exception as e:
                    print(f"[DCA | {pair}] Re-place failed: {e}")
            else:
                bot['dca_state'] = 'ACCUMULATING' if held > 0 else 'ARMED'
                bot.pop('pending_buy_oid', None)
                bot.pop('pending_buy_time', None)
                bot.pop('buy_retries', None)
                save_bots()
        return

    # ==========================================
    # STEP 3: CHECK PENDING SELL FILLS
    # ==========================================
    _dca_check_sell_fills(bot, pair)

    # ==========================================
    # STEP 4: MANAGE STALE SELLS
    # ==========================================
    _dca_manage_stale_sells(bot, pair, cur_px)

    # ==========================================
    # STEP 4.5: TIER RESET AT -3% DRAWDOWN
    # ==========================================
    held = bot.get('asset_held', 0)
    avg_entry = bot.get('avg_entry', 0)
    min_tradeable = float(bot.get('base_min_size', 0))
    if held >= min_tradeable and avg_entry > 0:
        profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
        highest_sold = bot.get('highest_tier_sold', 0)
        if profit_pct <= -3.0 and highest_sold > 0:
            print(f"[DCA | {pair}] TIER RESET: profit {profit_pct:.2f}% hit -3.0% with tier {highest_sold}% sold. Resetting tiers.")
            _dca_cancel_all_sells(bot, pair)
            bot['highest_tier_sold'] = 0
            bot['tier_reset_at'] = time.time()
            save_bots()

    # ==========================================
    # STEP 5: EVALUATE PROFIT TIERS
    # ==========================================
    held = bot.get('asset_held', 0)
    avg_entry = bot.get('avg_entry', 0)
    min_tradeable = float(bot.get('base_min_size', 0))
    if held >= min_tradeable and avg_entry > 0:
        # Cooldown: don't place new tier sells within 10 min of a tier reset
        tier_reset_at = bot.get('tier_reset_at', 0)
        if tier_reset_at > 0 and (time.time() - tier_reset_at) < 600:
            pass  # skip tier evaluation, still in cooldown
        else:
            if tier_reset_at > 0:
                bot.pop('tier_reset_at', None)  # cooldown expired, clean up

            profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
            highest_sold = bot.get('highest_tier_sold', 0)
            pending_sells = bot.get('pending_sells', [])
            pending_tiers = {s.get('tier') for s in pending_sells}

            # Deduct qty already committed to pending sells so tiers don't overcommit
            committed_qty = sum(s.get('qty', 0) for s in pending_sells)
            available_held = max(0, held - committed_qty)

            for tier_pct, sell_frac in DCA_PROFIT_TIERS:
                if tier_pct <= highest_sold:
                    continue  # already sold at this tier
                if tier_pct in pending_tiers:
                    continue  # already have a pending sell at this tier

                if profit_pct >= tier_pct and available_held > 0:
                    sell_qty = available_held * sell_frac
                    sell_px = avg_entry * (1 + tier_pct / 100.0)
                    if sell_px <= cur_px:
                        sell_px = cur_px + float(quote_inc)  # price already past tier, sell at market edge
                    str_price = snap_to_increment(sell_px, quote_inc)
                    str_qty = snap_to_increment(sell_qty, base_inc)

                    if float(str_qty) <= 0:
                        continue

                    if bot.get('paper'):
                        # Paper mode: simulate immediate sell fill
                        paper_fill_sell(bot, pair, tier_pct, str_qty, str_price, mult)
                        available_held -= float(str_qty)
                        continue

                    try:
                        oid = str(uuid.uuid4())
                        api_res = client.limit_order_gtc_sell(
                            client_order_id=oid, product_id=pair,
                            base_size=str_qty, limit_price=str_price, post_only=True
                        )
                        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                        if success or (isinstance(api_res, dict) and api_res.get('failure_reason') == 'UNKNOWN_FAILURE_REASON') or getattr(api_res, 'failure_reason', '') == 'UNKNOWN_FAILURE_REASON':
                            pending_sells.append({
                                'tier': tier_pct,
                                'oid': oid,
                                'price': float(str_price),
                                'qty': float(str_qty),
                                'placed_at': time.time(),
                            })
                            available_held -= float(str_qty)  # deduct for next tier in this loop
                            bot['pending_sells'] = pending_sells
                            save_bots()
                            print(f"[DCA | {pair}] Placed {tier_pct}% tier sell: {str_qty} at ${str_price}")
                        else:
                            fail = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                            print(f"[DCA | {pair}] Tier sell rejected: {fail}")
                    except Exception as e:
                        print(f"[DCA | {pair}] Tier sell error: {e}")

    # ==========================================
    # STEP 6: SIGNAL EVALUATION
    # ==========================================
    dca_state = bot.get('dca_state', 'SCANNING')

    if dca_state == 'PAUSED':
        return  # No buys while paused, tiers still managed above

    if dca_state in ('SCANNING', 'ARMED'):
        signal, reason, data = calculate_dca(df, dca_state, last_cross)

        if signal == 'ARM':
            bot['dca_state'] = 'ARMED'
            bot['armed_at'] = time.time()
            save_bots()
            print(f"[DCA | {pair}] ARMED: {reason}")

        elif signal == 'DISARM':
            bot['dca_state'] = 'SCANNING'
            save_bots()
            print(f"[DCA | {pair}] DISARMED: {reason}")

        elif signal == 'BUY':
            if bot['current_usd'] < base_min * cur_px * 0.5:
                print(f"[DCA | {pair}] Signal BUY but insufficient capital (${bot['current_usd']:.2f})")
                return

            # Correlation guard: if multiple bots are entering simultaneously,
            # reduce size to avoid overexposure to the same broad market move
            concurrent_entries = sum(
                1 for b in ACTIVE_BOTS.values()
                if b.get('strategy') == 'DCA'
                and b.get('pair') != pair
                and b.get('dca_state') in ('ARMED', 'BUYING')
            )
            corr_mult = 1.0
            if concurrent_entries >= 3:
                corr_mult = 0.33
                print(f"[DCA | {pair}] CORRELATION GUARD: {concurrent_entries} other bots entering, sizing to 33%")
            elif concurrent_entries >= 2:
                corr_mult = 0.50
                print(f"[DCA | {pair}] CORRELATION GUARD: {concurrent_entries} other bots entering, sizing to 50%")

            depth_mult = data.get('depth_multiplier', 1.0)
            buy_pct = bot.get('buy_pct', 2.0)
            buy_usd = bot['current_usd'] * (buy_pct / 100.0) * depth_mult * drawdown_mult * corr_mult
            min_qty = max(base_min, 0.25 / cur_px) if cur_px > 0 else base_min
            buy_qty = buy_usd / (cur_px * mult) if cur_px > 0 else min_qty
            if buy_qty < min_qty:
                buy_qty = min_qty
            if deriv_flag:
                buy_qty = max(1, int(buy_qty))

            limit_px = cur_px - float(quote_inc)
            str_price = snap_to_increment(limit_px, quote_inc)
            str_qty = snap_to_increment(buy_qty, base_inc)

            buy_usd = float(str_qty) * limit_px * mult
            if buy_usd > bot['current_usd'] * 0.99 or float(str_qty) <= 0:
                print(f"[DCA | {pair}] Buy size ${buy_usd:.2f} exceeds available ${bot['current_usd']:.2f}")
                return

            if bot.get('paper'):
                # Paper mode: simulate immediate fill
                paper_fill_buy(bot, pair, str_qty, str_price, mult)
                return

            try:
                oid = str(uuid.uuid4())
                api_res = client.limit_order_gtc_buy(
                    client_order_id=oid, product_id=pair,
                    base_size=str_qty, limit_price=str_price, post_only=True
                )
                success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                    bot['dca_state'] = 'BUYING'
                    bot['pending_buy_oid'] = oid
                    bot['pending_buy_time'] = time.time()
                    bot['buy_retries'] = 0
                    save_bots()
                    print(f"[DCA | {pair}] BUYING: {str_qty} at ${str_price} ({depth_mult:.2f}x min). {reason}")
                else:
                    print(f"[DCA | {pair}] Buy rejected: {fail_reason}")
            except Exception as e:
                print(f"[DCA | {pair}] Buy placement failed: {e}")

    elif dca_state == 'ACCUMULATING':
        # Check for re-arm cycle
        signal, reason, data = calculate_dca(df, dca_state, last_cross)

        if signal == 'CROSS_ABOVE':
            bot['last_cross_direction'] = 'ABOVE'
            save_bots()
            print(f"[DCA | {pair}] {reason}")

        elif signal == 'ARM':
            bot['dca_state'] = 'ARMED'
            bot['armed_at'] = time.time()
            bot['last_cross_direction'] = 'BELOW'
            save_bots()
            print(f"[DCA | {pair}] RE-ARMED: {reason}")


# ==========================================
# NPR EXECUTOR
# ==========================================

def npr_get_stop_and_trail(bot, cur_px):
    """Returns (should_exit, exit_reason) for WS tick-level monitoring."""
    if bot.get('position_side') == 'FLAT':
        return False, None
    event_stop = bot.get('event_stop', 0)
    side = bot.get('position_side')
    hwm = bot.get('high_water_mark', 0)
    lwm = bot.get('low_water_mark', 999999)
    trail_dist = bot.get('trail_distance', 0)
    partial = bot.get('partial_filled', False)
    if side == 'LONG':
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
            hwm = cur_px
    elif side == 'SHORT':
        if cur_px < lwm:
            bot['low_water_mark'] = cur_px
            lwm = cur_px
    if side == 'LONG' and event_stop > 0 and cur_px <= event_stop:
        return True, 'EVENT_STOP'
    if side == 'SHORT' and event_stop > 0 and cur_px >= event_stop:
        return True, 'EVENT_STOP'
    if partial and trail_dist > 0:
        if side == 'LONG' and hwm > 0 and cur_px <= (hwm - trail_dist):
            return True, 'TRAILING_STOP'
        elif side == 'SHORT' and lwm < 999999 and cur_px >= (lwm + trail_dist):
            return True, 'TRAILING_STOP'
    return False, None


def execute_npr(bot_id, bot, pair):
    """NPR: Three-check price action strategy for derivatives. 2m bars with entry timing."""
    import datetime
    from strategies import calculate_npr, NPR_CONFIG, _compute_zone

    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
        base_min = float(getattr(p_info, 'base_min_size', '0.00001'))
        if base_min * cur_px < 0.25:
            base_min = 0.25 / cur_px
    except Exception as e:
        print(f"[NPR | {pair}] Data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    mult = get_contract_multiplier(pair)
    npr_state = bot.get('npr_state', 'SCANNING')
    max_loss_trade = bot.get('max_loss_per_trade', 10.0)
    max_loss_day = bot.get('max_loss_per_day', max_loss_trade * 3)

    # === DAILY LOSS TRACKING ===
    today_str = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    if bot.get('daily_loss_date') != today_str:
        bot['daily_loss'] = 0.0
        bot['daily_loss_date'] = today_str
        if npr_state == 'DAILY_HALT':
            bot['npr_state'] = 'SCANNING'
            npr_state = 'SCANNING'
            print(f"[NPR | {pair}] New UTC day -- resuming from DAILY_HALT")
        save_bots()

    if bot.get('daily_loss', 0) >= max_loss_day:
        if npr_state != 'DAILY_HALT':
            bot['npr_state'] = 'DAILY_HALT'
            save_bots()
            print(f"[NPR | {pair}] DAILY HALT: Loss ${bot['daily_loss']:.2f} >= max ${max_loss_day:.2f}")
        return

    # === IN_POSITION: Manage exits ===
    if npr_state == 'IN_POSITION' and bot.get('position_side') != 'FLAT':
        side = bot['position_side']
        entry_px = bot.get('entry_price', cur_px)
        atr = bot.get('atr_at_entry', 1.0)
        partial = bot.get('partial_filled', False)

        should_exit, exit_reason = npr_get_stop_and_trail(bot, cur_px)
        if should_exit:
            try:
                held = bot['asset_held']
                if side == 'LONG':
                    str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                    str_qty = snap_to_increment(held, base_inc)
                    oid = str(uuid.uuid4())
                    client.limit_order_gtc_sell(client_order_id=oid, product_id=pair,
                                                base_size=str_qty, limit_price=str_price, post_only=True)
                else:
                    str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                    str_qty = snap_to_increment(held, base_inc)
                    oid = str(uuid.uuid4())
                    client.limit_order_gtc_buy(client_order_id=oid, product_id=pair,
                                               base_size=str_qty, limit_price=str_price, post_only=True)

                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

                pnl = (actual_exit - entry_px) * held * mult if side == 'LONG' else (entry_px - actual_exit) * held * mult
                record_trade(bot, entry_px, actual_exit, held, side, exit_reason, pair, mult, actual_fee=actual_fee)
                if pnl < 0:
                    bot['daily_loss'] = bot.get('daily_loss', 0) + abs(pnl)
                gross_proceeds = held * actual_exit * mult
                bot['current_usd'] += gross_proceeds - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                bot['npr_state'] = 'SCANNING'
                for key in ['event_stop', 'event_type', 'event_direction', 'event_bar_data',
                            'high_water_mark', 'low_water_mark', 'trail_distance', 'partial_filled',
                            'atr_at_entry', 'pending_order_oid', 'pending_order_time']:
                    bot.pop(key, None)
                save_bots()
                print(f"[NPR | {pair}] EXIT ({exit_reason}): PnL ${pnl:.2f}")
            except Exception as e:
                print(f"[NPR | {pair}] Exit order failed: {e}")
            return

        # Activate trailing after breakeven + 0.25 ATR
        if not partial:
            be_reached = (side == 'LONG' and cur_px >= entry_px + 0.25 * atr) or \
                         (side == 'SHORT' and cur_px <= entry_px - 0.25 * atr)
            if be_reached:
                event_power = bot.get('event_power', 1.0)
                entry_zone = abs(bot.get('zone', 1))
                if event_power >= 2.0:
                    bot['trail_distance'] = 1.5 * atr
                elif entry_zone == 3:
                    bot['trail_distance'] = 0.75 * atr
                else:
                    bot['trail_distance'] = 1.0 * atr
                bot['partial_filled'] = True
                if side == 'LONG':
                    bot['event_stop'] = entry_px + float(quote_inc)
                else:
                    bot['event_stop'] = entry_px - float(quote_inc)
                save_bots()
                print(f"[NPR | {pair}] Breakeven + trail. dist={bot['trail_distance']:.2f}")
        return

    # === ENTERING: Check pending order ===
    if npr_state == 'ENTERING':
        pending_oid = bot.get('pending_order_oid')
        if not pending_oid:
            bot['npr_state'] = 'SCANNING'
            save_bots()
            return
        elapsed = time.time() - bot.get('pending_order_time', 0)
        retries = bot.get('entry_retries', 0)

        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            filled = False
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_oid:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    break
        except Exception as e:
            print(f"[NPR | {pair}] Fill check error: {e}")
            return

        if filled and filled_size > 0:
            direction = bot.get('event_direction', 'BULL')
            bot['position_side'] = 'LONG' if direction == 'BULL' else 'SHORT'
            bot['entry_price'] = avg_fill_px
            bot['asset_held'] = filled_size
            bot['npr_state'] = 'IN_POSITION'
            bot['high_water_mark'] = avg_fill_px
            bot['low_water_mark'] = avg_fill_px
            bot['partial_filled'] = False
            for key in ['pending_order_oid', 'pending_order_time', 'entry_retries',
                        'signal_bar_time', '_entry_size', 'entry_bar_start']:
                bot.pop(key, None)
            save_bots()
            print(f"[NPR | {pair}] FILLED {bot['position_side']}: {filled_size:.8f} at ${avg_fill_px:.2f} "
                  f"Event={bot.get('event_type')} Zone={bot.get('zone')} Score={bot.get('check_score')}")
            return

        if elapsed >= 30:
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == pending_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                        break
            except Exception as e:
                print(f"[NPR | {pair}] Cancel error: {e}")

            bar_start = bot.get('entry_bar_start', 0)
            if retries < 2 and (time.time() - bar_start) < tf_sec:
                direction = bot.get('event_direction', 'BULL')
                entry_size = bot.get('_entry_size', base_min)
                if direction == 'BULL':
                    str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                else:
                    str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                str_qty = snap_to_increment(entry_size, base_inc)
                oid = str(uuid.uuid4())
                try:
                    if direction == 'BULL':
                        client.limit_order_gtc_buy(client_order_id=oid, product_id=pair,
                                                   base_size=str_qty, limit_price=str_price, post_only=True)
                    else:
                        client.limit_order_gtc_sell(client_order_id=oid, product_id=pair,
                                                   base_size=str_qty, limit_price=str_price, post_only=True)
                    bot['pending_order_oid'] = oid
                    bot['pending_order_time'] = time.time()
                    bot['entry_retries'] = retries + 1
                    save_bots()
                    print(f"[NPR | {pair}] Retry {retries+1}/2 at ${str_price}")
                except Exception as e:
                    print(f"[NPR | {pair}] Retry failed: {e}")
                return

            bot['npr_state'] = 'SCANNING'
            for key in ['pending_order_oid', 'pending_order_time', 'entry_retries',
                        'entry_bar_start', 'signal_bar_time', '_entry_size',
                        'event_type', 'event_direction', 'event_stop', 'event_bar_data',
                        'event_power', 'zone', 'check_score', 'position_checks', 'atr_at_entry']:
                bot.pop(key, None)
            save_bots()
            print(f"[NPR | {pair}] Entry abandoned")
        return

    # === SCANNING / SIGNAL_WAIT: Detect events ===
    if npr_state in ('SCANNING', 'SIGNAL_WAIT'):
        signal = calculate_npr(df)

        bar_start = (end_ts // tf_sec) * tf_sec
        elapsed_in_bar = end_ts - bar_start
        in_entry_window = elapsed_in_bar >= (tf_sec * 0.66)

        if signal['signal'] != 'HOLD':
            stop_distance = abs(cur_px - signal['event_stop'])
            if stop_distance <= 0:
                return
            position_size = max_loss_trade / (stop_distance * mult) if mult > 0 else 0
            position_cost = position_size * cur_px * mult
            if position_cost > bot['current_usd'] * 0.95:
                position_size = (bot['current_usd'] * 0.95) / (cur_px * mult)
            if position_size < base_min:
                print(f"[NPR | {pair}] Size too small for risk. Skip.")
                return

            if in_entry_window:
                direction = signal['event_direction']
                if direction == 'BULL':
                    str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                else:
                    str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                str_qty = snap_to_increment(position_size, base_inc)
                oid = str(uuid.uuid4())
                try:
                    if direction == 'BULL':
                        api_res = client.limit_order_gtc_buy(client_order_id=oid, product_id=pair,
                                                             base_size=str_qty, limit_price=str_price, post_only=True)
                    else:
                        api_res = client.limit_order_gtc_sell(client_order_id=oid, product_id=pair,
                                                             base_size=str_qty, limit_price=str_price, post_only=True)
                    success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                    fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                    if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                        bot['npr_state'] = 'ENTERING'
                        bot['pending_order_oid'] = oid
                        bot['pending_order_time'] = time.time()
                        bot['entry_retries'] = 0
                        bot['entry_bar_start'] = bar_start
                        bot['event_type'] = signal['event_type']
                        bot['event_direction'] = signal['event_direction']
                        bot['event_stop'] = signal['event_stop']
                        bot['event_power'] = signal['event_power']
                        bot['event_bar_data'] = signal['event_bar_data']
                        bot['zone'] = signal['zone']
                        bot['check_score'] = signal['check_score']
                        bot['position_checks'] = signal['position_checks']
                        bot['atr_at_entry'] = signal['atr']
                        bot['_entry_size'] = position_size
                        save_bots()
                        print(f"[NPR | {pair}] ENTERING: {signal['reason']}")
                    else:
                        print(f"[NPR | {pair}] Order rejected: {fail_reason}")
                except Exception as e:
                    print(f"[NPR | {pair}] Order failed: {e}")
            else:
                if npr_state != 'SIGNAL_WAIT':
                    bot['npr_state'] = 'SIGNAL_WAIT'
                    bot['signal_bar_time'] = float(df['start'].iloc[-1])
                    save_bots()
                    print(f"[NPR | {pair}] SIGNAL_WAIT: {signal['reason']} (bar {elapsed_in_bar:.0f}s/{tf_sec}s)")
        elif npr_state == 'SIGNAL_WAIT':
            bot['npr_state'] = 'SCANNING'
            bot.pop('signal_bar_time', None)
            save_bots()
