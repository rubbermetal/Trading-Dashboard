import time
import uuid
import pandas as pd
from shared import client
from strategies import calculate_quad_rotation, calculate_quad_super, calculate_orb, calculate_trap, calculate_momentum, calculate_dca
from bot_utils import (
    get_bot_tf, is_derivative, get_contract_multiplier, 
    snap_to_increment, record_trade, save_bots
)

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
            
            record_trade(bot, bot['entry_price'], current_px, abs(bot['asset_held']), 'LONG', exit_reason, pair, mult)
            
            profit = (current_px - bot['entry_price']) * abs(bot['asset_held']) * mult
            bot['current_usd'] = bot['allocated_usd'] + (profit * 0.995)
            
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
            
            record_trade(bot, bot['entry_price'], current_px, abs(bot['asset_held']), 'SHORT', exit_reason, pair, mult)
            
            profit = (bot['entry_price'] - current_px) * abs(bot['asset_held']) * mult
            bot['current_usd'] = bot['allocated_usd'] + (profit * 0.995)
            
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
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(bot['asset_held']))
            
            entry_px = bot.get('entry_price', current_px)
            record_trade(bot, entry_px, current_px, bot['asset_held'], 'LONG', 'SIGNAL', pair, mult)
            
            bot['current_usd'] += (bot['asset_held'] * current_px * mult) * 0.995 
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
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(abs(bot['asset_held'])))

            record_trade(bot, bot['avg_entry'], current_px, abs(bot['asset_held']), 'LONG', exit_reason, pair, mult)

            profit = (current_px - bot['avg_entry']) * abs(bot['asset_held']) * mult
            bot['current_usd'] = bot['allocated_usd'] + (profit * 0.995)
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
            client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(abs(bot['asset_held'])))

            record_trade(bot, bot['avg_entry'], current_px, abs(bot['asset_held']), 'SHORT', exit_reason, pair, mult)

            profit = (bot['avg_entry'] - current_px) * abs(bot['asset_held']) * mult
            bot['current_usd'] = bot['allocated_usd'] + (profit * 0.995)
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

                record_trade(bot, bot['entry_price'], cur_px, held, 'LONG', exit_reason, pair, mult)

                profit = (cur_px - bot['entry_price']) * held * mult
                bot['current_usd'] = bot['allocated_usd'] + (profit * 0.995)
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
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_oid:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    break
        except Exception as e:
            print(f"[MOMENTUM | {pair}] Fill check error: {e}")
            return

        if filled and filled_size > 0:
            # --- ORDER FILLED: Transition to LONG ---
            import pandas_ta as pta
            atr_series = pta.atr(df['high'], df['low'], df['close'], 14)
            entry_atr = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else cur_px * 0.01

            fee_est = avg_fill_px * filled_size * mult * 0.005

            bot['asset_held'] = filled_size
            bot['current_usd'] -= avg_fill_px * filled_size * mult
            bot['position_side'] = 'LONG'
            bot['entry_price'] = avg_fill_px
            bot['entry_atr'] = entry_atr
            bot['high_water_mark'] = avg_fill_px
            bot['stop_phase'] = 1
            bot['fee_estimate'] = fee_est
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
    (1.5,  0.25),  # 1.5% profit → sell 25% of remaining
    (2.5,  0.33),  # 2.5% → 33%
    (4.0,  0.45),  # 4.0% → 45%
    (6.0,  0.50),  # 6.0% → 50%
    (8.0,  0.50),  # 8.0% → 50%
    (10.0, 0.75),  # 10.0%+ → 75%
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

    mult = get_contract_multiplier(pair)
    try:
        order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "FILLED", "product_id": pair, "limit": 20
        })
        filled_orders = {o.get('client_order_id'): o for o in order_data.get('orders', []) if o.get('client_order_id')}
    except Exception as e:
        print(f"[DCA | {pair}] Sell fill check error: {e}")
        return

    changes = False
    for sell in list(pending):
        match = filled_orders.get(sell.get('oid'))
        if match:
            filled_size = float(match.get('filled_size', 0))
            avg_px = float(match.get('average_filled_price', sell.get('price', 0)))
            if filled_size > 0:
                bot['asset_held'] = max(0, bot.get('asset_held', 0) - filled_size)
                bot['current_usd'] += filled_size * avg_px * mult * 0.9975  # maker fee
                record_trade(bot, bot.get('avg_entry', avg_px), avg_px, filled_size, 'LONG', 'DCA_TIER', pair, mult)
                tier_pct = sell.get('tier', 0)
                bot['highest_tier_sold'] = max(bot.get('highest_tier_sold', 0), tier_pct)
                pending.remove(sell)
                changes = True
                print(f"[DCA | {pair}] Tier {tier_pct}% sell FILLED: {filled_size:.8f} at ${avg_px:.2f}")

    if changes:
        bot['pending_sells'] = pending
        # Check if position fully closed
        if bot.get('asset_held', 0) * bot.get('avg_entry', 1) < 0.50:  # less than $0.50 remaining
            print(f"[DCA | {pair}] Position fully scaled out. Resetting to SCANNING.")
            bot['position_side'] = 'FLAT'
            bot['avg_entry'] = 0
            bot['total_cost'] = 0
            bot['total_buys'] = 0
            bot['buy_count_this_cycle'] = 0
            bot['highest_tier_sold'] = 0
            bot['pending_sells'] = []
            bot['dca_state'] = 'SCANNING'
            bot['last_cross_direction'] = 'ABOVE'
        save_bots()

def _dca_manage_stale_sells(bot, pair, cur_px):
    """Cancel pending sells where profit has retreated below half the tier threshold."""
    pending = bot.get('pending_sells', [])
    avg_entry = bot.get('avg_entry', 0)
    if not pending or avg_entry <= 0:
        return

    mult = get_contract_multiplier(pair)
    profit_pct = ((cur_px - avg_entry) / avg_entry) * 100

    for sell in list(pending):
        tier_pct = sell.get('tier', 0)
        cancel_threshold = tier_pct / 2.0  # cancel if profit below half the tier

        if profit_pct < cancel_threshold:
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
            print(f"[DCA | {pair}] Cancelled stale {tier_pct}% sell (profit now {profit_pct:.2f}% < {cancel_threshold:.2f}%)")

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
    # STEP 1: PAUSE CHECK (15% drawdown)
    # ==========================================
    if held > 0 and avg_entry > 0:
        drawdown_pct = ((avg_entry - cur_px) / avg_entry) * 100
        if dca_state != 'PAUSED' and drawdown_pct >= 15:
            bot['dca_state'] = 'PAUSED'
            bot['paused_at'] = time.time()
            print(f"[DCA | {pair}] PAUSED: {drawdown_pct:.1f}% drawdown from avg entry ${avg_entry:.2f}")
            save_bots()
        elif dca_state == 'PAUSED' and drawdown_pct < 13:  # 2% hysteresis
            bot['dca_state'] = 'ACCUMULATING'
            bot['paused_at'] = 0
            print(f"[DCA | {pair}] UN-PAUSED: drawdown recovered to {drawdown_pct:.1f}%")
            save_bots()
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
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_buy:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
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

            bot['asset_held'] = total_held
            bot['total_cost'] = old_cost + new_cost
            bot['avg_entry'] = new_avg
            bot['entry_price'] = new_avg
            bot['current_usd'] -= new_cost
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
                buy_qty = base_min * depth_mult
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
    # STEP 5: EVALUATE PROFIT TIERS
    # ==========================================
    held = bot.get('asset_held', 0)
    avg_entry = bot.get('avg_entry', 0)
    if held > 0 and avg_entry > 0:
        profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
        highest_sold = bot.get('highest_tier_sold', 0)
        pending_sells = bot.get('pending_sells', [])
        pending_tiers = {s.get('tier') for s in pending_sells}

        for tier_pct, sell_frac in DCA_PROFIT_TIERS:
            if tier_pct <= highest_sold:
                continue  # already sold at this tier
            if tier_pct in pending_tiers:
                continue  # already have a pending sell at this tier

            if profit_pct >= tier_pct:
                sell_qty = held * sell_frac
                sell_px = avg_entry * (1 + tier_pct / 100.0)
                str_price = snap_to_increment(sell_px, quote_inc)
                str_qty = snap_to_increment(sell_qty, base_inc)

                if float(str_qty) <= 0:
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
                        })
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

            depth_mult = data.get('depth_multiplier', 1.0)
            buy_qty = base_min * depth_mult
            if deriv_flag:
                buy_qty = max(1, int(buy_qty))

            limit_px = cur_px - float(quote_inc)
            str_price = snap_to_increment(limit_px, quote_inc)
            str_qty = snap_to_increment(buy_qty, base_inc)

            buy_usd = float(str_qty) * limit_px * mult
            if buy_usd > bot['current_usd'] * 0.99 or float(str_qty) <= 0:
                print(f"[DCA | {pair}] Buy size ${buy_usd:.2f} exceeds available ${bot['current_usd']:.2f}")
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
