import time
import uuid
import pandas as pd
from shared import client
from strategies import calculate_quad_rotation, calculate_quad_super, calculate_orb, calculate_trap
from bot_utils import (
    get_bot_tf, is_derivative, get_contract_multiplier, 
    record_trade, save_bots
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
        
    parsed = [{'start': int(c['start']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close']), 'volume': float(c['volume'])} for c in candles]
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
