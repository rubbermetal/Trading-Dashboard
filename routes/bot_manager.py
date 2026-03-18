import os
import json
import time
import uuid
import threading
import pandas as pd
import pandas_ta as ta
from decimal import Decimal, ROUND_DOWN
from flask import Blueprint, jsonify, request
from datetime import datetime, timezone
from shared import client, ACTIVE_BOTS, new_bot_stats
from strategies import calculate_quad_rotation, calculate_quad_super, calculate_orb, calculate_advanced_grid, calculate_trap

bot_manager_bp = Blueprint('bot_manager', __name__)
BOTS_FILE = "bots.json"

# ==========================================
# TIMEFRAME CONFIGURATION
# ==========================================
# Coinbase granularity key -> (API string, seconds per candle)
TF_MAP = {
    "1m":  ("ONE_MINUTE",      60),
    "5m":  ("FIVE_MINUTE",     300),
    "15m": ("FIFTEEN_MINUTE",  900),
    "30m": ("THIRTY_MINUTE",   1800),
    "1h":  ("ONE_HOUR",        3600),
    "6h":  ("SIX_HOURS",       21600),
    "1d":  ("ONE_DAY",         86400),
}

# Strategy -> default timeframe
STRATEGY_DEFAULT_TF = {
    "QUAD":       "15m",
    "QUAD_SUPER": "15m",
    "ORB":        "5m",
    "GRID":       "1h",
    "TRAP":       "15m",
}

def get_bot_tf(bot):
    """Returns (cb_granularity_string, seconds) for a bot's configured timeframe."""
    tf_key = bot.get('timeframe', STRATEGY_DEFAULT_TF.get(bot.get('strategy', ''), '15m'))
    return TF_MAP.get(tf_key, TF_MAP['15m'])

# ==========================================
# DERIVATIVE SIZING & MULTIPLIER LOGIC
# ==========================================
def is_derivative(pair):
    return '-CDE' in pair or '-PERP' in pair

def get_contract_multiplier(pair):
    if not is_derivative(pair):
        return 1.0
    if 'BTC' in pair: return 0.01
    if 'ETH' in pair: return 0.1
    if 'DOGE' in pair: return 100.0
    if 'SHIB' in pair or 'PEPE' in pair: return 1000000.0
    return 1.0

# ==========================================
# INCREMENT SNAPPING LOGIC
# ==========================================
def snap_to_increment(value, increment):
    """Snaps a float to the exact multiple of the asset's tick/lot increment."""
    try:
        v = Decimal(str(value))
        i = Decimal(str(increment))
        snapped = (v / i).quantize(Decimal('1'), rounding=ROUND_DOWN) * i
        # Return as plain string without scientific notation or trailing zeros
        result = f"{snapped:f}"
        return result.rstrip('0').rstrip('.') if '.' in result else result
    except:
        return str(value)

# ==========================================
# TRADE RECORDING & STATS HELPERS
# ==========================================
def ensure_stats(bot):
    """Backfill stats object for bots created before the upgrade."""
    if 'stats' not in bot:
        bot['stats'] = new_bot_stats()
    # Ensure any newly added keys exist on older stats dicts
    defaults = new_bot_stats()
    for k, v in defaults.items():
        if k not in bot['stats']:
            bot['stats'][k] = v
    return bot['stats']

def record_trade(bot, entry_px, exit_px, size, side, exit_reason, pair, multiplier=1.0):
    """
    Records a completed trade into the bot's stats and trade_log.
    side: 'LONG' or 'SHORT'
    exit_reason: 'SIGNAL', 'STOP_LOSS', 'TRAILING_STOP', 'GRID_FLIP', 'MANUAL'
    """
    stats = ensure_stats(bot)
    
    if side == 'LONG':
        raw_pnl = (exit_px - entry_px) * abs(size) * multiplier
    else:
        raw_pnl = (entry_px - exit_px) * abs(size) * multiplier
    
    # Rough fee estimate (0.5% round-trip for taker, lower for maker)
    fee_est = abs(size) * multiplier * exit_px * 0.005
    net_pnl = raw_pnl - fee_est

    stats['total_trades'] += 1
    stats['total_pnl'] += net_pnl
    stats['total_fees_est'] += fee_est

    if net_pnl >= 0:
        stats['winning_trades'] += 1
        if net_pnl > stats['largest_win']:
            stats['largest_win'] = net_pnl
    else:
        stats['losing_trades'] += 1
        if net_pnl < stats['largest_loss']:
            stats['largest_loss'] = net_pnl

    if exit_reason in ('STOP_LOSS', 'TRAILING_STOP'):
        stats['stopped_out'] += 1

    stats['trade_log'].append({
        'pair': pair,
        'side': side,
        'entry_price': round(entry_px, 6),
        'exit_price': round(exit_px, 6),
        'size': round(abs(size), 8),
        'pnl': round(net_pnl, 4),
        'fee_est': round(fee_est, 4),
        'exit_reason': exit_reason,
        'timestamp': datetime.now(timezone.utc).isoformat()
    })
    
    save_bots()

# ==========================================
# STATE RECOVERY
# ==========================================
def save_bots():
    with open(BOTS_FILE, 'w') as f:
        json.dump(ACTIVE_BOTS, f)

def load_bots():
    if os.path.exists(BOTS_FILE):
        try:
            with open(BOTS_FILE, 'r') as f:
                loaded = json.load(f)
                ACTIVE_BOTS.update(loaded)
                for bot_id, data in ACTIVE_BOTS.items():
                    if data.get('status') == 'RUNNING':
                        threading.Thread(target=run_bot, args=(bot_id,), daemon=True).start()
        except Exception as e:
            print(f"Failed to load bots: {e}")

# ==========================================
# THE STRATEGY ENGINE
# ==========================================
def run_bot(bot_id):
    print(f"[BOT ENGINE] Started thread for Bot ID: {bot_id}")
    while True:
        bot = ACTIVE_BOTS.get(bot_id)
        
        if not bot or bot.get('status') != 'RUNNING':
            print(f"[BOT ENGINE] Stopping thread for Bot ID: {bot_id}")
            break 
            
        pair = bot['pair']
        strategy = bot['strategy']
        
        try:
            if strategy == 'QUAD':
                execute_quad(bot_id, bot, pair, mode='STANDARD')
            elif strategy == 'QUAD_SUPER':
                execute_quad(bot_id, bot, pair, mode='SUPER')
            elif strategy == 'ORB':
                execute_orb(bot_id, bot, pair)
            elif strategy == 'GRID':
                execute_grid_bot(bot_id, bot, pair)
            elif strategy == 'TRAP':
                execute_trap(bot_id, bot, pair)
        except Exception as e:
            print(f"[BOT ENGINE] Error in {pair} {strategy} bot: {e}")
            
        time.sleep(15)

# ==========================================
# STRATEGY EXECUTORS
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

# ==========================================
# TRAP STRATEGY EXECUTOR
# ==========================================
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
        # Spot only for longs
        alloc = bot['current_usd'] * 0.25
        if deriv_flag:
            qty = int((alloc * 0.99) / (current_px * mult))
            if qty < 1:
                print(f"[TRAP BOT | {pair}] Insufficient for 1 contract at 25%")
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
            print(f"[TRAP BOT | {pair}] Cannot short spot. Skipping.")
            return

        alloc = bot['current_usd'] * 0.25
        qty = int((alloc * 0.99) / (current_px * mult))
        if qty < 1:
            print(f"[TRAP BOT | {pair}] Insufficient for 1 contract at 25%")
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
        alloc = bot['current_usd'] * 0.99  # Use remaining capital
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

            # Calculate new weighted average entry
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
# WS USER CHANNEL: REAL-TIME GRID TRACKER
# ==========================================
# Deduplication: prevent WS and REST from double-processing the same fill
_processed_fill_oids = set()

def process_grid_fill(order_id, filled_size, filled_value, status, pair):
    """Processes real-time WS fills, calculates increments, and flips the grid order."""
    if order_id in _processed_fill_oids:
        return
    
    changes_made = False
    for bot_id, bot in ACTIVE_BOTS.items():
        if bot.get('strategy') != 'GRID' or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair: continue
        
        settings = bot.get('settings', {})
        active_grids = settings.get('active_grids', [])
        
        for i, grid in enumerate(active_grids):
            if grid['oid'] == order_id and status == 'FILLED':
                _processed_fill_oids.add(order_id)
                # Keep set from growing unbounded
                if len(_processed_fill_oids) > 500:
                    _processed_fill_oids.clear()
                changes_made = True
                base_inc = settings.get('base_inc', '0.00000001')
                quote_inc = settings.get('quote_inc', '0.01')
                step_size = settings.get('step_size')
                mult = get_contract_multiplier(pair)
                deriv_flag = is_derivative(pair)
                
                print(f"[WS ENGINE] Flipped Order ID {order_id}. Initiating Maker Flip.")
                
                if grid['side'] == 'BUY':
                    new_price = grid['price'] + step_size
                    new_oid = str(uuid.uuid4())
                    
                    str_price = snap_to_increment(new_price, quote_inc)
                    str_qty = snap_to_increment(filled_size, base_inc)
                    
                    try:
                        api_res = client.limit_order_gtc_sell(
                            client_order_id=new_oid, 
                            product_id=pair, 
                            base_size=str_qty, 
                            limit_price=str_price, 
                            post_only=True
                        )
                        
                        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                        
                        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                            active_grids[i] = {"price": float(str_price), "side": "SELL", "oid": new_oid}
                            bot['asset_held'] += float(str_qty)
                            bot['current_usd'] -= float(filled_value)
                        else:
                            print(f"[WS GRID FLIP] Sell Rejected: {fail_reason}")
                            
                    except Exception as e: print(f"[WS GRID FLIP] Sell Exception: {e}")
                    
                elif grid['side'] == 'SELL':
                    # A SELL fill completes a round-trip: bought at (price - step), sold at price
                    buy_price = grid['price'] - step_size
                    sell_price = grid['price']
                    record_trade(bot, buy_price, sell_price, filled_size, 'LONG', 'GRID_FLIP', pair, mult)
                    
                    new_price = grid['price'] - step_size
                    new_oid = str(uuid.uuid4())
                    
                    if deriv_flag:
                        grid_qty = int(settings['chunk_size'] / (new_price * mult))
                    else:
                        quote_sz = settings['chunk_size'] * 0.99
                        grid_qty = float(quote_sz) / new_price
                        
                    str_price = snap_to_increment(new_price, quote_inc)
                    str_qty = snap_to_increment(grid_qty, base_inc)
                    
                    try:
                        api_res = client.limit_order_gtc_buy(
                            client_order_id=new_oid, 
                            product_id=pair, 
                            base_size=str_qty, 
                            limit_price=str_price, 
                            post_only=True
                        )
                        
                        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                        
                        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                            active_grids[i] = {"price": float(str_price), "side": "BUY", "oid": new_oid}
                            bot['asset_held'] -= float(filled_size)
                            bot['current_usd'] += float(filled_value)
                        else:
                            print(f"[WS GRID FLIP] Buy Rejected: {fail_reason}")
                            
                    except Exception as e: print(f"[WS GRID FLIP] Buy Exception: {e}")
                    
    if changes_made:
        save_bots()

def start_user_ws():
    """Background Daemon for listening to User Channel WS execution events."""
    try:
        from coinbase.websocket import WSClient
        
        def on_message(msg):
            try:
                data = json.loads(msg) if isinstance(msg, str) else msg
                for event in data.get('events', []):
                    if event.get('type') == 'update':
                        for order in event.get('orders', []):
                            if order.get('status') == 'FILLED':
                                oid = order.get('client_order_id') or order.get('order_id')
                                f_size = float(order.get('cumulative_quantity', 0))
                                f_val = float(order.get('total_value_after_fees', 0))
                                # Fallback value calculation if fees object is empty
                                if f_val == 0: f_val = f_size * float(order.get('avg_price', 0))
                                pair = order.get('product_id')
                                process_grid_fill(oid, f_size, f_val, 'FILLED', pair)
            except Exception as e:
                pass # Suppress noisy WS parsing errors

        api_key = os.getenv('COINBASE_API_KEY_NAME', '')
        api_secret = os.getenv('COINBASE_API_PRIVATE_KEY', '')
        
        if api_key and api_secret:
            ws_client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message)
            ws_client.open()
            ws_client.subscribe(product_ids=[], channel_name="user")
            ws_client.run_forever_with_exception_fallback()
        else:
            print("[WS ENGINE] Missing API Keys in Env. WS Tracking Disabled.")
    except Exception as e:
        print(f"[WS ENGINE] Init error: {e}")

# Start the WebSocket Listener
threading.Thread(target=start_user_ws, daemon=True).start()

# ==========================================
# GRID HELPERS
# ==========================================
def cancel_order_safe(order_id):
    """Attempts to cancel an order, returns True on success."""
    try:
        res = client.cancel_orders(order_ids=[order_id])
        results = res.get('results', []) if isinstance(res, dict) else getattr(res, 'results', [])
        if results:
            r = results[0] if isinstance(results[0], dict) else results[0]
            return r.get('success', False) if isinstance(r, dict) else getattr(r, 'success', False)
        return False
    except Exception as e:
        print(f"[GRID] Cancel failed for {order_id}: {e}")
        return False

def place_grid_buy(pair, price, chunk_usd, base_inc, quote_inc, deriv_flag, mult):
    """Places a single grid buy order. Returns grid dict or None."""
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
            client_order_id=oid, product_id=pair,
            base_size=str_qty, limit_price=str_price, post_only=True
        )
        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
            return {"price": float(str_price), "side": "BUY", "oid": oid}
        else:
            print(f"[GRID] BUY rejected at {str_price}: {fail_reason}")
    except Exception as e:
        print(f"[GRID] BUY exception at {str_price}: {e}")
    return None

def place_grid_sell(pair, price, qty_or_chunk, base_inc, quote_inc, deriv_flag, mult, use_chunk=False, chunk_usd=0):
    """Places a single grid sell order. Returns grid dict or None."""
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
            client_order_id=oid, product_id=pair,
            base_size=str_qty, limit_price=str_price, post_only=True
        )
        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
            return {"price": float(str_price), "side": "SELL", "oid": oid}
        else:
            print(f"[GRID] SELL rejected at {str_price}: {fail_reason}")
    except Exception as e:
        print(f"[GRID] SELL exception at {str_price}: {e}")
    return None

# ==========================================
# REST-BASED FILL CHECKER (15s FALLBACK)
# ==========================================
def grid_check_fills(bot_id, bot, pair):
    """
    Single REST call to check all grid orders for fills.
    If a BUY filled -> place SELL at price + step.
    If a SELL filled -> record trade, place BUY at price - step.
    """
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])
    if not active_grids:
        return

    step_size = settings.get('step_size', 0)
    chunk_usd = settings.get('chunk_size', 0)
    base_inc = settings.get('base_inc', '0.00000001')
    quote_inc = settings.get('quote_inc', '0.01')
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)
    is_halted = settings.get('halted', False)

    if step_size <= 0:
        return

    # One API call: fetch recent filled orders for this pair
    try:
        order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "FILLED",
            "product_id": pair,
            "limit": 50
        })
        filled_orders = order_data.get('orders', [])
    except Exception as e:
        print(f"[GRID REST | {pair}] Fill check API error: {e}")
        return
    
    # Build lookup: client_order_id -> order data
    filled_map = {}
    for o in filled_orders:
        coid = o.get('client_order_id', '')
        oid = o.get('order_id', '')
        if coid: filled_map[coid] = o
        if oid: filled_map[oid] = o

    changes_made = False

    for i, grid in enumerate(list(active_grids)):
        grid_oid = grid.get('oid', '')
        if not grid_oid:
            continue
        
        # Skip if already processed by WS
        if grid_oid in _processed_fill_oids:
            continue
        
        filled_match = filled_map.get(grid_oid)
        if not filled_match:
            continue
        
        # Mark as processed to prevent WS double-flip
        _processed_fill_oids.add(grid_oid)
        if len(_processed_fill_oids) > 500:
            _processed_fill_oids.clear()

        filled_size = float(filled_match.get('filled_size', 0))
        avg_price = float(filled_match.get('average_filled_price', grid['price']))
        filled_value = filled_size * avg_price
        
        if filled_size <= 0:
            continue

        print(f"[GRID REST | {pair}] Fill detected: {grid['side']} at {grid['price']:.2f}")

        if grid['side'] == 'BUY':
            # During halt: don't place new sells from buy fills if we want to just exit
            # Actually we DO want to place the sell -- that's how we close the position profitably
            new_price = grid['price'] + step_size
            new_grid = place_grid_sell(pair, new_price, filled_size, base_inc, quote_inc, deriv_flag, mult)
            
            if new_grid:
                # Find the index in the current list (may have shifted)
                try:
                    idx = active_grids.index(grid)
                    active_grids[idx] = new_grid
                except ValueError:
                    active_grids.append(new_grid)
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value
                changes_made = True
                print(f"[GRID REST | {pair}] Flipped BUY -> SELL at {new_price:.2f}")
            else:
                # Remove the filled grid even if flip failed
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value
                changes_made = True
                print(f"[GRID REST | {pair}] BUY filled but SELL flip failed. Inventory held.")

        elif grid['side'] == 'SELL':
            # Record completed round-trip
            buy_price = grid['price'] - step_size
            record_trade(bot, buy_price, grid['price'], filled_size, 'LONG', 'GRID_FLIP', pair, mult)

            if is_halted:
                # During halt: don't place new buys, just remove the grid
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] -= filled_size
                bot['current_usd'] += filled_value
                changes_made = True
                print(f"[GRID REST | {pair}] SELL filled during halt. Position closed. No new BUY.")
            else:
                # Normal: flip to BUY
                new_price = grid['price'] - step_size
                new_grid = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
                
                if new_grid:
                    try:
                        idx = active_grids.index(grid)
                        active_grids[idx] = new_grid
                    except ValueError:
                        active_grids.append(new_grid)
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

# ==========================================
# GRID EMERGENCY HALT (PRESERVES SELLS)
# ==========================================
def grid_emergency_halt(bot_id, bot, pair, cur_px, reason):
    """
    Cancel BUY orders only. Keep existing SELL orders to close inventory profitably.
    If holding inventory with no sell orders, place limit sells at entry + step to exit at profit.
    """
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])
    base_inc = settings.get('base_inc', '0.00000001')
    quote_inc = settings.get('quote_inc', '0.01')
    step_size = settings.get('step_size', 0)
    mult = get_contract_multiplier(pair)
    deriv_flag = is_derivative(pair)
    
    print(f"[GRID HALT | {pair}] {reason}")
    
    # 1. Cancel BUY orders only -- keep sells to close positions
    buy_grids = [g for g in active_grids if g['side'] == 'BUY']
    sell_grids = [g for g in active_grids if g['side'] == 'SELL']
    
    cancelled = 0
    for grid in buy_grids:
        if cancel_order_safe(grid['oid']):
            cancelled += 1
    
    print(f"[GRID HALT | {pair}] Cancelled {cancelled}/{len(buy_grids)} BUY orders. Keeping {len(sell_grids)} SELL orders.")
    
    # 2. If we hold inventory but have no sell orders, place limit sells to exit
    held = abs(bot.get('asset_held', 0))
    if held > 0 and len(sell_grids) == 0 and step_size > 0:
        # Place a sell at current price + half a step (tight but still profitable vs entry)
        exit_px = cur_px + (step_size * 0.5)
        str_price = snap_to_increment(exit_px, quote_inc)
        str_qty = snap_to_increment(held, base_inc)
        
        if float(str_qty) > 0:
            oid = str(uuid.uuid4())
            try:
                api_res = client.limit_order_gtc_sell(
                    client_order_id=oid, product_id=pair,
                    base_size=str_qty, limit_price=str_price, post_only=True
                )
                success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                if success or (isinstance(api_res, dict) and api_res.get('failure_reason', '') == 'UNKNOWN_FAILURE_REASON'):
                    sell_grids.append({"price": float(str_price), "side": "SELL", "oid": oid})
                    print(f"[GRID HALT | {pair}] Placed exit SELL at {str_price} for {str_qty} units.")
                else:
                    print(f"[GRID HALT | {pair}] Exit SELL rejected. Will retry next cycle.")
            except Exception as e:
                print(f"[GRID HALT | {pair}] Exit SELL exception: {e}")
    
    # 3. Update active_grids to only contain remaining sells
    bot['settings']['active_grids'] = sell_grids if sell_grids else []
    bot['settings']['halted'] = True
    bot['settings']['halted_reason'] = reason
    bot['settings']['halted_at'] = datetime.now(timezone.utc).isoformat()
    save_bots()

# ==========================================
# GRID FOLLOW ENGINE
# ==========================================
def grid_follow(bot_id, bot, pair, cur_px, df):
    """
    Slides the grid window to follow price.
    Cancels orders that are too far from price and deploys new ones near it.
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
    
    if not active_grids or step_size <= 0 or chunk_usd <= 0:
        return
    
    # Determine the grid window boundaries
    buy_grids = [g for g in active_grids if g['side'] == 'BUY']
    sell_grids = [g for g in active_grids if g['side'] == 'SELL']
    
    all_prices = [g['price'] for g in active_grids]
    if not all_prices:
        return
    
    grid_low = min(all_prices)
    grid_high = max(all_prices)
    
    # How many steps has price moved beyond the grid edges?
    # We allow 1 step of buffer before triggering follow
    changes_made = False
    
    # --- PRICE MOVED ABOVE GRID: Cancel lowest buys, add new levels above ---
    if cur_px > grid_high + step_size and mode != 'SHORT':
        # Find the lowest buy orders to recycle
        stale = sorted(buy_grids, key=lambda g: g['price'])
        # Recycle up to 2 orders per cycle to avoid API spam
        to_recycle = stale[:min(2, len(stale))]
        
        for old_grid in to_recycle:
            if cancel_order_safe(old_grid['oid']):
                # Place a new buy one step below current price
                new_buy_px = cur_px - step_size
                # Avoid duplicates
                existing_prices = {round(g['price'], 2) for g in active_grids}
                while round(new_buy_px, 2) in existing_prices:
                    new_buy_px -= step_size
                
                new_grid = place_grid_buy(pair, new_buy_px, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
                if new_grid:
                    active_grids.remove(old_grid)
                    active_grids.append(new_grid)
                    changes_made = True
                    print(f"[GRID FOLLOW | {pair}] Recycled BUY {old_grid['price']:.2f} -> {new_buy_px:.2f}")
                else:
                    active_grids.remove(old_grid)
                    changes_made = True
    
    # --- PRICE MOVED BELOW GRID: Cancel highest sells, add new levels below ---
    elif cur_px < grid_low - step_size and mode != 'LONG':
        stale = sorted(sell_grids, key=lambda g: -g['price'])
        to_recycle = stale[:min(2, len(stale))]
        
        for old_grid in to_recycle:
            if cancel_order_safe(old_grid['oid']):
                new_sell_px = cur_px + step_size
                existing_prices = {round(g['price'], 2) for g in active_grids}
                while round(new_sell_px, 2) in existing_prices:
                    new_sell_px += step_size
                
                new_grid = place_grid_sell(pair, new_sell_px, 0, base_inc, quote_inc, deriv_flag, mult,
                                          use_chunk=True, chunk_usd=chunk_usd)
                if new_grid:
                    active_grids.remove(old_grid)
                    active_grids.append(new_grid)
                    changes_made = True
                    print(f"[GRID FOLLOW | {pair}] Recycled SELL {old_grid['price']:.2f} -> {new_sell_px:.2f}")
                else:
                    active_grids.remove(old_grid)
                    changes_made = True
    
    # --- LONG MODE: Price moved below grid, recycle highest buys downward ---
    elif cur_px < grid_low - step_size and mode == 'LONG':
        stale = sorted(buy_grids, key=lambda g: -g['price'])
        to_recycle = stale[:min(2, len(stale))]
        
        for old_grid in to_recycle:
            if cancel_order_safe(old_grid['oid']):
                new_buy_px = cur_px - step_size
                existing_prices = {round(g['price'], 2) for g in active_grids}
                while round(new_buy_px, 2) in existing_prices:
                    new_buy_px -= step_size
                
                new_grid = place_grid_buy(pair, new_buy_px, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
                if new_grid:
                    active_grids.remove(old_grid)
                    active_grids.append(new_grid)
                    changes_made = True
                    print(f"[GRID FOLLOW | {pair}] Followed DOWN: BUY {old_grid['price']:.2f} -> {new_buy_px:.2f}")
                else:
                    active_grids.remove(old_grid)
                    changes_made = True
    
    # --- SHORT MODE: Price moved above grid, recycle lowest sells upward ---
    elif cur_px > grid_high + step_size and mode == 'SHORT':
        stale = sorted(sell_grids, key=lambda g: g['price'])
        to_recycle = stale[:min(2, len(stale))]
        
        for old_grid in to_recycle:
            if cancel_order_safe(old_grid['oid']):
                new_sell_px = cur_px + step_size
                existing_prices = {round(g['price'], 2) for g in active_grids}
                while round(new_sell_px, 2) in existing_prices:
                    new_sell_px += step_size
                
                new_grid = place_grid_sell(pair, new_sell_px, 0, base_inc, quote_inc, deriv_flag, mult,
                                          use_chunk=True, chunk_usd=chunk_usd)
                if new_grid:
                    active_grids.remove(old_grid)
                    active_grids.append(new_grid)
                    changes_made = True
                    print(f"[GRID FOLLOW | {pair}] Followed UP: SELL {old_grid['price']:.2f} -> {new_sell_px:.2f}")
                else:
                    active_grids.remove(old_grid)
                    changes_made = True
    
    if changes_made:
        # Update floor/ceiling to reflect new grid window
        remaining_prices = [g['price'] for g in active_grids]
        if remaining_prices:
            settings['lower_price'] = min(remaining_prices)
            settings['upper_price'] = max(remaining_prices)
        save_bots()

# ==========================================
# REFACTORED GRID EXECUTOR (4-STATE MACHINE)
# ==========================================
def execute_grid_bot(bot_id, bot, pair):
    """
    Runs every 15s. Four states:
    1. HALTED: Adverse conditions hit -- only process remaining sell orders, no new buys
    2. CHECK FILLS: Always check if any grid orders filled (REST fallback)
    3. FOLLOW: Grids exist, follow=True -> slide window to track price
    4. INIT: No active grids -> deploy initial grid
    
    Emergency halt triggers: ADX >= 25 or tail-risk (2 ATR below floor)
    """
    settings = bot.get('settings', {})
    
    # 1. Fetch market data
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
        
    if len(candles) < 50:
        print(f"[GRID BOT | {pair}] Only {len(candles)} candles, need 50. Waiting...")
        return
        
    parsed = [{'start': int(c['start']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close']), 'volume': float(c['volume'])} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)
    
    # 2. Regime check
    try:
        adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
        atr_series = ta.atr(df['high'], df['low'], df['close'], length=14)
        curr_adx = float(adx_df.iloc[-1, 0]) if adx_df is not None and not adx_df.empty else 0.0
        curr_atr = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else 0.0
    except:
        curr_adx, curr_atr = 0.0, 0.0

    active_grids = settings.get('active_grids')
    has_grids = active_grids and len(active_grids) > 0
    is_halted = settings.get('halted', False)
    lower = settings.get('lower_price')
    
    # --- STATE: HALTED (only process sell fills, no new buys) ---
    if is_halted and has_grids:
        # Still check fills so sell orders get processed
        grid_check_fills(bot_id, bot, pair)
        
        # If all sells have filled (no more grids or only empty list), clear halt
        remaining = settings.get('active_grids', [])
        held = abs(bot.get('asset_held', 0))
        
        if not remaining and held < 0.000001:
            settings.pop('halted', None)
            settings.pop('halted_reason', None)
            settings.pop('halted_at', None)
            settings.pop('active_grids', None)
            settings.pop('step_size', None)
            settings.pop('chunk_size', None)
            save_bots()
            print(f"[GRID BOT | {pair}] Halt cleared -- all positions closed. Ready to re-deploy when conditions improve.")
        elif held > 0 and not any(g['side'] == 'SELL' for g in remaining):
            # Still holding inventory but no sell orders -- re-place exit sell
            step_size = settings.get('step_size', cur_px * 0.006)
            exit_px = cur_px + (step_size * 0.5)
            str_price = snap_to_increment(exit_px, quote_inc)
            str_qty = snap_to_increment(held, base_inc)
            if float(str_qty) > 0:
                oid = str(uuid.uuid4())
                try:
                    api_res = client.limit_order_gtc_sell(
                        client_order_id=oid, product_id=pair,
                        base_size=str_qty, limit_price=str_price, post_only=True
                    )
                    success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                    if success or (isinstance(api_res, dict) and api_res.get('failure_reason', '') == 'UNKNOWN_FAILURE_REASON'):
                        remaining.append({"price": float(str_price), "side": "SELL", "oid": oid})
                        save_bots()
                        print(f"[GRID HALT | {pair}] Re-placed exit SELL at {str_price}")
                except Exception as e:
                    print(f"[GRID HALT | {pair}] Exit SELL retry failed: {e}")
        return
    
    # --- ALWAYS: Check fills on active grids (REST fallback) ---
    if has_grids:
        grid_check_fills(bot_id, bot, pair)
        # Re-read after fill check (grids may have changed)
        active_grids = settings.get('active_grids', [])
        has_grids = active_grids and len(active_grids) > 0
    
    # --- TRIGGER: Emergency halt check (only when grids are live and not already halted) ---
    if has_grids and not is_halted:
        if curr_adx >= 25:
            grid_emergency_halt(bot_id, bot, pair, cur_px, f"ADX={curr_adx:.1f} >= 25. Trend too strong.")
            return
        
        if lower and curr_atr > 0:
            tail_level = lower - (2.0 * curr_atr)
            if cur_px < tail_level:
                grid_emergency_halt(bot_id, bot, pair, cur_px, f"Tail risk: price {cur_px:.2f} < {tail_level:.2f} (floor - 2*ATR)")
                return
    
    # --- STATE: FOLLOW (grids exist, follow enabled) ---
    if has_grids:
        follow_enabled = settings.get('follow', False)
        if follow_enabled:
            grid_follow(bot_id, bot, pair, cur_px, df)
        return
    
    # --- STATE: INIT (no active grids -> deploy) ---
    if curr_adx >= 25:
        print(f"[GRID BOT | {pair}] DORMANT: ADX={curr_adx:.1f} >= 25. Waiting to deploy.")
        return

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
    
    # Generate levels
    buy_levels, sell_levels = [], []
    level = lower
    while level <= upper:
        if level < cur_px * 0.999:
            buy_levels.append(level)
        elif level > cur_px * 1.001:
            sell_levels.append(level)
        level += step
    
    if mode == 'LONG':
        sell_levels = []
    elif mode == 'SHORT':
        buy_levels = []
        if not deriv_flag:
            print(f"[GRID BOT | {pair}] WARNING: SHORT mode on spot requires inventory.")
    
    total_orders = len(buy_levels) + len(sell_levels)
    if total_orders == 0:
        print(f"[GRID BOT | {pair}] No valid levels in range.")
        return
        
    chunk_size_usd = bot['current_usd'] / total_orders
    
    # Place BUY orders
    for price in buy_levels:
        g = place_grid_buy(pair, price, chunk_size_usd, base_inc, quote_inc, deriv_flag, mult)
        if g: new_grids.append(g)
    
    # Place SELL orders
    if sell_levels:
        if deriv_flag:
            for price in sell_levels:
                g = place_grid_sell(pair, price, 0, base_inc, quote_inc, deriv_flag, mult,
                                  use_chunk=True, chunk_usd=chunk_size_usd)
                if g: new_grids.append(g)
        else:
            # Spot: buy inventory first
            total_sell_qty = sum(float(chunk_size_usd * 0.99) / p for p in sell_levels)
            total_sell_cost = total_sell_qty * cur_px
            
            if total_sell_cost <= bot['current_usd'] * 0.95:
                try:
                    buy_oid = str(uuid.uuid4())
                    client.market_order_buy(client_order_id=buy_oid, product_id=pair,
                                          quote_size=str(round(total_sell_cost * 0.99, 2)))
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

@bot_manager_bp.route('/api/bots', methods=['GET'])
def get_bots():
    response_data = {}
    for bot_id, bot in ACTIVE_BOTS.items():
        live_px = 0.0
        try:
            p = client.get_product(product_id=bot['pair'])
            live_px = float(p.price)
        except: pass
        
        mult = get_contract_multiplier(bot['pair'])
        pos_side = bot.get('position_side', 'LONG') 
        
        if bot['current_usd'] > 0 and bot['asset_held'] == 0: 
            net_val = bot['current_usd']
        elif pos_side == 'LONG': 
            net_val = bot['current_usd'] + (bot['asset_held'] * live_px * mult)
        elif pos_side == 'SHORT':
            profit = (bot.get('entry_price', live_px) - live_px) * abs(bot['asset_held']) * mult
            net_val = bot['allocated_usd'] + profit
        else: 
            net_val = bot['allocated_usd']
            
        pnl = net_val - bot['allocated_usd']
        b_copy = bot.copy()
        b_copy['live_pnl'] = pnl
        b_copy['net_value'] = net_val
        
        # Ensure timeframe exists (migration for pre-upgrade bots)
        if 'timeframe' not in bot:
            bot['timeframe'] = STRATEGY_DEFAULT_TF.get(bot.get('strategy', ''), '15m')
        b_copy['timeframe'] = bot['timeframe']
        
        # Ensure stats exist (migration for pre-upgrade bots)
        stats = ensure_stats(bot)
        b_copy['stats'] = stats.copy()
        # Don't send full trade_log in list view (can be large)
        b_copy['stats']['trade_log'] = len(stats.get('trade_log', []))
        
        # Computed convenience fields
        total_t = stats['total_trades']
        b_copy['stats']['win_rate'] = round((stats['winning_trades'] / total_t) * 100, 1) if total_t > 0 else 0.0
        b_copy['stats']['avg_pnl'] = round(stats['total_pnl'] / total_t, 4) if total_t > 0 else 0.0
        
        response_data[bot_id] = b_copy
    return jsonify(response_data)

@bot_manager_bp.route('/api/bots/<bot_id>/trades', methods=['GET'])
def get_bot_trades(bot_id):
    """Returns the full trade log for a specific bot."""
    if bot_id not in ACTIVE_BOTS:
        return jsonify(success=False, error="Bot not found.")
    stats = ensure_stats(ACTIVE_BOTS[bot_id])
    return jsonify(stats.get('trade_log', []))

# ==========================================
# GRID PREVIEW / AUTO-CALCULATOR
# ==========================================
@bot_manager_bp.route('/api/bots/grid_preview', methods=['POST'])
def grid_preview():
    """Calculates grid parameters for preview before deployment."""
    d = request.json
    pair = d.get('pair', '')
    capital = float(d.get('capital', 0))
    step_pct = float(d.get('step_pct', 0.6)) / 100.0  # Convert to decimal
    min_order_usd = float(d.get('min_order_usd', 5))
    mode = d.get('mode', 'BOTH').upper()

    if capital <= 0 or not pair:
        return jsonify(error="Invalid pair or capital.")

    try:
        p = client.get_product(product_id=pair)
        cur_px = float(p.price)
    except:
        return jsonify(error="Could not fetch price for pair.")

    if cur_px <= 0:
        return jsonify(error="Invalid price.")

    # Grid count from capital / min order
    grid_count = max(2, int(capital / min_order_usd))
    per_order_usd = capital / grid_count
    step_usd = cur_px * step_pct

    # Floor/ceiling based on mode
    if mode == 'LONG':
        # All grids below current price
        lower_price = cur_px - (grid_count * step_usd)
        upper_price = cur_px
    elif mode == 'SHORT':
        # All grids above current price
        lower_price = cur_px
        upper_price = cur_px + (grid_count * step_usd)
    else:
        # BOTH: split evenly around price
        half = grid_count // 2
        lower_price = cur_px - (half * step_usd)
        upper_price = cur_px + ((grid_count - half) * step_usd)

    # Estimated profit per grid flip (step_pct - fee)
    # Maker fee ~0.4% round-trip on Advanced; user sets step to cover it
    profit_per_flip = per_order_usd * step_pct  # Gross profit per flip

    return jsonify({
        "current_price": round(cur_px, 6),
        "grid_count": grid_count,
        "lower_price": round(lower_price, 2),
        "upper_price": round(upper_price, 2),
        "step_usd": round(step_usd, 4),
        "step_pct": round(step_pct * 100, 2),
        "per_order_usd": round(per_order_usd, 2),
        "profit_per_flip": round(profit_per_flip, 4),
        "mode": mode
    })

@bot_manager_bp.route('/api/bots/start', methods=['POST'])
def start_bot():
    d = request.json
    strategy = d['strategy'].upper()
    tf = d.get('timeframe', STRATEGY_DEFAULT_TF.get(strategy, '15m'))
    if tf not in TF_MAP:
        tf = STRATEGY_DEFAULT_TF.get(strategy, '15m')
    
    bot_id = str(uuid.uuid4())[:8]
    ACTIVE_BOTS[bot_id] = {
        "pair": d['pair'].upper(),
        "strategy": strategy,
        "status": "RUNNING",
        "allocated_usd": float(d['amount']),
        "current_usd": float(d['amount']),
        "asset_held": 0.0,
        "position_side": "FLAT",
        "timeframe": tf,
        "settings": d.get('settings', {}),
        "stats": new_bot_stats(),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    # Record the initial deposit
    ACTIVE_BOTS[bot_id]['stats']['deposits'] = float(d['amount'])
    save_bots()
    threading.Thread(target=run_bot, args=(bot_id,), daemon=True).start()
    return jsonify(success=True, message=f"Bot started on {tf} timeframe!")

@bot_manager_bp.route('/api/bots/timeframe/<bot_id>', methods=['POST'])
def update_bot_tf(bot_id):
    """Update a bot's timeframe. Takes effect on the next strategy cycle (15s)."""
    if bot_id not in ACTIVE_BOTS:
        return jsonify(success=False, error="Bot not found.")
    d = request.json
    tf = d.get('timeframe', '')
    if tf not in TF_MAP:
        return jsonify(success=False, error=f"Invalid timeframe. Use: {', '.join(TF_MAP.keys())}")
    
    ACTIVE_BOTS[bot_id]['timeframe'] = tf
    save_bots()
    return jsonify(success=True, message=f"Timeframe updated to {tf}. Active on next cycle.")

@bot_manager_bp.route('/api/bots/follow/<bot_id>', methods=['POST'])
def update_bot_follow(bot_id):
    """Toggle follow mode for a grid bot."""
    if bot_id not in ACTIVE_BOTS:
        return jsonify(success=False, error="Bot not found.")
    bot = ACTIVE_BOTS[bot_id]
    if bot.get('strategy') != 'GRID':
        return jsonify(success=False, error="Follow mode is only available for GRID bots.")
    try:
        d = request.json
        follow = bool(d.get('follow', False))
        bot.setdefault('settings', {})['follow'] = follow
        save_bots()
        msg = "Follow mode enabled." if follow else "Follow mode disabled."
        return jsonify(success=True, message=msg)
    except Exception as e:
        return jsonify(success=False, error=str(e))

@bot_manager_bp.route('/api/bots/stop/<bot_id>', methods=['POST'])
def stop_bot(bot_id):
    if bot_id in ACTIVE_BOTS:
        ACTIVE_BOTS[bot_id]['status'] = "STOPPED"
        save_bots()
        return jsonify(success=True, message="Bot gracefully stopped.")
    return jsonify(success=False, error="Bot not found.")

@bot_manager_bp.route('/api/bots/restart/<bot_id>', methods=['POST'])
def restart_bot(bot_id):
    if bot_id in ACTIVE_BOTS:
        if ACTIVE_BOTS[bot_id]['status'] == 'STOPPED':
            ACTIVE_BOTS[bot_id]['status'] = "RUNNING"
            save_bots()
            threading.Thread(target=run_bot, args=(bot_id,), daemon=True).start()
            return jsonify(success=True, message="Bot restarted successfully.")
        return jsonify(success=False, error="Bot is already running.")
    return jsonify(success=False, error="Bot not found.")

@bot_manager_bp.route('/api/bots/delete/<bot_id>', methods=['DELETE'])
def delete_bot(bot_id):
    if bot_id in ACTIVE_BOTS:
        if ACTIVE_BOTS[bot_id]['status'] == 'RUNNING':
            return jsonify(success=False, error="Stop the bot before deleting it.")
        del ACTIVE_BOTS[bot_id]
        save_bots()
        return jsonify(success=True, message="Bot permanently deleted.")
    return jsonify(success=False, error="Bot not found.")

@bot_manager_bp.route('/api/bots/deposit/<bot_id>', methods=['POST'])
def deposit_to_bot(bot_id):
    """Add funds to a bot's sandbox account."""
    if bot_id not in ACTIVE_BOTS:
        return jsonify(success=False, error="Bot not found.")
    d = request.json
    amount = float(d.get('amount', 0))
    if amount <= 0:
        return jsonify(success=False, error="Amount must be positive.")
    
    bot = ACTIVE_BOTS[bot_id]
    bot['current_usd'] += amount
    bot['allocated_usd'] += amount
    stats = ensure_stats(bot)
    stats['deposits'] += amount
    save_bots()
    return jsonify(success=True, message=f"Deposited ${amount:.2f} into bot {bot_id}.")

@bot_manager_bp.route('/api/bots/withdraw/<bot_id>', methods=['POST'])
def withdraw_from_bot(bot_id):
    """Remove funds from a bot's sandbox account (only idle USD, not held assets)."""
    if bot_id not in ACTIVE_BOTS:
        return jsonify(success=False, error="Bot not found.")
    d = request.json
    amount = float(d.get('amount', 0))
    if amount <= 0:
        return jsonify(success=False, error="Amount must be positive.")
    
    bot = ACTIVE_BOTS[bot_id]
    if amount > bot['current_usd']:
        return jsonify(success=False, error=f"Insufficient idle USD. Available: ${bot['current_usd']:.2f}")
    
    bot['current_usd'] -= amount
    bot['allocated_usd'] -= amount
    stats = ensure_stats(bot)
    stats['withdrawals'] += amount
    save_bots()
    return jsonify(success=True, message=f"Withdrew ${amount:.2f} from bot {bot_id}.")

load_bots()
