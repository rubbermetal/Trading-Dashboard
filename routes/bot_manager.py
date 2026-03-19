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
            # Match by EITHER our client_order_id OR Coinbase's server order_id
            matched = (grid.get('oid') == order_id) or (grid.get('cb_oid') == order_id)
            if matched and status == 'FILLED':
                _processed_fill_oids.add(order_id)
                # Also add the other ID to prevent REST double-processing
                if grid.get('oid'): _processed_fill_oids.add(grid['oid'])
                if grid.get('cb_oid'): _processed_fill_oids.add(grid['cb_oid'])
                # Keep set from growing unbounded
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
                
                print(f"[WS ENGINE] Fill detected: {grid['side']} at {grid['price']:.2f} (order {order_id[:8]}...)")
                
                if grid['side'] == 'BUY':
                    new_price = grid['price'] + step_size
                    new_grid = place_grid_sell(pair, new_price, filled_size, base_inc, quote_inc, deriv_flag, mult)
                    
                    if new_grid:
                        active_grids[i] = new_grid
                        bot['asset_held'] += float(filled_size)
                        bot['current_usd'] -= float(filled_value)
                    else:
                        print(f"[WS GRID FLIP] Sell placement failed at {new_price:.2f}")
                        bot['asset_held'] += float(filled_size)
                        bot['current_usd'] -= float(filled_value)

                    # RISK ENGINE: Activate trailing stop
                    risk = bot['settings'].setdefault('risk', {})
                    total_levels = risk.get('total_buy_levels', 10)
                    level_idx = grid.get('level_idx', total_levels // 2)
                    activate_trail(bot, grid['price'], float(filled_size), level_idx,
                                   total_levels, step_size,
                                   sell_grid=new_grid if new_grid else None)
                    
                elif grid['side'] == 'SELL':
                    buy_price = grid['price'] - step_size
                    sell_price = grid['price']
                    record_trade(bot, buy_price, sell_price, filled_size, 'LONG', 'GRID_FLIP', pair, mult)

                    # RISK ENGINE: Deactivate trail for corresponding fill
                    deactivate_trail_by_sell(bot, sell_oid=grid.get('oid'), sell_cb_oid=grid.get('cb_oid'))
                    
                    if is_halted:
                        try: active_grids.pop(i)
                        except: pass
                        bot['asset_held'] -= float(filled_size)
                        bot['current_usd'] += float(filled_value)
                        print(f"[WS ENGINE] SELL filled during halt. No new BUY.")
                    else:
                        new_price = grid['price'] - step_size
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
                                client_oid = order.get('client_order_id', '')
                                server_oid = order.get('order_id', '')
                                f_size = float(order.get('cumulative_quantity', 0))
                                f_val = float(order.get('total_value_after_fees', 0))
                                # Fallback value calculation if fees object is empty
                                if f_val == 0: f_val = f_size * float(order.get('avg_price', 0))
                                pair = order.get('product_id')
                                # Try matching by client_order_id first, then server order_id
                                if client_oid:
                                    process_grid_fill(client_oid, f_size, f_val, 'FILLED', pair)
                                if server_oid:
                                    process_grid_fill(server_oid, f_size, f_val, 'FILLED', pair)
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
def cancel_all_pair_orders(pair):
    """
    Nuclear option: fetch ALL open orders for a pair from Coinbase, cancel them all.
    Returns the count of orders successfully cancelled.
    This bypasses any ID mismatch issues by getting real order_ids from Coinbase.
    """
    cancelled = 0
    try:
        open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "OPEN",
            "product_id": pair,
            "limit": 100
        })
        open_orders = open_res.get('orders', [])
        if not open_orders:
            return 0

        # Coinbase cancel API accepts batches of real order_ids
        real_ids = [o['order_id'] for o in open_orders if o.get('order_id')]
        if not real_ids:
            return 0

        # Cancel in batches of 10 (API limit safety)
        for i in range(0, len(real_ids), 10):
            batch = real_ids[i:i+10]
            try:
                res = client.cancel_orders(order_ids=batch)
                results = res.get('results', []) if isinstance(res, dict) else getattr(res, 'results', [])
                for r in results:
                    r_dict = r if isinstance(r, dict) else r.__dict__ if hasattr(r, '__dict__') else {}
                    if r_dict.get('success', False):
                        cancelled += 1
            except Exception as e:
                print(f"[GRID] Batch cancel error: {e}")
            time.sleep(0.2)  # Rate limit spacing

        print(f"[GRID] cancel_all_pair_orders({pair}): {cancelled}/{len(real_ids)} cancelled")
    except Exception as e:
        print(f"[GRID] cancel_all_pair_orders({pair}) fetch error: {e}")
    return cancelled

def cancel_order_safe(grid_entry):
    """
    Cancel a single grid order using its Coinbase-assigned order_id (cb_oid).
    Falls back to looking up the order by client_order_id if cb_oid is missing.
    Returns True on success.
    """
    cb_oid = grid_entry.get('cb_oid', '')
    client_oid = grid_entry.get('oid', '')

    # If we have the real Coinbase order_id, use it directly
    if cb_oid:
        try:
            res = client.cancel_orders(order_ids=[cb_oid])
            results = res.get('results', []) if isinstance(res, dict) else getattr(res, 'results', [])
            if results:
                r = results[0] if isinstance(results[0], dict) else results[0]
                success = r.get('success', False) if isinstance(r, dict) else getattr(r, 'success', False)
                if success:
                    return True
        except Exception as e:
            print(f"[GRID] Cancel by cb_oid failed for {cb_oid}: {e}")

    # Fallback: look up the real order_id from Coinbase by matching client_order_id
    if client_oid:
        try:
            open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "OPEN",
                "limit": 50
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
    """Places a single grid buy order. Returns grid dict with cb_oid or None."""
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
            # Extract Coinbase's server-assigned order_id
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
    """Places a single grid sell order. Returns grid dict with cb_oid or None."""
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
            # Extract Coinbase's server-assigned order_id
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
        grid_cb_oid = grid.get('cb_oid', '')
        
        # Skip if already processed by WS (check both IDs)
        if grid_oid in _processed_fill_oids or grid_cb_oid in _processed_fill_oids:
            continue
        
        # Match by either our client_order_id or Coinbase's server order_id
        filled_match = filled_map.get(grid_oid) or filled_map.get(grid_cb_oid)
        if not filled_match:
            continue
        
        # Mark BOTH IDs as processed to prevent WS double-flip
        if grid_oid: _processed_fill_oids.add(grid_oid)
        if grid_cb_oid: _processed_fill_oids.add(grid_cb_oid)
        # Also add the server order_id from the fill itself
        fill_server_id = filled_match.get('order_id', '')
        if fill_server_id: _processed_fill_oids.add(fill_server_id)
        if len(_processed_fill_oids) > 500:
            _processed_fill_oids.clear()

        filled_size = float(filled_match.get('filled_size', 0))
        avg_price = float(filled_match.get('average_filled_price', grid['price']))
        filled_value = filled_size * avg_price
        
        if filled_size <= 0:
            continue

        print(f"[GRID REST | {pair}] Fill detected: {grid['side']} at {grid['price']:.2f}")

        if grid['side'] == 'BUY':
            new_price = grid['price'] + step_size
            new_grid = place_grid_sell(pair, new_price, filled_size, base_inc, quote_inc, deriv_flag, mult)
            
            if new_grid:
                try:
                    idx = active_grids.index(grid)
                    active_grids[idx] = new_grid
                except ValueError:
                    active_grids.append(new_grid)
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value
                changes_made = True

                # RISK ENGINE: Activate trailing stop for this fill
                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels,
                               step_size, sell_grid=new_grid)
                print(f"[GRID REST | {pair}] BUY filled -> SELL at {new_price:.2f} "
                      f"(trail active, depth={risk.get('depth_score', 0)})")
            else:
                try: active_grids.remove(grid)
                except ValueError: pass
                bot['asset_held'] += filled_size
                bot['current_usd'] -= filled_value
                changes_made = True

                # Still activate trail even if sell placement failed
                risk = bot['settings'].setdefault('risk', {})
                total_levels = risk.get('total_buy_levels', 10)
                level_idx = grid.get('level_idx', total_levels // 2)
                activate_trail(bot, avg_price, filled_size, level_idx, total_levels, step_size)
                print(f"[GRID REST | {pair}] BUY filled, SELL flip failed. Trail active, inventory held.")

        elif grid['side'] == 'SELL':
            buy_price = grid['price'] - step_size
            record_trade(bot, buy_price, grid['price'], filled_size, 'LONG', 'GRID_FLIP', pair, mult)

            # RISK ENGINE: Deactivate trail for the corresponding buy fill
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
def grid_emergency_halt(bot_id, bot, pair, cur_px, reason, halt_mode='NEUTRAL'):
    """
    Tri-modal halt based on direction:
    FAVORABLE: Keep buys (trend is in our favor), widen trails
    ADVERSE: Cancel all buys, tighten trails
    NEUTRAL: Cancel buys as precaution, normal trails
    """
    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})
    active_grids = settings.get('active_grids', [])
    base_inc = settings.get('base_inc', '0.00000001')
    quote_inc = settings.get('quote_inc', '0.01')
    step_size = settings.get('step_size', 0)
    mult = get_contract_multiplier(pair)
    deriv_flag = is_derivative(pair)

    print(f"[GRID HALT | {pair}] {reason}")

    sell_grids = [g for g in active_grids if g['side'] == 'SELL']
    buy_grids = [g for g in active_grids if g['side'] == 'BUY']

    cancelled = 0
    cancelled_prices = risk.setdefault('cancelled_buy_levels', [])

    # In FAVORABLE halt: keep buys alive (trend is carrying us)
    # In ADVERSE/NEUTRAL: cancel buys
    if halt_mode != 'FAVORABLE':
        try:
            open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "OPEN", "product_id": pair, "limit": 100
            })
            open_orders = open_res.get('orders', [])

            sell_oids = {g.get('oid', '') for g in sell_grids}
            sell_cb_oids = {g.get('cb_oid', '') for g in sell_grids}

            buy_order_ids = []
            for o in open_orders:
                server_id = o.get('order_id', '')
                client_id = o.get('client_order_id', '')
                if client_id in sell_oids or server_id in sell_cb_oids:
                    continue
                if server_id:
                    buy_order_ids.append(server_id)

            for i in range(0, len(buy_order_ids), 10):
                batch = buy_order_ids[i:i+10]
                try:
                    res = client.cancel_orders(order_ids=batch)
                    results = res.get('results', []) if isinstance(res, dict) else getattr(res, 'results', [])
                    for r in results:
                        r_dict = r if isinstance(r, dict) else r.__dict__ if hasattr(r, '__dict__') else {}
                        if r_dict.get('success', False):
                            cancelled += 1
                except Exception as e:
                    print(f"[GRID HALT] Batch cancel error: {e}")
                time.sleep(0.2)

            # Track cancelled buy prices for redeployment
            for g in buy_grids:
                cancelled_prices.append(g['price'])

        except Exception as e:
            print(f"[GRID HALT | {pair}] Failed to fetch open orders: {e}")

        print(f"[GRID HALT | {pair}] {halt_mode}: Cancelled {cancelled} orders. Keeping {len(sell_grids)} sells.")
        # Remove buy grids from tracking
        active_grids_remaining = sell_grids
    else:
        print(f"[GRID HALT | {pair}] FAVORABLE: Keeping {len(buy_grids)} buys + {len(sell_grids)} sells alive.")
        active_grids_remaining = active_grids  # Keep everything

    # If holding inventory with no sells, place exit sell
    held = abs(bot.get('asset_held', 0))
    if held > 0 and not any(g['side'] == 'SELL' for g in active_grids_remaining) and step_size > 0:
        exit_px = cur_px + (step_size * 0.5)
        exit_grid = place_grid_sell(pair, exit_px, held, base_inc, quote_inc, deriv_flag, mult)
        if exit_grid:
            active_grids_remaining.append(exit_grid)
            print(f"[GRID HALT | {pair}] Placed exit SELL at {exit_px:.2f}")

    bot['settings']['active_grids'] = active_grids_remaining
    bot['settings']['halted'] = True
    bot['settings']['halted_reason'] = reason
    bot['settings']['halted_at'] = datetime.now(timezone.utc).isoformat()
    risk['halt_mode'] = halt_mode
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
            if cancel_order_safe(old_grid):
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
            if cancel_order_safe(old_grid):
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
            if cancel_order_safe(old_grid):
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
            if cancel_order_safe(old_grid):
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
# GRID RISK ENGINE
# ==========================================

def compute_direction(df):
    """Direction check using SMA 5 slope. Returns 'RISING', 'FALLING', or 'CHOPPY'."""
    if len(df) < 8:
        return "CHOPPY"
    try:
        sma5 = ta.sma(df['close'], 5)
        if sma5 is None or sma5.dropna().empty:
            return "CHOPPY"
        cur_sma = float(sma5.iloc[-1])
        prev_sma = float(sma5.iloc[-4]) if len(sma5) >= 4 else cur_sma
        cur_px = float(df['close'].iloc[-1])

        if prev_sma <= 0:
            return "CHOPPY"
        slope = (cur_sma - prev_sma) / prev_sma

        if cur_px > cur_sma and slope > 0:
            return "RISING"
        elif cur_px < cur_sma and slope < 0:
            return "FALLING"
        return "CHOPPY"
    except:
        return "CHOPPY"

def get_trail_distance(level_index, total_levels, step_size):
    """
    Calculate base trailing stop distance based on level position in grid.
    level_index: 0 = lowest price (bottom of grid, tightest trail)
                 total_levels-1 = highest price (top of grid, widest trail)
    """
    if total_levels <= 1:
        return step_size * 1.0

    # Position from top: 0=top (widest), total_levels-1=bottom (tightest)
    pos_from_top = (total_levels - 1) - level_index
    third = total_levels / 3.0

    if pos_from_top < third:
        return step_size * 3.0    # Top third: room to breathe
    elif pos_from_top < third * 2:
        return step_size * 2.0    # Middle third
    elif level_index == 0:
        return step_size * 1.0    # Very last level: grid-failed ejection
    else:
        return step_size * 1.5    # Bottom third

def calculate_max_loss(buy_levels, step_size, chunk_usd):
    """Worst-case max loss: every buy fills then every trail triggers at max distance."""
    total = len(buy_levels)
    if total == 0 or step_size <= 0:
        return 0.0
    max_loss = 0.0
    for i, lvl_px in enumerate(buy_levels):
        if lvl_px <= 0:
            continue
        trail_dist = get_trail_distance(i, total, step_size)
        qty = (chunk_usd * 0.99) / lvl_px
        max_loss += trail_dist * qty
    return max_loss

def init_risk_state(settings, buy_levels, step_size, chunk_usd, cur_px):
    """Initialize risk engine state at grid deployment."""
    total = len(buy_levels)
    max_loss = calculate_max_loss(buy_levels, step_size, chunk_usd)

    cb_price = 0.0
    if total > 0:
        lowest_trail = get_trail_distance(0, total, step_size)
        cb_price = buy_levels[0] - lowest_trail

    settings['risk'] = {
        "depth_score": 0,
        "direction": "CHOPPY",
        "halt_mode": None,
        "risk_current": round(max_loss, 4),
        "risk_max": round(max_loss, 4),
        "circuit_breaker_price": round(cb_price, 2),
        "per_fill_trails": [],
        "cancelled_buy_levels": [],
        "recovery_timestamps": [],
        "recovery_velocity": 0.0,
        "total_buy_levels": total,
    }
    return max_loss

def activate_trail(bot, fill_price, quantity, level_index, total_levels, step_size, sell_grid=None):
    """Create a trailing stop entry when a buy order fills."""
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
    """Remove a trailing stop when its corresponding sell order fills normally."""
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])

    for i, t in enumerate(trails):
        matched = False
        if sell_oid and t.get('sell_oid') == sell_oid:
            matched = True
        if sell_cb_oid and t.get('sell_cb_oid') == sell_cb_oid:
            matched = True
        if matched:
            trails.pop(i)
            timestamps = risk.setdefault('recovery_timestamps', [])
            timestamps.append(time.time())
            if len(timestamps) > 20:
                risk['recovery_timestamps'] = timestamps[-20:]
            break

    risk['depth_score'] = len(trails)

def check_trailing_stops(bot, cur_px, pair):
    """Check all active trailing stops. Market sell if any trigger."""
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
        if cur_px > t['high_water_mark']:
            t['high_water_mark'] = cur_px
        effective = t['base_trail_distance'] * t.get('trail_multiplier', 1.0)
        t['effective_trail'] = effective
        trigger_price = t['high_water_mark'] - effective
        if cur_px <= trigger_price:
            triggered.append(t)

    for t in triggered:
        qty = t['quantity']
        print(f"[RISK ENGINE | {pair}] TRAILING STOP: fill@{t['fill_price']:.2f} "
              f"HWM={t['high_water_mark']:.2f} trail={t['effective_trail']:.2f} exit@{cur_px:.2f}")
        try:
            oid = str(uuid.uuid4())
            str_qty = snap_to_increment(qty, base_inc)
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

            record_trade(bot, t['fill_price'], cur_px, qty, 'LONG', 'TRAILING_STOP', pair, mult)
            bot['asset_held'] -= qty
            bot['current_usd'] += qty * cur_px * 0.995

            # Cancel the corresponding grid sell order
            for j, g in enumerate(list(active_grids)):
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
            t['effective_trail'] * t['quantity'] for t in trails
        ), 4)
        save_bots()
        return True
    return False

def adjust_trail_multipliers(bot, halt_mode, depth):
    """Adjust trail distance multipliers based on halt mode, depth, and recovery."""
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])
    total_levels = risk.get('total_buy_levels', 10)
    velocity = risk.get('recovery_velocity', 0)

    for t in trails:
        m = 1.0
        # Halt mode
        if halt_mode == 'FAVORABLE':
            m *= 1.5
        elif halt_mode == 'ADVERSE':
            m *= 0.75
        # Depth escalation
        if depth >= 6:
            m *= 0.75
        elif depth >= 4:
            if t.get('level_index', 0) < total_levels * 0.3:
                m *= 0.75
        # Recovery momentum: widen deep fills to let runners run
        if velocity >= 2.0 and t.get('level_index', 0) < total_levels * 0.4:
            m *= 1.25
        t['trail_multiplier'] = round(m, 3)

def compute_recovery_velocity(risk):
    """Count depth levels recovered in the last 5 minutes."""
    timestamps = risk.get('recovery_timestamps', [])
    now = time.time()
    recent = [ts for ts in timestamps if now - ts < 300]
    velocity = float(len(recent))
    risk['recovery_velocity'] = velocity
    return velocity

def evaluate_depth_escalation(bot, pair, direction, cur_px):
    """Evaluate and act on depth-based risk: may cancel open buy orders."""
    risk = bot['settings'].get('risk', {})
    depth = risk.get('depth_score', 0)
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])

    if depth < 4:
        return

    buy_grids = sorted(
        [g for g in active_grids if g['side'] == 'BUY'],
        key=lambda g: g['price']
    )
    if not buy_grids:
        return

    cancelled_prices = risk.setdefault('cancelled_buy_levels', [])

    if depth >= 6:
        # CRITICAL: cancel ALL remaining buys
        count = 0
        for g in list(buy_grids):
            cancel_order_safe(g)
            cancelled_prices.append(g['price'])
            if g in active_grids:
                active_grids.remove(g)
            count += 1
        if count:
            print(f"[RISK ENGINE | {pair}] CRITICAL depth={depth}: Cancelled ALL {count} open buys")
            save_bots()

    elif depth >= 4 and direction == 'FALLING':
        # ELEVATED + FALLING: cancel bottom 1-2 buys
        to_cancel = buy_grids[:min(2, len(buy_grids))]
        count = 0
        for g in to_cancel:
            cancel_order_safe(g)
            cancelled_prices.append(g['price'])
            if g in active_grids:
                active_grids.remove(g)
            count += 1
        if count:
            print(f"[RISK ENGINE | {pair}] ELEVATED depth={depth} FALLING: Cancelled {count} lowest buys")
            save_bots()

def evaluate_buy_redeployment(bot, pair, direction, cur_px, step_size,
                               base_inc, quote_inc, deriv_flag, mult, chunk_usd):
    """Redeploy cancelled buy orders when conditions improve."""
    risk = bot['settings'].get('risk', {})
    depth = risk.get('depth_score', 0)
    cancelled = risk.get('cancelled_buy_levels', [])
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])

    if not cancelled or step_size <= 0 or chunk_usd <= 0:
        return
    if direction == 'FALLING' and depth > 3:
        return
    if depth > 3:
        return

    existing_prices = {round(g['price'], 2) for g in active_grids}
    total_levels = risk.get('total_buy_levels', 10)
    deployed = 0
    max_per_cycle = min(len(cancelled), 3)

    for i in range(max_per_cycle):
        new_price = cur_px - (step_size * (i + 1))
        while round(new_price, 2) in existing_prices:
            new_price -= step_size

        g = place_grid_buy(pair, new_price, chunk_usd, base_inc, quote_inc, deriv_flag, mult)
        if g:
            g['level_idx'] = max(0, total_levels - 1)
            active_grids.append(g)
            existing_prices.add(round(new_price, 2))
            deployed += 1

    if deployed:
        risk['cancelled_buy_levels'] = cancelled[deployed:]
        save_bots()
        print(f"[RISK ENGINE | {pair}] Redeployed {deployed} buys. "
              f"{len(cancelled) - deployed} still cancelled.")

def check_circuit_breaker(bot, cur_px, pair):
    """Absolute floor: market sell everything if total loss exceeds 6% of allocated capital."""
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])
    settings = bot.get('settings', {})
    if not trails:
        return False

    total_loss = 0.0
    for t in trails:
        loss = (t['fill_price'] - cur_px) * t['quantity']
        if loss > 0:
            total_loss += loss

    allocated = bot.get('allocated_usd', 0)
    if allocated <= 0:
        return False

    loss_pct = total_loss / allocated
    cb_threshold = 0.06

    if loss_pct >= cb_threshold:
        print(f"[CIRCUIT BREAKER | {pair}] TRIGGERED: ${total_loss:.2f} = "
              f"{loss_pct*100:.1f}% of ${allocated:.2f}")

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
                for t in trails:
                    record_trade(bot, t['fill_price'], cur_px, t['quantity'],
                                 'LONG', 'CIRCUIT_BREAKER', pair, mult)

                bot['asset_held'] = 0.0
                bot['current_usd'] += held * cur_px * 0.995
            except Exception as e:
                print(f"[CIRCUIT BREAKER | {pair}] Sell failed: {e}")

        risk['per_fill_trails'] = []
        risk['depth_score'] = 0
        risk['cancelled_buy_levels'] = []
        settings['active_grids'] = []
        settings['halted'] = True
        settings['halted_reason'] = (f"CIRCUIT BREAKER: {loss_pct*100:.1f}% loss "
                                      f"exceeded {cb_threshold*100:.0f}% threshold")
        settings['halted_at'] = datetime.now(timezone.utc).isoformat()
        risk['halt_mode'] = 'ADVERSE'
        save_bots()
        return True
    return False

def manage_runner_exits(bot, pair, cur_px):
    """
    For deep fills in recovery with high velocity: cancel the fixed sell order
    and let the trailing stop (with widened multiplier) manage the exit.
    This converts profitable fills from 'fixed grid flip' to 'trailing runner'.
    """
    risk = bot['settings'].get('risk', {})
    trails = risk.get('per_fill_trails', [])
    velocity = risk.get('recovery_velocity', 0)
    settings = bot.get('settings', {})
    active_grids = settings.get('active_grids', [])
    step_size = settings.get('step_size', 0)

    if velocity < 2.0 or step_size <= 0:
        return

    converted = 0
    for t in trails:
        profit_steps = (cur_px - t['fill_price']) / step_size if step_size > 0 else 0

        if profit_steps >= 2.0 and t.get('sell_oid'):
            # This fill is deep in profit and has a fixed sell -- convert to runner
            for j, g in enumerate(list(active_grids)):
                if ((t['sell_oid'] and g.get('oid') == t['sell_oid']) or
                    (t.get('sell_cb_oid') and g.get('cb_oid') == t.get('sell_cb_oid'))):
                    cancel_order_safe(g)
                    active_grids.remove(g)
                    t['sell_oid'] = ''
                    t['sell_cb_oid'] = ''
                    converted += 1
                    print(f"[RISK ENGINE | {pair}] RUNNER: fill@{t['fill_price']:.2f} "
                          f"now +{profit_steps:.1f} steps. Sell cancelled, trailing only.")
                    break

    if converted:
        save_bots()

# ==========================================
# REFACTORED GRID EXECUTOR (RISK ENGINE INTEGRATED)
# ==========================================
def execute_grid_bot(bot_id, bot, pair):
    """
    Runs every 15s. Grid Risk Engine integrated.

    Cycle order:
    1. Fetch price + candles
    2. Direction (SMA 5)
    3. ADX / ATR
    4. Sync depth score
    5. Circuit breaker check
    6. Trailing stop check
    7. Halt evaluation (ADX + direction -> halt mode)
    8. If halted: adjust trails, manage buys, check recovery
    9. If not halted: depth escalation, recovery velocity, runners
    10. REST fill check
    11. Buy redeployment
    12. Follow logic (blocked if depth > 3)
    13. INIT (no grids -> deploy with risk state)
    """
    settings = bot.get('settings', {})
    risk = settings.setdefault('risk', {})

    # --- 1. Fetch market data ---
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (100 * tf_sec)
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles",
                         params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
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

    parsed = [{'start': int(c['start']), 'high': float(c['high']), 'low': float(c['low']),
                'close': float(c['close']), 'volume': float(c['volume'])} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    # --- 2. Direction ---
    direction = compute_direction(df)
    risk['direction'] = direction

    # --- 3. ADX / ATR ---
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

    # --- 4. Sync depth ---
    trails = risk.get('per_fill_trails', [])
    depth = len(trails)
    risk['depth_score'] = depth

    # --- 5. Circuit breaker ---
    if check_circuit_breaker(bot, cur_px, pair):
        return

    # --- 6. Trailing stops ---
    if check_trailing_stops(bot, cur_px, pair):
        # Re-read state after potential sells
        active_grids = settings.get('active_grids', [])
        has_grids = bool(active_grids)
        trails = risk.get('per_fill_trails', [])
        depth = len(trails)
        risk['depth_score'] = depth

    # --- 7. Halt evaluation ---
    if has_grids and not is_halted:
        should_halt = False
        halt_reason = ''
        halt_mode = 'NEUTRAL'

        if curr_adx >= 25:
            should_halt = True
            if direction == 'RISING':
                halt_mode = 'FAVORABLE'
            elif direction == 'FALLING':
                halt_mode = 'ADVERSE'
            halt_reason = f"ADX={curr_adx:.1f} >= 25. Mode: {halt_mode}. Direction: {direction}."

        if not should_halt and lower and curr_atr > 0:
            tail_level = lower - (2.0 * curr_atr)
            if cur_px < tail_level:
                should_halt = True
                halt_mode = 'ADVERSE'
                halt_reason = f"Tail risk: price {cur_px:.2f} < {tail_level:.2f} (floor - 2*ATR)"

        if should_halt:
            risk['halt_mode'] = halt_mode
            grid_emergency_halt(bot_id, bot, pair, cur_px, halt_reason, halt_mode)
            # Immediately adjust trails for the new halt mode
            adjust_trail_multipliers(bot, halt_mode, depth)
            save_bots()
            return

    # --- 8. Halted state management ---
    if is_halted:
        halt_mode = risk.get('halt_mode', 'NEUTRAL')

        # Direction can shift each cycle -> update halt mode
        if direction == 'RISING':
            halt_mode = 'FAVORABLE'
        elif direction == 'FALLING':
            halt_mode = 'ADVERSE'
        else:
            halt_mode = 'NEUTRAL'
        risk['halt_mode'] = halt_mode

        # Adjust trails for current halt mode
        adjust_trail_multipliers(bot, halt_mode, depth)

        # Cancel open buys in adverse/neutral halt
        if halt_mode in ('ADVERSE', 'NEUTRAL'):
            buy_grids = [g for g in (active_grids or []) if g['side'] == 'BUY']
            if buy_grids:
                cancelled_prices = risk.setdefault('cancelled_buy_levels', [])
                for g in list(buy_grids):
                    cancel_order_safe(g)
                    cancelled_prices.append(g['price'])
                    if g in active_grids:
                        active_grids.remove(g)
                if buy_grids:
                    print(f"[GRID HALT | {pair}] {halt_mode}: Cancelled {len(buy_grids)} buys")

        # Check fills (sells may still complete during halt)
        if has_grids:
            grid_check_fills(bot_id, bot, pair)

        # Recalculate after fills
        active_grids = settings.get('active_grids', [])
        trails = risk.get('per_fill_trails', [])
        depth = len(trails)
        risk['depth_score'] = depth
        held = abs(bot.get('asset_held', 0))

        # Check if halt can clear
        if depth == 0 and held < 0.000001:
            if curr_adx < 25:
                settings.pop('halted', None)
                settings.pop('halted_reason', None)
                settings.pop('halted_at', None)
                risk['halt_mode'] = None
                risk['cancelled_buy_levels'] = []
                settings.pop('active_grids', None)
                settings.pop('step_size', None)
                settings.pop('chunk_size', None)
                save_bots()
                print(f"[GRID BOT | {pair}] Halt cleared. ADX={curr_adx:.1f}. Ready to redeploy.")
            else:
                print(f"[GRID BOT | {pair}] HALTED. Depth=0 but ADX={curr_adx:.1f} >= 25.")
        elif held > 0.000001 and depth == 0:
            # Inventory held but no trails tracking it -- re-place exit sell
            remaining = settings.get('active_grids', [])
            if not any(g['side'] == 'SELL' for g in remaining):
                step_size = settings.get('step_size', cur_px * 0.006)
                exit_px = cur_px + (step_size * 0.5)
                deriv_flag = is_derivative(pair)
                mult = get_contract_multiplier(pair)
                exit_grid = place_grid_sell(pair, exit_px, held, base_inc, quote_inc, deriv_flag, mult)
                if exit_grid:
                    remaining.append(exit_grid)
                    save_bots()
                    print(f"[GRID HALT | {pair}] Re-placed exit SELL at {exit_px:.2f}")

        save_bots()
        return

    # --- 9. Not halted: depth escalation + recovery ---
    compute_recovery_velocity(risk)
    adjust_trail_multipliers(bot, None, depth)

    if depth >= 4:
        evaluate_depth_escalation(bot, pair, direction, cur_px)

    # Runner mode: convert deep profitable fills to trail-only
    if risk.get('recovery_velocity', 0) >= 2.0:
        manage_runner_exits(bot, pair, cur_px)

    # Update current risk
    risk['risk_current'] = round(sum(
        t.get('effective_trail', t['base_trail_distance']) * t['quantity']
        for t in risk.get('per_fill_trails', [])
    ), 4) if risk.get('per_fill_trails') else risk.get('risk_max', 0)

    # --- 10. REST fill check ---
    active_grids = settings.get('active_grids', [])
    has_grids = active_grids and len(active_grids) > 0

    if has_grids:
        grid_check_fills(bot_id, bot, pair)
        active_grids = settings.get('active_grids', [])
        has_grids = active_grids and len(active_grids) > 0

    # --- 11. Buy redeployment ---
    step_size = settings.get('step_size', 0)
    chunk_usd = settings.get('chunk_size', 0)
    deriv_flag = is_derivative(pair)
    mult_val = get_contract_multiplier(pair)

    if risk.get('cancelled_buy_levels'):
        evaluate_buy_redeployment(bot, pair, direction, cur_px, step_size,
                                   base_inc, quote_inc, deriv_flag, mult_val, chunk_usd)

    # --- 12. Follow logic (blocked if depth > 3) ---
    if has_grids:
        follow_enabled = settings.get('follow', False)
        if follow_enabled:
            if depth > 3:
                print(f"[RISK ENGINE | {pair}] Follow BLOCKED: depth={depth} > 3")
            else:
                grid_follow(bot_id, bot, pair, cur_px, df)
        return

    # --- 13. INIT (no active grids -> deploy with risk state) ---
    if curr_adx >= 25:
        print(f"[GRID BOT | {pair}] DORMANT: ADX={curr_adx:.1f} >= 25. Waiting to deploy.")
        return

    # Sweep orphans before deploying
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

    # Initialize risk state BEFORE placing orders
    max_loss = init_risk_state(settings, buy_levels, step, chunk_size_usd, cur_px)
    print(f"[GRID INIT | {pair}] Max loss envelope: ${max_loss:.2f} "
          f"({max_loss/bot['allocated_usd']*100:.1f}% of capital)")

    # Place BUY orders (with level index for trail distance calculation)
    for idx, price in enumerate(buy_levels):
        g = place_grid_buy(pair, price, chunk_size_usd, base_inc, quote_inc, deriv_flag, mult)
        if g:
            g['level_idx'] = idx
            new_grids.append(g)

    # Place SELL orders
    if sell_levels:
        if deriv_flag:
            for price in sell_levels:
                g = place_grid_sell(pair, price, 0, base_inc, quote_inc, deriv_flag, mult,
                                   use_chunk=True, chunk_usd=chunk_size_usd)
                if g: new_grids.append(g)
        else:
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
        print(f"[GRID BOT] Deployed {len(new_grids)} levels ({mode} mode, "
              f"{step_pct*100:.1f}% step, follow={'ON' if settings.get('follow') else 'OFF'})")

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

        # Risk engine state (GRID bots only)
        if bot.get('strategy') == 'GRID':
            risk = bot.get('settings', {}).get('risk', {})
            b_copy['risk'] = {
                'depth_score': risk.get('depth_score', 0),
                'total_buy_levels': risk.get('total_buy_levels', 0),
                'direction': risk.get('direction', 'CHOPPY'),
                'halt_mode': risk.get('halt_mode'),
                'risk_current': risk.get('risk_current', 0),
                'risk_max': risk.get('risk_max', 0),
                'recovery_velocity': risk.get('recovery_velocity', 0),
                'cancelled_buys': len(risk.get('cancelled_buy_levels', [])),
                'active_trails': len(risk.get('per_fill_trails', [])),
            }
        
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
    profit_per_flip = per_order_usd * step_pct  # Gross profit per flip

    # Risk Engine: Calculate max loss envelope
    # Generate buy levels for max loss calc
    preview_buy_levels = []
    lvl = lower_price
    while lvl <= upper_price:
        if lvl < cur_px * 0.999:
            preview_buy_levels.append(lvl)
        lvl += step_usd

    max_loss_val = calculate_max_loss(preview_buy_levels, step_usd, per_order_usd)
    risk_pct = (max_loss_val / capital * 100) if capital > 0 else 0
    flips_to_recover = int(max_loss_val / profit_per_flip) + 1 if profit_per_flip > 0 else 0

    return jsonify({
        "current_price": round(cur_px, 6),
        "grid_count": grid_count,
        "lower_price": round(lower_price, 2),
        "upper_price": round(upper_price, 2),
        "step_usd": round(step_usd, 4),
        "step_pct": round(step_pct * 100, 2),
        "per_order_usd": round(per_order_usd, 2),
        "profit_per_flip": round(profit_per_flip, 4),
        "mode": mode,
        "max_loss": round(max_loss_val, 2),
        "risk_pct": round(risk_pct, 1),
        "flips_to_recover": flips_to_recover,
        "buy_levels": len(preview_buy_levels)
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