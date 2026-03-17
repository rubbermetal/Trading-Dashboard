import os
import json
import time
import uuid
import threading
import pandas as pd
from decimal import Decimal, ROUND_DOWN
from flask import Blueprint, jsonify, request
from datetime import datetime, timezone
from shared import client, ACTIVE_BOTS, new_bot_stats
from strategies import calculate_quad_rotation, calculate_quad_super, calculate_orb, calculate_advanced_grid

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
    parsed = [{'start': int(c['start']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close'])} for c in candles]
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
# WS USER CHANNEL: REAL-TIME GRID TRACKER
# ==========================================
def process_grid_fill(order_id, filled_size, filled_value, status, pair):
    """Processes real-time WS fills, calculates increments, and flips the grid order."""
    changes_made = False
    for bot_id, bot in ACTIVE_BOTS.items():
        if bot.get('strategy') != 'GRID' or bot.get('status') != 'RUNNING': continue
        if bot.get('pair') != pair: continue
        
        settings = bot.get('settings', {})
        active_grids = settings.get('active_grids', [])
        
        for i, grid in enumerate(active_grids):
            if grid['oid'] == order_id and status == 'FILLED':
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

        api_key = os.getenv('COINBASE_API_KEY', '')
        api_secret = os.getenv('COINBASE_API_SECRET', '')
        
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
# REFACTORED GRID INITIALIZER
# ==========================================
def execute_grid_bot(bot_id, bot, pair):
    """Initializes the grid with mode-aware order placement. WS engine handles ongoing flips."""
    settings = bot.get('settings', {})
    
    # 1. Fetch Constraints & Increments
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
    except Exception as e: return
        
    if len(candles) < 50: return
        
    parsed = [{'start': int(c['start']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close']), 'volume': float(c['volume'])} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)

    inventory_value = bot['asset_held'] * cur_px * mult
    inventory_pct = inventory_value / bot['allocated_usd'] if bot['allocated_usd'] > 0 else 0.0

    lower = settings.get('lower_price')
    upper = settings.get('upper_price')
    grid_count = settings.get('grid_count')
    mode = settings.get('mode', 'LONG').upper()
    step_pct = settings.get('step_pct', 0.6) / 100.0  # Convert to decimal

    signal, reason = calculate_advanced_grid(df, lower, upper, grid_count, inventory_pct)
    
    if "HALT" in reason or "DORMANT" in reason:
        print(f"[GRID BOT | {pair}] {reason}")
        return 

    # 2. Grid Array Initialization (only runs once)
    if 'active_grids' not in settings:
        step = cur_px * step_pct  # Step based on percentage, not range division
        active_grids = []
        
        # Store constraints for WS thread
        settings['base_inc'] = str(base_inc)
        settings['quote_inc'] = str(quote_inc)
        
        # Determine which levels to place based on mode
        buy_levels = []   # Prices below current where we place BUY limits
        sell_levels = []   # Prices above current where we place SELL limits
        
        # Generate levels from floor to ceiling at step intervals
        level = lower
        while level <= upper:
            if level < cur_px * 0.999:  # Below current price
                buy_levels.append(level)
            elif level > cur_px * 1.001:  # Above current price
                sell_levels.append(level)
            level += step
        
        # Filter by mode
        if mode == 'LONG':
            sell_levels = []
        elif mode == 'SHORT':
            buy_levels = []
            if not deriv_flag:
                print(f"[GRID BOT | {pair}] WARNING: SHORT mode on spot requires asset inventory.")
        # BOTH: use both lists as-is
        
        total_orders = len(buy_levels) + len(sell_levels)
        if total_orders == 0:
            print(f"[GRID BOT | {pair}] No valid grid levels in range. Check floor/ceiling.")
            return
            
        chunk_size_usd = bot['current_usd'] / total_orders
        
        # --- Place BUY orders below price ---
        for price in buy_levels:
            if deriv_flag:
                grid_qty = int(chunk_size_usd / (price * mult))
                if grid_qty < 1: continue
            else:
                grid_qty = float(chunk_size_usd * 0.99) / price

            str_price = snap_to_increment(price, quote_inc)
            str_qty = snap_to_increment(grid_qty, base_inc)
            if float(str_qty) <= 0: continue

            oid = str(uuid.uuid4())
            try:
                api_res = client.limit_order_gtc_buy(
                    client_order_id=oid, product_id=pair,
                    base_size=str_qty, limit_price=str_price, post_only=True
                )
                success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                    active_grids.append({"price": float(str_price), "side": "BUY", "oid": oid})
                else:
                    print(f"[GRID INIT] BUY Blocked at {str_price}: {fail_reason}")
            except Exception as e: print(f"[GRID INIT] BUY Exception at {str_price}: {e}")
        
        # --- Place SELL orders above price ---
        if sell_levels:
            if deriv_flag:
                # Derivatives: place sells directly, no inventory needed
                for price in sell_levels:
                    grid_qty = int(chunk_size_usd / (price * mult))
                    if grid_qty < 1: continue
                    
                    str_price = snap_to_increment(price, quote_inc)
                    str_qty = snap_to_increment(grid_qty, base_inc)
                    if float(str_qty) <= 0: continue

                    oid = str(uuid.uuid4())
                    try:
                        api_res = client.limit_order_gtc_sell(
                            client_order_id=oid, product_id=pair,
                            base_size=str_qty, limit_price=str_price, post_only=True
                        )
                        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                            active_grids.append({"price": float(str_price), "side": "SELL", "oid": oid})
                        else:
                            print(f"[GRID INIT] SELL Blocked at {str_price}: {fail_reason}")
                    except Exception as e: print(f"[GRID INIT] SELL Exception at {str_price}: {e}")
            else:
                # Spot: market buy inventory first, then place sell limits
                total_sell_qty = sum(float(chunk_size_usd * 0.99) / p for p in sell_levels)
                total_sell_cost = total_sell_qty * cur_px
                
                if total_sell_cost <= bot['current_usd'] * 0.95:
                    try:
                        buy_oid = str(uuid.uuid4())
                        client.market_order_buy(
                            client_order_id=buy_oid, product_id=pair,
                            quote_size=str(round(total_sell_cost * 0.99, 2))
                        )
                        bot['asset_held'] += total_sell_qty
                        bot['current_usd'] -= total_sell_cost
                        print(f"[GRID INIT] Market bought {total_sell_qty:.6f} inventory for sell grid")
                        time.sleep(1)  # Let fill settle
                        
                        for price in sell_levels:
                            grid_qty = float(chunk_size_usd * 0.99) / price
                            str_price = snap_to_increment(price, quote_inc)
                            str_qty = snap_to_increment(grid_qty, base_inc)
                            if float(str_qty) <= 0: continue

                            oid = str(uuid.uuid4())
                            try:
                                api_res = client.limit_order_gtc_sell(
                                    client_order_id=oid, product_id=pair,
                                    base_size=str_qty, limit_price=str_price, post_only=True
                                )
                                success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                                fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                                if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                                    active_grids.append({"price": float(str_price), "side": "SELL", "oid": oid})
                                else:
                                    print(f"[GRID INIT] SELL Blocked at {str_price}: {fail_reason}")
                            except Exception as e: print(f"[GRID INIT] SELL Exception at {str_price}: {e}")
                    except Exception as e:
                        print(f"[GRID INIT] Inventory buy failed: {e}. Skipping sell side.")
                else:
                    print(f"[GRID INIT] Insufficient capital for sell-side inventory. Skipping sell grid.")
        
        if active_grids:
            bot['settings']['active_grids'] = active_grids
            bot['settings']['step_size'] = step
            bot['settings']['chunk_size'] = chunk_size_usd
            save_bots()
            print(f"[GRID BOT] Deployed {len(active_grids)} levels ({mode} mode, {step_pct*100:.1f}% step)")
            
    # REST Monitoring loop is deliberately bypassed; handoff to WS Listener complete.

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
