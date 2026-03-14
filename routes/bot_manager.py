import os
import json
import time
import uuid
import threading
import pandas as pd
from decimal import Decimal, ROUND_DOWN
from flask import Blueprint, jsonify, request
from shared import client, ACTIVE_BOTS
from strategies import calculate_quad_rotation, calculate_quad_super, calculate_orb, calculate_advanced_grid

bot_manager_bp = Blueprint('bot_manager', __name__)
BOTS_FILE = "bots.json"

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
    end_ts = int(time.time())
    start_ts = end_ts - (288 * 300) 
    
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": "FIVE_MINUTE"})
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
        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(abs(bot['asset_held'])))
            
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
        try:
            oid = str(uuid.uuid4())
            client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(abs(bot['asset_held'])))
            
            profit = (bot['entry_price'] - current_px) * abs(bot['asset_held']) * mult
            bot['current_usd'] = bot['allocated_usd'] + (profit * 0.995)
            
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot.pop('low_water_mark', None)
            bot.pop('trail_active', None)
            save_bots()
        except Exception as e: print(f"Exit failed: {e}")

def execute_quad(bot_id, bot, pair, mode='STANDARD'):
    end_ts = int(time.time())
    start_ts = end_ts - (250 * 900) 
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": "FIFTEEN_MINUTE"})
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
            save_bots()
        except Exception as e: print(f"Order failed: {e}")
        
    elif signal == 'SELL' and bot['asset_held'] > 0:
        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(bot['asset_held']))
            
            bot['current_usd'] += (bot['asset_held'] * current_px * mult) * 0.995 
            bot['asset_held'] = 0.0
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
    """Initializes the grid. Ongoing tracking is now handled purely by the WS engine."""
    settings = bot.get('settings', {})
    
    # 1. Fetch Constraints & Increments
    end_ts = int(time.time())
    start_ts = end_ts - (100 * 3600) 
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": "ONE_HOUR"})
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        
        # Pull exact exchange precision markers
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

    signal, reason = calculate_advanced_grid(df, lower, upper, grid_count, inventory_pct)
    
    if "HALT" in reason or "DORMANT" in reason:
        print(f"[GRID BOT | {pair}] {reason}")
        return 

    # 2. Grid Array Initialization
    if 'active_grids' not in settings:
        step = (upper - lower) / grid_count
        levels = [lower + (i * step) for i in range(grid_count + 1)]
        
        active_grids = []
        chunk_size_usd = bot['current_usd'] / grid_count 
        
        # Store constraints globally for the WS thread to use later
        settings['base_inc'] = str(base_inc)
        settings['quote_inc'] = str(quote_inc)
        
        for price in levels:
            if price < cur_px:
                if deriv_flag:
                    grid_qty = int(chunk_size_usd / (price * mult))
                    if grid_qty < 1: continue 
                else:
                    quote_sz = chunk_size_usd * 0.99
                    grid_qty = float(quote_sz) / price

                # Snapped Strings
                str_price = snap_to_increment(price, quote_inc)
                str_qty = snap_to_increment(grid_qty, base_inc)
                
                if float(str_qty) <= 0: continue

                oid = str(uuid.uuid4())
                try:
                    # TIF GTC Buy
                    api_res = client.limit_order_gtc_buy(
                        client_order_id=oid, 
                        product_id=pair, 
                        base_size=str_qty, 
                        limit_price=str_price, 
                        post_only=True
                    )
                    
                    success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                    fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                    
                    if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                        active_grids.append({"price": float(str_price), "side": "BUY", "oid": oid})
                    else:
                        print(f"[GRID INIT ERROR] Blocked: {fail_reason}")
                except Exception as e: print(f"[GRID INIT EXCEPTION]: {e}")
                
        if active_grids:
            bot['settings']['active_grids'] = active_grids
            bot['settings']['step_size'] = step
            bot['settings']['chunk_size'] = chunk_size_usd 
            save_bots()
            print(f"[GRID BOT] Sent {len(active_grids)} Limit GTC Maker levels to engine.")
            
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
        response_data[bot_id] = b_copy
    return jsonify(response_data)

@bot_manager_bp.route('/api/bots/start', methods=['POST'])
def start_bot():
    d = request.json
    bot_id = str(uuid.uuid4())[:8]
    ACTIVE_BOTS[bot_id] = {
        "pair": d['pair'].upper(),
        "strategy": d['strategy'].upper(),
        "status": "RUNNING",
        "allocated_usd": float(d['amount']),
        "current_usd": float(d['amount']),
        "asset_held": 0.0,
        "position_side": "FLAT",
        "settings": d.get('settings', {})
    }
    save_bots()
    threading.Thread(target=run_bot, args=(bot_id,), daemon=True).start()
    return jsonify(success=True, message="Bot started!")

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

load_bots()
