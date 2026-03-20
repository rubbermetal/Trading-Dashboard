import os
import json
import time
import uuid
import threading
from flask import Blueprint, jsonify, request
from datetime import datetime, timezone

# --- Shared & Core Imports ---
from shared import client, ACTIVE_BOTS, new_bot_stats
from bot_utils import (
    save_bots, ensure_stats, get_contract_multiplier, 
    TF_MAP, STRATEGY_DEFAULT_TF
)

# --- Engine & Strategy Imports ---
from bot_executors import execute_orb, execute_quad, execute_trap
from grid_engine import execute_grid_bot, calculate_max_loss, calculate_grid_pnl
from bot_ws import start_ws_engine

bot_manager_bp = Blueprint('bot_manager', __name__)
BOTS_FILE = "bots.json"

# ==========================================
# STATE RECOVERY & ENGINE LOOP
# ==========================================
def load_bots():
    """Loads bot state from disk and restarts running thread loops."""
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

def run_bot(bot_id):
    """The master thread loop for a single bot. Evaluates every 15s."""
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
# API ENDPOINTS (DASHBOARD UI)
# ==========================================
@bot_manager_bp.route('/api/bots', methods=['GET'])
def get_bots():
    """Returns the ledger state of all virtual bots for the dashboard."""
    response_data = {}
    for bot_id, bot in ACTIVE_BOTS.items():
        live_px = 0.0
        try:
            p = client.get_product(product_id=bot['pair'])
            live_px = float(p.price)
        except: pass
        
        mult = get_contract_multiplier(bot['pair'])
        
        # --- PnL Calculation ---
        if bot.get('strategy') == 'GRID':
            # GRID bots: value = idle cash + inventory at market price
            pnl, net_val = calculate_grid_pnl(bot, live_px)
        else:
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
        
        # Ensure timeframe exists
        if 'timeframe' not in bot:
            bot['timeframe'] = STRATEGY_DEFAULT_TF.get(bot.get('strategy', ''), '15m')
        b_copy['timeframe'] = bot['timeframe']
        
        # Format stats for UI delivery
        stats = ensure_stats(bot)
        b_copy['stats'] = stats.copy()
        b_copy['stats']['trade_log'] = len(stats.get('trade_log', []))
        
        total_t = stats['total_trades']
        b_copy['stats']['win_rate'] = round((stats['winning_trades'] / total_t) * 100, 1) if total_t > 0 else 0.0
        b_copy['stats']['avg_pnl'] = round(stats['total_pnl'] / total_t, 4) if total_t > 0 else 0.0

        # Inject Risk Engine state for GRID bots
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
    if bot_id not in ACTIVE_BOTS:
        return jsonify(success=False, error="Bot not found.")
    stats = ensure_stats(ACTIVE_BOTS[bot_id])
    return jsonify(stats.get('trade_log', []))

@bot_manager_bp.route('/api/bots/grid_preview', methods=['POST'])
def grid_preview():
    """Calculates deterministic grid parameters for preview before deployment."""
    d = request.json
    pair = d.get('pair', '')
    capital = float(d.get('capital', 0))
    step_pct = float(d.get('step_pct', 0.6)) / 100.0
    min_order_usd = float(d.get('min_order_usd', 5))
    mode = d.get('mode', 'BOTH').upper()

    if capital <= 0 or not pair: return jsonify(error="Invalid pair or capital.")

    try:
        p = client.get_product(product_id=pair)
        cur_px = float(p.price)
    except:
        return jsonify(error="Could not fetch price for pair.")

    if cur_px <= 0: return jsonify(error="Invalid price.")

    grid_count = max(2, int(capital / min_order_usd))
    per_order_usd = capital / grid_count
    step_usd = cur_px * step_pct

    if mode == 'LONG':
        lower_price = cur_px - (grid_count * step_usd)
        upper_price = cur_px
    elif mode == 'SHORT':
        lower_price = cur_px
        upper_price = cur_px + (grid_count * step_usd)
    else:
        half = grid_count // 2
        lower_price = cur_px - (half * step_usd)
        upper_price = cur_px + ((grid_count - half) * step_usd)

    profit_per_flip = per_order_usd * step_pct

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
    ACTIVE_BOTS[bot_id]['stats']['deposits'] = float(d['amount'])
    save_bots()
    threading.Thread(target=run_bot, args=(bot_id,), daemon=True).start()
    return jsonify(success=True, message=f"Bot started on {tf} timeframe!")

@bot_manager_bp.route('/api/bots/timeframe/<bot_id>', methods=['POST'])
def update_bot_tf(bot_id):
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
    if bot_id not in ACTIVE_BOTS: return jsonify(success=False, error="Bot not found.")
    d = request.json
    amount = float(d.get('amount', 0))
    if amount <= 0: return jsonify(success=False, error="Amount must be positive.")
    
    bot = ACTIVE_BOTS[bot_id]
    bot['current_usd'] += amount
    bot['allocated_usd'] += amount
    ensure_stats(bot)['deposits'] += amount
    save_bots()
    return jsonify(success=True, message=f"Deposited ${amount:.2f} into bot {bot_id}.")

@bot_manager_bp.route('/api/bots/withdraw/<bot_id>', methods=['POST'])
def withdraw_from_bot(bot_id):
    if bot_id not in ACTIVE_BOTS: return jsonify(success=False, error="Bot not found.")
    d = request.json
    amount = float(d.get('amount', 0))
    if amount <= 0: return jsonify(success=False, error="Amount must be positive.")
    
    bot = ACTIVE_BOTS[bot_id]
    if amount > bot['current_usd']: return jsonify(success=False, error=f"Insufficient idle USD. Available: ${bot['current_usd']:.2f}")
    
    bot['current_usd'] -= amount
    bot['allocated_usd'] -= amount
    ensure_stats(bot)['withdrawals'] += amount
    save_bots()
    return jsonify(success=True, message=f"Withdrew ${amount:.2f} from bot {bot_id}.")

# ==========================================
# APP INITIALIZATION
# ==========================================
start_ws_engine()
load_bots()
