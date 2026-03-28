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
from bot_executors import execute_orb, execute_quad, execute_trap, execute_momentum, execute_dca, execute_npr
from grid_engine import execute_grid_bot, calculate_max_loss, calculate_grid_pnl, cancel_all_pair_orders, cancel_order_safe
from bot_ws import start_ws_engine

bot_manager_bp = Blueprint('bot_manager', __name__)
BOTS_FILE = "bots.json"

# ==========================================
# ORDER CLEANUP HELPERS
# ==========================================
def _cancel_by_client_oid(pair, client_oid):
    """Cancel a single open order by its client_order_id. Returns True if cancelled."""
    try:
        open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "OPEN", "product_id": pair, "limit": 50
        })
        for o in open_res.get('orders', []):
            if o.get('client_order_id') == client_oid:
                real_id = o.get('order_id')
                if real_id:
                    client.cancel_orders(order_ids=[real_id])
                    return True
                break
    except Exception as e:
        print(f"[CLEANUP] Cancel by client_oid error: {e}")
    return False

def _cleanup_bot_orders(bot_id, bot):
    """Cancel all open exchange orders belonging to a bot. Returns {cancelled, errors}."""
    strategy = bot.get('strategy', '')
    pair = bot.get('pair', '')
    cancelled = 0
    errors = []

    try:
        if strategy == 'GRID':
            # GRID can have many orders — cancel each tracked grid individually
            # to avoid nuking orders from other bots on the same pair
            for g in bot.get('settings', {}).get('active_grids', []):
                try:
                    if cancel_order_safe(g):
                        cancelled += 1
                except Exception as e:
                    errors.append(str(e))
            bot.get('settings', {})['active_grids'] = []
            risk = bot.get('settings', {}).get('risk', {})
            risk['per_fill_trails'] = []
            risk['cancelled_buy_levels'] = []
            risk['depth_score'] = 0
            print(f"[CLEANUP] GRID {pair}: cancelled {cancelled} grid orders")

        elif strategy == 'MOMENTUM':
            oid = bot.get('pending_order_oid')
            if oid and _cancel_by_client_oid(pair, oid):
                cancelled += 1
                print(f"[CLEANUP] MOMENTUM {pair}: cancelled pending entry")
            bot.pop('pending_order_oid', None)
            bot.pop('pending_order_time', None)
            bot.pop('signal_retries', None)

        elif strategy == 'DCA':
            buy_oid = bot.get('pending_buy_oid')
            if buy_oid and _cancel_by_client_oid(pair, buy_oid):
                cancelled += 1
                print(f"[CLEANUP] DCA {pair}: cancelled pending buy")
            bot.pop('pending_buy_oid', None)
            bot.pop('pending_buy_time', None)
            # Cancel all pending sell tiers
            for sell in bot.get('pending_sells', []):
                sell_oid = sell.get('oid', '')
                if sell_oid:
                    try:
                        if _cancel_by_client_oid(pair, sell_oid):
                            cancelled += 1
                    except Exception as e:
                        errors.append(str(e))
            if bot.get('pending_sells'):
                print(f"[CLEANUP] DCA {pair}: cancelled {cancelled - (1 if buy_oid else 0)} pending sells")
            bot['pending_sells'] = []

        elif strategy == 'NPR':
            oid = bot.get('pending_order_oid')
            if oid and _cancel_by_client_oid(pair, oid):
                cancelled += 1
                print(f"[CLEANUP] NPR {pair}: cancelled pending entry")
            bot.pop('pending_order_oid', None)
            bot.pop('pending_order_time', None)
            bot.pop('entry_retries', None)

        # ORB, QUAD, QUAD_SUPER, TRAP use market orders only — no cleanup needed

    except Exception as e:
        errors.append(str(e))
        print(f"[CLEANUP] Error cleaning up bot {bot_id}: {e}")

    return {'cancelled': cancelled, 'errors': errors}

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
            elif strategy == 'MOMENTUM':
                execute_momentum(bot_id, bot, pair)
            elif strategy == 'DCA':
                execute_dca(bot_id, bot, pair)
            elif strategy == 'NPR':
                execute_npr(bot_id, bot, pair)
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

        # Inject MOMENTUM state
        if bot.get('strategy') == 'MOMENTUM':
            b_copy['momentum'] = {
                'stop_phase': bot.get('stop_phase', 0),
                'entry_atr': bot.get('entry_atr', 0),
                'high_water_mark': bot.get('high_water_mark', 0),
                'fee_estimate': bot.get('fee_estimate', 0),
                'pending': bool(bot.get('pending_order_oid')),
                'retries': bot.get('signal_retries', 0),
            }

        # Inject DCA state
        if bot.get('strategy') == 'DCA':
            avg_e = bot.get('avg_entry', 0)
            profit_pct = ((live_px - avg_e) / avg_e * 100) if avg_e > 0 and live_px > 0 else 0
            # Determine next tier
            highest_sold = bot.get('highest_tier_sold', 0)
            next_tier = None
            for t_pct, _ in [(3.0, 0.20), (5.0, 0.25), (7.5, 0.30), (10.0, 0.35), (15.0, 0.50), (20.0, 0.75)]:
                if t_pct > highest_sold:
                    next_tier = t_pct
                    break
            # Correlation guard status
            concurrent = sum(
                1 for b in ACTIVE_BOTS.values()
                if b.get('strategy') == 'DCA'
                and b.get('pair') != bot.get('pair')
                and b.get('dca_state') in ('ARMED', 'BUYING')
            )
            b_copy['dca'] = {
                'state': bot.get('dca_state', 'SCANNING'),
                'avg_entry': round(avg_e, 6),
                'total_buys': bot.get('total_buys', 0),
                'profit_pct': round(profit_pct, 2),
                'next_tier': next_tier,
                'highest_tier_sold': highest_sold,
                'pending_buy': bool(bot.get('pending_buy_oid')),
                'buy_pct': bot.get('buy_pct', 2.0),
                'pending_sells': len(bot.get('pending_sells', [])),
                'paused': bot.get('dca_state') == 'PAUSED',
                'corr_count': concurrent,
            }
        
        if bot.get('strategy') == 'NPR':
            b_copy['npr'] = {
                'state': bot.get('npr_state', 'SCANNING'),
                'event_type': bot.get('event_type'),
                'event_direction': bot.get('event_direction'),
                'zone': bot.get('zone', 0),
                'check_score': bot.get('check_score', 0),
                'event_stop': bot.get('event_stop', 0),
                'daily_loss': round(bot.get('daily_loss', 0), 2),
                'max_loss_day': bot.get('max_loss_per_day', 30),
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

@bot_manager_bp.route('/api/bots/clone/<bot_id>', methods=['POST'])
def clone_bot(bot_id):
    """Clone a bot's strategy/settings to a new pair with fresh capital."""
    if bot_id not in ACTIVE_BOTS:
        return jsonify(success=False, error="Bot not found.")
    d = request.json or {}
    source = ACTIVE_BOTS[bot_id]
    new_pair = d.get('pair', source['pair']).upper()
    new_amount = float(d.get('amount', source.get('allocated_usd', 50)))

    new_id = str(uuid.uuid4())[:8]
    ACTIVE_BOTS[new_id] = {
        "pair": new_pair,
        "strategy": source['strategy'],
        "status": "RUNNING",
        "allocated_usd": new_amount,
        "current_usd": new_amount,
        "asset_held": 0.0,
        "position_side": "FLAT",
        "timeframe": source.get('timeframe', '15m'),
        "settings": {k: v for k, v in source.get('settings', {}).items() if k not in ('active_grids', 'risk')},
        "stats": new_bot_stats(),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    if source.get('buy_pct'):
        ACTIVE_BOTS[new_id]['buy_pct'] = source['buy_pct']
    ACTIVE_BOTS[new_id]['stats']['deposits'] = new_amount
    save_bots()
    threading.Thread(target=run_bot, args=(new_id,), daemon=True).start()
    return jsonify(success=True, message=f"Cloned {source['strategy']} to {new_pair} with ${new_amount:.2f}", bot_id=new_id)

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
        bot = ACTIVE_BOTS[bot_id]
        # Set STOPPED first so the run_bot thread and WS engine stop placing new orders
        bot['status'] = "STOPPED"
        save_bots()

        # Cancel all open exchange orders belonging to this bot
        result = _cleanup_bot_orders(bot_id, bot)
        save_bots()

        cancelled = result['cancelled']
        errors = result['errors']
        msg = f"Bot stopped. {cancelled} open order(s) cancelled."
        if errors:
            msg += f" {len(errors)} error(s) during cleanup."
        return jsonify(success=True, message=msg)
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
        bot = ACTIVE_BOTS[bot_id]
        if bot['status'] == 'RUNNING':
            return jsonify(success=False, error="Stop the bot before deleting it.")

        # Verify no orphaned orders remain on the exchange
        pair = bot.get('pair', '')
        orphan_count = 0
        try:
            bot_oids = set()
            for g in bot.get('settings', {}).get('active_grids', []):
                if g.get('oid'): bot_oids.add(g['oid'])
            if bot.get('pending_order_oid'): bot_oids.add(bot['pending_order_oid'])
            if bot.get('pending_buy_oid'): bot_oids.add(bot['pending_buy_oid'])
            for s in bot.get('pending_sells', []):
                if s.get('oid'): bot_oids.add(s['oid'])

            if bot_oids:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "product_id": pair, "limit": 100
                })
                orphan_ids = []
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') in bot_oids:
                        orphan_count += 1
                        if o.get('order_id'):
                            orphan_ids.append(o['order_id'])

                # Cancel only this bot's orphaned orders (not all pair orders)
                for i in range(0, len(orphan_ids), 10):
                    client.cancel_orders(order_ids=orphan_ids[i:i+10])
        except Exception as e:
            print(f"[DELETE] Orphan check error for {bot_id}: {e}")

        del ACTIVE_BOTS[bot_id]
        save_bots()
        msg = "Bot permanently deleted."
        if orphan_count > 0:
            msg += f" {orphan_count} orphaned order(s) were cleaned up."
        return jsonify(success=True, message=msg)
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


@bot_manager_bp.route('/api/pair_stats', methods=['GET'])
def get_pair_stats():
    """Returns permanent per-strategy-per-pair stats."""
    from bot_utils import load_permanent_stats
    stats = load_permanent_stats()
    return jsonify(stats)

@bot_manager_bp.route('/api/pair_stats/reset/<key>', methods=['POST'])
def reset_pair_stats(key):
    """Reset stats for a specific strategy:pair key."""
    from bot_utils import load_permanent_stats, save_permanent_stats
    stats = load_permanent_stats()
    if key in stats:
        del stats[key]
        save_permanent_stats(stats)
        return jsonify(success=True, message=f"Reset stats for {key}")
    return jsonify(success=False, error=f"No stats found for {key}")


@bot_manager_bp.route('/api/bots/buy_pct/<bot_id>', methods=['POST'])
def set_buy_pct(bot_id):
    """Change the DCA buy percentage on the fly."""
    if bot_id not in ACTIVE_BOTS:
        return jsonify(error="Bot not found")
    bot = ACTIVE_BOTS[bot_id]
    if bot.get('strategy') != 'DCA':
        return jsonify(error="Only DCA bots support buy_pct")
    data = request.get_json()
    pct = float(data.get('buy_pct', 2.0))
    if pct < 0.1 or pct > 50:
        return jsonify(error="buy_pct must be between 0.1 and 50")
    bot['buy_pct'] = round(pct, 1)
    from bot_utils import save_bots
    save_bots()
    return jsonify(success=True, message=f"Buy % set to {bot['buy_pct']}%")
