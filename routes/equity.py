import sqlite3
import time
import json
import os
import uuid
import threading
from flask import Blueprint, jsonify, request

equity_bp = Blueprint('equity', __name__)

DB_PATH = "equity.db"
LOG_INTERVAL = 900  # 15 minutes

# ==========================================
# DEAD-MAN SWITCH
# ==========================================
_dms_config = {
    "enabled": False,
    "timeout_hours": 48,
    "drawdown_threshold": 35,  # emergency-only — well above normal DCA operating range
    "last_heartbeat": time.time(),
    "triggered": False,
}
_DMS_CONFIG_FILE = "deadman_config.json"

def _load_dms():
    if os.path.exists(_DMS_CONFIG_FILE):
        try:
            with open(_DMS_CONFIG_FILE, 'r') as f:
                _dms_config.update(json.load(f))
        except: pass
    _dms_config['last_heartbeat'] = time.time()  # reset on startup

def _save_dms():
    with open(_DMS_CONFIG_FILE, 'w') as f:
        json.dump(_dms_config, f)

_load_dms()

def _init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equity_log (
            ts INTEGER PRIMARY KEY,
            total_value REAL,
            bot_locked REAL,
            free_usd REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_equity_log (
            ts INTEGER,
            bot_id TEXT,
            strategy TEXT,
            pair TEXT,
            value REAL,
            PRIMARY KEY (ts, bot_id)
        )
    """)
    conn.commit()
    conn.close()

_init_db()

def _log_equity():
    """Background thread: snapshot portfolio value every 15 minutes."""
    while True:
        try:
            from routes.portfolio import fetch_data
            from shared import ACTIVE_BOTS, client

            pos, total, hist, spot_map, total_usd_balance = fetch_data()
            ts = int(time.time())

            # Per-bot values
            bot_locked = 0.0
            bot_rows = []
            for bid, bot in ACTIVE_BOTS.items():
                idle = bot.get('current_usd', 0.0)
                held = bot.get('asset_held', 0.0)
                pair = bot.get('pair', '')
                px = spot_map.get(pair.split('-')[0], {}).get('px', 0)
                if px == 0 and held > 0:
                    try:
                        p = client.get_product(product_id=pair)
                        px = float(p.price)
                    except: pass
                bot_val = idle + held * px
                bot_locked += bot_val
                bot_rows.append((ts, bid, bot.get('strategy', ''), pair, round(bot_val, 2)))

            free_usd = max(0.0, total_usd_balance - sum(b.get('current_usd', 0.0) for b in ACTIVE_BOTS.values()))

            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT OR REPLACE INTO equity_log (ts, total_value, bot_locked, free_usd) VALUES (?, ?, ?, ?)",
                (ts, round(total, 2), round(bot_locked, 2), round(free_usd, 2))
            )
            if bot_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO bot_equity_log (ts, bot_id, strategy, pair, value) VALUES (?, ?, ?, ?, ?)",
                    bot_rows
                )
            conn.commit()
            conn.close()
            # --- Dead-man switch check ---
            if _dms_config.get('enabled') and not _dms_config.get('triggered'):
                timeout_sec = _dms_config.get('timeout_hours', 12) * 3600
                since_heartbeat = time.time() - _dms_config.get('last_heartbeat', time.time())
                if since_heartbeat >= timeout_sec:
                    # Check if any bot is in significant drawdown
                    threshold = _dms_config.get('drawdown_threshold', 15)
                    for bid, bot in ACTIVE_BOTS.items():
                        if bot.get('status') != 'RUNNING': continue
                        avg_entry = bot.get('avg_entry', 0) or bot.get('entry_price', 0)
                        held = bot.get('asset_held', 0)
                        if avg_entry <= 0 or held <= 0: continue
                        pair = bot.get('pair', '')
                        px = spot_map.get(pair.split('-')[0], {}).get('px', 0)
                        if px <= 0: continue
                        drawdown = ((avg_entry - px) / avg_entry) * 100
                        if drawdown >= threshold:
                            # Pause buying only — don't kill the bot or its position
                            try:
                                from bot_utils import save_bots
                                if bot.get('dca_state') != 'PAUSED':
                                    bot['dca_state'] = 'PAUSED'
                                    bot['paused_at'] = time.time()
                                    save_bots()
                                    print(f"[DEAD-MAN] Paused {bid} ({pair}) at -{drawdown:.1f}% drawdown — no heartbeat for {since_heartbeat/3600:.1f}h")
                                    from notifier import notify
                                    notify("DEAD-MAN SWITCH", f"Paused {bot.get('strategy','')} {pair} at -{drawdown:.1f}% drawdown. No heartbeat for {since_heartbeat/3600:.1f}h. Bot still holds position, just stopped new buys.", priority="urgent", tags=["rotating_light"])
                            except Exception as e:
                                print(f"[DEAD-MAN] Error pausing {bid}: {e}")
                    _dms_config['triggered'] = True
                    _save_dms()

        except Exception as e:
            print(f"[EQUITY] Log error: {e}")

        time.sleep(LOG_INTERVAL)

threading.Thread(target=_log_equity, daemon=True).start()

# ==========================================
# API ENDPOINTS
# ==========================================
@equity_bp.route('/api/heartbeat', methods=['POST'])
def heartbeat():
    """Called by the UI periodically to reset the dead-man timer."""
    _dms_config['last_heartbeat'] = time.time()
    _dms_config['triggered'] = False
    _save_dms()
    return jsonify(success=True)

@equity_bp.route('/api/deadman', methods=['GET'])
def get_deadman():
    return jsonify({
        **_dms_config,
        'seconds_since_heartbeat': int(time.time() - _dms_config.get('last_heartbeat', time.time())),
    })

@equity_bp.route('/api/deadman', methods=['POST'])
def set_deadman():
    d = request.json
    _dms_config.update(d)
    _save_dms()
    return jsonify(success=True, config=_dms_config)

@equity_bp.route('/api/equity')
def get_equity():
    """Return equity history. Query params: range=24h|7d|30d|all (default 7d)"""
    range_param = request.args.get('range', '7d')
    now = int(time.time())
    range_map = {
        '24h': now - 86400,
        '7d': now - 604800,
        '30d': now - 2592000,
        '90d': now - 7776000,
        'all': 0
    }
    since = range_map.get(range_param, range_map['7d'])

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        # Portfolio equity curve
        rows = conn.execute(
            "SELECT ts, total_value, bot_locked, free_usd FROM equity_log WHERE ts >= ? ORDER BY ts",
            (since,)
        ).fetchall()
        equity = [dict(r) for r in rows]

        # Per-bot breakdown (latest snapshot only for the waterfall)
        bot_rows = conn.execute(
            "SELECT bot_id, strategy, pair, value FROM bot_equity_log WHERE ts = (SELECT MAX(ts) FROM bot_equity_log)"
        ).fetchall()
        bots = [dict(r) for r in bot_rows]

        # Per-bot time series
        bot_series = {}
        if range_param in ('24h', '7d'):
            series_rows = conn.execute(
                "SELECT ts, bot_id, value FROM bot_equity_log WHERE ts >= ? ORDER BY ts",
                (since,)
            ).fetchall()
            for r in series_rows:
                bid = r['bot_id']
                if bid not in bot_series:
                    bot_series[bid] = []
                bot_series[bid].append({'ts': r['ts'], 'value': r['value']})

        conn.close()

        # Stats
        if equity:
            first_val = equity[0]['total_value']
            last_val = equity[-1]['total_value']
            change = last_val - first_val
            change_pct = (change / first_val * 100) if first_val > 0 else 0
            high = max(e['total_value'] for e in equity)
            low = min(e['total_value'] for e in equity)
        else:
            change = change_pct = high = low = 0

        return jsonify({
            'equity': equity,
            'bots': bots,
            'bot_series': bot_series,
            'stats': {
                'change': round(change, 2),
                'change_pct': round(change_pct, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'points': len(equity)
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)})
