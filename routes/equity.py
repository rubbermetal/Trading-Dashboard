import sqlite3
import time
import threading
from flask import Blueprint, jsonify, request

equity_bp = Blueprint('equity', __name__)

DB_PATH = "equity.db"
LOG_INTERVAL = 900  # 15 minutes

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
        except Exception as e:
            print(f"[EQUITY] Log error: {e}")

        time.sleep(LOG_INTERVAL)

threading.Thread(target=_log_equity, daemon=True).start()

# ==========================================
# API ENDPOINTS
# ==========================================
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
