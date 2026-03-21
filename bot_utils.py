# bot_utils.py
import os
import json
import threading
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
from shared import ACTIVE_BOTS, new_bot_stats

BOTS_FILE = "bots.json"
BOTS_LOCK = threading.Lock()

# ==========================================
# TIMEFRAME CONFIGURATION
# ==========================================
TF_MAP = {
    "1m":  ("ONE_MINUTE",      60),
    "5m":  ("FIVE_MINUTE",     300),
    "15m": ("FIFTEEN_MINUTE",  900),
    "30m": ("THIRTY_MINUTE",   1800),
    "1h":  ("ONE_HOUR",        3600),
    "6h":  ("SIX_HOURS",       21600),
    "1d":  ("ONE_DAY",         86400),
}

STRATEGY_DEFAULT_TF = {
    "QUAD":       "15m",
    "QUAD_SUPER": "15m",
    "ORB":        "5m",
    "GRID":       "1h",
    "TRAP":       "15m",
    "MOMENTUM":   "5m",
    "DCA":        "5m",
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
    defaults = new_bot_stats()
    for k, v in defaults.items():
        if k not in bot['stats']:
            bot['stats'][k] = v
    return bot['stats']

def save_bots():
    with BOTS_LOCK:
        with open(BOTS_FILE, 'w') as f:
            json.dump(ACTIVE_BOTS, f)

def record_trade(bot, entry_px, exit_px, size, side, exit_reason, pair, multiplier=1.0):
    """
    Records a completed trade into the bot's stats and trade_log.
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
