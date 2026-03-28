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
    "NPR":        "2m",
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
# KELLY CRITERION SIZING
# ==========================================
MIN_KELLY_TRADES = 20  # minimum trades before Kelly overrides default
DEFAULT_BUY_PCT = 5.0  # the default buy_pct — Kelly only applies when bot is at this value

def calculate_kelly_pct(strategy, pair):
    """
    Calculate half-Kelly buy_pct from permanent stats for a strategy:pair.
    Returns None if insufficient data, otherwise a buy_pct value (0.5 to 10.0).
    """
    stats = load_permanent_stats()
    key = f"{strategy}:{pair}"
    s = stats.get(key)
    if not s or s.get('total_trades', 0) < MIN_KELLY_TRADES:
        return None

    wins = s.get('winning_trades', 0)
    losses = s.get('losing_trades', 0)
    total = wins + losses
    if total == 0 or wins == 0:
        return None

    win_rate = wins / total  # W

    # Calculate average win and average loss from trade log in the bot's stats
    # Fall back to permanent stats totals if we can't get per-trade data
    total_pnl = s.get('total_pnl', 0)
    if losses > 0 and wins > 0:
        # Estimate avg win/loss from totals
        # avg_win ≈ total positive pnl / wins, avg_loss ≈ total negative pnl / losses
        # We have total_pnl, largest_win, largest_loss but not sum of wins vs sum of losses
        # Use: avg_win ≈ (total_pnl + |total_loss_est|) / wins
        # Simpler: use largest_win/largest_loss as proxies with dampening
        avg_win = s.get('largest_win', 0) * 0.6   # dampen — largest != average
        avg_loss = abs(s.get('largest_loss', 0)) * 0.6
        if avg_loss == 0:
            avg_loss = avg_win * 0.5  # assume loss is half of win if no losses recorded
    elif losses == 0:
        # All wins — Kelly would say go all-in, which is dangerous
        # Use a conservative estimate: assume eventual losses at 50% of avg win
        avg_win = total_pnl / wins if wins > 0 else 0
        avg_loss = avg_win * 0.5
    else:
        return None

    if avg_loss <= 0 or avg_win <= 0:
        return None

    R = avg_win / avg_loss  # payoff ratio

    # Kelly formula: f = W - (1-W)/R
    kelly_f = win_rate - (1 - win_rate) / R

    if kelly_f <= 0:
        # Negative Kelly means no edge — use minimum sizing
        return 0.5

    # Half-Kelly for safety, clamped to reasonable range
    half_kelly_pct = kelly_f * 50  # scale: kelly_f of 0.04 → 2%, 0.10 → 5%
    half_kelly_pct = max(0.5, min(10.0, half_kelly_pct))

    return round(half_kelly_pct, 1)


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

def record_trade(bot, entry_px, exit_px, size, side, exit_reason, pair, multiplier=1.0, actual_fee=None):
    """
    Records a completed trade into the bot's stats and trade_log.
    If actual_fee is provided (from Coinbase fill data), uses that instead of estimating.
    """
    stats = ensure_stats(bot)
    
    if side == 'LONG':
        raw_pnl = (exit_px - entry_px) * abs(size) * multiplier
    else:
        raw_pnl = (entry_px - exit_px) * abs(size) * multiplier
    
    # Use actual fee from Coinbase if available, otherwise estimate
    if actual_fee is not None:
        fee = actual_fee
    else:
        fee = abs(size) * multiplier * exit_px * 0.0025  # fallback: maker fee estimate
    net_pnl = raw_pnl - fee

    stats['total_trades'] += 1
    stats['total_pnl'] += net_pnl
    stats['total_fees_est'] += fee

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
        'fee_est': round(fee, 4),
        'exit_reason': exit_reason,
        'timestamp': datetime.now(timezone.utc).isoformat()
    })
    strategy = bot.get("strategy", "UNKNOWN")
    try:
        update_permanent_stats(strategy, pair, entry_px, exit_px, size, side, exit_reason, round(net_pnl, 4), actual_fee=fee)
    except Exception as e:
        print(f"[record_trade] update_permanent_stats failed: {e}")
    save_bots()

_stats_lock = threading.Lock()

def load_permanent_stats():
    try:
        with open('stats.json', 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_permanent_stats(stats):
    with _stats_lock:
        with open('stats.json', 'w') as f:
            json.dump(stats, f, indent=2)

def update_permanent_stats(strategy, pair, entry_px, exit_px, size, side, exit_reason, pnl, actual_fee=None):
    key = f"{strategy}:{pair}"
    stats = load_permanent_stats()
    if key not in stats:
        stats[key] = {'strategy': strategy, 'pair': pair, 'total_trades': 0, 'winning_trades': 0,
            'losing_trades': 0, 'stopped_out': 0, 'total_pnl': 0.0, 'largest_win': 0.0,
            'largest_loss': 0.0, 'total_fees_est': 0.0, 'total_volume': 0.0,
            'first_trade': None, 'last_trade': None, 'win_rate': 0.0}
    r = stats[key]
    r['total_trades'] += 1
    r['total_pnl'] = round(r['total_pnl'] + pnl, 6)
    r['total_volume'] = round(r['total_volume'] + abs(size * exit_px), 2)
    fee = actual_fee if actual_fee is not None else abs(size * exit_px * 0.0025)
    r['total_fees_est'] = round(r['total_fees_est'] + fee, 6)
    now = datetime.now(timezone.utc).isoformat()
    if not r['first_trade']: r['first_trade'] = now
    r['last_trade'] = now
    if pnl >= 0:
        r['winning_trades'] += 1
        r['largest_win'] = round(max(r['largest_win'], pnl), 6)
    else:
        r['losing_trades'] += 1
        r['largest_loss'] = round(min(r['largest_loss'], pnl), 6)
    if exit_reason in ('STOP_LOSS', 'EVENT_STOP', 'TRAILING_STOP'):
        r['stopped_out'] += 1
    r['win_rate'] = round((r['winning_trades'] / r['total_trades']) * 100, 1) if r['total_trades'] > 0 else 0
    save_permanent_stats(stats)

# ==========================================
# FEE HELPERS
# ==========================================
def extract_fee(order_obj):
    """Extract total_fees from a Coinbase order dict. Returns float or None."""
    try:
        fee = order_obj.get('total_fees')
        if fee is not None:
            return float(fee)
    except (TypeError, ValueError):
        pass
    return None

def poll_market_fill(client_oid, pair, retries=3, delay=1.0):
    """Poll for a market order fill to get actual fill price and fees.
    Returns (avg_fill_px, filled_size, total_fee) or (None, None, None)."""
    from shared import client as _client
    import time as _time
    for _ in range(retries):
        _time.sleep(delay)
        try:
            order_data = _client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == client_oid:
                    return (
                        float(o.get('average_filled_price', 0)),
                        float(o.get('filled_size', 0)),
                        extract_fee(o)
                    )
        except Exception:
            pass
    return (None, None, None)
