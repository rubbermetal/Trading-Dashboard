"""Input validation for trade and bot endpoints."""
import math
import time
from functools import wraps
from flask import request, jsonify

# Simple token-bucket rate limiter (per-client, per-endpoint)
_rate_buckets = {}
RATE_LIMIT = 10  # max requests per window
RATE_WINDOW = 60  # seconds


def rate_limit(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        key = (request.remote_addr, f.__name__)
        now = time.time()
        bucket = _rate_buckets.setdefault(key, [])
        # Prune old entries
        bucket[:] = [t for t in bucket if now - t < RATE_WINDOW]
        if len(bucket) >= RATE_LIMIT:
            return jsonify(error=f"Rate limit exceeded ({RATE_LIMIT}/{RATE_WINDOW}s)"), 429
        bucket.append(now)
        return f(*args, **kwargs)
    return wrapper


def _to_finite_float(val):
    """float(val) or None when not a finite number."""
    try:
        v = float(val)
    except (ValueError, TypeError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def validate_start_bot(data):
    if not data or not isinstance(data, dict):
        return False, "Request body required"
    if not data.get('pair') or not isinstance(data.get('pair'), str):
        return False, "pair is required (string)"
    if not data.get('strategy') or not isinstance(data.get('strategy'), str):
        return False, "strategy is required (string)"
    try:
        amount = float(data.get('amount', 0))
    except (ValueError, TypeError):
        return False, "amount must be a number"
    if amount <= 0:
        return False, "amount must be positive"
    valid_strategies = ('QUAD', 'QUAD_SUPER', 'ORB', 'GRID', 'TRAP', 'MOMENTUM', 'DCA', 'NPR', 'VWAP_MR', 'SQUEEZE')
    if data['strategy'].upper() not in valid_strategies:
        return False, f"Invalid strategy. Must be one of: {', '.join(valid_strategies)}"
    return True, None


def validate_trade(data):
    """Validates the real /api/trade contract.

    CLOSE:  {action: 'CLOSE', pair, size, side?, type?}
    Other:  {side: BUY|SELL, pair, order_type: MARKET|MAKER_LIMIT|LIMIT,
             amount, limit_price (required for LIMIT)}
    """
    if not data or not isinstance(data, dict):
        return False, "Request body required"
    if not data.get('pair') or not isinstance(data.get('pair'), str):
        return False, "pair is required (string)"

    if data.get('action') == 'CLOSE':
        size = _to_finite_float(data.get('size'))
        if size is None:
            return False, "size must be a number"
        if size <= 0:
            return False, "size must be positive"
        return True, None

    side = str(data.get('side', '')).upper()
    if side not in ('BUY', 'SELL'):
        return False, "side must be BUY or SELL"
    order_type = str(data.get('order_type', '')).upper()
    if order_type not in ('MARKET', 'MAKER_LIMIT', 'LIMIT'):
        return False, "order_type must be MARKET, MAKER_LIMIT, or LIMIT"
    amount = _to_finite_float(data.get('amount'))
    if amount is None:
        return False, "amount must be a number"
    if amount <= 0:
        return False, "amount must be positive"
    if order_type == 'LIMIT':
        limit_price = _to_finite_float(data.get('limit_price'))
        if limit_price is None:
            return False, "limit_price must be a number"
        if limit_price <= 0:
            return False, "limit_price must be positive for limit orders"
    return True, None


def validate_bracket(data):
    """Validates the real /api/bracket contract: pair, size, entry_price, side,
    and at least one of tp_price / sl_price (both positive when present, and
    sane relative to entry for the position side)."""
    if not data or not isinstance(data, dict):
        return False, "Request body required"
    if not data.get('pair') or not isinstance(data.get('pair'), str):
        return False, "pair is required (string)"

    size = _to_finite_float(data.get('size'))
    if size is None:
        return False, "size must be a number"
    if size <= 0:
        return False, "size must be positive"

    entry_price = _to_finite_float(data.get('entry_price'))
    if entry_price is None:
        return False, "entry_price must be a number"
    if entry_price <= 0:
        return False, "entry_price must be positive"

    side = str(data.get('side', 'BUY')).upper()
    if side not in ('BUY', 'SELL'):
        return False, "side must be BUY or SELL"

    tp_price = sl_price = None
    if data.get('tp_price') is not None:
        tp_price = _to_finite_float(data.get('tp_price'))
        if tp_price is None:
            return False, "tp_price must be a number"
        if tp_price <= 0:
            return False, "tp_price must be positive"
    if data.get('sl_price') is not None:
        sl_price = _to_finite_float(data.get('sl_price'))
        if sl_price is None:
            return False, "sl_price must be a number"
        if sl_price <= 0:
            return False, "sl_price must be positive"

    if tp_price is None and sl_price is None:
        return False, "Set at least one of tp_price or sl_price"

    # Side-relative sanity: long = sl < entry < tp, short = tp < entry < sl
    if side == 'BUY':
        if sl_price is not None and sl_price >= entry_price:
            return False, "sl_price must be below entry_price for a long position"
        if tp_price is not None and tp_price <= entry_price:
            return False, "tp_price must be above entry_price for a long position"
    else:
        if sl_price is not None and sl_price <= entry_price:
            return False, "sl_price must be above entry_price for a short position"
        if tp_price is not None and tp_price >= entry_price:
            return False, "tp_price must be below entry_price for a short position"
    return True, None


def validate_trail(data):
    """Validates the real /api/trail contract: pair, pct (0.1-50), size, cur_px, side."""
    if not data or not isinstance(data, dict):
        return False, "Request body required"
    if not data.get('pair') or not isinstance(data.get('pair'), str):
        return False, "pair is required (string)"

    pct = _to_finite_float(data.get('pct'))
    if pct is None:
        return False, "pct must be a number"
    if pct < 0.1 or pct > 50:
        return False, "pct must be between 0.1 and 50"

    cur_px = _to_finite_float(data.get('cur_px'))
    if cur_px is None:
        return False, "cur_px must be a number"
    if cur_px <= 0:
        return False, "cur_px must be positive"

    size = _to_finite_float(data.get('size'))
    if size is None:
        return False, "size must be a number"
    if size <= 0:
        return False, "size must be positive"

    side = str(data.get('side', 'SELL')).upper()
    if side not in ('BUY', 'SELL'):
        return False, "side must be BUY or SELL"
    return True, None
