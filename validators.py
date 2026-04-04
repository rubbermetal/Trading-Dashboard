"""Input validation for trade and bot endpoints."""
import time
from functools import wraps
from flask import request, jsonify

# Simple token-bucket rate limiter (per-endpoint)
_rate_buckets = {}
RATE_LIMIT = 10  # max requests per window
RATE_WINDOW = 60  # seconds


def rate_limit(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        key = f.__name__
        now = time.time()
        bucket = _rate_buckets.setdefault(key, [])
        # Prune old entries
        bucket[:] = [t for t in bucket if now - t < RATE_WINDOW]
        if len(bucket) >= RATE_LIMIT:
            return jsonify(error=f"Rate limit exceeded ({RATE_LIMIT}/{RATE_WINDOW}s)"), 429
        bucket.append(now)
        return f(*args, **kwargs)
    return wrapper


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
    if not data or not isinstance(data, dict):
        return False, "Request body required"
    if not data.get('pair') or not isinstance(data.get('pair'), str):
        return False, "pair is required"
    action = data.get('action', '').upper()
    if action not in ('BUY', 'SELL'):
        return False, "action must be BUY or SELL"
    order_type = data.get('order_type', '').upper()
    if order_type not in ('MARKET', 'LIMIT', 'MAKER'):
        return False, "order_type must be MARKET, LIMIT, or MAKER"
    if order_type in ('LIMIT', 'MAKER'):
        try:
            price = float(data.get('price', 0))
            if price <= 0:
                return False, "price must be positive for limit/maker orders"
        except (ValueError, TypeError):
            return False, "price must be a number"
    try:
        amount = float(data.get('amount', 0))
        if amount <= 0:
            return False, "amount must be positive"
    except (ValueError, TypeError):
        return False, "amount must be a number"
    return True, None


def validate_bracket(data):
    if not data or not isinstance(data, dict):
        return False, "Request body required"
    for field in ('pair', 'size', 'entry_price', 'tp_price', 'sl_price'):
        if field not in data:
            return False, f"Missing required field: {field}"
    for num_field in ('size', 'entry_price', 'tp_price', 'sl_price'):
        try:
            v = float(data[num_field])
            if v <= 0:
                return False, f"{num_field} must be positive"
        except (ValueError, TypeError):
            return False, f"{num_field} must be a number"
    return True, None


def validate_trail(data):
    if not data or not isinstance(data, dict):
        return False, "Request body required"
    if not data.get('pair'):
        return False, "pair is required"
    for num_field in ('size', 'entry_price', 'trail_pct'):
        if num_field in data:
            try:
                v = float(data[num_field])
                if v <= 0:
                    return False, f"{num_field} must be positive"
            except (ValueError, TypeError):
                return False, f"{num_field} must be a number"
    return True, None
