"""
Lightweight push notification system using ntfy.sh.
No API keys needed — just subscribe to your topic in the ntfy app.

Usage:
    from notifier import notify
    notify("Bot Entry", "DCA BTC-USD bought 0.001 at $66,500", priority="default")
"""
import os
import json
import threading
import requests
from logger import get_logger

log = get_logger('notifier')

CONFIG_FILE = "notify_config.json"

# Defaults
_config = {
    "enabled": False,
    "provider": "ntfy",         # only ntfy for now
    "ntfy_topic": "",           # e.g. "my-trading-dashboard-abc123"
    "ntfy_server": "https://ntfy.sh",
    # Event toggles
    "on_bot_entry": True,
    "on_bot_exit": True,
    "on_bracket_hit": True,
    "on_sniper_trigger": True,
    "on_twap_complete": True,
    "on_drawdown_warn": True,
    "drawdown_threshold": 15,   # notify when drawdown exceeds this %
}

def _load_config():
    global _config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                saved = json.load(f)
                _config.update(saved)
        except: pass

def _save_config():
    with open(CONFIG_FILE, 'w') as f:
        json.dump(_config, f, indent=2)

_load_config()

def get_config():
    return dict(_config)

def update_config(updates):
    _config.update(updates)
    _save_config()
    return _config

def notify(title, message, priority="default", tags=None):
    """Send a push notification. Non-blocking (runs in thread)."""
    if not _config.get('enabled') or not _config.get('ntfy_topic'):
        return

    def _send():
        try:
            url = f"{_config['ntfy_server']}/{_config['ntfy_topic']}"
            headers = {"Title": title, "Priority": priority}
            if tags:
                headers["Tags"] = ",".join(tags) if isinstance(tags, list) else tags
            requests.post(url, data=message.encode('utf-8'), headers=headers, timeout=5)
        except Exception as e:
            log.error(f"Send failed: {e}")

    threading.Thread(target=_send, daemon=True).start()

def notify_bot_entry(pair, strategy, price, size):
    if _config.get('on_bot_entry'):
        notify(
            f"{strategy} Entry",
            f"{pair} bought {size} @ ${price:,.2f}",
            tags=["chart_with_upwards_trend"]
        )

def notify_bot_exit(pair, strategy, price, pnl, reason):
    if _config.get('on_bot_exit'):
        sign = "+" if pnl >= 0 else ""
        emoji = "money_with_wings" if pnl >= 0 else "chart_with_downwards_trend"
        notify(
            f"{strategy} Exit: {sign}${pnl:.2f}",
            f"{pair} closed @ ${price:,.2f} ({reason})",
            tags=[emoji]
        )

def notify_bracket_hit(pair, hit_type, price, pnl):
    if _config.get('on_bracket_hit'):
        notify(
            f"Bracket {hit_type} Hit",
            f"{pair} @ ${price:,.2f} | PnL ${pnl:+.2f}",
            priority="high" if hit_type == "SL" else "default",
            tags=["rotating_light" if hit_type == "SL" else "dart"]
        )

def notify_sniper(pair, side, price):
    if _config.get('on_sniper_trigger'):
        notify(
            f"Sniper Triggered",
            f"{pair} {side} fired @ ${price:,.2f}",
            tags=["direct_hit"]
        )

def notify_twap_complete(pair, side, total_usd, slices):
    if _config.get('on_twap_complete'):
        notify(
            f"TWAP Complete",
            f"{pair} {side} ${total_usd:.2f} in {slices} slices",
            tags=["white_check_mark"]
        )

def notify_drawdown(pair, strategy, drawdown_pct):
    if _config.get('on_drawdown_warn') and drawdown_pct >= _config.get('drawdown_threshold', 15):
        notify(
            f"Drawdown Alert",
            f"{strategy} {pair} at -{drawdown_pct:.1f}%",
            priority="high",
            tags=["warning"]
        )
