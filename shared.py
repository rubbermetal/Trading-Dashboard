import os
from coinbase.rest import RESTClient
from dotenv import load_dotenv

# Load API Keys
load_dotenv()
client = RESTClient(api_key=os.getenv("COINBASE_API_KEY_NAME"), api_secret=os.getenv("COINBASE_API_PRIVATE_KEY"))

# Shared State
MANUAL_SPOT_ENTRIES = {
    "BTC": 60000.00,  
    "ETH": 3000.00    
}

TRAILING_STOPS = {} 
REBALANCE_TARGETS = {}
SCREENER_DATA = []

# ==========================================
# NEW: BOT LEDGER & VIRTUAL WALLETS
# ==========================================
# Format: 
# "bot_id": {
#    "pair": "BTC-USD", "strategy": "QUAD", "status": "RUNNING",
#    "allocated_usd": 100.0, "current_usd": 100.0, "asset_held": 0.0,
#    "settings": {} 
# }
ACTIVE_BOTS = {}

def new_bot_stats():
    """Returns a fresh stats object for a newly created bot."""
    return {
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "stopped_out": 0,
        "total_pnl": 0.0,
        "largest_win": 0.0,
        "largest_loss": 0.0,
        "total_fees_est": 0.0,
        "deposits": 0.0,       # cumulative added capital
        "withdrawals": 0.0,    # cumulative removed capital
        "trade_log": []        # list of completed trade dicts
    }
