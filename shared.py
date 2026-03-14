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
