import time
import threading
import pandas as pd
import pandas_ta as ta
from flask import Blueprint, jsonify
from shared import client, SCREENER_DATA

screener_bp = Blueprint('screener', __name__)

# The Watchlist: Add or remove any coins you want to track here
WATCHLIST = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "AVAX-USD", "LINK-USD", "ADA-USD", "SHIB-USD"]

def safe_scanner():
    """Background scanner with live-price injection and crash recovery."""
    while True:
        try:
            temp_data = []
            end_ts = int(time.time())
            start_ts = end_ts - (150 * 86400) # Fetch daily candles for the indicators
            
            for pair in WATCHLIST:
                try:
                    # 1. Fetch the absolute LIVE price for the UI
                    p = client.get_product(product_id=pair)
                    live_px = float(p.price)

                    # 2. Fetch the historical candles for the math
                    res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": "ONE_DAY"})
                    candle_data = res.get('candles', [])
                    
                    if len(candle_data) > 15:
                        parsed = [{'start': int(c.get('start', 0)), 'close': float(c.get('close', 0))} for c in candle_data]
                        df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)
                        
                        # Inject the live price into the current daily candle for real-time RSI math
                        df.loc[df.index[-1], 'close'] = live_px
                        
                        rsi_series = ta.rsi(df['close'], length=14)
                        macd_df = ta.macd(df['close'])
                        
                        rsi_val = rsi_series.dropna().iloc[-1] if rsi_series is not None and not rsi_series.dropna().empty else 50
                        
                        # Determine Status
                        rsi_status = "Oversold (Buy)" if rsi_val < 30 else "Overbought (Sell)" if rsi_val > 70 else "Neutral"
                        
                        macd_status = "Neutral"
                        if macd_df is not None and not macd_df.dropna().empty:
                            macd_val = macd_df.iloc[:, 0].dropna().iloc[-1]
                            signal_val = macd_df.iloc[:, 2].dropna().iloc[-1]
                            macd_status = "Bullish" if macd_val > signal_val else "Bearish"

                        temp_data.append({
                            "asset": pair,
                            "price": f"${live_px:,.4f}",
                            "rsi": rsi_status,
                            "macd": macd_status
                        })
                except Exception as e:
                    print(f"[Screener] Skipping {pair} this cycle: {e}")
                
                # Safety pause between individual coin requests
                time.sleep(1)
            
            # Safely update the shared memory only if we successfully got data
            if temp_data:
                SCREENER_DATA.clear()
                SCREENER_DATA.extend(temp_data)
                
        except Exception as e:
            print(f"[Screener] Thread recovered from error: {e}")
            
        # Sleep for 60 seconds before scanning the list again
        time.sleep(60)

# Start the scanner thread the moment the app boots
threading.Thread(target=safe_scanner, daemon=True).start()

@screener_bp.route('/api/screener')
def get_screener():
    return jsonify(SCREENER_DATA)
