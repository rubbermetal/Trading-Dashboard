import pandas as pd
import pandas_ta as ta

def calculate_quad_rotation(df):
    """
    Version 1: Strict Pullback (2020 Bull Flag)
    Requires Macro/Med strength, Price > 20 & 50 EMA, and a Trigger Stoch dip to 20.
    """
    if len(df) < 200:
        return "HOLD", "Not enough data"

    # Trend Indicators
    df['EMA_20'] = ta.ema(df['close'], length=20)
    df['EMA_50'] = ta.ema(df['close'], length=50)
    df['EMA_200'] = ta.ema(df['close'], length=200)
    
    # Stochastic Indicators
    stoch_macro = ta.stoch(df['high'], df['low'], df['close'], k=60, d=10, smooth_k=10)
    stoch_med = ta.stoch(df['high'], df['low'], df['close'], k=40, d=4, smooth_k=4)
    stoch_fast = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3, smooth_k=3)
    stoch_trig = ta.stoch(df['high'], df['low'], df['close'], k=9, d=3, smooth_k=3)

    try:
        # STRICT REQUIREMENT: Extracting the Smoothed %D line (index 1)
        df['Stoch_Macro_D'] = stoch_macro.iloc[:, 1]
        df['Stoch_Med_D'] = stoch_med.iloc[:, 1]
        df['Stoch_Fast_D'] = stoch_fast.iloc[:, 1]
        df['Stoch_Trig_D'] = stoch_trig.iloc[:, 1]
    except:
        return "HOLD", "Math error"

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    if pd.isna(curr['EMA_50']) or pd.isna(curr['Stoch_Macro_D']):
        return "HOLD", "Warming up indicators"

    # 1. CONTEXTUAL RULES
    trend_bias = (curr['close'] > curr['EMA_20']) and (curr['close'] > curr['EMA_50'])
    macro_strength = curr['Stoch_Macro_D'] > 80
    med_strength = curr['Stoch_Med_D'] > 80 

    # 2. ENTRY TRIGGER
    # Trigger Stoch (9) is oversold
    trig_oversold = curr['Stoch_Trig_D'] <= 20
    # Price is touching or very near the 20 EMA (low dips into it or is within 0.5%)
    ema_touch = curr['low'] <= (curr['EMA_20'] * 1.005)

    if trend_bias and macro_strength and med_strength and trig_oversold and ema_touch:
        return "BUY", "Strict Pullback: Price touched 20 EMA with Stoch 9 oversold."

    # 3. EXIT LOGIC
    # Fast Rotation: 9-period Stoch crosses back above 80
    if prev['Stoch_Trig_D'] < 80 and curr['Stoch_Trig_D'] >= 80:
        return "SELL", "Target Reached: Trigger Stoch (9) hit 80."

    return "HOLD", "Waiting for pullback setup"

def calculate_quad_super(df):
    """
    Version 2: Super Signal (Quad + Divergence)
    Looks for a 4-stoch capitulation flush, followed by a price lower-low and stoch higher-low.
    """
    if len(df) < 200:
        return "HOLD", "Not enough data"

    stoch_macro = ta.stoch(df['high'], df['low'], df['close'], k=60, d=10, smooth_k=10)
    stoch_med = ta.stoch(df['high'], df['low'], df['close'], k=40, d=4, smooth_k=4)
    stoch_fast = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3, smooth_k=3)
    stoch_trig = ta.stoch(df['high'], df['low'], df['close'], k=9, d=3, smooth_k=3)

    try:
        # STRICT REQUIREMENT: Extracting the Smoothed %D line (index 1)
        df['Stoch_Macro_D'] = stoch_macro.iloc[:, 1]
        df['Stoch_Med_D'] = stoch_med.iloc[:, 1]
        df['Stoch_Fast_D'] = stoch_fast.iloc[:, 1]
        df['Stoch_Trig_D'] = stoch_trig.iloc[:, 1]
    except:
        return "HOLD", "Math error"

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    # EXIT LOGIC (Checked first to ensure we lock profits)
    # Primary Target: Fast rotation of Trigger Stoch reaching 80
    if prev['Stoch_Trig_D'] < 80 and curr['Stoch_Trig_D'] >= 80:
        return "SELL", "Super Signal Exit: Trigger Stoch (9) hit 80."

    # MANDATORY DIVERGENCE LOGIC
    # Look back over the last 15 candles to find Stage 1 (The Anchor Flush)
    quad_oversold_idx = None
    for i in range(2, 16):
        row = df.iloc[-i]
        if (row['Stoch_Macro_D'] < 20 and row['Stoch_Med_D'] < 20 and 
            row['Stoch_Fast_D'] < 20 and row['Stoch_Trig_D'] < 20):
            quad_oversold_idx = -i
            break

    if quad_oversold_idx is not None:
        anchor_low = df.iloc[quad_oversold_idx]['low']
        
        # Stage 2: Price makes a Lower Low
        if curr['low'] < anchor_low or curr['close'] < df.iloc[quad_oversold_idx]['close']:
            
            # Stage 3: Stochastics MUST hold above 20 and curl up (Higher Low)
            stoch_curling_up = (curr['Stoch_Trig_D'] > prev['Stoch_Trig_D']) and (curr['Stoch_Fast_D'] > prev['Stoch_Fast_D'])
            stoch_holding = (curr['Stoch_Trig_D'] > 20) and (curr['Stoch_Fast_D'] > 20)
            
            # Entry Trigger: Reversal candle confirms the higher low
            reversal_candle = curr['close'] > curr['open']
            
            if stoch_curling_up and stoch_holding and reversal_candle:
                return "BUY", "SUPER SIGNAL: Divergence confirmed after Quad Flush!"

    return "HOLD", "Waiting for Capitulation Divergence"

def calculate_advanced_grid(df, lower_price, upper_price, grids, current_inventory_pct=0.0):
    """
    Advanced Grid Engine: Fee-aware, Regime-filtered, and Tail-risk protected.
    """
    if len(df) < 50:
        return "HOLD", "Not enough data"

    # Constraint 2: Fee-Adjusted Grid Spacing (Minimum 1.0% width)
    min_step = lower_price * 0.01
    actual_step = (upper_price - lower_price) / grids
    if actual_step < min_step:
        grids = max(1, int((upper_price - lower_price) / min_step))
        actual_step = (upper_price - lower_price) / grids

    # Indicator Calculations (ADX and ATR)
    adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
    if adx_df is not None and not adx_df.empty:
        df['ADX'] = adx_df.iloc[:, 0]
    else:
        df['ADX'] = 0.0

    df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    curr = df.iloc[-1]
    
    if pd.isna(curr['ADX']) or pd.isna(curr['ATR']):
        return "HOLD", "Calculating indicators..."

    # Constraint 4: Volatility-Based Tail Risk (2.0 ATR below lower bound)
    tail_risk_level = lower_price - (2.0 * curr['ATR'])
    if curr['close'] < tail_risk_level:
        return "HOLD", f"EMERGENCY HALT: Price fell > 2.0 ATR below grid (Halt level: {tail_risk_level:.2f})"

    # Constraint 5: Inventory-Aware Recovery (Waiting Mode)
    if current_inventory_pct >= 0.99:
        return "HOLD", "INVENTORY RECOVERY: Deploy Post-Only Maker SELL limits at grid levels."

    # Constraint 3: Market Regime Gating (ADX < 25)
    if curr['ADX'] >= 25:
        return "HOLD", f"DORMANT: Strong trend detected (ADX: {curr['ADX']:.2f}). Pausing to avoid falling knife."

    # Constraint 1: Maker-Only Execution (instructed via reason string)
    return "HOLD", f"GRID ACTIVE: Deploy Post-Only Maker limits. Spacing >= 1.0% ({actual_step:.2f})"

def calculate_orb(df, pos_side="FLAT"):
    """
    Crypto Opening Range Breakout (ORB) - Updated for 60-Minute Window
    """
    if len(df) < 100:  
        return "HOLD", "Not enough data"

    df['EMA_20'] = ta.ema(df['close'], length=20)
    df['Typical_Price'] = (df['high'] + df['low'] + df['close']) / 3
    df['VWAP'] = (df['Typical_Price'] * df['volume']).cumsum() / df['volume'].cumsum()

    df['datetime'] = pd.to_datetime(df['start'], unit='s', utc=True)
    current_date = df['datetime'].iloc[-1].date()
    
    # 60-Minute Range Calculation (00:00 to 01:00 UTC)
    opening_range = df[(df['datetime'].dt.date == current_date) & 
                       (df['datetime'].dt.hour == 0)]
    
    # If it's currently 00:XX, we don't have the full range yet
    if len(opening_range) < 12: 
        return "HOLD", "Defining 60-minute range (00:00 - 01:00 UTC)"

    range_high = opening_range['high'].max()
    range_low = opening_range['low'].min()
    midpoint = (range_high + range_low) / 2
    curr = df.iloc[-1]

    # 1. PRIORITY EXITS
    if pos_side == 'LONG':
        if curr['close'] < midpoint:
            return "EXIT_LONG", "Price fell below 60m ORB Midpoint (Stop Loss)"
        return "HOLD", "Riding Long Position"
        
    elif pos_side == 'SHORT':
        if curr['close'] > midpoint:
            return "EXIT_SHORT", "Price rose above 60m ORB Midpoint (Stop Loss)"
        return "HOLD", "Riding Short Position"

    # 2. EXPIRATION CHECK: Trades taken within 6 hours of the session start
    hours_since_open = (df['datetime'].iloc[-1] - opening_range['datetime'].iloc[0]).total_seconds() / 3600
    if hours_since_open > 6:
        return "HOLD", "60m ORB Setup Expired (Outside window)"

    # 3. ENTRIES: Only evaluated if we are FLAT
    # Must be after 01:00 UTC
    if curr['datetime'].hour < 1:
        return "HOLD", "Waiting for range completion"

    if curr['close'] > range_high and curr['close'] > curr['VWAP'] and curr['close'] > curr['EMA_20']:
        return "LONG", f"Breakout Above 60m High ({range_high}). SL at {midpoint}"

    if curr['close'] < range_low and curr['close'] < curr['VWAP'] and curr['close'] < curr['EMA_20']:
        return "SHORT", f"Breakout Below 60m Low ({range_low}). SL at {midpoint}"

    return "HOLD", f"Ranging inside 60m Box ({range_low} - {range_high})"
