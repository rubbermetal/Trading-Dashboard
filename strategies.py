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

def calculate_trap(df, pos_side="FLAT", entry_stage=0, avg_entry=0.0, breakout_data=None):
    """
    TRAP Strategy: Consolidation Squeeze -> Momentum Breakout
    
    Entry Logic:
    1. SMA 20 and SMA 200 must be flat (low slope)
    2. SMA 20 and SMA 200 must be converged (within 1.5%)
    3. Power candle breaks out of zone with body > 1x ATR
    4. Close beyond zone edge + 0.5x ATR
    5. Volume > 1.5x average confirms conviction
    
    Position Sizing:
    - Stage 1: 25% on breakout candle
    - Stage 2: 75% on first pullback (opposite color, body < 1x ATR, < 50% retrace)
    
    Exit:
    - TP: 2.5% from avg entry
    - SL: 1x ATR from avg entry (direction-aware)
    """
    if len(df) < 210:
        return "HOLD", "Not enough data", {}

    c = df['close']
    o = df['open']
    h = df['high']
    l = df['low']

    # --- Indicators ---
    df['SMA_20'] = ta.sma(c, 20)
    df['SMA_200'] = ta.sma(c, 200)
    df['ATR'] = ta.atr(h, l, c, 14)
    df['VOL_AVG'] = ta.sma(df['volume'], 20)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    if pd.isna(curr['SMA_200']) or pd.isna(curr['ATR']) or curr['ATR'] <= 0:
        return "HOLD", "Warming up indicators", {}

    atr = curr['ATR']
    sma20 = curr['SMA_20']
    sma200 = curr['SMA_200']
    vol_avg = curr['VOL_AVG'] if not pd.isna(curr['VOL_AVG']) else 0

    # --- EXIT LOGIC (checked first) ---
    if pos_side in ('LONG', 'SHORT') and avg_entry > 0:
        if pos_side == 'LONG':
            pnl_pct = (curr['close'] - avg_entry) / avg_entry
            sl_price = avg_entry - atr
            if curr['close'] <= sl_price:
                return "EXIT_LONG", f"STOP LOSS: Price {curr['close']:.2f} hit ATR stop {sl_price:.2f}", {}
            if pnl_pct >= 0.025:
                return "EXIT_LONG", f"TAKE PROFIT: +{pnl_pct*100:.1f}% from entry {avg_entry:.2f}", {}
        elif pos_side == 'SHORT':
            pnl_pct = (avg_entry - curr['close']) / avg_entry
            sl_price = avg_entry + atr
            if curr['close'] >= sl_price:
                return "EXIT_SHORT", f"STOP LOSS: Price {curr['close']:.2f} hit ATR stop {sl_price:.2f}", {}
            if pnl_pct >= 0.025:
                return "EXIT_SHORT", f"TAKE PROFIT: +{pnl_pct*100:.1f}% from entry {avg_entry:.2f}", {}

    # --- STAGE 2: ADD to position on pullback ---
    if pos_side != 'FLAT' and entry_stage == 1 and breakout_data:
        bo_close = breakout_data.get('close', 0)
        bo_open = breakout_data.get('open', 0)
        bo_body = abs(bo_close - bo_open)

        curr_body = abs(curr['close'] - curr['open'])
        is_opposite_color = (pos_side == 'LONG' and curr['close'] < curr['open']) or \
                           (pos_side == 'SHORT' and curr['close'] > curr['open'])

        if is_opposite_color:
            # Check: body < 1x ATR (not a substantial reversal)
            body_ok = curr_body < atr
            # Check: doesn't retrace > 50% of breakout
            if pos_side == 'LONG':
                retrace = (bo_close - curr['close']) / bo_body if bo_body > 0 else 999
            else:
                retrace = (curr['close'] - bo_close) / bo_body if bo_body > 0 else 999
            retrace_ok = retrace < 0.5

            if body_ok and retrace_ok:
                return "ADD_LONG" if pos_side == 'LONG' else "ADD_SHORT", \
                    f"ADD: Pullback candle retrace {retrace*100:.0f}%, body {curr_body:.2f} < ATR {atr:.2f}", {}
            elif not body_ok:
                return "HOLD", f"Pullback too strong (body {curr_body:.2f} >= ATR {atr:.2f}). Skipping add.", {}

        return "HOLD", "Waiting for pullback candle to add position", {}

    # --- STAGE 1: BREAKOUT DETECTION (only when FLAT) ---
    if pos_side != 'FLAT':
        return "HOLD", "In position, monitoring TP/SL", {}

    # 1. SMA FLATNESS: slope of SMA over last 20 bars as percentage
    sma20_start = df['SMA_20'].iloc[-21] if len(df) > 21 else df['SMA_20'].iloc[0]
    sma200_start = df['SMA_200'].iloc[-21] if len(df) > 21 else df['SMA_200'].iloc[0]

    sma20_slope = abs((sma20 - sma20_start) / sma20_start) if sma20_start > 0 else 999
    sma200_slope = abs((sma200 - sma200_start) / sma200_start) if sma200_start > 0 else 999

    if sma20_slope > 0.003:
        return "HOLD", f"SMA20 not flat (slope: {sma20_slope*100:.2f}% > 0.3%)", {}
    if sma200_slope > 0.0015:
        return "HOLD", f"SMA200 not flat (slope: {sma200_slope*100:.2f}% > 0.15%)", {}

    # 2. SMA CONVERGENCE: within 1.5% of each other
    sma_gap = abs(sma20 - sma200) / max(sma20, sma200)
    if sma_gap > 0.015:
        return "HOLD", f"SMAs not converged (gap: {sma_gap*100:.1f}% > 1.5%)", {}

    # 3. DEFINE THE TRAP ZONE
    zone_upper = max(sma20, sma200)
    zone_lower = min(sma20, sma200)
    zone_mid = (zone_upper + zone_lower) / 2

    # 4. POWER CANDLE CHECK
    curr_body = abs(curr['close'] - curr['open'])
    is_bull = curr['close'] > curr['open']
    is_bear = curr['close'] < curr['open']

    body_big = curr_body > atr
    volume_confirmed = curr['volume'] > (vol_avg * 1.5) if vol_avg > 0 else False

    if not body_big:
        return "HOLD", f"No power candle (body {curr_body:.2f} < ATR {atr:.2f})", {}
    if not volume_confirmed:
        return "HOLD", f"Volume weak ({curr['volume']:.0f} < 1.5x avg {vol_avg:.0f})", {}

    # 5. BREAKOUT DIRECTION
    breakout_threshold_long = zone_upper + (0.5 * atr)
    breakout_threshold_short = zone_lower - (0.5 * atr)

    bo_data = {
        'open': float(curr['open']),
        'close': float(curr['close']),
        'high': float(curr['high']),
        'low': float(curr['low']),
        'atr': float(atr)
    }

    if is_bull and curr['close'] > breakout_threshold_long:
        return "BREAKOUT_LONG", \
            f"TRAP BREAKOUT LONG: Close {curr['close']:.2f} > zone {zone_upper:.2f} + 0.5*ATR. Body={curr_body:.2f}, Vol={curr['volume']:.0f}", \
            bo_data

    if is_bear and curr['close'] < breakout_threshold_short:
        return "BREAKOUT_SHORT", \
            f"TRAP BREAKOUT SHORT: Close {curr['close']:.2f} < zone {zone_lower:.2f} - 0.5*ATR. Body={curr_body:.2f}, Vol={curr['volume']:.0f}", \
            bo_data

    return "HOLD", f"Consolidating in zone ({zone_lower:.2f} - {zone_upper:.2f}). Waiting for breakout.", {}

# ==========================================
# MOMENTUM STRATEGY
# ==========================================
def calculate_momentum(df):
    """
    MOMENTUM: Trend-pullback reversal using dual smoothed ROC + ADX power filter.
    
    Entry requires ALL:
    1. ADX(14) >= 25 (strong trend)
    2. SMA(20) > SMA(200) (uptrend context)
    3. ROC(5) smoothed SMA(5) <= -0.30 AND ROC(14) smoothed SMA(5) <= -0.30 (real dip)
    4. Both smoothed ROCs curling up (inflection: was flat/falling, now rising)
    
    Returns: ("BUY", reason) or ("HOLD", reason)
    Also returns current ATR for stop calibration: ("BUY", reason, atr_value)
    """
    if len(df) < 210:
        return "HOLD", "Not enough data", 0.0

    c = df['close']
    h = df['high']
    l = df['low']

    # --- Indicators ---
    sma20 = ta.sma(c, 20)
    sma200 = ta.sma(c, 200)
    atr_series = ta.atr(h, l, c, 14)

    # ADX
    adx_df = ta.adx(h, l, c, 14)
    if adx_df is None or adx_df.empty:
        return "HOLD", "ADX calculation failed", 0.0

    # ROC(5) smoothed with SMA(2) = "Fast ROC"  (spec says SMA(5) but SMA(2) intentional — SMA(5) too smooth)
    roc5_raw = ta.roc(c, 5)
    roc5_smooth = ta.sma(roc5_raw, 2) if roc5_raw is not None else None

    # ROC(14) smoothed with SMA(2) = "Slow ROC"  (spec says SMA(5) but SMA(2) intentional — SMA(5) too smooth)
    roc14_raw = ta.roc(c, 14)
    roc14_smooth = ta.sma(roc14_raw, 2) if roc14_raw is not None else None

    # Validate all indicators are ready
    if sma20 is None or sma200 is None or atr_series is None:
        return "HOLD", "Warming up indicators", 0.0
    if roc5_smooth is None or roc14_smooth is None:
        return "HOLD", "ROC indicators warming up", 0.0

    curr_sma20 = sma20.iloc[-1]
    curr_sma200 = sma200.iloc[-1]
    curr_adx = float(adx_df.iloc[-1, 0])
    curr_atr = float(atr_series.iloc[-1])

    if pd.isna(curr_sma200) or pd.isna(curr_adx) or pd.isna(curr_atr) or curr_atr <= 0:
        return "HOLD", "Warming up indicators", 0.0

    # Current and previous 2 bars for curl-up detection
    fast_cur = roc5_smooth.iloc[-1]
    fast_prev = roc5_smooth.iloc[-2]
    fast_prev2 = roc5_smooth.iloc[-3]

    slow_cur = roc14_smooth.iloc[-1]
    slow_prev = roc14_smooth.iloc[-2]
    slow_prev2 = roc14_smooth.iloc[-3]

    if pd.isna(fast_cur) or pd.isna(fast_prev) or pd.isna(fast_prev2):
        return "HOLD", "ROC smoothing warming up", 0.0
    if pd.isna(slow_cur) or pd.isna(slow_prev) or pd.isna(slow_prev2):
        return "HOLD", "ROC smoothing warming up", 0.0

    # --- CONDITION 1: Trend Power (ADX >= 25) ---
    if curr_adx < 25:
        return "HOLD", f"ADX too low ({curr_adx:.1f} < 25)", curr_atr

    # --- CONDITION 2: Trend Direction (SMA 20 > SMA 200) ---
    if curr_sma20 <= curr_sma200:
        return "HOLD", f"SMA20 ({curr_sma20:.2f}) <= SMA200 ({curr_sma200:.2f}). No uptrend.", curr_atr

    # --- CONDITION 3: Dip Depth (both ROCs <= -0.30) ---
    if fast_cur > -0.30:
        return "HOLD", f"Fast ROC not deep enough ({fast_cur:.2f} > -0.30)", curr_atr
    if slow_cur > -0.30:
        return "HOLD", f"Slow ROC not deep enough ({slow_cur:.2f} > -0.30)", curr_atr

    # --- CONDITION 4: Fast ROC Curl-Up ---
    # Current rising AND previous was flat or falling
    fast_curling = (fast_cur > fast_prev) and (fast_prev <= fast_prev2)
    if not fast_curling:
        if fast_cur <= fast_prev:
            return "HOLD", f"Fast ROC still falling ({fast_prev:.2f} -> {fast_cur:.2f})", curr_atr
        else:
            return "HOLD", f"Fast ROC rising but prev wasn't flat/falling ({fast_prev2:.2f} -> {fast_prev:.2f} -> {fast_cur:.2f})", curr_atr

    # --- CONDITION 5: Slow ROC Curl-Up ---
    slow_curling = (slow_cur > slow_prev) and (slow_prev <= slow_prev2)
    if not slow_curling:
        if slow_cur <= slow_prev:
            return "HOLD", f"Slow ROC still falling ({slow_prev:.2f} -> {slow_cur:.2f})", curr_atr
        else:
            return "HOLD", f"Slow ROC rising but prev wasn't flat/falling ({slow_prev2:.2f} -> {slow_prev:.2f} -> {slow_cur:.2f})", curr_atr

    # --- ALL CONDITIONS MET ---
    reason = (f"MOMENTUM BUY: ADX={curr_adx:.1f}, "
              f"FastROC={fast_cur:.2f} (curl from {fast_prev:.2f}), "
              f"SlowROC={slow_cur:.2f} (curl from {slow_prev:.2f}), "
              f"SMA20={curr_sma20:.2f} > SMA200={curr_sma200:.2f}")
    return "BUY", reason, curr_atr

# ==========================================
# DCA STRATEGY
# ==========================================
def calculate_dca(df, dca_state="SCANNING", last_cross_direction="ABOVE"):
    """
    DCA: Signal-gated accumulation via dual smoothed ROC cross/curl cycle.
    
    State machine signals:
    - "ARM": Both ROCs crossed below zero (dip starting)
    - "DISARM": Both ROCs crossed back above zero before conditions met
    - "BUY": Armed + both ROCs <= -0.50 + either curling up + ADX >= 20
    - "HOLD": No action this cycle
    
    Returns: (signal, reason, extra_data)
    extra_data includes: fast_roc, slow_roc, adx, depth_multiplier
    """
    empty_data = {'fast_roc': 0, 'slow_roc': 0, 'adx': 0, 'depth_multiplier': 1.0}
    
    if len(df) < 210:
        return "HOLD", "Not enough data", empty_data

    c = df['close']
    h = df['high']
    l = df['low']

    # --- Indicators ---
    adx_df = ta.adx(h, l, c, 14)
    if adx_df is None or adx_df.empty:
        return "HOLD", "ADX calculation failed", empty_data

    roc5_raw = ta.roc(c, 5)
    roc5_smooth = ta.sma(roc5_raw, 3) if roc5_raw is not None else None
    roc14_raw = ta.roc(c, 14)
    roc14_smooth = ta.sma(roc14_raw, 3) if roc14_raw is not None else None

    if roc5_smooth is None or roc14_smooth is None:
        return "HOLD", "ROC indicators warming up", empty_data

    curr_adx = float(adx_df.iloc[-1, 0])

    fast_cur = roc5_smooth.iloc[-1]
    fast_prev = roc5_smooth.iloc[-2]
    fast_prev2 = roc5_smooth.iloc[-3]
    slow_cur = roc14_smooth.iloc[-1]
    slow_prev = roc14_smooth.iloc[-2]
    slow_prev2 = roc14_smooth.iloc[-3]

    if any(pd.isna(v) for v in [fast_cur, fast_prev, fast_prev2, slow_cur, slow_prev, slow_prev2, curr_adx]):
        return "HOLD", "Indicators warming up", empty_data

    fast_cur = float(fast_cur)
    fast_prev = float(fast_prev)
    fast_prev2 = float(fast_prev2)
    slow_cur = float(slow_cur)
    slow_prev = float(slow_prev)
    slow_prev2 = float(slow_prev2)

    # Depth for sizing tier — heavier buys at deeper dips (where DCA edge lives)
    depth = min(fast_cur, slow_cur)
    if depth <= -3.0:
        depth_mult = 6.0    # capitulation — max aggression
    elif depth <= -2.0:
        depth_mult = 4.0    # heavy dip
    elif depth <= -1.0:
        depth_mult = 2.5    # solid dip
    elif depth <= -0.50:
        depth_mult = 1.5    # moderate dip
    else:
        depth_mult = 1.0    # scout buy at threshold

    data = {
        'fast_roc': round(fast_cur, 4),
        'slow_roc': round(slow_cur, 4),
        'adx': round(curr_adx, 2),
        'depth_multiplier': depth_mult,
    }

    # --- SCANNING: wait for both ROCs below zero AND at depth threshold ---
    # Zero-cross is just a prerequisite — ARM only when depth is reached
    if dca_state == "SCANNING":
        both_below_now = fast_cur < 0 and slow_cur < 0
        at_depth = fast_cur <= -0.30 and slow_cur <= -0.30
        if both_below_now and at_depth:
            return "ARM", f"Depth threshold reached (Fast={fast_cur:.2f} Slow={slow_cur:.2f}, both <= -0.30)", data
        return "HOLD", f"Scanning. Fast={fast_cur:.2f} Slow={slow_cur:.2f}", data

    # --- ARMED: target acquired, waiting for curl-up confirmation to fire ---
    if dca_state == "ARMED":
        # Disarm: Both ROCs crossed back above zero — dip fizzled without curling
        both_above_now = fast_cur >= 0 and slow_cur >= 0
        if both_above_now:
            return "DISARM", f"Both ROCs back above zero (Fast={fast_cur:.2f} Slow={slow_cur:.2f}). Dip resolved.", data

        # Check buy conditions — depth already confirmed by ARM, now need curl + ADX
        # 1. Either ROC curling up (momentum reversing)
        fast_curling = fast_cur > fast_prev
        slow_curling = slow_cur > slow_prev
        if not fast_curling and not slow_curling:
            return "HOLD", f"Armed. Waiting for curl-up (Fast={fast_prev:.2f}->{fast_cur:.2f}, Slow={slow_prev:.2f}->{slow_cur:.2f})", data

        # 2. ADX filter
        if curr_adx < 10:
            return "HOLD", f"Armed. Curl detected but ADX too low ({curr_adx:.1f} < 10)", data

        # All conditions met — fire
        curl_source = "Fast" if fast_curling else "Slow"
        reason = (f"DCA BUY: {curl_source} ROC curled up. "
                  f"Fast={fast_cur:.2f} Slow={slow_cur:.2f} ADX={curr_adx:.1f} "
                  f"Depth={depth:.2f} Size={depth_mult:.2f}x")
        return "BUY", reason, data

    # Default: if state is ACCUMULATING/BUYING/PAUSED, signal logic handles re-arm
    # Re-arm cycle: both ROCs must cross above zero (reset), then reach depth again
    if last_cross_direction == "BELOW":
        # Waiting for both ROCs to cross back above zero (reset)
        both_above_now = fast_cur >= 0 and slow_cur >= 0
        either_below_prev = fast_prev < 0 or slow_prev < 0
        if both_above_now and either_below_prev:
            return "CROSS_ABOVE", f"Both ROCs above zero (reset). Ready to re-arm on next depth cross.", data
    elif last_cross_direction == "ABOVE":
        # Waiting for depth threshold — not just zero-cross
        both_below_now = fast_cur < 0 and slow_cur < 0
        at_depth = fast_cur <= -0.30 and slow_cur <= -0.30
        if both_below_now and at_depth:
            return "ARM", f"Re-armed: Depth threshold reached (Fast={fast_cur:.2f} Slow={slow_cur:.2f})", data

    return "HOLD", f"Accumulating. Fast={fast_cur:.2f} Slow={slow_cur:.2f} Cross={last_cross_direction}", data


# ==========================================
# NPR STRATEGY
# ==========================================

NPR_CONFIG = {
    "elephant_body_mult": 2.5, "elephant_lookback": 20,
    "tail_ratio_min": 2.0, "tail_body_max_pct": 0.35,
    "one80_min_first_bar_pct": 0.5,
    "ma_separation_max_atr": 3.0, "ma_flatness_slope_max": 0.0015,
    "zone1_atr": 1.5, "zone3_atr": 3.0,
    "sma_proximity_atr": 0.5, "stop_buffer_atr": 0.5,
}

def _detect_events(df, config):
    events = []
    if len(df) < 22: return events
    curr, prev = df.iloc[-1], df.iloc[-2]
    c_o, c_c, c_h, c_l = float(curr['open']), float(curr['close']), float(curr['high']), float(curr['low'])
    p_o, p_c, p_h, p_l = float(prev['open']), float(prev['close']), float(prev['high']), float(prev['low'])
    c_body, c_range, p_body = abs(c_c - c_o), c_h - c_l, abs(p_c - p_o)
    lb = config['elephant_lookback']
    bodies = df['close'].iloc[-(lb+1):-1].values - df['open'].iloc[-(lb+1):-1].values
    avg_body = float(pd.Series(abs(bodies)).mean()) if len(bodies) > 0 else c_body
    atr = float(df['atr'].iloc[-1]) if 'atr' in df.columns and not pd.isna(df['atr'].iloc[-1]) else c_range
    sb = config['stop_buffer_atr'] * atr
    p_bear, p_bull = p_c < p_o, p_c > p_o
    c_bull, c_bear = c_c > c_o, c_c < c_o
    first_ok = p_body >= config['one80_min_first_bar_pct'] * avg_body
    # 180 bars
    if first_ok and p_body > 0 and c_body > 0:
        if p_bear and c_bull and c_h > p_h:
            if min(c_o, c_c) <= min(p_o, p_c):
                events.append({'type':'180','direction':'BULL','stop_price':min(p_l,c_l)-sb,'power':2.0,
                    'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l},'prev':{'o':p_o,'c':p_c,'h':p_h,'l':p_l}}})
        if p_bull and c_bear and c_l < p_l:
            if max(c_o, c_c) >= max(p_o, p_c):
                events.append({'type':'180','direction':'BEAR','stop_price':max(p_h,c_h)+sb,'power':2.0,
                    'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l},'prev':{'o':p_o,'c':p_c,'h':p_h,'l':p_l}}})
    # Tail bars
    if c_range > 0 and c_body > 0 and (c_body / c_range) <= config['tail_body_max_pct']:
        lo_tail = min(c_o, c_c) - c_l
        if c_body > 0 and lo_tail / c_body >= config['tail_ratio_min']:
            events.append({'type':'TAIL','direction':'BULL','stop_price':c_l-sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
        hi_tail = c_h - max(c_o, c_c)
        if c_body > 0 and hi_tail / c_body >= config['tail_ratio_min']:
            events.append({'type':'TAIL','direction':'BEAR','stop_price':c_h+sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
    # Elephant bars
    if c_body >= config['elephant_body_mult'] * avg_body and avg_body > 0:
        if c_bull:
            events.append({'type':'ELEPHANT','direction':'BULL','stop_price':c_l-sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
        elif c_bear:
            events.append({'type':'ELEPHANT','direction':'BEAR','stop_price':c_h+sb,'power':1.0,
                'bar_data':{'curr':{'o':c_o,'c':c_c,'h':c_h,'l':c_l}}})
    events.sort(key=lambda e: {'180':0,'TAIL':1,'ELEPHANT':2}.get(e['type'],9))
    return events

def _compute_zone(price, sma20, sma200, atr, config):
    ma_mid = (sma20 + sma200) / 2.0
    d = (price - ma_mid) / atr if atr > 0 else 0
    z1, z3 = config['zone1_atr'], config['zone3_atr']
    if d >= z3: return 3, d
    elif d >= z1: return 2, d
    elif d >= 0: return 1, d
    elif d >= -z1: return -1, d
    elif d >= -z3: return -2, d
    else: return -3, d

def _zone_allows_direction(zone, direction):
    if direction == 'BULL': return zone not in (-1, 3)
    return zone not in (1, -3)

def _score_trade(event, zone, sma20, sma200, price, atr, config):
    pos_checks = []
    if abs(zone) == 1: pos_checks.append('zone_1')
    elif abs(zone) == 3: pos_checks.append('zone_3')
    if atr > 0 and abs(price - sma20) <= config['sma_proximity_atr'] * atr: pos_checks.append('near_20sma')
    if atr > 0 and abs(price - sma200) <= config['sma_proximity_atr'] * atr: pos_checks.append('near_200sma')
    pos_score = min(len(pos_checks), 1.0) if pos_checks else 0.5
    return 1.0 + pos_score + 1.0, pos_checks

def calculate_npr(df):
    empty = {'signal':'HOLD','event_type':None,'event_direction':None,'event_stop':0,
             'event_power':1.0,'event_bar_data':None,'zone':0,'check_score':0,
             'position_checks':[],'atr':0,'reason':'No signal'}
    if len(df) < 210: empty['reason'] = 'Not enough data'; return empty
    c, h, l = df['close'], df['high'], df['low']
    sma20 = ta.sma(c, 20); sma200 = ta.sma(c, 200); atr_s = ta.atr(h, l, c, 14)
    if sma20 is None or sma200 is None or atr_s is None: empty['reason'] = 'Warming up'; return empty
    s20, s200, atr_v = float(sma20.iloc[-1]), float(sma200.iloc[-1]), float(atr_s.iloc[-1])
    px = float(c.iloc[-1])
    if pd.isna(s200) or pd.isna(atr_v) or atr_v <= 0: empty['reason'] = 'Warming up'; return empty
    empty['atr'] = atr_v; df['atr'] = atr_s; cfg = NPR_CONFIG
    s200_st = float(sma200.iloc[-21]) if len(sma200) > 21 else float(sma200.iloc[0])
    slope = abs((s200 - s200_st) / s200_st) if s200_st > 0 else 999
    if slope > cfg['ma_flatness_slope_max']: empty['reason'] = f"SMA200 not flat ({slope*100:.3f}%)"; return empty
    gap = abs(s20 - s200) / atr_v
    if gap > cfg['ma_separation_max_atr']: empty['reason'] = f"SMAs separated ({gap:.1f} ATR)"; return empty
    zone, dist = _compute_zone(px, s20, s200, atr_v, cfg); empty['zone'] = zone
    events = _detect_events(df, cfg)
    if not events: empty['reason'] = f"No event. Zone={zone:+d}"; return empty
    for ev in events:
        d = ev['direction']
        if not _zone_allows_direction(zone, d): continue
        sc, pc = _score_trade(ev, zone, s20, s200, px, atr_v, cfg)
        if sc < 2.0: continue
        if abs(zone) == 2 and (sc < 3.0 or ev['type'] not in ('180','ELEPHANT')): continue
        sig = 'LONG' if d == 'BULL' else 'SHORT'
        reason = f"NPR {ev['type']} {d} zone {zone:+d} (score={sc:.1f} checks={pc}) Stop=${ev['stop_price']:.2f}"
        return {'signal':sig,'event_type':ev['type'],'event_direction':d,'event_stop':ev['stop_price'],
                'event_power':ev['power'],'event_bar_data':ev['bar_data'],'zone':zone,
                'check_score':sc,'position_checks':pc,'atr':atr_v,'reason':reason}
    empty['reason'] = f"Event {events[0]['type']} {events[0]['direction']} blocked (zone={zone:+d})"; return empty


# ==========================================
# VWAP MEAN REVERSION (VWAP_MR)
# ==========================================
def calculate_vwap_mr(df):
    """
    VWAP Mean Reversion: Buy when price dips below daily VWAP by > 1 std dev
    with RSI < 35. Exit at VWAP touch or upper band.
    Expects 5m candles with 'volume' column.
    """
    if len(df) < 100:
        return "HOLD", "Not enough data", 0.0

    c, h, l, v = df['close'], df['high'], df['low'], df['volume']

    # VWAP: cumulative (typ_price * volume) / cumulative volume
    typical = (h + l + c) / 3
    cum_tv = (typical * v).cumsum()
    cum_v = v.cumsum()
    vwap = cum_tv / cum_v

    # VWAP standard deviation bands
    vwap_sq = ((typical - vwap) ** 2 * v).cumsum() / cum_v
    vwap_std = vwap_sq.apply(lambda x: x ** 0.5 if x > 0 else 0)

    # RSI
    rsi_s = ta.rsi(c, 14)

    # ATR for trailing stop
    atr_s = ta.atr(h, l, c, 14)

    if vwap is None or rsi_s is None or atr_s is None:
        return "HOLD", "Indicators warming up", 0.0

    cur_px = float(c.iloc[-1])
    cur_vwap = float(vwap.iloc[-1])
    cur_std = float(vwap_std.iloc[-1])
    cur_rsi = float(rsi_s.iloc[-1])
    cur_atr = float(atr_s.iloc[-1])

    if pd.isna(cur_vwap) or pd.isna(cur_rsi) or pd.isna(cur_atr) or cur_std <= 0:
        return "HOLD", "Warming up indicators", 0.0

    lower_band = cur_vwap - cur_std
    upper_band = cur_vwap + cur_std
    deviation = (cur_vwap - cur_px) / cur_std if cur_std > 0 else 0

    # ENTRY: price below VWAP - 1 std dev AND RSI < 35
    if cur_px < lower_band and cur_rsi < 35:
        return "BUY", f"VWAP reversion: price ${cur_px:.0f} < lower band ${lower_band:.0f}, RSI {cur_rsi:.0f}", cur_atr

    # EXIT: price touches VWAP or upper band
    if cur_px >= cur_vwap:
        return "SELL", f"VWAP touched: price ${cur_px:.0f} >= VWAP ${cur_vwap:.0f}", cur_atr

    return "HOLD", f"Deviation {deviation:.2f}σ, RSI {cur_rsi:.0f}", cur_atr


# ==========================================
# BOLLINGER SQUEEZE BREAKOUT (SQUEEZE)
# ==========================================
def calculate_squeeze(df):
    """
    Bollinger Squeeze: BB narrows inside Keltner Channels (squeeze).
    Enter on release when momentum turns positive (long) or negative (short).
    Exit on momentum reversal or ATR trailing stop.
    Expects 15m candles.
    """
    if len(df) < 210:
        return "HOLD", "Not enough data", 0.0

    c, h, l = df['close'], df['high'], df['low']

    # Bollinger Bands (20, 2.0)
    bb = ta.bbands(c, length=20, std=2.0)
    # Keltner Channels (20, 1.5)
    kc = ta.kc(h, l, c, length=20, scalar=1.5)
    # Momentum: linear regression value of close over 20 bars (squeeze histogram)
    mom = ta.mom(c, length=12)
    # ATR
    atr_s = ta.atr(h, l, c, 14)

    if bb is None or kc is None or mom is None or atr_s is None:
        return "HOLD", "Indicators warming up", 0.0

    try:
        # BB columns: BBL, BBM, BBU, BBB, BBP
        bb_lower = bb.iloc[:, 0]
        bb_upper = bb.iloc[:, 2]
        # KC columns: KCLe, KCBe, KCUe
        kc_lower = kc.iloc[:, 0]
        kc_upper = kc.iloc[:, 2]
    except (IndexError, KeyError):
        return "HOLD", "Band calculation error", 0.0

    cur_atr = float(atr_s.iloc[-1])
    if pd.isna(cur_atr):
        return "HOLD", "ATR warming up", 0.0

    # Squeeze detection: BB inside KC
    squeeze = []
    for i in range(max(0, len(df) - 5), len(df)):
        try:
            sq = float(bb_lower.iloc[i]) > float(kc_lower.iloc[i]) and float(bb_upper.iloc[i]) < float(kc_upper.iloc[i])
            squeeze.append(sq)
        except (IndexError, ValueError):
            squeeze.append(False)

    cur_squeeze = squeeze[-1] if squeeze else False
    prev_squeeze = squeeze[-2] if len(squeeze) >= 2 else False

    cur_mom = float(mom.iloc[-1])
    prev_mom = float(mom.iloc[-2]) if len(mom) >= 2 else 0

    if pd.isna(cur_mom) or pd.isna(prev_mom):
        return "HOLD", "Momentum warming up", 0.0

    # RELEASE: was in squeeze, now released, momentum has direction
    squeeze_release = prev_squeeze and not cur_squeeze

    if squeeze_release and cur_mom > 0 and cur_mom > prev_mom:
        return "BUY", f"Squeeze LONG release: momentum {cur_mom:.2f} rising", cur_atr

    if squeeze_release and cur_mom < 0 and cur_mom < prev_mom:
        return "SHORT", f"Squeeze SHORT release: momentum {cur_mom:.2f} falling", cur_atr

    # EXIT signals for existing positions
    if cur_mom < 0 and prev_mom > 0:
        return "EXIT_LONG", f"Momentum reversed negative ({cur_mom:.2f})", cur_atr
    if cur_mom > 0 and prev_mom < 0:
        return "EXIT_SHORT", f"Momentum reversed positive ({cur_mom:.2f})", cur_atr

    # Status info
    if cur_squeeze:
        return "HOLD", f"IN SQUEEZE — momentum {cur_mom:.2f}, awaiting release", cur_atr

    return "HOLD", f"No squeeze. Momentum {cur_mom:.2f}", cur_atr
