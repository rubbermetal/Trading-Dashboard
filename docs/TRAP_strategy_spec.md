# TRAP STRATEGY — FULL SPECIFICATION
# Oliver Velez Elephant Bar / Narrow-Band Breakout Alignment
# Date: 2026-04-09

## OVERVIEW

TRAP is a consolidation-breakout strategy aligned with Oliver Velez's Elephant
Bar / Narrow-Band methodology. It identifies markets where price has been
squeezing — both the SMA(20) and SMA(200) are flat and converged — then enters
aggressively when an elephant bar breaks out of that zone with volume
confirmation.

The name reflects the setup: price is "trapped" in a tight range between two
flat moving averages. When it breaks free with an elephant bar, the strategy
enters in up to three stages via equal-size pyramiding.

Longs and shorts. Shorts only on derivatives.
Default timeframe: 15 minutes.
Orders are market (taker).

---

## 1. INDICATORS

- **SMA(20):** Short moving average — must be flat
- **SMA(200):** Long moving average — must be flat
- **ATR(14):** Average True Range — used for elephant bar sizing, stops, and R-multiples
- **Volume SMA(20):** 20-bar average volume — used for confirmation threshold

Minimum data required: 210 candles (200 for SMA + buffer).

---

## 2. THE TRAP ZONE

When SMA(20) and SMA(200) are both flat and converged, they form a
compression zone:

```
zone_upper = max(SMA_20, SMA_200)
zone_lower = min(SMA_20, SMA_200)
zone_mid   = (zone_upper + zone_lower) / 2
```

The zone defines where price has been compressed. A breakout is only
valid when price closes convincingly beyond the zone boundary.

---

## 3. SIGNAL GENERATION

### Pre-conditions for Entry (FLAT only)

All must pass before evaluating the breakout candle:

**1. SMA(20) Flatness**
```
|SMA_20_current - SMA_20_20_bars_ago| / SMA_20_20_bars_ago <= 0.003 (0.3%)
```

**2. SMA(200) Flatness**
```
|SMA_200_current - SMA_200_20_bars_ago| / SMA_200_20_bars_ago <= 0.0015 (0.15%)
```

**3. SMA Convergence**
```
|SMA_20 - SMA_200| / max(SMA_20, SMA_200) <= 0.015 (1.5%)
```

**4. SMA Ordering (Velez)**
- Long breakout: SMA(20) must be above SMA(200)
- Short breakout: SMA(20) must be below SMA(200)

**5. Elephant Bar — Body Size (Velez: 1.5x ATR + 70th percentile)**
```
candle body = |close - open| > 1.5 × ATR(14)
AND candle body > 70th percentile of bodies in last 20 bars
```
The current candle must have a body larger than 1.5× ATR AND larger than
70% of recent candle bodies. This is Velez's "elephant bar" definition.

**6. Elephant Bar — Clears Prior Bars (Velez: 87% follow-through)**
```
Bullish: close > high of at least 3 bearish bars in last 10
Bearish: close < low of at least 3 bullish bars in last 10
```
An elephant bar that clears 3+ prior opposite-color bars has an 87%
follow-through rate per Velez's research.

**7. Volume Confirmation**
```
current volume > Volume_SMA(20) × 1.5
```
Volume must be at least 1.5× the 20-bar average.

**8. SMA(20) Post-Breakout Angle (Velez: > 30 degrees)**
```
slope = (SMA_20_current - SMA_20_5_bars_ago) / ATR(14)
angle = |arctan(slope)| in degrees
angle must be > 30 degrees
```
For long: SMA20 must be rising. For short: SMA20 must be falling.
ATR normalization makes the angle scale-independent across assets.

### Breakout Direction

**Long Breakout (BREAKOUT_LONG):**
```
candle is bullish (close > open)
AND close > zone_upper + (0.5 × ATR)
AND SMA(20) > SMA(200)
```

**Short Breakout (BREAKOUT_SHORT):**
```
candle is bearish (close < open)
AND close < zone_lower - (0.5 × ATR)
AND SMA(20) < SMA(200)
```

---

## 4. THREE-STAGE PYRAMIDING (Velez)

TRAP builds its position in up to three equal-size stages. This follows
Velez's pyramiding approach with controlled risk per add.

### Stage 1 — Elephant Bar Entry (10% of capital)

Executed immediately when BREAKOUT_LONG or BREAKOUT_SHORT fires.

- **Size:** `current_usd × 0.10`
- **Order type:** Market buy (long) or market sell (short)
- Records: `entry_stage = 1`, `avg_entry = current_price`, `breakout_data`,
  `tp_stage = 0`

### Stage 2 — First Pullback Add (10% of allocated capital)

Evaluates on every subsequent cycle while `position_side != FLAT` and
`entry_stage == 1`. Fires when pullback candle meets all three conditions:

**1. Opposite Color**
- Long position: current candle is bearish
- Short position: current candle is bullish

**2. Body Size < 1× ATR**
```
|close - open| < ATR(14)
```

**3. Retrace < 50% of Breakout Candle Body**
```
Long: (breakout_close - current_close) / breakout_body < 0.5
Short: (current_close - breakout_close) / breakout_body < 0.5
```

- **Size:** `allocated_usd × 0.10` (capped at available cash)
- Records: `entry_stage = 2`, updates `avg_entry` (weighted average)

### Stage 3 — Second Pullback Add (10% of allocated capital)

Same conditions as Stage 2, evaluated when `entry_stage == 2`.

- **Size:** `allocated_usd × 0.10`
- Records: `entry_stage = 3`, updates `avg_entry`

Maximum committed capital: 30% (3 × 10%).

---

## 5. EXIT LOGIC (Velez R-Multiple System)

Exit conditions are evaluated **before** entry/add conditions on every cycle.
Only applies when `position_side != FLAT` and `avg_entry > 0`.

### Stop Loss — 2× ATR or Elephant Bar Low/High (whichever wider)

```
Long:  sl_price = min(avg_entry - 2×ATR, elephant_bar_low)
Short: sl_price = max(avg_entry + 2×ATR, elephant_bar_high)
```

After T1 is hit (`tp_stage >= 1`), stop moves to breakeven:
```
Long:  sl_price = max(original_sl, avg_entry)
Short: sl_price = min(original_sl, avg_entry)
```

### R-Unit Definition

```
R = avg_entry - sl_price  (for LONG)
R = sl_price - avg_entry  (for SHORT)
```

R is the risk per unit — the distance from entry to stop.

### Target 1 — +2.5R (Partial Exit)

```
Long:  (close - avg_entry) / R >= 2.5
Short: (avg_entry - close) / R >= 2.5
```
Signal = PARTIAL_EXIT_LONG / PARTIAL_EXIT_SHORT
Action: Sell 50% of position, set `tp_stage = 1`, move stop to breakeven.

### Target 2 — +4.0R (Full Exit)

```
Long:  (close - avg_entry) / R >= 4.0
Short: (avg_entry - close) / R >= 4.0
```
Signal = EXIT_LONG / EXIT_SHORT
Action: Sell remaining position, full reset.

### Extended Exit — Price Far from SMA(20) (Velez)

```
Long:  (close - SMA_20) / SMA_20 > 0.05 (5% above)
Short: (SMA_20 - close) / SMA_20 > 0.05 (5% below)
```
Signal = EXIT_LONG / EXIT_SHORT
Action: Full exit when price overextends from the mean.

---

## 6. ORDER EXECUTION

### Stage 1 Entry
- **Size:** `current_usd × 0.10` (10% of idle capital)
- Spot LONG: `quote_size = alloc × 0.99`
- Derivatives LONG: `base_size = floor(alloc × 0.99 / (price × multiplier))`
- SHORT: derivatives only

### Stage 2 & 3 Adds
- **Size:** `allocated_usd × 0.10` (10% of initial capital, capped at available)
- Same order type routing as Stage 1
- Updates `avg_entry` via weighted average

### Partial Exit (T1)
- **Size:** 50% of `abs(asset_held)`
- Records `record_trade()` with exit_reason = "TARGET_1"
- Keeps position open with remaining 50%
- Sets `tp_stage = 1`

### Full Exit (T2, SL, Extended)
- **Size:** full `abs(asset_held)`
- Records `record_trade()` with exit_reason = "STOP_LOSS", "SIGNAL", or "TARGET_2"
- Resets all state

---

## 7. BOT STATE

### Standard Fields
- `pair`, `strategy` ("TRAP"), `status`
- `allocated_usd`, `current_usd`, `asset_held`
- `position_side` ("FLAT", "LONG", or "SHORT"), `entry_price`
- `timeframe` (default "15m")

### Strategy-Specific Fields
- `entry_stage`: int — 0 (flat), 1-3 (stage number)
- `avg_entry`: float — weighted average entry price across all stages
- `tp_stage`: int — 0 (no TP hit), 1 (T1 hit, stop at breakeven)
- `breakout_data`: dict — snapshot of the elephant bar candle:
  ```
  {open, close, high, low, atr}
  ```
  Used for stop loss calculation (elephant bar low/high) and Stage 2/3
  retrace measurement.

### Cleared on Full Exit
- `entry_stage → 0`, `avg_entry → 0.0`, `tp_stage` removed, `breakout_data` removed

---

## 8. EXECUTION ORDER (Every 15-Second Cycle)

1. Fetch candles (15m, 250 bars)
2. Calculate SMA(20), SMA(200), ATR(14), Volume SMA(20)
3. Read `position_side`, `entry_stage`, `avg_entry`, `breakout_data`, `tp_stage`
4. **If in position (LONG or SHORT):**
   a. Calculate stop price (2× ATR or elephant bar low/high; breakeven if T1 hit)
   b. Check STOP LOSS → exit if triggered
   c. Check T2 (4.0R) → full exit if triggered (only when tp_stage >= 1)
   d. Check T1 (2.5R) → partial exit if triggered (only when tp_stage == 0)
   e. Check extended (5% from SMA20) → full exit if triggered
   f. If `entry_stage in (1, 2)`: check pullback add conditions
5. **If FLAT:**
   a. Check SMA flatness (both)
   b. Check SMA convergence
   c. Check SMA ordering
   d. Check elephant bar (1.5× ATR + 70th percentile)
   e. Check clears 3+ prior bars
   f. Check volume
   g. Check SMA20 angle > 30°
   h. Check breakout direction
   i. Execute Stage 1 if conditions met
6. Save state on any change

---

## 9. INTEGRATION NOTES

### File: strategies.py
- `calculate_trap(df, pos_side, entry_stage, avg_entry, breakout_data, tp_stage)`
- Returns: `(signal, reason, bo_data)`
  - `signal`: "BREAKOUT_LONG", "BREAKOUT_SHORT", "ADD_LONG", "ADD_SHORT",
    "EXIT_LONG", "EXIT_SHORT", "PARTIAL_EXIT_LONG", "PARTIAL_EXIT_SHORT",
    or "HOLD"
  - `bo_data`: elephant bar candle snapshot on BREAKOUT signals, `{}` otherwise

### File: bot_executors.py
- `execute_trap(bot_id, bot, pair)`
- Passes all state fields including `tp_stage` into `calculate_trap()`
- Handles weighted avg entry update on adds
- Handles 50% partial exit for T1

### File: bot_manager.py
- `run_bot()`: routes TRAP → `execute_trap(...)`

### Derivative Support
- Short entries on `-PERP` and `-CDE` pairs only
- Spot: LONG only

---

## 10. EXAMPLE SCENARIO (SOL-USD, 15m)

1. SOL has been ranging for 3 days.
   - SMA_20 slope = 0.1% (< 0.3%) ✓
   - SMA_200 slope = 0.05% (< 0.15%) ✓
   - SMA gap = 0.8% (< 1.5%) ✓, SMA_20 > SMA_200 ✓
   - ATR = $0.80. Zone: $95.00 – $95.75

2. A 15m candle closes at $97.00:
   - Body = $1.30 > 1.5× ATR ($1.20) ✓ elephant bar
   - Body > 70th percentile of last 20 bars ✓
   - Clears 4 prior bearish bar highs ✓
   - Volume = 2.1× average ✓
   - SMA20 angle = 42° > 30° ✓
   - $97.00 > zone_upper ($95.75) + 0.5×ATR ($0.40) = $96.15 ✓
   - Signal: BREAKOUT_LONG → buy 10% at $97.00
   - entry_stage = 1, avg_entry = $97.00
   - Elephant bar low = $95.80
   - SL = min($97.00 - 2×$0.80, $95.80) = min($95.40, $95.80) = $95.40
   - R = $97.00 - $95.40 = $1.60

3. Next candle: $96.70 (bearish, body = $0.30 < ATR, retrace = 23% < 50%)
   - ADD_LONG fires → buy 10% at $96.70
   - avg_entry = (0.5 × $97.00 + 0.5 × $96.70) = $96.85
   - entry_stage = 2

4. Another pullback: $96.55 (bearish, body = $0.15, retrace = 35% < 50%)
   - ADD_LONG fires → buy 10% at $96.55
   - avg_entry = weighted average ≈ $96.75
   - entry_stage = 3 (fully loaded)

5. SOL rallies to $100.75:
   - pnl = $100.75 - $96.75 = $4.00
   - R = $96.75 - $95.40 = $1.35 (recalculated with new avg)
   - r_multiple = $4.00 / $1.35 = 2.96 ≥ 2.5 ✓
   - PARTIAL_EXIT_LONG: sell 50%, tp_stage = 1, stop → breakeven ($96.75)

6. SOL continues to $102.15:
   - pnl = $102.15 - $96.75 = $5.40
   - r_multiple = $5.40 / $1.35 = 4.0 ✓
   - EXIT_LONG: sell remaining 50%, full reset

**Stop loss scenario:**
- After Stage 3, SOL drops to $95.40
- SL = $95.40 (original elephant bar based)
- After T1 hit, stop would be at breakeven $96.75 instead
