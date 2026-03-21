# TRAP STRATEGY — FULL SPECIFICATION
# Reference Document for Implementation
# Date: 2026-03-21

## OVERVIEW

TRAP is a consolidation-breakout strategy. It identifies markets where price
has been squeezing — both the SMA(20) and SMA(200) are flat and converged —
then enters aggressively when a high-momentum candle breaks out of that zone
with volume confirmation.

The name reflects the setup: price is "trapped" in a tight range between two
flat moving averages. When it breaks free with a power candle, the strategy
enters in two stages — a smaller initial position on the breakout candle,
then a larger add on the first pullback.

Longs and shorts. Shorts only on derivatives.
Default timeframe: 15 minutes.
Orders are market (taker).

---

## 1. INDICATORS

- **SMA(20):** Short moving average — must be flat
- **SMA(200):** Long moving average — must be flat
- **ATR(14):** Average True Range — used for zone, TP, and SL sizing
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

All four must pass before evaluating the breakout candle:

**1. SMA(20) Flatness**
```
|SMA_20_current - SMA_20_20_bars_ago| / SMA_20_20_bars_ago <= 0.003 (0.3%)
```
The 20-period SMA must not have moved more than 0.3% over the last 20 bars.

**2. SMA(200) Flatness**
```
|SMA_200_current - SMA_200_20_bars_ago| / SMA_200_20_bars_ago <= 0.0015 (0.15%)
```
The 200-period SMA must not have moved more than 0.15% over the last 20 bars.
Stricter threshold because the slow MA should be almost perfectly still.

**3. SMA Convergence**
```
|SMA_20 - SMA_200| / max(SMA_20, SMA_200) <= 0.015 (1.5%)
```
The two SMAs must be within 1.5% of each other. This confirms the compression:
price has been chopping in a tight range and the two MAs have converged.

**4. Power Candle**
```
candle body = |close - open| > ATR(14)
```
The current candle must have a body larger than one ATR. This is the "power"
requirement — a normal candle cannot trigger the strategy.

**5. Volume Confirmation**
```
current volume > Volume_SMA(20) × 1.5
```
Volume must be at least 1.5× the 20-bar average. Conviction behind the move.

### Breakout Direction

**Long Breakout (BREAKOUT_LONG):**
```
candle is bullish (close > open)
AND close > zone_upper + (0.5 × ATR)
```

**Short Breakout (BREAKOUT_SHORT):**
```
candle is bearish (close < open)
AND close < zone_lower - (0.5 × ATR)
```

The 0.5 × ATR buffer beyond the zone edge prevents entries on candles that
barely nick the boundary. The close must be a meaningful distance beyond the
zone, not just a wick.

---

## 4. TWO-STAGE POSITION BUILDING

TRAP builds its position in two stages, not all at once. This improves the
average entry price and reduces the impact of false breakouts.

### Stage 1 — Breakout Entry (25% of capital)

Executed immediately when BREAKOUT_LONG or BREAKOUT_SHORT fires.

- **Size:** `current_usd × 0.25`
- **Order type:** Market buy (long) or market sell (short)
- Records: `entry_stage = 1`, `avg_entry = current_price`, `breakout_data`
  (stores the breakout candle's OHLC and ATR for Stage 2 reference)

### Stage 2 — Pullback Add (remaining ~75% of capital)

Evaluates on every subsequent cycle while `position_side != FLAT` and
`entry_stage == 1`. Fires when the first pullback candle meets all three
conditions:

**1. Opposite Color**
- Long position: current candle is bearish (`close < open`)
- Short position: current candle is bullish (`close > open`)
- A same-color candle is still continuation — not a pullback.

**2. Body Size < 1× ATR**
```
|close - open| < ATR(14)
```
The pullback candle must not be a power candle itself. A large opposite-color
candle suggests reversal, not a healthy pause.

**3. Retrace < 50% of Breakout Candle Body**
```
Long: (breakout_close - current_close) / breakout_body < 0.5
Short: (current_close - breakout_close) / breakout_body < 0.5
```
Price has not retraced more than half of the breakout candle's move.
Deeper retraces suggest the breakout has failed.

- **Size:** `current_usd × 0.99` (remaining capital, approximately 75%)
- **Order type:** Market buy (long) or market sell (short)
- Records: `entry_stage = 2`, updates `avg_entry` (weighted average of both fills)

If Stage 2 never fires (no qualifying pullback candle before exit), the
strategy exits with only the Stage 1 position.

---

## 5. EXIT LOGIC

Exit conditions are evaluated **before** entry/add conditions on every cycle.
Only applies when `position_side != FLAT` and `avg_entry > 0`.

### Take Profit — +2.5% from Avg Entry

```
Long:  (close - avg_entry) / avg_entry >= 0.025
Short: (avg_entry - close) / avg_entry >= 0.025
```
Signal = EXIT_LONG / EXIT_SHORT, reason = "TAKE PROFIT"

### Stop Loss — 1× ATR from Avg Entry

```
Long:  close <= avg_entry - ATR(14)
Short: close >= avg_entry + ATR(14)
```
Signal = EXIT_LONG / EXIT_SHORT, reason = "STOP LOSS"

Note: ATR used for the stop loss is the **current** ATR (recalculated each
cycle), not the ATR at entry. This means the stop distance breathes slightly
with volatility.

---

## 6. ORDER EXECUTION

### Stage 1 Entry
- **Size:** `current_usd × 0.25` (25% of idle capital)
- Spot LONG: `quote_size = alloc × 0.99`
- Derivatives LONG: `base_size = floor(alloc × 0.99 / (price × multiplier))`
- SHORT: derivatives only; skip with return if spot pair

### Stage 2 Add
- **Size:** `current_usd × 0.99` (all remaining idle capital ≈ 75%)
- Same order type routing as Stage 1
- Updates `avg_entry`:
  ```
  new_avg = (old_size × old_avg + new_qty × current_price) / (old_size + new_qty)
  ```

### Exit
- **Order type:** Market sell (long) or market buy (short)
- **Size:** full `abs(asset_held)`
- Records `record_trade()` with exit_reason = "STOP_LOSS" or "SIGNAL" (TP)
- Capital: `current_usd = allocated_usd + (profit × 0.995)`
- Resets: `asset_held = 0`, `position_side = FLAT`, `entry_stage = 0`,
  `avg_entry = 0`, removes `breakout_data`

---

## 7. BOT STATE

### Standard Fields
- `pair`, `strategy` ("TRAP"), `status`
- `allocated_usd`, `current_usd`, `asset_held`
- `position_side` ("FLAT", "LONG", or "SHORT"), `entry_price`
- `timeframe` (default "15m")

### Strategy-Specific Fields
- `entry_stage`: int — 0 (flat), 1 (Stage 1 filled), 2 (Stage 2 filled)
- `avg_entry`: float — weighted average entry price across both stages
- `breakout_data`: dict — snapshot of the breakout candle:
  ```
  {open, close, high, low, atr}
  ```
  Used by Stage 2 to calculate retrace against the breakout body.

### Cleared on Exit
- `entry_stage → 0`, `avg_entry → 0.0`, `breakout_data` removed

---

## 8. EXECUTION ORDER (Every 15-Second Cycle)

1. Fetch candles (15m, 250 bars)
2. Calculate SMA(20), SMA(200), ATR(14), Volume SMA(20)
3. Read `position_side`, `entry_stage`, `avg_entry`, `breakout_data`
4. **If in position (LONG or SHORT):**
   a. Check TAKE PROFIT → exit if triggered
   b. Check STOP LOSS → exit if triggered
   c. If `entry_stage == 1`: check Stage 2 add conditions
5. **If FLAT:**
   a. Check SMA flatness (both)
   b. Check SMA convergence
   c. Check power candle + volume
   d. Check breakout direction
   e. Execute Stage 1 if conditions met
6. Save state on any change

---

## 9. INTEGRATION NOTES

### File: strategies.py
- `calculate_trap(df, pos_side, entry_stage, avg_entry, breakout_data)`
- Returns: `(signal, reason, bo_data)`
  - `signal`: "BREAKOUT_LONG", "BREAKOUT_SHORT", "ADD_LONG", "ADD_SHORT",
    "EXIT_LONG", "EXIT_SHORT", or "HOLD"
  - `bo_data`: breakout candle snapshot on BREAKOUT signals, `{}` otherwise

### File: bot_executors.py
- `execute_trap(bot_id, bot, pair)`
- Passes all state fields into `calculate_trap()`
- Handles weighted avg entry update on Stage 2

### File: bot_manager.py
- `run_bot()`: routes TRAP → `execute_trap(...)`

### Derivative Support
- Short entries (BREAKOUT_SHORT, ADD_SHORT) on `-PERP` and `-CDE` pairs only
- Spot: LONG only — SHORT entry returns immediately if not derivative

---

## 10. EXAMPLE SCENARIO (SOL-USD, 15m)

1. SOL has been ranging for 3 days.
   - SMA_20 slope = 0.1% (< 0.3%) ✓
   - SMA_200 slope = 0.05% (< 0.15%) ✓
   - SMA gap = 0.8% (< 1.5%) ✓
   - ATR = $0.80. Zone: $95.00 – $95.75

2. A 15m candle closes at $96.60:
   - Body = $0.95 > ATR ($0.80) ✓ power candle
   - Volume = 2.1× average ✓ confirmed
   - $96.60 > zone_upper ($95.75) + 0.5×ATR ($0.40) = $96.15 ✓
   - Signal: BREAKOUT_LONG → buy 25% of capital at $96.60
   - entry_stage = 1, avg_entry = $96.60, breakout_data stored

3. Next candle: $96.20 (bearish, body = $0.40 < ATR, retrace = 15% < 50%)
   - ADD_LONG fires → buy remaining 75% at $96.20
   - avg_entry = (0.25 × $96.60 + 0.75 × $96.20) = $96.30
   - entry_stage = 2

4. SOL rallies to $98.75
   - profit = ($98.75 - $96.30) / $96.30 = 2.5% ✓
   - TAKE PROFIT: EXIT_LONG

**Stop loss scenario:**
- After Stage 2, SOL drops to $95.50
- SL = avg_entry − ATR = $96.30 − $0.80 = $95.50 ✓
- STOP LOSS: EXIT_LONG, small loss contained
