# ORB STRATEGY — FULL SPECIFICATION
# Reference Document for Implementation
# Date: 2026-03-21

## OVERVIEW

ORB (Opening Range Breakout) is a session-based directional breakout strategy.
It defines the price range established in the first 60 minutes of each UTC day
(00:00–01:00 UTC), then trades breakouts beyond that range when confirmed by
VWAP and EMA alignment.

The setup is only valid for 6 hours after the session open. Trades are managed
with a midpoint stop loss and a trailing stop that activates on sufficient profit.

Longs and shorts. Shorts only available on derivatives (not spot).
Default timeframe: 5 minutes.
Orders are market (taker).

---

## 1. OPENING RANGE DEFINITION

### Range Window
- **Start:** 00:00 UTC (midnight)
- **End:** 01:00 UTC
- **Duration:** 60 minutes
- **Minimum bars required:** 12 candles on the bot's timeframe (e.g. 12 × 5m = 60m)

If fewer than 12 candles exist within the 00:xx UTC hour for the current date,
the range is not yet defined. Return HOLD and wait.

### Range Values
```
range_high = max(high) across all candles in the 00:xx hour
range_low  = min(low) across all candles in the 00:xx hour
midpoint   = (range_high + range_low) / 2
```

The midpoint serves as the stop loss reference for all positions.

---

## 2. INDICATORS

- **EMA(20):** Trend direction filter at entry
- **VWAP (cumulative):** Rolling volume-weighted average price, calculated
  across the entire day's candles as a cumulative series:
  `VWAP = cumsum(typical_price × volume) / cumsum(volume)`
  where `typical_price = (high + low + close) / 3`

---

## 3. SIGNAL GENERATION

### Entry Window
- Entries are only evaluated **after** 01:00 UTC (range must be complete)
- Entries expire **6 hours after the session open** (after 06:00 UTC)
- If `hours_since_open > 6`: return HOLD, setup expired

### Long Entry — ALL must be true
1. `close > range_high` — price has broken above the range
2. `close > VWAP` — price is above the session's volume-weighted average
3. `close > EMA(20)` — price is above the 20-period EMA

Signal = **"LONG"**

### Short Entry — ALL must be true
1. `close < range_low` — price has broken below the range
2. `close < VWAP` — price is below the session VWAP
3. `close < EMA(20)` — price is below the 20-period EMA

Signal = **"SHORT"**
*Note: Short entries result in no action on spot pairs — derivatives only.*

### Why Three Filters
- Breaking the range alone is not enough; false breakouts are common at the
  open. VWAP ensures the majority of the day's volume is on the correct side
  of the trade. EMA(20) provides a secondary momentum confirmation.

### Signal Output
- **"LONG"** — long breakout confirmed
- **"SHORT"** — short breakout confirmed
- **"EXIT_LONG"** — midpoint stop triggered while long
- **"EXIT_SHORT"** — midpoint stop triggered while short
- **"HOLD"** — none of the above

---

## 4. EXIT LOGIC

### Primary Exit — Midpoint Stop Loss

Evaluated every cycle when in position. Checked **before** entry evaluation.

- **Long:** `close < midpoint` → EXIT_LONG ("Price fell below 60m ORB Midpoint")
- **Short:** `close > midpoint` → EXIT_SHORT ("Price rose above 60m ORB Midpoint")

The midpoint is a natural mean-reversion level. If price re-enters the range,
the breakout has failed and the position is closed immediately.

### Secondary Exit — Trailing Stop

Software-managed, activated once profit threshold is reached.

**Activation threshold:** +3.0% unrealized profit from entry price
- Once activated, a high-water mark (LONG) or low-water mark (SHORT) is
  initialized at the current price.

**Trail distance:** 1.5% from the water mark

**Trigger:**
- Long: `current_price <= high_water_mark × (1 - 0.015)`
- Short: `current_price >= low_water_mark × (1 + 0.015)`

The trailing stop overrides the midpoint exit once active. If the trailing
stop triggers, exit with reason "TRAILING_STOP".

**Water mark ratchet:**
- LONG: HWM updates whenever `current_price > high_water_mark`
- SHORT: LWM updates whenever `current_price < low_water_mark`

---

## 5. ORDER EXECUTION

### Long Entry
- **Order type:** Market buy (taker)
- **Size (spot):** `quote_size = current_usd × 0.99`
- **Size (derivatives):** `base_size = floor(current_usd × 0.99 / (price × multiplier))`
- **Minimum capital:** > $5.00 idle USD
- Records: `asset_held`, `current_usd = 0`, `entry_price`, `position_side = LONG`

### Short Entry (derivatives only)
- **Order type:** Market sell (taker)
- **Size:** `base_size = floor(current_usd × 0.99 / (price × multiplier))`
- Records: `asset_held = -qty`, `current_usd = 0`, `entry_price`, `position_side = SHORT`
- Spot pairs: log warning, skip execution (cannot short spot)

### Long Exit
- **Order type:** Market sell (taker)
- **Size:** full `abs(asset_held)`
- Records `record_trade()` with exit_reason = "TRAILING_STOP" or "STOP_LOSS"
- Capital: `current_usd = allocated_usd + (profit × 0.995)`

### Short Exit
- **Order type:** Market buy (taker)
- **Size:** full `abs(asset_held)`
- Records `record_trade()` with appropriate exit_reason
- Capital: `current_usd = allocated_usd + (profit × 0.995)`

---

## 6. BOT STATE

### Standard Fields
- `pair`, `strategy` ("ORB"), `status`
- `allocated_usd`, `current_usd`, `asset_held`
- `position_side` ("FLAT", "LONG", or "SHORT"), `entry_price`
- `timeframe` (default "5m")

### Strategy-Specific Fields
- `trail_active`: bool — True once the 3% threshold has been crossed
- `high_water_mark`: float — highest price since trail activation (LONG)
- `low_water_mark`: float — lowest price since trail activation (SHORT)

### Cleared on Exit
- `trail_active`, `high_water_mark` (or `low_water_mark`)

---

## 7. EXECUTION ORDER (Every 15-Second Cycle)

1. Fetch candles (5m, 288 bars — covers ~24 hours)
2. Calculate EMA(20) and VWAP
3. Build opening range for current UTC date (00:xx candles)
4. If range not ready (< 12 candles): return HOLD
5. Read `position_side`
6. If LONG or SHORT:
   a. Check trailing stop (if active)
   b. Check midpoint stop
   c. Return (no entry evaluation while in position)
7. If FLAT:
   a. Check expiry (> 6 hours since open): return HOLD
   b. Check time (before 01:00 UTC): return HOLD
   c. Evaluate LONG / SHORT entry conditions
8. If signal matches position_side context: execute order
9. Save state on any change

---

## 8. INTEGRATION NOTES

### File: strategies.py
- `calculate_orb(df, pos_side)` — returns `(signal, reason)`
- Signal includes both entry signals and exit signals
- Trailing stop logic is handled in the executor, not the strategy

### File: bot_executors.py
- `execute_orb(bot_id, bot, pair)`
- Handles trailing stop activation and HWM/LWM ratcheting before calling strategy
- Injects trailing stop EXIT signals into signal variable before execution block

### File: bot_manager.py
- `run_bot()`: routes ORB → `execute_orb(...)`

### Derivative Support
- Short entries supported on `-PERP` and `-CDE` pairs only
- Spot pairs: LONG only (SHORT entry is skipped with a warning log)

---

## 9. EXAMPLE SCENARIO (ETH-USD, 5m)

1. 00:00–01:00 UTC: ETH trades between $2,000 (low) and $2,050 (high)
   - range_high = 2050, range_low = 2000, midpoint = 2025
2. 01:15 UTC: ETH pushes to $2,065
   - close (2065) > range_high (2050) ✓
   - close (2065) > VWAP (2040) ✓
   - close (2065) > EMA_20 (2035) ✓
   - Signal: LONG → market buy
3. ETH rises to $2,100 (+2.4%). Trail not yet active.
4. ETH reaches $2,120 (+3.4%). Trail activates. HWM = $2,120.
5. ETH pulls back to $2,088 ($2,120 × 0.985 = $2,088.20)
   - Trailing stop triggered: EXIT_LONG
   - Profit captured from $2,065 to $2,088 ≈ +1.1%

**Failed breakout scenario:**
1. ETH breaks above range_high but fades back below midpoint ($2,025)
2. `close < midpoint` → EXIT_LONG (midpoint stop)
3. Small loss, contained by the range midpoint
