# MOMENTUM STRATEGY — FULL SPECIFICATION
# Reference Document for Implementation
# Date: 2026-03-20

## OVERVIEW

Momentum is a trend-pullback reversal strategy. It identifies moments where
a strong trend (ADX ≥ 25) has experienced a meaningful dip (both smoothed
ROCs negative ≤ -0.30) that is losing downward momentum (both ROCs curling
up). Entry uses a maker limit order just below price to capture the spread.
Exit is a three-phase dynamic trailing stop based on ATR, tightening as
profit grows to guarantee at least breakeven once profitable.

Longs only. Works on both spot and derivatives.
Default timeframe: 5 minutes.

---

## 1. SIGNAL GENERATION

### Indicators Required
- ADX(14)
- ROC(5) smoothed with SMA(5)   → "Fast ROC"
- ROC(14) smoothed with SMA(5)  → "Slow ROC"
- SMA(20)
- SMA(200)
- ATR(14) — used for stop sizing, not signal

### Entry Conditions (ALL must be true simultaneously)

1. **Trend Power:**   ADX(14) ≥ 25
2. **Trend Direction:** SMA(20) > SMA(200)
3. **Dip Depth:**     Fast ROC ≤ -0.30  AND  Slow ROC ≤ -0.30
4. **Curl-Up (Fast):**
   - Current Fast ROC > Previous Fast ROC
   - Previous Fast ROC ≤ Fast ROC two bars ago (was flat or falling)
5. **Curl-Up (Slow):**
   - Current Slow ROC > Previous Slow ROC
   - Previous Slow ROC ≤ Slow ROC two bars ago (was flat or falling)

The curl-up requires a two-bar lookback: the bar prior was flat or falling,
and the current bar is rising. This catches the exact inflection candle.
Both ROCs must independently show this inflection — one curling while the
other is still falling is not a valid signal.

### Why These Filters Work Together
- ADX confirms a trend worth trading, but says nothing about direction.
- SMA 20 > 200 filters for uptrend context (avoids buying bounces in
  downtrends where ADX is also high).
- Dual ROC with -0.30 threshold ensures a real dip occurred, not just noise.
- The curl-up is the timing trigger — both momentum timeframes agree the
  selling pressure is exhausting at the same moment.

### Signal Output
- "BUY" — all conditions met, ready for entry
- "HOLD" — conditions not met, with reason string for logging

---

## 2. ORDER ENTRY — MAKER LIMIT WITH FILL MANAGEMENT

### Why Maker Limit (Not Market)
Momentum curl-ups happen at inflection points where price often stalls
before reversing. A maker limit order 1 tick below the current price
captures the entry while avoiding taker fees (0.60% saved on round trip
vs. double market orders at Intro 1 tier).

### Placement
- Fetch current best bid price from order book (or use cur_px - quote_increment)
- Place post_only=True limit buy at that price
- Record the order and timestamp

### Fill Timeout & Re-evaluation
- If order is unfilled after 90 seconds:
  - Re-run signal calculation on fresh candles
  - If signal is still BUY: cancel old order, place new one at updated price
  - If signal is no longer BUY: cancel order, return to scanning
- Max 3 re-placements per signal event (total 4.5 minutes of attempts)
- After 3 failed re-placements: abandon entry, return to scanning
- Log each re-placement for debugging

### Fill Confirmation
- On fill: record entry_price = average_filled_price (not the limit price)
- Calculate ATR at entry for stop calibration
- Transition to stop management (Section 3)

### Why Not Chase
The alternative — re-placing higher each time — becomes a market order with
extra steps and loses the maker fee advantage. If the best setups move too
fast to fill, the strategy is better served waiting for the next signal
than paying 0.60% taker on a chasing entry. The 90-second window with
re-evaluation balances patience with responsiveness.

---

## 3. DYNAMIC TRAILING STOP — THREE PHASES

The stop is software-managed (not a Coinbase order), evaluated every cycle
(15 seconds). Uses ATR(14) at entry time as the base distance unit.
High water mark (HWM) tracks the highest price reached since entry.

### Phase 1: INITIAL (Entry → Breakeven Zone)

**Trigger:** Immediately on fill
**Trail Distance:** 1.5 × ATR below HWM
**Purpose:** Give the reversal room to develop. Momentum curl-ups can
            have a final shakeout before the move.
**Stop Price:** HWM - (1.5 × ATR)

### Phase 2: TIGHTENED (Breakeven Zone → Profitable)

**Trigger:** Unrealized PnL ≥ estimated round-trip fees
  - Fee estimate: entry_price × size × 0.005 (0.25% maker each side)
  - Once (cur_px - entry_price) × size ≥ fee_estimate → Phase 2
**Trail Distance:** 1.0 × ATR below HWM
**Purpose:** Protect against giving back the entire fee buffer. Still
            enough room for normal pullbacks within the trend.
**Stop Price:** max(HWM - (1.0 × ATR), entry_price - (0.5 × ATR))
  - The floor ensures the stop never goes below entry minus a small
    buffer while in this phase.

### Phase 3: LOCKED PROFIT (Above Breakeven)

**Trigger:** Unrealized PnL ≥ 2× estimated round-trip fees
  - This provides a clear buffer above breakeven before locking.
**Trail Distance:** 0.75 × ATR below HWM
**Stop Floor:** entry_price + fee_estimate_per_unit
  - Once in Phase 3, the stop NEVER goes below breakeven + fees.
  - This is the guarantee: worst-case exit is a tiny profit.
**Stop Price:** max(HWM - (0.75 × ATR), entry_price + fee_per_unit)
**Purpose:** Lock in at least breakeven while still letting the trend
            run. The 0.75 ATR trail catches normal retracements without
            being so tight that regular volatility stops you out.

### Phase Transitions
- Phases only move forward (1 → 2 → 3), never backward.
- Phase is determined by current unrealized PnL vs thresholds.
- If price drops to trigger the stop in any phase, market sell immediately.

### Execution — Dual Path (WS Primary, REST Fallback)

Identical to the grid bot's trailing stop architecture. The stop is
evaluated on EVERY WebSocket price tick (millisecond latency) as the
primary path, with a REST fallback every 15 seconds in case WS is down.

**WebSocket Path (primary — bot_ws.py process_price_tick):**
- On every ticker event for the bot's pair:
  1. Update HWM if cur_px > HWM
  2. Determine current phase from PnL thresholds
  3. Calculate stop price for that phase
  4. If cur_px ≤ stop_price: market sell immediately, record trade
- This is the same function that handles grid trailing stops. It will
  be extended to also loop MOMENTUM bots with position_side == 'LONG'.

**REST Path (fallback — execute_momentum cycle):**
- Every 15 seconds, same logic as WS path but using fetched price.
- Catches any stops that WS missed due to connection issues.
- Also handles the entry state machine (signal scanning, fill timeout,
  re-placement) which does NOT need tick-level speed.

**Why This Matters:**
On a 5m timeframe, the bars that trigger entries are inflection candles
where price is volatile and directional. A 15-second poll could let
price gap 0.5-1.0% through the stop before detection. WS ticks ensure
the stop fires within milliseconds of the trigger price being breached.

### ATR Refresh
The ATR used for stop calculation is captured at entry and stored in
bot state. It is NOT recalculated every cycle. This prevents the stop
from widening during high-volatility bars after entry (which is when
you most need the stop to hold firm).

---

## 4. EXIT RECORDING

On any exit (trailing stop hit via WS tick or REST fallback):
- Determine exit_reason based on phase:
  - Phase 1: "STOP_LOSS" (never reached breakeven)
  - Phase 2: "TRAILING_STOP" (was near breakeven)
  - Phase 3: "TRAILING_STOP" (locked profit)
- Call record_trade() with appropriate entry/exit/size/reason
- Reset bot to FLAT, clear all stop state
- Return to signal scanning on next REST cycle
- Both WS and REST paths use identical exit logic to avoid divergence

---

## 5. BOT STATE (Added to bot dict in bots.json)

### Standard Fields (shared with all bots)
- pair, strategy ("MOMENTUM"), status, allocated_usd, current_usd
- asset_held, position_side ("FLAT" or "LONG"), entry_price
- timeframe (default "5m")

### Strategy-Specific Fields (in bot dict, not settings)
- entry_atr: float          — ATR(14) at time of fill, frozen for stop calc
- high_water_mark: float    — highest price since entry
- stop_phase: int           — 1, 2, or 3
- fee_estimate: float       — estimated round-trip fee in USD for this position
- pending_order_oid: str    — client OID of unfilled limit buy (if waiting)
- pending_order_time: float — timestamp of when limit order was placed
- signal_retries: int       — number of re-placements attempted (max 3)

### Cleared on Exit
- entry_atr, high_water_mark, stop_phase, fee_estimate
- pending_order_oid, pending_order_time, signal_retries

---

## 6. EXECUTION ORDER (Every 15-Second REST Cycle)

The REST cycle handles entry logic and acts as a stop fallback.
The WS tick handler (bot_ws.py) is the primary stop evaluation path.

### REST Cycle (execute_momentum, every 15 seconds):

1. Fetch price + candles (5m, 300 bars)
2. If FLAT and no pending order:
   a. Calculate indicators (ADX, both ROCs, SMAs, ATR)
   b. Evaluate signal
   c. If BUY: place maker limit, record OID + timestamp
3. If FLAT with pending order:
   a. Check if order filled (REST lookup)
   b. If filled: transition to LONG, set Phase 1 stop state, save entry_atr
   c. If unfilled and elapsed > 90 seconds:
      - Re-evaluate signal on fresh candles
      - If still BUY and retries < 3: cancel, re-place, increment retries
      - Otherwise: cancel, clear pending state, return to scanning
4. If LONG (REST fallback for stop — WS is primary):
   a. Fetch current price
   b. Update HWM
   c. Determine stop phase from PnL
   d. Calculate stop price
   e. If triggered: market sell, record trade, reset to FLAT

### WS Tick Path (process_price_tick, every tick):

1. For each MOMENTUM bot with position_side == 'LONG':
   a. Update HWM if cur_px > high_water_mark
   b. Compute PnL and determine phase
   c. Calculate stop_price per phase
   d. If cur_px ≤ stop_price: market sell, record trade, reset to FLAT
   e. save_bots() on any state change

---

## 7. INTEGRATION NOTES

### File: strategies.py
- New function: calculate_momentum(df)
- Returns: ("BUY", reason) or ("HOLD", reason)
- Computes all indicators internally, same pattern as other strategies

### File: bot_executors.py
- New function: execute_momentum(bot_id, bot, pair)
- Handles the full state machine: FLAT → PENDING → LONG → EXIT
- Maker limit entry with fill timeout and re-evaluation
- Three-phase trailing stop evaluation

### File: bot_ws.py
- process_price_tick() is the PRIMARY stop evaluation path for MOMENTUM.
  Currently loops GRID bots only. Extend to also check MOMENTUM bots
  where strategy == 'MOMENTUM' and position_side == 'LONG'.
- For each matching bot on every tick:
  1. Update HWM if cur_px > high_water_mark
  2. Compute PnL: (cur_px - entry_price) × asset_held × multiplier
  3. Compute fee_estimate from bot state
  4. Determine phase: PnL < fees → 1, PnL < 2×fees → 2, else → 3
  5. Calculate stop_price per phase formula (Section 3)
  6. If cur_px ≤ stop_price: market sell, record_trade, reset to FLAT
- The _check_new_pairs() function already handles dynamic WS subscription
  for new bot pairs, so MOMENTUM bots get WS coverage automatically.
- process_grid_fill() is GRID-specific. MOMENTUM fill detection uses REST
  polling in execute_momentum (simpler, since there's only ever 1 order).

### File: bot_manager.py
- run_bot() switch: add elif strategy == 'MOMENTUM': execute_momentum(...)
- Frontend deploy: no special settings panel needed (unlike GRID).

### File: index.html
- Add "MOMENTUM" option to strategy dropdown
- Bot card: show stop_phase badge (Phase 1/2/3) and HWM when in position
- Bot chart: draw entry price line + current stop price line

### Derivative Support
- Uses is_derivative() and get_contract_multiplier() from bot_utils
- Sizing: contracts = floor((allocated * 0.99) / (price * multiplier))
- No short support (longs only)

---

## 8. EXAMPLE SCENARIO (ETH-USD, 5m)

1. ADX = 28 (strong trend), SMA 20 = $2050 > SMA 200 = $1950
2. Fast ROC = -0.45 (was -0.52, -0.48 prior two bars → curling up ✓)
3. Slow ROC = -0.38 (was -0.41, -0.39 prior two bars → curling up ✓)
4. Signal: BUY
5. ETH price: $2040. Place limit buy at $2039.99 (post_only)
6. Fills at $2039.99. ATR(14) = $18.50. Fee est = $2039.99 × size × 0.005
7. Phase 1: stop at $2039.99 - (1.5 × $18.50) = $2012.24
8. Price rises to $2058. HWM = $2058. PnL exceeds fees → Phase 2
9. Phase 2: stop at max($2058 - $18.50, $2039.99 - $9.25) = $2039.50
10. Price rises to $2085. PnL exceeds 2× fees → Phase 3
11. Phase 3: stop at max($2085 - $13.88, $2039.99 + fee_per_unit) ≈ $2071.12
12. Price pulls back to $2070 → stop triggered → market sell → profit locked
