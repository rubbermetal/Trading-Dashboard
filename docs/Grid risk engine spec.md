# GRID RISK ENGINE  FULL SPECIFICATION
# Reference Document for Implementation
# Date: 2026-03-18

## OVERVIEW
The Grid Risk Engine wraps around the existing grid bot, adding situational
awareness, graduated risk management, and runner capture. The grid still
places and flips orders. The risk engine decides whether those orders should
exist, adjusts trailing stops based on depth and direction, and manages
recovery. Max loss is known before deployment.

---

## 1. MAX LOSS ENVELOPE (Defined at Deployment)

Every grid level gets a trailing stop distance based on its position in the
grid. Distances graduate  wider at top, tighter at bottom.

- Top third of levels:    trail = 3 steps below fill price
- Middle third of levels: trail = 2 steps below fill price
- Bottom third of levels: trail = 1.5 steps below fill price
- Last level (floor):     trail = 1 step below fill price

Trailing stop activates per-fill the moment a buy order fills. High water
mark starts at fill price, ratchets up with price. Stop triggers only if
price drops trail distance from the high water mark.

Max Loss = sum of (trail_distance_in_price  chunk_size_in_asset) for all
levels. This is deterministic from grid geometry alone. Calculated before
any order is placed.

Displayed:
- Preview (before deploy): Max Loss $, risk as % of capital, flips to recover
- Bot card (while running): Current Risk $ (recalculated live as grid moves)

---

## 2. DEPTH SCORE & ESCALATION

Depth = number of buy fills that have NOT had a corresponding sell complete.
Tracked live every cycle. Drives all risk decisions.

### Depth 0-1 (SURFACE)
- Normal grid operations
- No risk engine intervention

### Depth 2-3 (CAUTION)
- Recovery tracking begins (timestamps, velocity)
- No order changes yet
- Bot logs state for decision-making

### Depth 4-5 (ELEVATED)
- Risk engine evaluates open buy orders
- If direction = FALLING: cancel bottom 1-2 open buys to reduce exposure
- If direction = RISING or CHOPPY: leave buys active (potential recovery entries)
- Trailing stops on deepest fills tighten by 25%

### Depth 6+ (CRITICAL)
- Cancel ALL remaining open buy orders regardless of direction
- Focus shifts to managing existing inventory for recovery or controlled exit
- Trailing stops on all fills tighten to minimum distances
- No new buys until depth recovers to 3 or below

---

## 3. DIRECTION CHECK (Price + Fast SMA)

Uses bot's own candle timeframe (e.g. 5m for grid).
Evaluated every cycle (15 seconds).

### Inputs
- SMA 5 current value
- SMA 5 value from 3 candles ago
- Current price

### Slope Calculation
slope = (SMA5_current - SMA5_3_candles_ago) / SMA5_3_candles_ago

### Direction Output
- FALLING: price < SMA5 AND slope < 0
- RISING:  price > SMA5 AND slope > 0
- CHOPPY:  anything else

Used by: depth escalation, halt behavior, recovery logic, buy redeployment.

---

## 4. HALT BEHAVIOR (Replaces Binary Halt)

Triggered when ADX >= 25 (existing trigger preserved).
Instead of one behavior, halt now has three modes based on direction.

### FAVORABLE HALT (direction = RISING, inventory held)
- Do NOT panic sell
- Widen per-fill trailing stops to 1.5x normal distance
- Keep existing sell (flip) orders active
- Cancel open buy orders as precaution
- Let trend carry inventory; trails protect if it reverses

### ADVERSE HALT (direction = FALLING, inventory held)
- Tighten per-fill trailing stops to 0.75x normal distance
- Cancel ALL remaining open buy orders immediately
- Keep existing sell orders active (they may still fill on bounces)
- If no sell orders exist, place exit sells via tightened trails

### NEUTRAL HALT (direction = CHOPPY)
- Keep current trailing stop distances (no adjustment)
- Cancel open buy orders as precaution
- Wait for directional resolution

### All Halt Modes
- Existing sell orders (grid flip orders) remain active
- If sells fill during halt, trade completes normally (counted as win)
- Depth score continues to update
- Direction re-evaluated every cycle
- Halt mode can shift (e.g. adverse -> favorable) as conditions change

---

## 5. RECOVERY LOGIC

### Sell-Driven Recovery
As sell orders fill, depth decreases. Recovery behavior depends on
how much profit each fill represents.

- Fills within 1 step of profit (normal flips):
  Flip normally. Replace sold level with new buy at level - step.
  These are the transaction generators.

- Fills more than 2 steps in profit (deep fills recovering):
  Switch to trailing exit instead of fixed grid sell.
  Don't sell at grid level  let trail manage exit.
  If recovery has momentum, these ride further.
  If stalls, trail catches it at profit > normal flip.

### Recovery Velocity
Track how many depth levels recovered in last N candles.
- Fast recovery (2+ levels in 10 candles): bias toward trailing deep fills
- Slow recovery (grinding back): flip everything normally, take guaranteed profit

### Buy Redeployment After Cancellation
When risk engine cancelled open buys (depth 4+ response):
- Monitor depth as sells fill
- When depth <= 3 AND direction = RISING or CHOPPY:
  Redeploy buy orders below current price
  Uses current price for grid centering, not original grid
  Only deploys buys appropriate to remaining capital
- When depth = 0 (all inventory cleared):
  If halted: wait for ADX < 25, then full fresh grid deploy
  If not halted: immediately redeploy full grid centered on current price
- Buys are NEVER redeployed while direction = FALLING and depth > 3

---

## 6. CIRCUIT BREAKER (Absolute Floor)

Non-negotiable hard stop. Overrides all other logic.

If total unrealized loss across ALL held inventory exceeds X% of
allocated capital (configurable, default ~5-6%), market sell everything.

### Implementation
- Checked every cycle (15 seconds)
- Fires market sell for entire inventory
- Cancels all open orders for the pair
- Sets bot to halted state
- Logs as CIRCUIT_BREAKER exit reason

### Optional Hardware Stop
Single real stop-limit order on Coinbase at the price that would
produce the max loss amount. Insurance against bot/Pi going offline.
Updated whenever grid geometry changes (follow, redeployment).

---

## 7. UI DISPLAY

### Preview (Before Deployment)
- Max Loss: $X.XX (worst-case, all trailing stops trigger at max distance)
- Per-Flip Profit: $X.XX (single successful grid flip)
- Flips to Recover Max Loss: N
- Risk as % of Capital: X.X%

### Bot Card (While Running)
- Current Risk: $X.XX (live recalc with current positions and trail distances)
- Depth: N/total (color coded: green/yellow/orange/red)
- Direction: RISING / FALLING / CHOPPY
- Halt Status: CLEAR or HALTED (favorable/adverse/neutral)
- Recovery: "Recovering 2/5" or "Stable" etc.

---

## 8. PER-FILL TRAILING STOP MECHANICS

### Activation
- Trailing stop created the moment a buy fill is confirmed (WS or REST)
- High water mark initialized to fill price
- Trail distance set based on level position in grid (Section 1)

### Every Cycle (15 seconds)
- Fetch current price
- If current price > high water mark: update high water mark
- If current price < (high water mark - trail distance): TRIGGER
  - Market sell the fill's quantity
  - Record trade with exit reason TRAILING_STOP
  - Remove from inventory tracking
  - Decrease depth score

### Trail Distance Adjustments (Dynamic)
- Halt favorable: multiply trail distance by 1.5 (give room to run)
- Halt adverse: multiply trail distance by 0.75 (protect faster)
- Depth 4-5: multiply deepest fill trails by 0.75
- Depth 6+: multiply all trails by 0.75
- Recovery with momentum: multiply deep fills by 1.25 (let runners run)

Adjustments stack multiplicatively on the base distance from Section 1.

---

## 9. STATE TRACKING (Added to bot settings in bots.json)

New fields in bot.settings:
- depth_score: int (current unfilled buy count)
- direction: "RISING" | "FALLING" | "CHOPPY"
- halt_mode: "FAVORABLE" | "ADVERSE" | "NEUTRAL" | null
- risk_current: float (current max risk in USD)
- risk_max: float (deployment max risk in USD)
- cancelled_buy_levels: list (prices of buys cancelled by risk engine)
- per_fill_trails: dict keyed by grid index {
    fill_price: float,
    high_water_mark: float,
    trail_distance: float,
    trail_multiplier: float,
    quantity: float
  }
- recovery_velocity: float (levels recovered per N candles)
- circuit_breaker_price: float (price that triggers absolute floor)

---

## 10. EXECUTION ORDER (Every 15-Second Cycle)

1. Fetch price and candle data
2. Calculate SMA 5 slope and direction
3. Calculate ADX (existing)
4. Update depth score
5. Check circuit breaker (if triggered, exit everything, skip rest)
6. Check/update per-fill trailing stops (if any trigger, execute sells)
7. Evaluate halt status (ADX check + direction -> halt mode)
8. If halted: adjust trail distances per halt mode, manage buy cancellation
9. If not halted: evaluate depth escalation, cancel/keep buys accordingly
10. Check for filled orders (REST fallback, existing logic)
11. Process flips (existing logic, but deep fills may trail instead of fixed sell)
12. Evaluate buy redeployment if buys were previously cancelled
13. Follow logic if enabled (existing, but risk engine can block if depth > 3)
14. Save state

---

## IMPLEMENTATION NOTES

- All trailing stop logic is bot-managed (software), not Coinbase orders
- Circuit breaker optionally backed by single real Coinbase stop order
- Direction check adds minimal overhead (SMA 5 on existing candle data)
- Depth score is derived from active_grids state (no new API calls)
- Trail state persisted to bots.json for crash recovery
- Follow mode blocked when depth > 3 (don't slide grid deeper into trouble)
- Recovery redeployment uses same place_grid_buy helper (gets cb_oid tracking)
- **Secure Cancellations (Anti-Race Condition):** The engine must NEVER assume an API cancellation request succeeds. When the risk engine dictates a buy order cancellation (e.g., adverse halt, depth >= 4), it must explicitly verify the cancellation via the exchange API (`cancel_order_safe`). If the order cannot be cancelled (because it just filled), it must remain in local state to be processed by the REST fill checker on the next cycle.
