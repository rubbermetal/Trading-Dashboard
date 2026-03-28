# DCA STRATEGY — FULL SPECIFICATION
# Reference Document for Implementation
# Date: 2026-03-22 (updated — see change log at bottom)

## OVERVIEW

DCA is a signal-gated accumulation strategy. It builds a position through
repeated small maker buys triggered by confirmed momentum dips, then scales
out profits in tiers as price recovers. Unlike single-entry strategies,
DCA expects multiple buys over time and manages the full position lifecycle
as a portfolio — tracking weighted average entry, tiered profit targets,
and a drawdown pause mechanism.

Longs only. Works on both spot and derivatives.
Default timeframe: 5 minutes.
All orders are maker (post_only=True). Never market orders.

---

## 1. SIGNAL GENERATION — ARM / FIRE CYCLE

### Indicators Required
- ROC(5) smoothed with SMA(5)   → "Fast ROC"
- ROC(14) smoothed with SMA(5)  → "Slow ROC"
- ADX(14)
- ATR(14) — used for limit price offset

### State Machine

```
SCANNING → ARMED → BUYING → ACCUMULATING ──→ (re-arm) → ARMED
                                  │
                                  ├──→ TAKING_PROFIT (limit sells at tiers)
                                  │
                                  └──→ PAUSED (drawdown > 15%)
```

### State: SCANNING
Waiting for both ROCs to reach the depth threshold (not just zero-cross).
- Every cycle: check if Fast ROC <= -0.30 AND Slow ROC <= -0.30
- Zero-cross alone is not sufficient — the dip must reach meaningful depth
- On depth threshold reached → transition to ARMED
- If already holding a position, profit tiers are still evaluated every cycle

### State: ARMED
Dip target acquired. Waiting for momentum curl-up + ADX confirmation.
- **Curl-Up:** EITHER Fast ROC OR Slow ROC is rising (current > previous)
- **Trend Power:** ADX(14) ≥ 10
- When both conditions met → transition to BUYING
- If both ROCs cross back ABOVE zero → disarm, return to SCANNING

### State: BUYING
Placing a maker limit buy order.
- Place post_only limit buy at (current_price - 1 tick) — same as MOMENTUM
- Fill timeout: 90 seconds
- If filled → update position (weighted avg entry, total held), transition
  to ACCUMULATING
- If unfilled after 90s: cancel, re-check signal
  - If signal still valid: re-place (max 3 retries)
  - If signal dead: transition to ACCUMULATING (if holding) or SCANNING (if not)

### State: ACCUMULATING
Has a position. Normal operating state.
- Every cycle: evaluate profit tiers (Section 3)
- Every cycle: check for new ARMED signal (re-arm check)
  - Both ROCs must first cross ABOVE zero (reset), THEN reach depth threshold again
  - This prevents repeated buys on the same dip
  - On fresh depth threshold reached (both <= -0.30) → transition to ARMED (can buy again)
- Manages outstanding limit sell orders from profit-taking

### State: TAKING_PROFIT
Placing maker limit sell orders at tier prices. Sub-state of ACCUMULATING.
- When a tier threshold is crossed, place a limit sell at the tier price
- If sell fills → update position size, check next tier
- Outstanding sells that haven't filled are tracked and cancelled if price
  drops back below the tier (price moved away)

### State: PAUSED
Catastrophic drawdown circuit breaker.
- Triggered when: drawdown >= 25% from avg entry
- While paused: NO new buys, but existing sells remain active
- Resume when: drawdown recovers to < 22% (3% hysteresis)
- All other logic (profit tiers, sell management) continues normally

### Graduated Drawdown Response (before PAUSE)
Instead of a hard stop, buying continues at reduced size through moderate drawdowns:
- 0% to -10%: normal buy size (1.0x)
- -10% to -20%: half-size buys (0.5x) — still accumulating, reducing exposure rate
- -20% to -25%: quarter-size buys (0.25x) — minimal, still pulling avg entry down
- Beyond -25%: PAUSED — full stop, something is fundamentally broken

---

## 2. BUY SIZING

### Base Size
Fetch `base_min_size` from Coinbase `get_product()` for the pair.
This is the minimum tradeable quantity (e.g., 0.00001 BTC, 0.001 ETH).
Convert to USD: `min_buy_usd = base_min_size × current_price`

### Sizing Tiers (based on deeper of the two ROCs at time of buy)

Take the more negative ROC value: `depth = min(fast_roc, slow_roc)`

| Depth Range        | Depth Multiplier  | Rationale                           |
|--------------------|-------------------|-------------------------------------|
| -0.30 to -0.50     | 1.0x              | Scout buy at threshold              |
| -0.50 to -1.0      | 1.5x              | Moderate dip, lean in               |
| -1.0 to -2.0       | 2.5x              | Solid dip, meaningful position      |
| -2.0 to -3.0       | 4.0x              | Heavy dip, aggressive accumulation  |
| Below -3.0         | 6.0x              | Capitulation, max aggression        |

Buy USD = `current_usd × (buy_pct/100) × depth_mult × drawdown_mult`

### Drawdown Multiplier (graduated risk reduction)

| Drawdown from Avg Entry | drawdown_mult | Rationale                          |
|--------------------------|---------------|------------------------------------|
| 0% to -10%               | 1.0           | Normal operation                   |
| -10% to -20%             | 0.5           | Half-size buys, still accumulating |
| -20% to -25%             | 0.25          | Quarter-size, minimal exposure     |
| Beyond -25%              | 0 (PAUSED)    | Catastrophic, full stop            |

Resume from PAUSED when drawdown recovers to -22% (3% hysteresis).

### Capital Guard
- If bot's idle USD < min_buy_usd: skip buy, log "insufficient capital"
- Buy size is always capped at available idle USD × 0.99 (leave buffer)

---

## 3. PROFIT-TAKING — TIERED SCALE-OUT

### Profit Measurement
Profit percentage is measured from the **weighted average entry price**
across ALL accumulated buys:

```
avg_entry = total_cost / total_quantity
profit_pct = (current_price - avg_entry) / avg_entry × 100
```

### Tier Schedule (percentage of REMAINING position)

| Profit %  | Sell % of Remaining | Cumulative Sold (from original) |
|-----------|--------------------|---------------------------------|
| 3.0%      | 20%                | 20.0%                           |
| 5.0%      | 25%                | 40.0%                           |
| 7.5%      | 30%                | 58.0%                           |
| 10.0%     | 35%                | 72.7%                           |
| 15.0%     | 50%                | 86.4%                           |
| 20.0%+    | 75%                | 96.6%                           |

The remaining ~3.4% rides as a "moonbag" until the next accumulation
cycle absorbs it into the new avg entry.

Rationale: First tier at 3% ensures net profit of ~2.2% after maker
fees on both sides (~0.4% × 2). Wider spacing gives trades room to
run and captures larger moves instead of scalping thin margins.

### Sell Execution
- All sells are maker limit orders (post_only=True)
- Limit price = tier price (avg_entry × (1 + tier_pct/100))
- When price crosses a tier threshold:
  1. Calculate sell quantity = remaining_position × tier_percentage
  2. Snap to base_increment
  3. Place limit sell at tier price
  4. Track the order (tier_level, oid, target_price)
- If price drops back below tier before fill: cancel the sell, re-place
  when tier is crossed again on next upswing
- When a sell fills:
  1. Update position: asset_held -= filled_size
  2. Update current_usd: += filled_value
  3. Record trade via record_trade()
  4. Advance tier pointer (never re-sell at same tier for same cycle)
  5. Recalculate remaining position for next tier

### Tier Reset
When the position is fully sold (or nearly — moonbag < min_size):
- Clear avg_entry, total_cost, tier state
- Transition back to SCANNING
- Any moonbag dust stays in asset_held for next accumulation to absorb

---

## 4. ORDER MANAGEMENT

### Pending Buy Orders
- Only 1 pending buy at a time
- Tracked by: pending_buy_oid, pending_buy_time
- 90-second timeout, max 3 retries per signal
- On fill: update weighted avg entry

### Pending Sell Orders
- Multiple sells can be pending simultaneously (one per active tier)
- Tracked in: pending_sells = [{tier, oid, price, qty, placed_at}]
- Each cycle: check if any pending sells have filled (REST poll)
- If price drops 0.5% below a pending sell's tier price: cancel it
  (price retreated, the tier is no longer active)
- Re-place when price crosses back above tier

### Weighted Average Entry Tracking

```
On each buy fill:
  old_cost = avg_entry × total_held
  new_cost = fill_price × fill_quantity
  total_held += fill_quantity
  avg_entry = (old_cost + new_cost) / total_held
  total_cost = avg_entry × total_held
```

This is critical for correct profit tier calculation. Persisted to
bots.json for crash recovery.

---

## 5. BOT STATE (in bot dict, persisted to bots.json)

### Standard Fields
- pair, strategy ("DCA"), status, allocated_usd, current_usd
- asset_held, position_side ("FLAT" or "LONG"), entry_price (= avg_entry)
- timeframe (default "5m")

### Strategy-Specific Fields
- dca_state: str — "SCANNING", "ARMED", "BUYING", "ACCUMULATING", "PAUSED"
- avg_entry: float — weighted average entry across all buys
- total_cost: float — total USD spent on current position
- total_buys: int — number of accumulated buys in current cycle
- buy_count_this_cycle: int — buys made since last full sell-out
- last_cross_direction: str — "ABOVE" or "BELOW" (tracks Fast vs Slow ROC)
- armed_at: float — timestamp when ARMED state entered (for logging)
- pending_buy_oid: str — client OID of pending buy
- pending_buy_time: float — timestamp of pending buy placement
- buy_retries: int — retry count for current buy attempt
- pending_sells: list — [{tier: float, oid: str, price: float, qty: float}]
- highest_tier_sold: float — highest profit tier that has completed a fill
- base_min_size: float — cached from get_product() (refreshed on deploy)
- base_increment: str — cached for snap_to_increment
- quote_increment: str — cached for snap_to_increment
- paused_at: float — timestamp when PAUSED state entered (0 if not paused)

### Cleared on Full Position Exit (tier reset)
- avg_entry → 0, total_cost → 0, total_buys → 0
- buy_count_this_cycle → 0, highest_tier_sold → 0
- pending_sells → [], position_side → "FLAT"
- dca_state → "SCANNING"

---

## 6. EXECUTION ORDER (Every 15-Second REST Cycle)

### WS Integration
- Sell orders are maker limits, not trailing stops, so WS tick-level
  evaluation is less critical than MOMENTUM/GRID.
- However, process_price_tick() should update a "live profit %" display
  field for DCA bots so the UI refreshes in real-time.
- Fill detection for sells uses REST polling (15s), which is acceptable
  since limit sells sit on the book and fill when matched.

### REST Cycle (execute_dca):

1. Fetch price + candles (5m, 300 bars)
2. Fetch product info (for base_min_size, increments — cache after first)
3. Compute indicators: Fast ROC, Slow ROC, ADX

4. **Check PAUSED state:**
   - If PAUSED and price > avg_entry × 0.87: un-pause → ACCUMULATING
   - If not paused and holding and price < avg_entry × 0.85: pause → PAUSED

5. **Check pending buy fill:**
   - If BUYING with pending_buy_oid: check fill status
   - If filled: update avg_entry, total_held, transition to ACCUMULATING
   - If timeout (90s): cancel, re-evaluate, retry or abandon

6. **Check pending sell fills:**
   - For each pending_sells entry: check if filled
   - If filled: update position, record_trade, advance tier
   - If price retreated below tier - 0.5%: cancel stale sell

7. **Evaluate profit tiers (if ACCUMULATING or PAUSED with position):**
   - Calculate profit_pct from avg_entry
   - For each tier above highest_tier_sold:
     - If profit_pct >= tier_threshold and no pending sell at this tier:
       Place maker limit sell

8. **Signal evaluation (if SCANNING or ARMED, and not PAUSED):**
   - SCANNING: check for Fast cross below Slow → ARMED
   - ARMED: check dip depth + curl-up + ADX → BUYING
     - Check disarm: Fast crossed back above Slow → SCANNING

9. Save state

---

## 7. UI DISPLAY

### Bot Card (loadBots)
- **State badge:** SCANNING (gray), ARMED (yellow), BUYING (blue),
  ACCUMULATING (green), TAKING_PROFIT (cyan), PAUSED (red)
- **Buys:** count of accumulated buys this cycle
- **Avg Entry:** weighted average entry price
- **Profit %:** current unrealized profit from avg entry
- **Tier:** next profit target (e.g., "Next: 2.5%")
- **Pending:** "BUY @ $X" or "SELL @ $X" if orders outstanding

### Bot Chart (openBotChart)
- **Avg Entry line:** gold, solid (like TRAP)
- **Tier lines:** dashed green lines at each profit tier price
- **Pause line:** dashed red line at avg_entry × 0.85 (if holding)
- **Trade markers:** same as other bots (arrows on fills)

---

## 8. EXAMPLE SCENARIO (ETH-USD, 5m)

1. Bot starts SCANNING with $200 allocated
2. Fast ROC crosses below Slow ROC → ARMED
3. Both ROCs reach -0.62 (between -0.50 and -0.75), Fast curls up, ADX=22
4. Signal fires: min buy = 0.001 ETH × $2000 = $2.00. Buy 0.001 ETH at $1999.99
5. Fills. avg_entry = $1999.99. State → ACCUMULATING
6. Price drops further. Fast ROC crosses above Slow (reset), then below again → re-ARMED
7. Both ROCs at -1.3 (between -1.0 and -2.0), Slow curls up, ADX=24
8. Larger buy: 0.001 × 1.10 = 0.0011 ETH at $1949.99
9. avg_entry = ($1999.99 × 0.001 + $1949.99 × 0.0011) / 0.0021 = $1973.79
10. Price recovers to $2003.40 → profit = 1.5% from avg entry
11. Sell 25% of 0.0021 = 0.000525 ETH at $2003.40 (maker limit)
12. Fills. Remaining: 0.001575 ETH. Record trade.
13. Price hits $2023.14 → profit = 2.5% from avg entry
14. Sell 33% of remaining = 0.00052 ETH at $2023.14
15. Continue scaling out as price rises...

---

## 9. INTEGRATION NOTES

### File: strategies.py
- New function: calculate_dca(df, dca_state, last_cross_direction)
- Returns: (signal, reason, extra_data)
  - signal: "ARM", "BUY", "DISARM", "HOLD"
  - extra_data: {fast_roc, slow_roc, adx, depth_tier_multiplier}

### File: bot_executors.py
- New function: execute_dca(bot_id, bot, pair)
- Full state machine: SCANNING → ARMED → BUYING → ACCUMULATING
- Profit tier management, sell placement/tracking/cancellation
- Weighted avg entry tracking

### File: bot_ws.py
- process_price_tick(): Add DCA bots for live profit % calculation
  (for UI display only — no stop execution needed since no trailing stops)

### File: bot_manager.py
- run_bot(): add elif strategy == 'DCA': execute_dca(...)
- /api/bots: inject dca state for frontend

### File: bot_utils.py
- STRATEGY_DEFAULT_TF: add "DCA": "5m"

### File: index.html (via patcher)
- Strategy dropdown: add DCA option
- Bot card: DCA-specific section (state, buys, avg entry, tier, pending)
- Bot chart: avg entry, tier lines, pause line, trade markers
- JS constants: add DCA to STRATEGY_TF and STRATEGY_DEFAULT_TFS

---

## CHANGE LOG

### 2026-03-22

**1. Curl-up detection loosened**
Changed from strict inflection `(current > prev AND prev <= prev2)` to simple rising `(current > prev)`.
Prevents missed one-bar windows where the inflection condition was too tight.

**2. ARM/DISARM trigger changed to zero-line cross**
- Previously: ARM on Fast ROC crossing below Slow ROC; DISARM on Fast crossing back above Slow.
- Now: ARM when both ROCs cross below zero. DISARM when both cross back above zero.
- Re-arm cycle (in ACCUMULATING) uses same zero-line logic: both must cross above zero first (reset), then back below zero to re-arm.
- Applied via `patch_dca_zero.py`.

**3. Depth threshold lowered from -0.50 to -0.30**
Both ROCs must be ≤ -0.30 (was -0.50) for a buy to trigger. Catches shallower but confirmed dips.

**4. Sizing tiers reworked**
Entry depth threshold updated to -0.30 to match new dip threshold. Full new tier scale pending documentation.

**bots.json — one-time state reset**
All DCA bots reset to SCANNING with `last_cross_direction = ABOVE` after the zero-line logic change, ensuring clean pickup of the new ARM triggers.

### 2026-03-28

**1. Profit tiers widened for meaningful net profit**
Old first tier was 1.5% — after ~0.8% round-trip maker fees, net was ~0.7%. Marginal.
New tiers: 3%, 5%, 7.5%, 10%, 15%, 20%. First tier nets ~2.2% after fees. Wider spacing
lets trades run instead of scalping. Bigger moonbag (~3.4%) rides extended moves.

**2. Depth multiplier made more aggressive**
Old: topped at 2.0x at -1.0 ROC. New: continuous scale up to 6.0x at -3.0+ ROC.
DCA's mathematical edge is buying heavily at deeper dips — the old curve was too flat.

**3. Graduated drawdown response replaces hard pause**
Old: hard pause at -15%, resume at -13%. Stopped buying exactly when prices were cheapest.
New: graduated — half-size at -10%, quarter-size at -20%, full pause only at -25%.
Keeps averaging down through normal drawdowns while still protecting against catastrophe.

**4. Tier reset threshold bumped from -2.5% to -3.0%**
Matches new first profit tier at 3%.

**5. ARM trigger moved from zero-cross to depth threshold**
Old: ARM fired when both ROCs crossed below zero — too early, dip barely starting.
New: SCANNING waits for both ROCs <= -0.30 before ARMing. ARM now means "target acquired,
waiting for curl-up confirmation." Re-arm cycle also requires depth, not just zero-cross.
