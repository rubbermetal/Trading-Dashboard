# Strategy Spec Conformance Report
**Date:** 2026-03-21
**Specs checked:** Grid risk engine spec.md, MOMENTUM_strategy_spec.md, DCA_strategy_spec.md

---

## Summary

The codebase is **highly conformant** with all three specs. The core logic — risk escalation, trailing stops, halt modes, circuit breaker, state machines, order management, and execution order — is correctly implemented throughout. There are 2 signal-correctness bugs and 6 minor/cosmetic deviations.

---

## Issues

### BUG-1 — MOMENTUM: ROC smoothing uses SMA(2) instead of SMA(5)
**File:** `strategies.py:404,408`
**Severity:** Bug (signal correctness)

Spec says Fast ROC = ROC(5) smoothed with SMA(5), Slow ROC = ROC(14) smoothed with SMA(5).

```python
# Current (wrong):
roc5_smooth  = ta.sma(roc5_raw,  2)
roc14_smooth = ta.sma(roc14_raw, 2)

# Should be:
roc5_smooth  = ta.sma(roc5_raw,  5)
roc14_smooth = ta.sma(roc14_raw, 5)
```

**Impact:** SMA(2) produces a noisier signal than SMA(5). Entry conditions will trigger more frequently and on shallower inflections than the spec intends. The curl-up detection in particular is more sensitive than designed.

---

### BUG-2 — DCA: ROC smoothing uses SMA(3) instead of SMA(5)
**File:** `strategies.py:507,509`
**Severity:** Bug (signal correctness)

Same issue as BUG-1, different value. Spec says SMA(5) for both ROCs.

```python
# Current (wrong):
roc5_smooth  = ta.sma(roc5_raw,  3)
roc14_smooth = ta.sma(roc14_raw, 3)

# Should be:
roc5_smooth  = ta.sma(roc5_raw,  5)
roc14_smooth = ta.sma(roc14_raw, 5)
```

**Impact:** ARM and BUY signals will fire on less-confirmed dips than the spec intends. Cross detection and curl-up conditions will be noisier, potentially causing more frequent re-arming cycles.

---

### MINOR-1 — DCA: Capital guard threshold is half of spec
**File:** `bot_executors.py:1059`
**Severity:** Minor

Spec says: "If bot's idle USD < min_buy_usd: skip buy."
`min_buy_usd = base_min_size × current_price`

```python
# Current:
if bot['current_usd'] < base_min * cur_px * 0.5:

# Should be:
if bot['current_usd'] < base_min * cur_px:
```

**Impact:** The bot will attempt buys when it has between 50%–100% of the minimum required capital, which may result in rejected orders rather than a clean skip. Low risk in practice as the subsequent size calculation will naturally fail, but it adds unnecessary API calls.

---

### MINOR-2 — DCA: Stale sell cancellation threshold differs from spec
**File:** `bot_executors.py:789`
**Severity:** Minor

Spec says: "If price drops back below a pending sell's tier price: cancel it."
Implementation cancels when `profit_pct < tier_pct / 2.0` — the sell is kept until profit retreats to *half* the tier threshold, not to the tier itself.

```python
# Current (lenient):
cancel_threshold = tier_pct / 2.0
if profit_pct < cancel_threshold:

# Spec intent (cancel at tier):
if profit_pct < tier_pct:
```

**Impact:** Pending sell orders stay on the book longer during pullbacks than the spec intends. The order will eventually either fill (fine) or get cancelled on a deeper pullback (also fine). Minor behavioural difference, arguably more patient than the spec.

---

### MINOR-3 — DCA: `pending_sells` entries missing `placed_at` timestamp
**File:** `bot_executors.py:1021`
**Severity:** Minor (missing field)

Spec says `pending_sells` entries should contain `{tier, oid, price, qty, placed_at}`.

```python
# Current (missing placed_at):
pending_sells.append({
    'tier': tier_pct,
    'oid': oid,
    'price': float(str_price),
    'qty': float(str_qty),
})
```

**Impact:** No placed_at timestamp means sell age cannot be tracked. Spec doesn't explicitly use this field for any decision logic in the current implementation, but it was specified for logging/debugging purposes.

---

### MINOR-4 — MOMENTUM: WS exit path uses wrong base_inc lookup
**File:** `bot_ws.py:142`
**Severity:** Minor (potential precision issue)

MOMENTUM bots store `base_inc` directly on the bot dict (set in `execute_momentum`), but the WS exit path looks for it inside a `settings` sub-dict which MOMENTUM bots don't have.

```python
# Current (falls back to default '0.00000001'):
str_qty = snap_to_increment(held, bot.get('settings', {}).get('base_inc', '0.00000001'))

# Should be:
str_qty = snap_to_increment(held, bot.get('base_inc', '0.00000001'))
```

**Impact:** For most assets the default `0.00000001` is fine or conservative. For assets with a coarser increment (e.g. DOGE at 1.0), the exit sell qty could be snapped to a finer increment than the exchange allows, causing order rejection at the worst possible moment (stop triggered).

---

### COSMETIC-1 — Grid: No explicit CAUTION state log at depth 2-3
**File:** `grid_engine.py` — `execute_grid_bot()`
**Severity:** Cosmetic (observability)

Spec §2 says: "Depth 2-3 (CAUTION): Recovery tracking begins, bot logs state for decision-making."

The implementation does track recovery timestamps at this depth (via `deactivate_trail_by_sell`) but does not print a log message or set an explicit CAUTION label when depth enters the 2-3 range. All decisions at this depth are correct (no order changes), but there is no visibility into the CAUTION state from logs.

---

### COSMETIC-2 — Grid: `grid_check_fills` runs before circuit breaker in execution order
**File:** `grid_engine.py:944` vs spec §10 order
**Severity:** Cosmetic (execution order)

Spec §10 defines the cycle order as: (4) update depth → (5) check circuit breaker → (6) check trailing stops → (10) check filled orders.

In `execute_grid_bot()`, `grid_check_fills` (step 10) runs near the top, before the circuit breaker check (step 5). This means a fresh fill can be processed and a new sell can be placed in the same cycle where the circuit breaker should have fired.

**Impact:** Extremely low. The circuit breaker fires on unrealized loss from inventory, not from a new fill. In the worst case, one extra sell gets placed moments before a market-sell wipes the position, which is then redundant but harmless.

---

## What Is Fully Conformant

- Grid trail distances (top/mid/bottom third tiering) — exact
- Grid direction check (SMA5 slope, 3-candle lookback) — exact
- Grid halt modes (FAVORABLE / ADVERSE / NEUTRAL) with correct multipliers — exact
- Grid anti-race condition (`cancel_order_safe` with verification) — implemented
- Grid circuit breaker (6%, market sell, cancel all, log CIRCUIT_BREAKER) — exact
- Grid per-fill trail lifecycle (activate on fill, HWM ratchet, WS primary + REST fallback) — exact
- Grid follow logic blocked at depth > 3 — explicit guard in place
- Grid buy redeployment conditions (depth ≤ 3, not FALLING) — exact
- Grid dormant bypass (no inventory → abort to DORMANT on halt trigger) — implemented
- Grid state fields (all 9 spec fields present in bots.json) — complete
- MOMENTUM entry conditions (ADX, SMA cross, dual ROC thresholds, curl-up logic) — correct (signal shape correct, smoothing window wrong per BUG-1)
- MOMENTUM three-phase stop (phases 1/2/3, HWM, fee estimate, floors) — exact
- MOMENTUM dual-path execution (WS primary, REST fallback, shared `momentum_get_stop_price`) — exact
- MOMENTUM exit reasons (STOP_LOSS phase 1, TRAILING_STOP phase 2/3) — exact
- MOMENTUM ATR frozen at entry — implemented
- MOMENTUM maker limit entry with 90s timeout, max 3 retries — exact
- DCA state machine (SCANNING → ARMED → BUYING → ACCUMULATING → PAUSED) — exact
- DCA re-arm cycle (cross above reset, then cross below re-arm) — exact
- DCA profit tier schedule (all 6 tiers, correct percentages) — exact
- DCA weighted average entry formula — correct
- DCA pause/resume hysteresis (15% in, 13% out) — exact
- DCA WS live profit % update (display only, no stop) — implemented
- All three strategies wired into `run_bot()` and `/api/bots` — complete
