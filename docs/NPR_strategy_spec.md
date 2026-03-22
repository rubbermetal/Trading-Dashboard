# NPR STRATEGY — IMPLEMENTATION SPECIFICATION
# Derivatives Only, 2-Minute Timeframe
# Date: 2026-03-22

## OVERVIEW

NPR (Name-Position-Risk) is a price-action pattern strategy for derivatives.
It detects three event types (Elephant Bar, Tail Bar, 180 Bar), validates
direction against a zone system based on 20/200 SMA positioning, sizes by
risk (max loss per trade / stop distance), and manages exits via event stops
and zone-based profit targets.

Both LONG and SHORT. Derivatives only. 2-minute timeframe.
All orders are maker (post_only=True).

---

## 1. EVENT DETECTION (NAME IT)

### 1A. Elephant Bar
Large-bodied candle representing institutional footprint.
- body = abs(close - open)
- avg_body = SMA(body, 20) over last 20 completed bars
- Qualifies if: body >= 2.5 × avg_body
- Direction: close > open = BULL elephant, close < open = BEAR elephant
- Stop: just beyond the event bar's low (bull) or high (bear)

### 1B. Tail Bar
Candle with small body and disproportionately large wick.
- body = abs(close - open)
- full_range = high - low
- body_pct = body / full_range (must be <= 0.35)
- BULL tail: lower_tail = min(open, close) - low; lower_tail / body >= 2.0
- BEAR tail: upper_tail = high - max(open, close); upper_tail / body >= 2.0
- Body color is irrelevant (can be green, red, or doji)
- Stop: just beyond the tail's extreme (low for bull, high for bear)

### 1C. 180 Bar (Most Powerful)
Two-bar reversal where bar 2 completely overcomes bar 1.
- BULL 180: prev bar is bear (prev_close < prev_open), curr bar is bull
  - curr_high > prev_high (takes out high)
  - curr_open <= prev_close OR curr body bottom <= prev body bottom (eclipse)
  - prev bar body >= 0.5 × avg_body (first bar must be sizable)
- BEAR 180: prev bar is bull, curr bar is bear
  - curr_low < prev_low (takes out low)
  - curr_open >= prev_close OR curr body top >= prev body top (eclipse)
  - prev bar body >= 0.5 × avg_body
- 2× follow-through power → hold longer, use wider trailing stops
- Stop: just beyond the 180 pattern's extreme low (bull) or high (bear)

### Priority
If multiple events are detected simultaneously:
180 > Tail > Elephant

---

## 2. ZONE SYSTEM (POSITION IT)

### Prerequisites
- SMA(200) must be relatively flat: slope over 20 bars <= 0.15%
- SMA(20) and SMA(200) must be close: gap <= 3.0 × ATR(14)
- If either condition fails → no trades, return HOLD

### Zone Calculation
ma_mid = (SMA_20 + SMA_200) / 2
distance_atr = (current_price - ma_mid) / ATR(14)

| Zone | Distance from MA midpoint | Long OK? | Short OK? |
|------|--------------------------|----------|-----------|
| +1   | 0 to +1.5 ATR           | YES      | NO        |
| +2   | +1.5 to +3.0 ATR        | YES*     | YES*      |
| +3   | Beyond +3.0 ATR          | NO       | YES       |
| -1   | 0 to -1.5 ATR           | NO       | YES       |
| -2   | -1.5 to -3.0 ATR        | YES*     | YES*      |
| -3   | Beyond -3.0 ATR          | YES      | NO        |

*±2 zones: only with 180 or strong elephant (check_score >= 3)

### Direction Gate (ABSOLUTE)
- Bull event in -1 → NO TRADE
- Bear event in +1 → NO TRADE
- Bull event in +3 → NO TRADE
- Bear event in -3 → NO TRADE
This is never relaxed.

### Position Check Bonuses
- Event at ±1 zone: +1 position check
- Event near SMA(20) (within 0.5 ATR): +1 position check
- Event near flat SMA(200) (within 0.5 ATR): +1 position check (strongest)
- Multiple position checks = higher conviction

---

## 3. RISK SIZING (RISK IT)

### Configuration (set at deploy time)
- max_loss_per_trade: USD (default $10 for crypto, configurable)
- max_loss_per_day: 3 × max_loss_per_trade (default $30)
- stop_buffer: 0.5 × ATR beyond event extreme (crypto adapted from cents)

### Event Stop Calculation
- Bull elephant/tail: stop = event_low - (0.5 × ATR)
- Bear elephant/tail: stop = event_high + (0.5 × ATR)
- Bull 180: stop = min(prev_low, curr_low) - (0.5 × ATR)
- Bear 180: stop = max(prev_high, curr_high) + (0.5 × ATR)

### Position Sizing
stop_distance = abs(entry_price - event_stop)
position_size = max_loss_per_trade / (stop_distance × contract_multiplier)
Snap to increment. If < base_min_size → skip trade.

### Daily Loss Halt
- Track cumulative realized losses per UTC day
- If daily_loss >= max_loss_per_day → DAILY_HALT state
- Resume at UTC midnight (daily_loss resets)

---

## 4. ENTRY TIMING

2-minute bars = 120 seconds per bar.
Enter in the latter third = after 80 seconds into the bar.

### Detection Flow (every 15s REST cycle)
1. Fetch last 300 candles (2m)
2. Detect events on last COMPLETED bar (safe, confirmed)
3. Validate zone + direction gate
4. If valid signal exists:
   a. Calculate how far into the CURRENT forming bar we are
   b. bar_start = floor(current_time / 120) × 120
   c. elapsed = current_time - bar_start
   d. If elapsed >= 80: place maker limit order
   e. If elapsed < 80: store signal, wait for next cycle
5. Signal expires if the current bar completes without entry

### Entry Execution
- LONG: post_only limit buy at (current_price - 1 tick)
- SHORT: post_only limit sell at (current_price + 1 tick)
- Fill timeout: 30 seconds (tighter than other strategies due to 2m bars)
- Max 2 retries within the same bar
- If unfilled when bar closes → cancel, re-evaluate on next bar

---

## 5. EXIT LOGIC

### Event Stop (Primary — WS tick level)
- Monitored every tick via process_price_tick()
- If price hits event stop → immediate market-like limit exit (aggressive post_only)
- Record loss, add to daily_loss counter

### Profit Targets (Zone-Based)
From ±1 entry:
- Partial exit 50% at ±2 boundary (1.5 ATR from MA mid)
- Remainder trails with stop at breakeven + 0.25 ATR
- Full exit at ±3 boundary (3.0 ATR from MA mid) or trailing stop hit

From ±3 entry (counter-trend mean reversion):
- Partial exit 50% at ±2 boundary
- Full exit at ±1 boundary (back near MAs)
- Tighter trail: 1.0 ATR

### 180 Bar Trades (2× power)
- Wider trailing stop: 1.5 ATR instead of 1.0 ATR
- Hold through ±2 zone without partial — target ±3

### Trailing Stop Mechanism
- After first partial fill or after reaching breakeven + 0.25 ATR:
  - Track high_water_mark (long) or low_water_mark (short)
  - Trail distance depends on event type and entry zone
  - Standard: 1.0 ATR from HWM/LWM
  - 180 trades: 1.5 ATR from HWM/LWM
  - Counter-trend (±3): 0.75 ATR from HWM/LWM

---

## 6. CHECK SCORING

### Per Trade
- name_check: 1 if clean event detected, 0.5 if marginal
- position_checks: count of zone + SMA proximity bonuses (0 to 3)
- risk_check: always 1 (non-negotiable)
- total_score = name_check + min(position_checks, 1) + risk_check

### Minimum Score
- 3-check: name=1, position>=1, risk=1 → take trade
- 2-check: name=0.5 + position>=1 + risk=1, OR name=1 + position=0.5 + risk=1
- Below 2-check: skip

### ±2 Zone Filter
- Only enter ±2 zone if total_score >= 3 AND event is 180 or elephant

---

## 7. BOT STATE

### Standard Fields
- pair, strategy ("NPR"), status, allocated_usd, current_usd
- asset_held, position_side ("FLAT"/"LONG"/"SHORT"), entry_price
- timeframe (always "2m")

### NPR-Specific Fields
- npr_state: "SCANNING" / "SIGNAL_WAIT" / "ENTERING" / "IN_POSITION" / "DAILY_HALT"
- event_type: "ELEPHANT" / "TAIL" / "180" / null
- event_direction: "BULL" / "BEAR" / null
- event_stop: float — the stop price
- event_bar_data: dict — OHLC of the event bar(s)
- zone: int — current zone (-3 to +3)
- check_score: float — total checks
- position_checks: list — which position bonuses applied
- entry_bar_start: float — timestamp of the bar we're entering on
- signal_bar_time: float — timestamp of the bar the event was detected on
- pending_order_oid: str
- pending_order_time: float
- entry_retries: int
- partial_filled: bool — whether 50% profit target was hit
- high_water_mark: float — for trailing stop
- low_water_mark: float — for trailing stop (shorts)
- trail_distance: float — ATR-based trail width
- daily_loss: float — cumulative losses today
- daily_loss_date: str — YYYY-MM-DD of current tracking day
- max_loss_per_trade: float — configured at deploy
- max_loss_per_day: float — 3 × max_loss_per_trade
- atr_at_entry: float — frozen ATR for stop/target calculations

---

## 8. EXECUTION FLOW (Every 15-Second REST Cycle)

1. Check daily halt: if daily_loss >= max_loss_per_day → DAILY_HALT
   - If new UTC day: reset daily_loss, resume SCANNING

2. If IN_POSITION:
   a. Check pending exit orders (partial profit, full exit)
   b. WS handles tick-level stop (backup check here too)
   c. Compute current zone for profit targets
   d. Place/manage profit target limit orders

3. If ENTERING with pending order:
   a. Check fill status
   b. If filled: transition to IN_POSITION, set stops/targets
   c. If timeout (30s): cancel, retry or abandon

4. If SCANNING or SIGNAL_WAIT:
   a. Fetch candles, compute indicators
   b. Check MA prerequisites (flatness, separation)
   c. Detect events on last completed bar
   d. Validate direction vs zone
   e. Compute risk sizing
   f. If valid: check bar timing
      - If >= 80s into current bar: place order → ENTERING
      - If < 80s: store signal → SIGNAL_WAIT
   g. If SIGNAL_WAIT and now >= 80s: place stored signal

5. Save state

---

## 9. WS INTEGRATION

### process_price_tick() — NPR block
- For each NPR bot with status RUNNING and state IN_POSITION:
  - Check event stop: if price <= event_stop (long) or >= event_stop (short)
    → Immediate exit via aggressive limit order
  - Update HWM/LWM for trailing stop
  - Check trailing stop: if price <= HWM - trail_distance (long)
    or >= LWM + trail_distance (short) → exit

### Pair subscription
- NPR bots added to _check_new_pairs() alongside GRID, MOMENTUM, DCA

---

## 10. UI DISPLAY

### Bot Card
- State badge: SCANNING (gray), SIGNAL_WAIT (yellow), ENTERING (blue),
  IN_POSITION (green), DAILY_HALT (red)
- Event: ELEPHANT/TAIL/180 badge with direction arrow
- Zone: +1/+2/+3/-1/-2/-3 with color coding
- Score: check count badge
- Daily P&L: running total
- Trades today: count

### Bot Chart
- SMA(20) overlay (gold)
- SMA(200) overlay (blue)
- Zone boundaries as horizontal bands (green ±1, yellow ±2, red ±3)
- Entry price line
- Event stop line (red dashed)
- Profit target lines (green dashed)
- Trade markers (arrows with PnL)

### Oscillator Sub-Pane
- None needed — NPR is pure price action + MA positioning
