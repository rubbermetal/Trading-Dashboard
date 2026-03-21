# QUAD STRATEGY — FULL SPECIFICATION
# Reference Document for Implementation
# Date: 2026-03-21

## OVERVIEW

QUAD is a multi-timeframe stochastic momentum strategy. It uses four stochastic
oscillators at different periods to measure momentum at macro, medium, fast, and
trigger timeframes simultaneously. The strategy waits for high-timeframe momentum
to confirm a bullish trend, then enters on a short-timeframe pullback to the 20 EMA.

Two variants exist sharing the same exit logic but differing entry conditions:

- **QUAD (Standard):** Strict pullback — enters when macro/med stochs are
  overbought and the trigger stoch dips to oversold at the EMA.
- **QUAD_SUPER:** Capitulation divergence — waits for a full quad-oversold
  flush, then enters on a price lower-low with stochastic higher-low divergence.

Longs only. Works on spot and derivatives.
Default timeframe: 15 minutes.
Orders are market (taker). No maker limit logic.

---

## 1. INDICATORS

All four stochastics use the **%D (smoothed)** line, not raw %K.

| Name    | Period (k, d, smooth_k) | Role                          |
|---------|-------------------------|-------------------------------|
| Macro   | 60, 10, 10              | Long-term momentum context    |
| Med     | 40, 4, 4                | Medium-term momentum context  |
| Fast    | 14, 3, 3                | Short-term momentum filter    |
| Trigger | 9, 3, 3                 | Entry/exit timing             |

Additional indicators:
- EMA(20) — trend proximity and touch detection
- EMA(50) — secondary trend filter (QUAD Standard only)
- EMA(200) — not used in signal; available for future context

Minimum data required: 200 candles.

---

## 2. QUAD STANDARD — SIGNAL GENERATION

### Entry Conditions (ALL must be true simultaneously)

1. **Trend Bias:**
   - `close > EMA(20)` AND `close > EMA(50)`
   - Price must be above both short and medium EMAs (in uptrend)

2. **Macro Strength:**
   - `Macro %D > 80`
   - Confirms the dominant trend is strongly bullish

3. **Medium Strength:**
   - `Med %D > 80`
   - Confirms intermediate momentum is also overbought

4. **Trigger Oversold:**
   - `Trigger %D <= 20`
   - The fastest oscillator has pulled back to oversold — timing the dip

5. **EMA Touch:**
   - `candle low <= EMA(20) × 1.005`
   - Price is at or within 0.5% above the 20 EMA
   - Ensures entry at the EMA support level, not mid-air

When all five conditions are met: signal = **"BUY"**

### Why These Filters Work Together

- The high-period stochs (Macro/Med) act as a trend filter. They must be
  overbought (> 80) to confirm the uptrend has real strength behind it.
- The fast stoch (Trigger) acts as a timing tool. A dip to < 20 within a
  strong trend is a brief oversold condition, not a reversal.
- The EMA touch anchors the entry spatially — you're buying the pullback
  at a known support level, not chasing price.

### Exit Condition

- `prev Trigger %D < 80` AND `curr Trigger %D >= 80`
- The trigger stoch crosses back above 80 from below
- This marks the moment the short-term oversold condition has resolved
- Signal = **"SELL"**

### Signal Output

- **"BUY"** — all entry conditions met
- **"SELL"** — trigger stoch crossed above 80 (from < 80)
- **"HOLD"** — conditions not met

---

## 3. QUAD_SUPER — SIGNAL GENERATION

QUAD_SUPER requires a three-stage setup. All stages must occur in sequence.

### Stage 1: Anchor Flush (Quad Oversold)

Search backwards through the last 15 candles for a candle where **all four**
stochastics are simultaneously below 20:

```
Macro %D < 20  AND  Med %D < 20  AND  Fast %D < 20  AND  Trigger %D < 20
```

The first such candle found (most recent) becomes the **anchor candle**.
If no anchor exists in the lookback window, return HOLD.

### Stage 2: Price Lower Low

After an anchor candle is found, the current candle must show:
```
current low < anchor low  OR  current close < anchor close
```

Price must make a lower low relative to the anchor, confirming the flush
has extended (a second leg down, the divergence setup).

### Stage 3: Stochastic Higher Low + Reversal Candle

While price made a lower low, the stochastics must hold above 20 and
turn upward simultaneously:

- `Trigger %D > prev Trigger %D` (trigger curling up)
- `Fast %D > prev Fast %D` (fast curling up)
- `Trigger %D > 20` AND `Fast %D > 20` (stochs holding above oversold)
- `close > open` (current candle is a bullish reversal candle)

When all three stages are confirmed: signal = **"BUY"** ("SUPER SIGNAL")

### Exit Condition (same as QUAD Standard)

Checked before entry conditions on every cycle:
- `prev Trigger %D < 80` AND `curr Trigger %D >= 80`
- Signal = **"SELL"**

### Signal Output

- **"BUY"** — super signal conditions met (Stage 1 + 2 + 3)
- **"SELL"** — trigger stoch crossed above 80
- **"HOLD"** — conditions not met or still setting up

---

## 4. ORDER EXECUTION

Both QUAD and QUAD_SUPER use identical execution logic.

### Entry

- **Order type:** Market buy (taker)
- **Size:** `allocated_usd × 0.99` (1% buffer)
  - Spot: `quote_size` = USD amount
  - Derivatives: `base_size` = `floor(allocated_usd × 0.99 / (price × multiplier))`
- **Minimum capital:** > $5.00 idle USD
- Records: `asset_held`, `current_usd = 0`, `entry_price`, `position_side = LONG`

### Exit

- **Order type:** Market sell (taker)
- **Size:** full `asset_held`
- Records: `record_trade()` with exit_reason = "SIGNAL"
- Resets: `asset_held = 0`, `position_side = FLAT`
- Capital: `current_usd = allocated_usd + (profit × 0.995)`

### No Staging

QUAD is a single-entry, single-exit strategy. There is no multi-stage position
building. The full position is taken on the BUY signal and exited on the SELL signal.

---

## 5. BOT STATE

### Standard Fields (shared with all bots)
- `pair`, `strategy` ("QUAD" or "QUAD_SUPER"), `status`
- `allocated_usd`, `current_usd`, `asset_held`
- `position_side` ("FLAT" or "LONG"), `entry_price`
- `timeframe` (default "15m")

No strategy-specific state fields. QUAD is stateless between cycles.

---

## 6. EXECUTION ORDER (Every 15-Second Cycle)

1. Fetch candles (15m, 250 bars)
2. Calculate all indicators (4 stochastics, EMAs)
3. Read `position_side`
4. If LONG: evaluate SELL signal → exit if triggered
5. If FLAT: evaluate BUY signal → enter if triggered
6. Save state on any change

---

## 7. INTEGRATION NOTES

### File: strategies.py
- `calculate_quad_rotation(df)` — QUAD Standard
- `calculate_quad_super(df)` — QUAD_SUPER
- Both return: `(signal, reason)`

### File: bot_executors.py
- `execute_quad(bot_id, bot, pair, mode='STANDARD')`
- `mode='SUPER'` routes to `calculate_quad_super()`

### File: bot_manager.py
- `run_bot()`: routes QUAD → execute_quad(mode='STANDARD')
- `run_bot()`: routes QUAD_SUPER → execute_quad(mode='SUPER')

### Derivative Support
- Uses `is_derivative()` and `get_contract_multiplier()` from bot_utils
- Spot: no short support (QUAD is longs only)
- Derivatives: no short signals exist in QUAD

---

## 8. EXAMPLE SCENARIO (BTC-USD, 15m)

1. Macro %D = 85, Med %D = 83 (both > 80 — strong trend confirmed)
2. BTC dips to the 20 EMA. Low touches EMA_20 × 1.002
3. Trigger %D drops to 18 (oversold)
4. price > EMA_20 > EMA_50
5. Signal: BUY → market buy 99% of capital
6. BTC recovers and Trigger %D crosses above 80
7. Signal: SELL → market sell full position, lock profit

**QUAD_SUPER scenario:**
1. All four stochs simultaneously drop below 20 (quad flush, anchor set)
2. BTC makes a new low vs the flush candle
3. Trigger %D = 25 (> anchor's 12), Fast %D = 28 (> anchor's 15) — higher low
4. Current candle is bullish (close > open)
5. Signal: BUY → entry on divergence confirmation
6. Exit: same as QUAD (Trigger %D crosses 80)
