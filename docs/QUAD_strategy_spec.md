# DTRS QUAD ROTATION STRATEGY — FULL SPECIFICATION
# Day Trader Rock Star's Quad Rotation — Unified 5-Tier Implementation
# Date: 2026-04-07

## OVERVIEW

DTRS Quad Rotation is the complete Day Trader Rock Star strategy, unifying all
signal types from the Pine Script indicators and both dashboard entry patterns
into ONE strategy with five confidence-tiered entry signals.

Four stochastic oscillators (9/3, 14/3, 40/4, 60/10) are monitored for:
- Strict Pullback — trend-confirmed dip to EMA support (highest conviction)
- Super Signal — capitulation divergence after quad flush
- Holy Grail — quad oversold + RSI divergence confirmation
- Sequential Rotation — fast-to-slow stochastic turn cascade
- K/D Cross — trigger stochastic crossover in oversold zone

Five entry signal types, each with a different confidence tier:

| # | Entry Signal | Confidence | Capital % | SL (ATR) | TP (ATR) | R:R |
|---|---|---|---|---|---|---|
| 1 | **Strict Pullback** | 0.90 | ~89% | 2.5x | 4.0x | 1:1.6 |
| 2 | **Super Signal** | 0.85 | ~84% | 2.5x | 3.5x | 1:1.4 |
| 3 | **Holy Grail** | 0.80 | ~79% | 2.0x | 3.0x | 1:1.5 |
| 4 | **Sequential Rotation** | 0.70 | ~69% | 2.0x | 3.0x | 1:1.5 |
| 5 | **K/D Cross** | 0.40 | ~40% | 1.5x | 2.0x | 1:1.33 |

Longs only. Works on spot and derivatives.
Default timeframe: 15 minutes.
Orders are market (taker). Single-entry, single-exit.

**QUAD_SUPER is deprecated** — its Super Signal divergence logic is now folded
into QUAD as entry tier #2. Existing QUAD_SUPER bots route to the unified strategy.

---

## 1. INDICATORS

All four stochastics use both **%K (smoothed)** and **%D (smoothed)** lines.
Decisions are based on %D; %K is used for K/D crossover detection.

| Name | Period (k, smooth_k, d) | Role |
|------|-------------------------|------|
| Trigger | 9, 3, 3 | Entry/exit timing, K/D crosses |
| Fast | 14, 3, 3 | Short-term momentum filter |
| Med | 40, 4, 4 | Medium-term momentum context |
| Macro | 60, 10, 10 | Long-term momentum context |

Additional indicators:
- **EMA(20)** and **EMA(50)** — trend bias and support detection (Strict Pullback entry)
- **RSI(5)** and **RSI(14)** — divergence = RSI(5) - RSI(14) (Holy Grail entry)
- **ATR(14)** — volatility measure for SL/TP calculation
Minimum data required: 200 candles.

---

## 2. SIGNAL COMPONENTS

Nine signal types are computed each cycle — seven from the Pine Script indicator
plus two from the original dashboard implementation.

### 2.0a Strict Pullback (from original dashboard QUAD)

Confirmed uptrend with pullback to EMA support:

```
close > EMA(20) AND close > EMA(50)        — trend bias
Macro %D > 80 AND Med %D > 80              — macro/med strength confirmed
Trigger %D <= 20                            — fast stochastic oversold (pullback)
candle low <= EMA(20) * 1.005              — price touching/near 20 EMA support
```

All 5 conditions simultaneously. Highest confidence — most selective filter.

### 2.0b Super Signal Divergence (from original QUAD_SUPER)

Three-stage capitulation reversal:

```
Stage 1: All 4 %D < 20 within last 15 candles (anchor flush)
Stage 2: Current low < anchor low OR close < anchor close (price lower low)
Stage 3: Trigger %D and Fast %D both curling up (> prev bar)
         Both holding above 20 (stochastic higher low)
         close > open (bullish reversal candle)
```

Multi-bar divergence — price making lower lows while stochastics hold higher lows.

### 2.1 Quad Alignment (Pine lines 75-76)

```
all_oversold  = Trig_D < 20 AND Fast_D < 20 AND Med_D < 20 AND Macro_D < 20
all_overbought = Trig_D > 80 AND Fast_D > 80 AND Med_D > 80 AND Macro_D > 80
```

All four %D lines simultaneously in extreme zone. Used by Holy Grail signals.

### 2.2 Turn Detection (Pine lines 79-86)

For each stochastic, detect the first bar of reversal while still in extreme zone:

```
turningUp = D < 20 AND D > D[1] AND D[1] <= D[2]
turningDn = D > 80 AND D < D[1] AND D[1] >= D[2]
```

Conditions: (a) still in extreme zone, (b) D reversed vs previous bar,
(c) previous bar was declining/flat (this bar is the inflection point).

8 boolean columns computed across the full DataFrame.

### 2.3 Sequential Rotation (Pine lines 91-110)

The core signal. Tracks `barssince` each stochastic turned, then verifies
the cascade order: fastest turned first (most bars ago), slowest turned last.

**Bullish Sequential Rotation** fires when ALL are true:
1. All 4 stochastics have previously turned up (barssince not None)
2. `bs_Trig > bs_Fast > bs_Med > bs_Macro` (fastest first, slowest last)
3. `bs_Macro < rotation_window` (slowest turned within the window)
4. `turningUp_Macro` is True on current bar (signal fires when slowest completes)

**Bearish Sequential Rotation**: mirror with turningDn.

The `rotation_window` parameter (default 20 bars, range 5-100) controls
how tight the cascade must be. Smaller = stricter, fewer signals.

### 2.4 Holy Grail (Pine lines 113-114)

Quad alignment confirmed by RSI divergence:

```
holy_grail_bull = all_oversold AND rsi_div > 0
holy_grail_bear = all_overbought AND rsi_div < 0
```

Highest conviction signal — all 4 stochastics capitulated but RSI shows
underlying momentum diverging (fast RSI stronger than slow RSI).

### 2.5 Counter-Trend Protection (Pine lines 118-119)

Warning: slowest stochastic stuck in extreme while fastest is at the opposite:

```
ct_bull_danger = Macro_D < 20 AND Macro_D <= prev_Macro_D AND Trig_D > 80
ct_bear_danger = Macro_D > 80 AND Macro_D >= prev_Macro_D AND Trig_D < 20
```

The macro is still falling/flat in oversold while trigger has rallied to
overbought — the fast move lacks macro confirmation. Protective exit signal.

### 2.6 K/D Cross (Pine lines 127-128)

Trigger stochastic K line crosses D line in extreme zone:

```
bull_cross = prev_K < prev_D AND curr_K > curr_D AND curr_D < 20
bear_cross = prev_K > prev_D AND curr_K < curr_D AND curr_D > 80
```

Uses Pine Script `ta.crossover` / `ta.crossunder` semantics (strict inequality).
Lower conviction than rotation or holy grail but faster-acting.

### 2.7 Convergence Band (Pine lines 122-123)

Weighted average of all four %D lines, favoring slower stochastics:

```
convergence = (d_Trig * 1 + d_Fast * 2 + d_Med * 3 + d_Macro * 4) / 10
spread = avg(|d_i - convergence|) for each stochastic
```

Informational only — included in reason strings. Low spread indicates
tight alignment (high conviction); high spread indicates divergence.

---

## 3. DECISION TREE

Priority-ordered signal generation. First match wins.

### Exit Signals (checked first — protection when LONG)

| Priority | Signal | exit_reason |
|----------|--------|-------------|
| 1 | Counter-Trend Bull Danger | COUNTER_TREND |
| 2 | Sequential Bear Rotation | SEQ_BEAR |
| 3 | Trigger K/D Bear Cross (overbought) | BEAR_KD |

### Entry Signals (when FLAT)

| Priority | Signal | signal_type | Confidence |
|----------|--------|-------------|------------|
| 1 | Strict Pullback | STRICT_PULLBACK | 0.90 |
| 2 | Super Signal Divergence | SUPER_SIGNAL | 0.85 |
| 3 | Holy Grail Bull | HOLY_GRAIL | 0.80 |
| 4 | Sequential Bull Rotation | SEQ_ROTATION | 0.70 |
| 5 | Trigger K/D Bull Cross (oversold) | KD_CROSS | 0.40 |

### Hold

When no signals match, returns HOLD with informational status including
quad alignment state and convergence band values.

---

## 4. CONFIDENCE-BASED ENTRY SIZING

Position size scales with signal conviction:

```
allocation_usd = current_usd * confidence * 0.99
```

The 0.99 factor is a 1% fee buffer. Examples on a $100 bot:

| Signal | Confidence | Allocation | Remaining |
|--------|-----------|------------|-----------|
| Holy Grail | 0.90 | $89.10 | $10.90 |
| Sequential Rotation | 0.70 | $69.30 | $30.70 |
| K/D Cross | 0.40 | $39.60 | $60.40 |

Remaining capital stays idle — this is a single-entry strategy.
No adding to position on subsequent signals while LONG.

---

## 5. STOP LOSS / TAKE PROFIT

ATR(14) at entry time is stored as `entry_atr`. SL and TP multipliers
scale with confidence — higher conviction allows wider stops and targets.

### Stop Loss (hard floor)

```
stop_price = entry_price - (sl_multiplier * entry_atr)
```

| Signal | SL Multiplier | Example (ATR=$500) |
|--------|---------------|-------------------|
| Holy Grail | 2.5x | -$1,250 from entry |
| Sequential Rotation | 2.0x | -$1,000 from entry |
| K/D Cross | 1.5x | -$750 from entry |

### Take Profit (hard ceiling)

```
target_price = entry_price + (tp_multiplier * entry_atr)
```

| Signal | TP Multiplier | Example (ATR=$500) |
|--------|---------------|-------------------|
| Holy Grail | 4.0x | +$2,000 from entry |
| Sequential Rotation | 3.0x | +$1,500 from entry |
| K/D Cross | 2.0x | +$1,000 from entry |

### Exit Priority (when LONG)

Evaluated every cycle in this order:

1. **Hard SL**: `current_price <= stop_price` → STOP_LOSS
2. **Counter-Trend**: strategy returns ct_bull_danger → SIGNAL
3. **Hard TP**: `current_price >= target_price` → TAKE_PROFIT
4. **Signal exits**: Sequential bear rotation or bear K/D cross → SIGNAL

---

## 6. PARAMETERS

| Parameter | Default | Range | Source |
|-----------|---------|-------|--------|
| rotation_window | 20 | 5-100 | `bot.settings.rotation_window` |

Configurable via the QUAD settings panel in the bot creation UI.
Tighter windows (5-10) require faster sequential cascades — fewer but
higher-quality rotation signals. Looser windows (50-100) accept slower
cascades — more signals but potentially weaker momentum confirmation.

---

## 7. BOT STATE

### Standard Fields (shared with all bots)
- `pair`, `strategy` ("QUAD"), `status`, `timeframe` (default "15m")
- `allocated_usd`, `current_usd`, `asset_held`
- `position_side` ("FLAT" or "LONG"), `entry_price`
- `settings` → `{rotation_window: 20}`

### QUAD-Specific Fields (set at entry, cleared at exit)
- `entry_atr` — ATR(14) at time of entry
- `entry_signal` — "HOLY_GRAIL", "SEQ_ROTATION", or "KD_CROSS"
- `stop_price` — hard stop loss price
- `target_price` — hard take profit price
- `high_water_mark` — highest price since entry

All QUAD-specific fields are removed from the bot dict on exit.

---

## 8. ORDER EXECUTION

### Entry
- **Order type**: Market buy (taker)
- **Size**: `current_usd * confidence * 0.99`
  - Spot: `quote_size` = USD amount
  - Derivatives: `base_size` = `int(allocation_usd / (price * multiplier))`
- **Minimum capital**: > $5.00 idle USD AND allocation > $5.00
- Records: `asset_held`, `entry_price`, `position_side = LONG`
- Sets: `stop_price`, `target_price`, `entry_atr`, `entry_signal`, `high_water_mark`

### Exit
- **Order type**: Market sell (taker)
- **Size**: full `asset_held`
- **Fill polling**: `poll_market_fill()` for actual fill price and fees
- Records: `record_trade()` with appropriate `exit_reason`
- Resets: `asset_held = 0`, `position_side = FLAT`
- Clears: all QUAD-specific state fields
- Capital: `current_usd += profit - fee + cost_basis`

### Exit Reasons
- `STOP_LOSS` — hard ATR-based stop triggered
- `TAKE_PROFIT` — hard ATR-based target reached
- `SIGNAL` — strategy-based exit (counter-trend, sequential bear, bear K/D)

---

## 9. EXECUTION ORDER (Every 15-Second Cycle)

1. Fetch candles (default 15m, 250 bars)
2. Calculate all indicators (4 stochastics, RSI, ATR)
3. Compute all 7 signal components
4. Run decision tree → (signal, reason, meta)
5. If LONG:
   a. Update high_water_mark
   b. Check hard SL → exit if triggered
   c. Check counter-trend signal → exit if triggered
   d. Check hard TP → exit if triggered
   e. Check signal-based exits → exit if triggered
6. If FLAT + BUY signal:
   a. Extract confidence, ATR, signal_type
   b. Calculate allocation from confidence
   c. Place market buy
   d. Set SL/TP from ATR multipliers
   e. Store state, save
7. Save state on any change

---

## 10. PINE SCRIPT CONFORMANCE

| Pine Signal | Pine Lines | Dashboard Implementation |
|---|---|---|
| Quad Alignment | 75-76 | `all_oversold` / `all_overbought` — exact match |
| Turn Detection | 79-86 | 8 vectorized boolean columns — exact match |
| Sequential Rotation | 91-110 | `barssince()` helper + cascade check — exact match |
| Holy Grail | 113-114 | `all_oversold AND rsi_div > 0` — exact match |
| Counter-Trend | 118-119 | `Macro stuck + Trigger opposite` — exact match |
| K/D Cross | 127-128 | Trigger K/D crossover in extreme — exact match |
| Convergence Band | 122-123 | Weighted average + spread — exact match (informational) |

**Not ported** (display-only features with no trading signal equivalent):
- Background color trend direction (EMA 21 vs 50)
- Zone fills and grid lines
- RSI divergence histogram visualization
- Dark mode toggle

---

## 11. INTEGRATION NOTES

### File: strategies.py
- `calculate_quad_rotation(df, rotation_window=20)` — returns `(signal, reason, meta)`
- `calculate_quad_super()` — REMOVED, folded into calculate_quad_rotation as tier 2

### File: bot_executors.py
- `_quad_exit(bot, pair, current_px, mult, exit_reason, reason)` — shared exit helper
- `_QUAD_SL_MULT` / `_QUAD_TP_MULT` — ATR multiplier dicts (5 tiers)
- `execute_quad(bot_id, bot, pair)` — unified, no mode parameter

### File: routes/bot_manager.py
- `run_bot()`: routes both QUAD and QUAD_SUPER → `execute_quad()`

### File: templates/index.html
- QUAD settings panel with rotation_window input
- Confidence tier summary displayed in settings panel

---

## 12. EXAMPLE SCENARIOS

### Sequential Rotation Entry (BTC-USD, 15m)

1. Trigger %D (9/3) turns up from 12 at bar -18 (still below 20, started rising)
2. Fast %D (14/3) turns up from 15 at bar -12
3. Med %D (40/4) turns up from 8 at bar -6
4. Macro %D (60/10) turns up from 11 on current bar → **Sequential Rotation fires**
5. All turns within 20-bar window ✓, order: Trig→Fast→Med→Macro ✓
6. Entry: Market buy 70% of capital. SL = entry - 2.0×ATR. TP = entry + 3.0×ATR.
7. BTC recovers. Macro %D reaches 85, Trigger %D reaches 92.
8. Sequential bear rotation fires when all 4 turn down in order → SELL

### Holy Grail Entry (ETH-USD, 15m)

1. All four stochastic %D values drop below 20 simultaneously (quad oversold)
2. RSI(5) = 32, RSI(14) = 28 → divergence = +4 (positive = bullish)
3. Holy Grail Bull fires → **highest conviction entry**
4. Entry: Market buy 90% of capital. SL = entry - 2.5×ATR. TP = entry + 4.0×ATR.
5. ETH recovers strongly. Price hits target_price → TAKE_PROFIT exit.

### Counter-Trend Exit

1. Bot is LONG from a K/D Cross entry.
2. Macro %D = 18 (stuck below 20, flat). Trigger %D = 83 (rallied to overbought).
3. Counter-Trend Bull Danger fires → **protective SELL**
4. Exit at market. Reason: fast momentum lacks macro confirmation.
