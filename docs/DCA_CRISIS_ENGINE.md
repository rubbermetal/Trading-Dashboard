# DCA Crisis Management Engine — Current Design & Open Questions

## What We Built

A continuous 5-factor scoring system that replaces the old binary circuit breaker (-25% sell everything). Instead of one catastrophic liquidation, the engine makes graduated cuts as conditions worsen, freeing capital for new DCA cycles while keeping a shrinking position for recovery.

## Results So Far (ETH-USD 5m, Jan 7 - Apr 7 2026)

| Approach | Return | Max DD | Trades | Win Rate |
|----------|--------|--------|--------|----------|
| Old circuit breaker (-25% nuke) | -$266 (-26.6%) | 44.0% | 6 | 66.7% |
| Smart CB (hold + micro-DCA) | -$210 (-21.0%) | 31.3% | 5 | 60.0% |
| **Scoring engine (graduated cuts)** | **-$57 (-5.7%)** | **19.5%** | 12 | 25.0% |
| Buy & hold | -$293 (-29.3%) | — | — | — |

The scoring engine outperformed everything by a wide margin. But -5.7% in a -29% market, while good, isn't profitable. The question is whether we can do better.

---

## How the Scoring Engine Works

### Five Factors (0-100 total score)

Every bar when underwater, compute:

**1. Drawdown Severity (0-25 pts)**
How far underwater. 0-5% = 0pts, 5-10% = 5pts, 10-15% = 12pts, 15-20% = 18pts, 20-25% = 22pts, 25%+ = 25pts.

**2. Capital Exposure (0-25 pts)**
`position_value / total_capital`. <15% = 0pts, 15-30% = 5pts, 30-50% = 12pts, 50-70% = 18pts, 70-85% = 22pts, 85%+ = 25pts.

**3. Trend Hostility (0-20 pts)**
From adaptive defense mode (higher-TF EMA). NORMAL (above EMA) = 0pts, CAUTIOUS (0-15% below) = 10pts, SCALP (15%+ below) = 20pts.

**4. Recovery Distance (0-15 pts)**
`drawdown_pct + first_tier_pct` = how far from first profit exit. <5% = 0pts, 5-10% = 5pts, 10-20% = 10pts, 20%+ = 15pts.

**5. Opportunity Cost (0-15 pts)**
`unrealized_loss / avg_profit_per_cycle` = how many winning cycles to earn it back. <3 cycles = 15pts (cut is cheap), 3-7 = 10pts, 7-15 = 5pts, 15+ = 0pts (too expensive to cut).

### Score to Action

| Score | Action | Cut % |
|-------|--------|-------|
| 0-30 | No action | 0% |
| 30-45 | Pause new buys | 0% |
| 45-60 | Light cut | 25% of position |
| 60-75 | Moderate cut | 50% of position |
| 75-90 | Heavy cut | 75% of position |
| 90-100 | Full cut | 100% |

### Rules
- **Progressive**: cuts apply to remaining position, not original. 25% then 50% = 62.5% total.
- **Cooldown**: 12 bars (1 hour on 5m) between cuts. Prevents whipsaw.
- **One-way ratchet**: only cuts again if score EXCEEDS the previous action's score.
- **Recovery**: when score drops below 20, resume normal operations.
- **Capital velocity**: freed cash immediately enters SCANNING for new DCA cycles.

---

## Adaptive Defense Mode (Trend Filter)

Uses a higher-timeframe EMA to determine market regime. Configurable TF (1h/4h/daily) and EMA length (default 50).

| Parameter | Normal (above EMA) | Cautious (0-15% below) | Scalp (15%+ below) |
|-----------|-------------------|----------------------|-------------------|
| Buy size mult | 1.0 | 0.60 | 0.30 |
| ARM threshold | -0.30 | -0.50 | -1.00 |
| Depth mult cap | 6.0 | 3.0 | 2.0 |
| TP tiers | 3/5/7.5/10/15/20% | 2/3/5% | 1.5% flat |

---

## What's Still Wrong

### 1. Win rate dropped from 91% to 25%
The crisis cuts count as losing trades, dragging win rate down. The actual DCA cycle win rate is still high, but the metric is misleading. Should crisis cuts be tracked separately from cycle trades?

### 2. The END_OF_DATA position (-$251 unrealized)
The scoring engine kept cutting but also kept accumulating (198 buys by end). The remaining position is large and underwater. The graduated cuts freed capital but the strategy kept re-entering and building new positions that also went underwater in the sustained decline.

**Core tension**: The engine frees capital via cuts, but the freed capital immediately goes back into DCA cycles that enter the SAME declining market. The capital velocity we preserved is cycling into more losing positions.

### 3. Possible improvements to explore

**A. Post-cut cooldown on re-entry**
After a crisis cut, don't just resume SCANNING immediately. Wait for the trend to actually improve (mode changes from CAUTIOUS/SCALP back to NORMAL) before starting new cycles. This prevents the freed capital from cycling back into the same downturn.

**B. Asymmetric re-entry**
After a cut, require a HIGHER bar for re-entry than normal. E.g., normal ARM threshold is -0.30, but post-crisis ARM threshold is -1.50 for the next 3 cycles. Only enter extreme dips after getting burned.

**C. Score-aware entry sizing**
Even when score is below 30 (no cuts), use the score to SCALE entry sizes. Score 20 = normal size. Score 25 = 80% size. This creates a smooth transition instead of binary normal/crisis.

**D. Cumulative loss tracking**
Track total realized losses in the session. After cumulative losses exceed X% of starting capital, reduce all future entry sizes permanently. The strategy is "wounded" and should trade smaller until it recovers.

**E. Separate the "keep cycling" from "keep accumulating" decisions**
Maybe the answer isn't to stop cycling, but to stop accumulating INTO THE SAME POSITION. After a cut, start new cycles as INDEPENDENT mini-positions instead of adding to the existing avg_entry. Each mini-cycle has its own entry, its own tiers, its own tiny stop. Capital velocity is preserved but the blowup risk of one massive accumulated position is eliminated.

**F. Volatility-aware entry**
Don't just use trend direction — use volatility magnitude. High ATR = bigger swings = wider stops needed = smaller positions. Low ATR = calmer = normal positions.

---

## File Locations

| File | What |
|------|------|
| `/home/pi/dashboard/strategies.py` | `calculate_dca()` — signal function with configurable `arm_threshold` |
| `/home/pi/dashboard/backtest_engine.py` | `_compute_crisis_score()` — 5-factor scoring function |
| `/home/pi/dashboard/backtest_engine.py` | `_run_dca_backtest()` — full DCA simulation with scoring engine |
| `/home/pi/dashboard/backtest_engine.py` | `_compute_trend_ema()` — higher-TF EMA for adaptive defense |
| `/home/pi/dashboard/routes/backtest.py` | API endpoint `/api/backtest/run` |
| `/home/pi/dashboard/templates/index.html` | BACKTESTING tab with DCA params (buy%, flat TP%, defense toggle, trend TF, EMA length) |
| `/home/pi/dashboard/data_fetcher.py` | Paginated Coinbase candle fetcher with CSV cache |
| `/home/pi/dashboard/bot_executors.py` | Live DCA executor (NOT yet updated with scoring engine — backtest only for now) |

## DCA Strategy Entry Logic (for reference)

The DCA buy signal fires when:
1. Both ROC(5) smoothed and ROC(14) smoothed cross below zero AND hit depth threshold (default -0.30)
2. Then one of them curls up (starts increasing) while ADX >= 10
3. Buy size = `capital * buy_pct% * depth_multiplier * drawdown_multiplier * defense_mode_multiplier`
4. Depth multiplier: -0.50 = 1.5x, -1.0 = 2.5x, -2.0 = 4x, -3.0 = 6x
5. After buying, must see both ROCs cross back above zero before re-arming for next buy

## Profit Tiers (default)

| Profit % | Sell fraction |
|----------|--------------|
| 3.0% | 20% of remaining |
| 5.0% | 25% |
| 7.5% | 30% |
| 10.0% | 35% |
| 15.0% | 50% |
| 20.0% | 75% |

Configurable: "flat TP" mode sells 100% at a single percentage (e.g., 3% flat).
