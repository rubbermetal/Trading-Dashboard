# Trading Dashboard

A self-hosted Flask trading dashboard running on a Raspberry Pi. Connects to Coinbase Advanced Trade via REST and WebSocket APIs to provide manual trading tools, a live market screener, and a multi-strategy automated bot engine.

---

## Overview

The dashboard has two sides:

- **Manual tools** — search assets, place spot/perpetual orders, set trailing stops, view portfolio PnL against manual entry prices.
- **Automated bots** — create and manage bots running one of several strategies. Each bot gets a virtual wallet (allocated USD) and tracks its own PnL, trade log, win rate, and fees independently.

All bot state persists to `bots.json` on disk and is reloaded automatically on restart.

---

## Architecture

```
app.py                  Flask entry point, registers all blueprints
shared.py               Coinbase REST client, global state (ACTIVE_BOTS, etc.)
bot_utils.py            Shared helpers: timeframe map, increment snapping,
                        contract multipliers, trade recording, save/load
strategies.py           Pure signal logic: QUAD, QUAD_SUPER, ORB, TRAP, GRID
bot_executors.py        Strategy executors for QUAD, ORB, TRAP (order management)
grid_engine.py          Full grid bot engine: placement, fill reconciliation,
                        risk engine, trailing stops, follow logic, circuit breaker
bot_ws.py               WebSocket daemon: real-time fill processing and
                        millisecond trailing stop execution
routes/
  bot_manager.py        Bot CRUD API, 15-second master loop, grid preview endpoint
  trading.py            Manual order entry, symbol search, trailing stops
  portfolio.py          Portfolio summary and PnL vs manual entries
  market_data.py        Candle/indicator data for the UI charts
  screener.py           Background scanner (RSI, MACD) across a watchlist
  scanner.py            Additional scanning routes
check_adx.py            CLI utility to check price and ADX for any pair
```

---

## Bot Strategies

Each bot runs in its own thread, evaluated every 15 seconds.

### QUAD / QUAD_SUPER
Multi-timeframe stochastic momentum strategies using four stochastic periods: Macro (60), Medium (40), Fast (14), and Trigger (9).

- **QUAD**: Requires macro and medium stochastics both above 80 (strong uptrend), price touching the 20 EMA, and the trigger stochastic dipping to oversold (≤20). Exits when the trigger stochastic crosses back above 80.
- **QUAD_SUPER**: Waits for a full four-stochastic capitulation flush (all four below 20), then looks for bullish divergence — price making a lower low while stochastics hold above 20 and curl up with a reversal candle. Higher conviction entry with the same exit logic.

Default timeframe: 15m.

### ORB (Opening Range Breakout)
Breakout strategy using 5-minute candles. Enters long or short on breakout of the opening range, then manages the position with a trailing stop that activates at +3% profit and trails by 1.5%.

Default timeframe: 5m.

### TRAP
Consolidation breakout strategy. Looks for low-volatility compression followed by a directional breakout.

Default timeframe: 15m.

### GRID
The most complex strategy. Places a ladder of buy and sell limit orders across a configured price range. Profits from mean-reversion oscillations — every filled buy flips to a sell one step above, and every filled sell flips to a buy one step below.

Default timeframe: 1h (for macro ADX/direction reads).

---

## Grid Bot — Detailed

### Deployment
On launch the bot places N buy orders evenly spaced between a lower and upper price, each funded by `chunk_size` USD. The grid preview endpoint calculates the full loss envelope before deployment.

### Execution — Dual-Path Architecture
Grid execution runs on two parallel paths:

**WebSocket path (`bot_ws.py`)** — primary
- Subscribes to the Coinbase `user` channel for real-time fill events and the `ticker` channel for live price ticks.
- Processes fills at network speed: immediately flips a filled buy to a sell one step above (or a filled sell to a buy one step below).
- Evaluates trailing stops on every price tick for millisecond-latency execution.

**REST path (`grid_engine.py` → `grid_check_fills`)** — fallback/reconciliation
- Runs every 15 seconds, polls the historical fills API, and reconciles any fills the WS may have missed.
- Acts as the definitive source of truth for order state.

### Order Spacing Guard
Every order placement path (WS flip, REST reconciliation, halt exit sells, redeployment, follow) checks that the new order price is at least 40% of step_size away from all existing orders (`has_order_nearby` / `find_safe_price`). If no safe price is found within 5 nudges:
- Sell-side: falls back to trail-only exit, inventory held.
- Buy-side: level is queued in `cancelled_buy_levels` for redeployment when conditions improve.

### Grid Follow
When price moves more than one full step beyond the grid edge, the bot recycles the most stale eligible order to a better position:
- Max 1 recycle per cycle. Orders placed less than 120 seconds ago are never recycled.
- New prices are grid-aligned (step multiples from the existing edge), not market-relative, preventing anchor drift between the REST and WS paths.
- Recycling only happens if the new position is meaningfully better (≥ 0.5 step improvement).
- If placement fails after a cancel, the cancelled level is queued for redeployment.

### Risk Engine (runs every 15 seconds)

**Direction** — 5-period SMA slope classifies market as RISING, FALLING, or CHOPPY.

**Depth Score** — equals the number of active per-fill trailing stop positions (open inventory levels). Each filled buy increments depth; each trailing stop exit decrements it.

**Depth Escalation**
- Depth ≥ 4 (Elevated): cancels all open buy orders.
- Depth ≥ 6 (Critical): cancels all open buy orders and tightens trailing stop multiplier to 0.75×.

**Halt Modes** — triggered by ADX ≥ 25 or depth escalation:
- `FAVORABLE`: rising trend, widened trail (1.5× multiplier). All buy orders cancelled as a precaution.
- `NEUTRAL`: sideways, standard trail.
- `ADVERSE`: falling trend, tightened trail (0.75× multiplier).
- `CRITICAL`: depth ≥ 6, tightest trail.

**Dormant Bypass** — if a halt is triggered but depth == 0 and no inventory is held, the bot cancels all orders and returns to DORMANT rather than applying halt math to an empty wallet.

**Circuit Breaker** — if total unrealized loss exceeds the configured threshold (~6% default), the bot executes a market sell on all inventory and halts immediately.

**Trailing Stop System**
- Each buy fill creates a per-fill trailing stop record tracking: fill price, quantity, high-water mark, and effective trail distance.
- Trail distance is tiered by level index (deeper fills get tighter trails: 1.0×, 2.0×, or 3.0× step_size).
- Halt mode modifiers are applied by `adjust_trail_multipliers` every 15 seconds.
- WS path checks high-water marks and fires market sells on every tick. REST path provides a 15-second fallback via `check_trailing_stops`.

**Buy Redeployment** — when `cancelled_buy_levels` is non-empty and conditions are favorable (RISING or CHOPPY, depth ≤ 3), the risk engine redeploys up to 3 levels per cycle using the spacing guard.

### PnL Calculation
Grid bot PnL is calculated as:
```
PnL = (idle_cash + inventory × live_price × contract_multiplier) − allocated_usd
```
This correctly accounts for multi-level fractional inventory across fills, unlike a simple position-side calculation.

---

## Screener

Background thread scans 8 pairs every cycle using 150 days of daily candles:
`BTC-USD, ETH-USD, SOL-USD, DOGE-USD, AVAX-USD, LINK-USD, ADA-USD, SHIB-USD`

Live price is injected into the current daily candle so RSI and MACD reflect real-time values.

---

## Setup

### Requirements
- Python 3.13+
- Coinbase Advanced Trade API key (CDP key format)

### Install
```bash
git clone https://github.com/rubbermetal/Trading-Dashboard
cd Trading-Dashboard
python3 -m venv venv
source venv/bin/activate
pip install flask coinbase-advanced-py pandas pandas_ta python-dotenv
```

### Configure
Create a `.env` file in the project root:
```
COINBASE_API_KEY_NAME=organizations/your-org-id/apiKeys/your-key-id
COINBASE_API_PRIVATE_KEY="-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----\n"
```

### Run
```bash
source venv/bin/activate
python app.py
```

Dashboard available at `http://<pi-ip>:5000`

### Run on startup (systemd)
```ini
[Unit]
Description=Trading Dashboard
After=network.target

[Service]
WorkingDirectory=/home/pi/dashboard
ExecStart=/home/pi/dashboard/venv/bin/python3 app.py
Restart=always
User=pi

[Install]
WantedBy=multi-user.target
```

---

## Utilities

**`check_adx.py`** — quick CLI check of price and 5-minute ADX for any pair:
```bash
/home/pi/dashboard/venv/bin/python3 check_adx.py ETH-USD
/home/pi/dashboard/venv/bin/python3 check_adx.py BTC-USD
```

---

## Files Excluded from Git

| File | Reason |
|------|--------|
| `.env` | API credentials |
| `bots.json` | Live bot state — only a fresh file should exist on a new clone |
| `check_adx.py` | Local utility script |
