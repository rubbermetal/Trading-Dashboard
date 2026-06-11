# Repository Review — Code, Technique, and Strategy

**Date:** 2026-06-11
**Scope:** Full repository — strategy logic (`strategies.py` + specs), backtest engine, candle data pipeline, live executors (`bot_executors.py`, `bot_ws.py`, `grid_engine.py`), Flask routes, validators, tests.
**Method:** Five parallel deep reviews (strategy logic, backtest validity, live execution, grid risk engine, web/infra), headline findings independently verified against source. Runnable tests pass (33/33 in `test_validators.py` + `test_bot_utils.py`); `test_strategies.py` cannot run anywhere `pandas-ta==0.4.71b0` won't install (requires Python ≥ 3.12).

---

## Executive summary

The architecture is good (specs, conformance report, persistence, WS + REST redundancy), but the system is **not safe to run live** in its current state. There are bugs that abandon real positions while the bot believes it is flat, paper bots that place real orders, two strategies that crash on every cycle, an unauthenticated API bound to all interfaces that can place real trades, and a backtest stack whose fills, fees, and underlying candle data are all biased optimistic — so live results will systematically undershoot backtests.

**Top 10, in order of financial danger:**

1. **NPR stop-loss exits silently fail, position abandoned** (EXEC-C1)
2. **Paper bots place real orders** on the WS path and DCA circuit breaker (EXEC-C4)
3. **No authentication on any endpoint, server on 0.0.0.0** — remote account drain (WEB-C1)
4. **Market-order rejections never checked** — failed exits flip state to FLAT anyway (EXEC-C2)
5. **`/api/trade` CLOSE always sells** — closing a short doubles it (WEB-C2)
6. **Grid dynamic bots: `NameError` on every BUY fill**, fill permanently lost (GRID-C1)
7. **GTFO fills processed by no one** — phantom inventory, repeated sells (GRID-C2)
8. **Crisis buy-cancel removes orders from tracking without cancelling on exchange** (GRID-C3)
9. **VWAP_MR / SQUEEZE executors crash every cycle**; if naively fixed, they run on time-reversed data (EXEC-H1 / STRAT-C1)
10. **Candle DB stores partial in-progress candles as closed and splices Binance USDT into Coinbase USD** (DATA-19/21)

---

## CRITICAL — must fix before any live deployment

### EXEC-C1. NPR stop exits use crossing post-only limits, then unconditionally mark FLAT
`bot_executors.py:2453-2477`, `bot_ws.py:229-253` (duplicated in dead file `npr_executor_code.py:100-120`).
A LONG stop exit sells 1 tick *below* market with `post_only=True`. A post-only order that would cross is **rejected** by Coinbase (HTTP 200, `success: false`, no exception). The response is never checked; the code then sets `asset_held = 0`, `position_side = 'FLAT'`, `npr_state = 'SCANNING'` and credits `current_usd` with phantom proceeds. When the event stop fires, the position is very likely still open with no owner and no stop.

### EXEC-C2. Market-order responses never checked for `success`
QUAD/ORB/TRAP entries and exits and all WS exit paths: `bot_executors.py:152-154, 266, 328, 436-438, 660`; `bot_ws.py:102, 156, 200`. Coinbase returns rejections as `success: false` without raising. A rejected exit sell leaves a real position open while the bot records the trade and goes FLAT. (Limit entries for NPR/MOMENTUM/DCA *do* check `success` — market orders never do.)

### EXEC-C3. Double-sell race between WS ticker thread and REST executor thread
Identical stop logic runs concurrently in `bot_ws.py:133-257` and `bot_executors.py:790-836, 2438-2487, 2780-2806` with no lock or in-flight flag. After one thread submits a market sell it blocks in `poll_market_fill` (1–3 s) *before* flipping to FLAT; a tick in that window makes the other thread submit a second full-size sell. On spot it errors silently; on `-PERP`/`-CDE` it opens an unintended short. `ACTIVE_BOTS` is mutated from ≥3 threads with no locking except inside `save_bots()`.

### EXEC-C4. Paper bots place real orders on two paths
- `bot_ws.py` `process_price_tick` has **no `bot.get('paper')` check anywhere** (verified: zero occurrences of "paper" in the file). Paper MOMENTUM/NPR/VWAP/SQUEEZE/GRID bots are WS-subscribed (`bot_ws.py:45-47`) and their trailing stops fire **real** `market_order_sell` calls (`bot_ws.py:156, 200, 236`).
- Legacy DCA circuit breaker (`bot_executors.py:1444-1455`) has no paper guard: real market sell at −25% drawdown for paper bots.

### GRID-C1. `NameError` on every BUY fill for dynamic grid bots — fill permanently lost
`grid_engine.py:272-274`: `regime = risk.get('regime', ...)` runs before `risk` is first assigned (line ~289). The fill's order IDs were already added to `_processed_fill_oids` (lines 256-259) before the crash, so the fill is skipped on the next cycle: `asset_held`/`current_usd` never update, no sell placed, no trail activated. Every REST-detected BUY fill on a `dynamic: True` bot corrupts inventory accounting.

### GRID-C2. GTFO exit fills are processed by no one — phantom inventory, repeated sells
`grid_engine.py:1227, 1255-1258, 1279, 1301-1316`; `bot_ws.py:311-313`. `enter_gtfo_mode` clears `active_grids` and places a bare limit sell tracked only as `risk['gtfo_order_id']`. `grid_check_fills` only matches `active_grids` entries, and the WS handler explicitly skips fills while GTFO is active. When the GTFO sell fills, no accounting happens; the completion check never passes; the resync branch then cancels-all and places a **new** sell for quantity the account no longer holds — repeatedly, until exchange rejection. The market-sell branch (`:1251`) has the same hole.

### GRID-C3. Crisis score ≥ 40 removes buy orders from tracking without cancelling them on the exchange
`grid_engine.py:1530-1536`: buy grids are removed from `active_grids` with **no `cancel_order_safe` call**. The limit buys remain live on Coinbase and — since this fires in a falling market — will fill. Neither fill path can match them: the bot buys coins it doesn't know it owns, with no trail, no circuit-breaker coverage, no PnL record. Direct violation of the spec's anti-race rule (Grid risk engine spec.md §"Secure Cancellations", line 269).

### WEB-C1. No authentication anywhere; server bound to 0.0.0.0
`app.py:51` `app.run(host='0.0.0.0', port=5000)`; zero auth on any route. Anyone on the network can place market orders (`/api/trade`), start bots with real capital (`/api/bots/start`), force a rebalance (`/api/execute_rebalance`), open leveraged manual positions (`/api/manual/enter`), and cancel protective stops (`/api/protections/cancel`). Minimum fix: bind to 127.0.0.1; correct fix: auth in front of all `/api/*`.

### WEB-C2. `/api/trade` CLOSE always sells — closing a short doubles it
`routes/trading.py:420-423`: the computed `side` variable is never used; `market_order_sell` is called unconditionally. Closing a derivative SHORT (frontend sends `side: 'SELL', type: 'DERIVATIVE'`) sells more contracts instead of buying back.

### STRAT-C1 / EXEC-H1. VWAP_MR and SQUEEZE executors crash every cycle; data would be reversed if naively fixed
`bot_executors.py:2728, 2835`:
```python
df = pd.DataFrame(parsed).sort_values(by=pd.RangeIndex(len(parsed))).reset_index(drop=True)
```
`sort_values(by=pd.RangeIndex(...))` raises `KeyError` — both strategies are dead and any existing position loses its REST stop fallback. Worse, `parsed` drops the `start` field and Coinbase returns candles **newest-first**, so removing the sort would compute cumulative VWAP backward in time. Fix requires extracting `start` and sorting ascending like every other executor.

### STRAT-C2. `TestQuadRotation` asserts the wrong return arity — tests can never pass
`tests/test_strategies.py:20, 25, 31` unpack 2 values; `calculate_quad_rotation` returns a 3-tuple on every path (`strategies.py:23` etc.). All three tests raise `ValueError`. (Masked in CI-less repos because `pandas-ta==0.4.71b0` requires Python ≥ 3.12 — also a portability bug in `requirements.txt`.)

---

## HIGH

### Execution / state
- **EXEC-H2. Retry-after-timeout can double-buy.** Fill detection scans only the last 10 FILLED orders (`bot_executors.py:849-851, 1493-1587, 2522-2588`); on a busy pair the fill scrolls out of the window, the timeout path cancels nothing (filled order isn't OPEN), then places a **new** order — capital committed twice, old position orphaned. Cancelled partial fills are likewise untracked.
- **EXEC-H3. Exit accounting resets the wallet to initial capital.** `bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee` (`bot_executors.py:274, 304, 669, 695, 823`; `bot_ws.py:165`) discards all prior realized PnL each full exit (and double-counts partial-exit profits). `current_usd` drives sizing, so sizing and drawdown are wrong after the first trade. `_quad_exit` uses a different (additive) convention — inconsistent.
- **EXEC-H4. Spot market entries assume estimated qty/price** instead of polling the fill (ORB `bot_executors.py:154-159`, QUAD `:438-445`); exits send unsnapped sizes (`:211, 266, 296, 328, 613, 660, 686`) and QUAD sends unrounded `quote_size` floats — LOT_SIZE/increment rejections that compound EXEC-C2.
- **EXEC-H5. `save_bots()` is a non-atomic truncate-and-write** (`bot_utils.py:154-157`) of a dict concurrently mutated by other threads; a `RuntimeError` mid-dump corrupts `bots.json`, and `load_bots()` silently starts empty while real positions/orders remain on the exchange. Use temp-file + `os.replace` (already done correctly for trails in `routes/trading.py:18-37`).
- **EXEC-H6. No exchange-side protective orders exist at all.** Every stop in the system is software-evaluated (15 s REST loop or WS ticks). Process death, hang, or the up-to-60 s WS reconnect backoff (`bot_ws.py:469-473`) during a dump leaves positions with no stop. The grid spec's optional hardware stop (§6) is unimplemented.

### Grid engine
- **GRID-H1. Unverified cancellations** in `manage_runner_exits`, `check_trailing_stops`, `convert_to_runners`, and the WS trail path (`grid_engine.py:848-851, 902-907, 1373-1377`; `bot_ws.py:115-116`): `cancel_order_safe(g)` return ignored before `active_grids.remove(g)` — the cancel/fill race can double-sell real inventory. (`grid_emergency_halt` does it correctly.)
- **GRID-H2. Kelly sizing has no aggregate cap.** Dynamic override applies one Kelly-sized order to *every* buy level (`grid_engine.py:1857-1860, 1107`) — e.g. 15 levels × 10% cap = 150% of allocation if all fill; redeployment (`:763-776`) also ignores remaining `current_usd`. And `compute_kelly_size` still orders `min_order_usd` when Kelly says edge ≤ 0.
- **GRID-H3. Circuit breaker wipes all protection state even when the liquidation sell fails** (`grid_engine.py:806-827`): on API error the trails are erased anyway, `if not trails: return False` means the CB can never re-fire, and the position rides the crash unprotected.
- **GRID-H4. BOTH-mode sell-side inventory has no trails and is invisible to the circuit breaker** (`grid_engine.py:1877-1890` vs CB loss calc `:787-791`), violating the spec's "ALL held inventory" rule (§6).
- **GRID-H5. Flip economics can be negative by construction.** Dynamic step floor 0.3% (`:1465`) and static default 0.6% (`:1790`) vs Coinbase base-tier maker fees ~0.4%/side (~0.8% round trip). The bot can't see it because fees are modeled as exit-side-only 0.25% (`bot_utils.py:181`) and fills deduct no entry fee. Enforce `step ≥ 2×fee + margin`.

### Strategy / methodology
- **STRAT-H1. Live lookahead/repaint: every strategy evaluates the current unclosed candle.** All executors fetch with `end_ts = now` (`bot_executors.py:76, 2387, 2708`) and strategies key off `df.iloc[-1]` (`strategies.py:57, 437, 887, 1020, 1097`). NPR's own spec mandates "last COMPLETED bar". The backtester feeds only closed bars — so live trades fire on intra-bar patterns that often don't complete, while backtests overstate performance. Drop the forming bar (`df.iloc[:-1]`) in live executors.
- **STRAT-H2. VWAP_MR has no daily session reset** (spec says "reset daily"): `strategies.py:1003-1005` cumsums over whatever 300-bar fetch window arrived, so the VWAP anchor and σ-bands drift on every 15 s cycle — entry levels are non-stationary artifacts of the polling window. Same pathology in ORB's VWAP filter (`strategies.py:253`).
- **STRAT-H3. VWAP_MR has no trend/regime filter** (`strategies.py:1034`): in a sustained downtrend price sits below −1σ with RSI < 35 for hours, and the executor re-enters at 95% of capital whenever flat (`bot_executors.py:2736`) — the canonical 24/7-crypto mean-reversion knife-catcher. Needs an ADX/trend gate and a max-deviation cutoff.
- **STRAT-H4. ORB spec and implementation describe two different strategies** (range window 00:00–01:00 UTC vs code default hour 14; midpoint stop vs `max(range_width, 1.5×ATR)`; +3%/1.5% trail vs 1.5×ATR; 2-tuple vs 3-tuple). The conformance report never audits ORB.

### Backtest validity
- **BT-H1. Same-bar lookahead in the grid simulator:** ADX halt, regime, crisis score, buy-cancels, and Kelly sizing are computed from the bar's **close**, then applied to the 1m sub-bar walk *of that same bar* (`backtest_engine.py:1096-1293` vs `:1359`). GTFO fills use the high of the bar whose close triggered it (`:1239-1252, 1149`).
- **BT-H2. Stop fills ignore gap-through:** exits always booked exactly at the stop price (`:1736-1738`, grid `:1371-1372`, DCA `:651-653`) — optimistic on every losing exit.
- **BT-H3. Sharpe/Sortino annualization wrong for any timeframe not in the lookup** (`:2104-2107`): the API only produces minute-strings (`'60m'`, `'1440m'`…) which never match `'1h'/'1d'` keys, falling back to the 15m factor — Sharpe inflated 2× for hourly, ~6.3× for daily tests.
- **DATA-18. `candle_db.query()` silently drops every aggregated bar missing even one 1m candle** (`candle_db.py:180-184`). Coinbase omits zero-trade minutes, so altcoin series lose large fractions of bars (daily bars are nearly impossible), producing gappy series iterated as contiguous.
- **DATA-19. Partial in-progress 1m candles stored as closed and never corrected** (`candle_db.py:64-93` + `INSERT OR IGNORE`; updater resumes after them, `scripts/update_candle_db.py:47`) — ~1 fossilized mid-minute snapshot per updater run feeding every backtest.
- **DATA-21. Binance USDT candles spliced into Coinbase USD series** (`scripts/backfill_from_binance.py:30-33, 126-129`), including interior gaps — USDT/USD basis and venue differences create artificial jumps, fake volatility, and fake grid/DCA fills at every seam. Splices should at minimum be tagged by source.

### Web
- **WEB-H1. `validate_trade` imported but never called** (`routes/trading.py:8` vs `:412-425`); and it validates a contract (`action`/`price`/`'MAKER'`) that doesn't match what the route/frontend actually send (`side`/`limit_price`/`'MAKER_LIMIT'`) — unwireable as-is.
- **WEB-H2. `/api/trail` consumes `d['pct']` which is never validated** (`validate_trail` checks different fields): `pct=0` fires instantly; `pct>100` on a SELL trail → negative trigger that never fires — silently unprotected position.
- **WEB-H3. Bot stop/restart race spawns duplicate engines** (`routes/bot_manager.py:476-505`): `run_bot` re-checks status only every 15 s; stop+restart within the window leaves two threads running the same bot placing double real orders. Restart check-and-set is also unlocked.
- **WEB-H4. Under a real WSGI server the manual-stop evaluator never starts** (`app.py:48-50` guards it with `__main__`) while every other engine thread starts at import — so gunicorn gives you unprotected manual positions *and* one set of duplicate bot/watcher threads per worker. The threading model only works as the Flask dev server, single process.
- **WEB-H5. Cancelling a triggered protection orphans the live exchange order** (`routes/trading.py:593-606`): `triggered_oid` is never checked; the GTC maker exit stays open on Coinbase untracked.

---

## MEDIUM

**Grid/DCA technique**
- Quarantine gate inverted: `if quarantine_until > now and adx >= 20` lets any ADX dip below 20 bypass the time quarantine entirely (`grid_engine.py:1500`) — redeploys straight back into the decline the crisis engine doc warns about.
- Buy redeployment runs while direction = FALLING for depth ≤ 3 (`:754-756`), contradicting spec §5 (RISING/CHOPPY only).
- Crisis hard-liquidation sorts trails ascending and exits the *lowest* entries, keeping the deepest-underwater 25% (`:1507-1519`) — maximizes remaining drawdown; sort should be descending.
- Recovery-velocity window hardcoded 300 s vs spec "10 candles" (= 50 min on 5m) (`:706`) — runner exits and trail-widening effectively disabled. The backtest mirrors the same flaw: 120 s/300 s windows collapse to ≤1 bar on 5m+ timeframes (`backtest_engine.py:1136, 1074`).
- Flip PnL reconstructs entry as `sell_price − current_step` (`grid_engine.py:328-329`; `bot_ws.py:358-360`) — wrong whenever dynamic sells (1.5–2 steps), spacing nudges, or step recomputation applied.
- Level-index corruption: flipped buys default to the middle tier; redeployed buys get the *top* tier (widest trail) near the bottom of a fall (`:774, 291, 311`) — breaks the deploy-time max-loss envelope.
- `_processed_fill_oids`: unlocked check-then-act between WS and REST threads, plus `clear()` at 500 entries wipes seconds-old dedupe history (`grid_engine.py:250-260`; `bot_ws.py:294-295`) — double-processing window, and Coinbase replays order snapshots on reconnect.

**Strategy logic**
- ORB minimum-candle check hardcodes 5m bars (`strategies.py:276`) — permanently dead on any other timeframe.
- ORB range-quality filter compares a 12-bar range to a 1-bar ATR with a 2.0× cap (`:353`) — statistically rejects most normal opening ranges (~√12 ≈ 3.5× expected).
- TRAP redefines R after breakeven (`:462`): R falls from ≥2×ATR to raw ATR, so the 4.0R Target-2 fires roughly twice as early as documented. Same in the SHORT branch (`:489`).
- TRAP's flatness gate (≤0.3% SMA20 move over 20 bars, `:545-548`) and angle gate (>0.577×ATR over 5 bars, `:599-604`) are nearly contradictory on 15m crypto — signals vanishingly rare.
- NPR check-scoring is decorative: score is always 2.5 or 3.0 (`:944-951`), so the `< 2.0` gate is dead code and the spec §6 conviction filter doesn't exist.
- DCA fire condition never re-checks depth after ARM (`:828-850`) — can buy at the top of the bounce at scout size, defeating "heavier buys at deeper dips".

**Backtest / accounting**
- DCA sub-cycle checks TP before stop on the same bar (`backtest_engine.py:633-651`) — optimistic intrabar ordering (main loop does it correctly).
- Trailing stops ratchet on the same bar's high before testing its low (`:1680-1733, 1367-1371`); severe in the no-1m fallback where a whole TF bar's high lifts the stop its own low then hits.
- Entries fill at the signal bar's close with zero slippage/spread modeled anywhere; live bots act after close — next-bar-open + slippage is the honest fill.
- Maker fee (0.06%) charged on market-style exits (crisis cuts, circuit breaker, GTFO, trailing stops, end-of-data closes — `:536, 1153, 1181, 1217, 1243, 1302, 1373, 836, 848, 1521`) that are taker (0.2%+) in reality — understates costs precisely on loss-heavy exits.
- Limit fills assumed on touch (`:580-588, 1446, 1784`) — optimistic bound on every grid flip and tier exit.
- DCA tier-ladder re-arm is unreachable dead code (`:572-574`: reset condition nested inside `profit > 0` guard).
- No out-of-sample / walk-forward tooling anywhere — every UI-exposed parameter is tuned in-sample by construction.
- Win-rate counts per-fill records (each grid flip/tier slice = one "trade") — a losing position cut after five winning slices reports 83% win rate.
- `data_fetcher.py:121` `drop_duplicates(subset='start')` keeps the stale cached (possibly partial) candle over the fresh one — should be `keep='last'`. Interior cache gaps never detected (`:107-116`), same blindness in `candle_db.get_backtest_candles` (`candle_db.py:242-264`).

**Execution / stats**
- Paper trades pollute permanent stats and Kelly sizing for **live** DCA buys (`record_trade` called unconditionally in ORB/QUAD/MOMENTUM/NPR paper exits; `paper_fill_sell` explicitly avoids this — the others violate it).
- Failed trail exit retries a full market sell on every tick even if the first sell filled (`bot_ws.py:119-121`); DCA circuit breaker re-attempts every 15 s on failure (`bot_executors.py:1462-1464`).
- `update_permanent_stats` read outside the lock (`bot_utils.py:232-258`) — concurrent trades lose stats.
- `poll_market_fill` scans last-10 FILLED with sometimes 1 s total budget; on miss, callers record trigger price and *requested* quantity — PnL/fee drift.

**Web**
- `/api/trade` reports success on rejected orders (`routes/trading.py:427-453` — response ignored).
- `/api/twap`, `/api/sniper`, `/api/scaled`, `/api/tpsl`: no validation (`duration_min=0` → slice every watcher tick; negative `price_from` accepted); TWAP counts slices filled without checking placement (`:250-265`); sniper marks TRIGGERED on rejected orders (`:297-304`).
- Brackets/TWAPs/snipers are memory-only (`shared.py:13-15`) — restart silently drops TP/SL monitoring on real positions (trails *are* persisted).
- `validate_bracket` requires both TP and SL while the route treats them as optional, and no side-relative sanity check (SL ≥ entry for a long accepted).
- Rebalance ignores order results — buys proceed even if every sell failed (`routes/portfolio.py:47-53, 316-324`); failed price fetch silently prices an asset at 0 and skews totals (`:91, 121`).
- ntfy topic exposed via unauthenticated GET and server redirectable via POST (`routes/trading.py:782-792`).
- Rate limiter keyed on function name, not client (`validators.py:12-24`) — one global bucket; 10 junk requests/min DoSes the legit user's trade button.
- `_BACKTEST_LOCK` defined but never used; check-then-act lets two jobs run concurrently (`routes/backtest.py:25, 121-125`).

---

## LOW

- `requirements.txt` pins `pandas-ta==0.4.71b0` (requires Python ≥ 3.12) with no documented Python version — tests/install fail on 3.11.
- `npr_executor_code.py` is dead code with no imports — divergence hazard; delete it.
- `calculate_advanced_grid` (`strategies.py:188`) is dead code that only the tests exercise — tests validate a function the product doesn't use.
- `notify_bot_exit` called with 4 args, defined with 5 (`notifier.py:83` vs `bot_executors.py:2776, 2804, 2883, 2911`) — `TypeError` swallowed and mislogged as "sell failed".
- GTFO limit price uses raw float math instead of `snap_to_increment` (`grid_engine.py:1241, 1257`) — off-increment rejections.
- Deploy divides by `allocated_usd` in a log string — crashes deploy for zero-allocation bots (`:1863`).
- `init_risk_state` wholesale-replaces the risk dict at deploy, erasing regime/quarantine/loss-history set earlier the same cycle (`:634-640, 1862`).
- Paper BOTH-mode grids sell phantom inventory (pre-seeded SELL levels have no `qty`; `:981-987`) — inflated paper PnL.
- Sortino uses std of losses instead of downside deviation (`backtest_engine.py:2169-2171`); `exposure_pct` reads ~100% always (`:2177`); `avg_trade_bars` is actually seconds (`:2189-2192`); TARGET_1 partials omit their entry-fee share in per-trade stats (`:1849-1851`).
- `screener_config.json` both tracked and gitignored — ignore rules don't apply to tracked files.
- Unhandled `float()` on user input → 500s (`routes/bot_manager.py:319-321, 558, 572, 618`; `routes/trading.py:887`); pervasive bare `except: pass` in portfolio valuation.
- Unreachable backtest-timeout branch with contradictory message (`routes/backtest.py:42-54`).
- NPR rejects true dojis (`c_body > 0` required, `strategies.py:909`) though the spec allows them.
- Doc drift: stale DCA/SQUEEZE docstrings (`strategies.py:751, 1063`); QUAD spec contradicts itself on Holy Grail confidence (0.80 vs 0.90); conformance report simultaneously closes and keeps open the MOMENTUM SMA(2)-vs-SMA(5) deviation.

---

## Verified clean

- No secrets in git history; keys via `.env` + dotenv with a permission check; `bots.json`/`stats.json`/`.env` gitignored; nothing key-like logged.
- SQL fully parameterized; trail persistence uses correct atomic write (`mkstemp` + `os.replace`); Flask debug off; CORS not enabled.
- Indicator math (RSI/ATR/ADX Wilder smoothing via pandas-ta), QUAD turn detection and Holy Grail formulas, NPR zone table and eclipse algebra, TRAP wider-stop selection — all match their specs.
- Backtest main loop checks SL before TP per sub-bar (conservative), `_compute_trend_ema` uses only prior periods, max-drawdown and cash/equity accounting are correct, 24/7 bar counts in `TF_BARS_PER_YEAR` are right for crypto.
- `requirements.txt` versions exactly pinned; UTC handled consistently in the backtest route.

---

## Recommended order of work

1. **Safety gates (hours):** bind to 127.0.0.1 / add auth; add `paper` guard to `bot_ws.py` and DCA circuit breaker; check `success` on every order response; fix the `/api/trade` CLOSE branch; fix `grid_engine.py:273` NameError.
2. **Position integrity (days):** market-order fallback when post-only exits are rejected; GTFO fill accounting; verify cancels before dropping tracking (GRID-C3/H1); single-flight lock around exits shared by WS+REST; atomic `save_bots()`; fix `current_usd` reset accounting.
3. **Backtest honesty (days):** next-bar-open fills + slippage; taker fees on market exits; gap-aware stop fills; fix Sharpe TF lookup; fix `candle_db` partial-candle and bar-dropping behavior; quarantine or tag Binance-backfilled rows.
4. **Strategy hygiene (ongoing):** drop the forming candle in live executors; anchor VWAPs to UTC day; add a trend gate + deviation cutoff to VWAP_MR before resurrecting it; reconcile ORB/TRAP/NPR specs with code; add walk-forward splits before trusting any tuned parameter.
