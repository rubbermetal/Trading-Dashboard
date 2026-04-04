# VWAP Mean Reversion (VWAP_MR) Strategy Spec

## Overview
Intraday mean-reversion strategy that buys when price dips below the daily VWAP lower band with RSI confirmation, and exits at VWAP touch or via ATR trailing stop.

## Timeframe
Default: 5m

## Indicators
- **VWAP**: Cumulative (typical_price * volume) / cumulative volume, reset daily
- **VWAP Standard Deviation Bands**: +/- 1 std dev from VWAP
- **RSI(14)**: Relative Strength Index
- **ATR(14)**: Average True Range (for trailing stop)

## Entry Conditions (ALL must be true)
1. Price < VWAP - 1 standard deviation (lower band)
2. RSI(14) < 35

## Exit Conditions (any triggers exit)
1. Price >= VWAP (mean reversion complete)
2. Price <= high_water_mark - 1.5x ATR (trailing stop)

## Execution
- Entry: Market buy (95% of available capital)
- Exit: Market sell on signal or trailing stop
- WS primary path for trailing stop (ms latency)
- REST fallback on 15s cycle checks VWAP touch

## Risk Management
- ATR trailing stop: 1.5x entry ATR from high water mark
- Single position per bot (no pyramiding)

## Screener
- SIGNAL: Price below VWAP -1σ AND RSI < 35
- SETUP: Price below VWAP -0.5σ AND RSI < 40
- AVOID: Price above VWAP +1σ (overextended)
- NEUTRAL: Otherwise
