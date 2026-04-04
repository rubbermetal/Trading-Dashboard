# Bollinger Squeeze Breakout (SQUEEZE) Strategy Spec

## Overview
Volatility compression breakout strategy. Detects when Bollinger Bands narrow inside Keltner Channels (the "squeeze"), then enters on the release when momentum confirms direction.

## Timeframe
Default: 15m

## Indicators
- **Bollinger Bands**: BB(20, 2.0) — 20-period SMA with 2 standard deviation bands
- **Keltner Channels**: KC(20, 1.5) — 20-period EMA with 1.5x ATR bands
- **Momentum**: MOM(12) — 12-period momentum (close - close[12])
- **ATR(14)**: Average True Range (for trailing stop)

## Squeeze Detection
A squeeze is active when: BB_lower > KC_lower AND BB_upper < KC_upper
(Bollinger Bands are contained within Keltner Channels)

## Entry Conditions
- **LONG**: Previous bar was in squeeze AND current bar is NOT in squeeze (release) AND momentum > 0 and rising
- **SHORT**: Previous bar was in squeeze AND current bar is NOT in squeeze (release) AND momentum < 0 and falling

## Exit Conditions (any triggers exit)
1. Momentum reversal: momentum crosses zero (positive to negative for longs)
2. Price <= high_water_mark - 2.0x ATR (trailing stop)

## Execution
- Entry: Market buy on squeeze release
- Exit: Market sell on momentum reversal or trailing stop
- WS primary path for trailing stop (ms latency)
- REST fallback on 15s cycle checks momentum

## Risk Management
- ATR trailing stop: 2.0x entry ATR from high water mark
- Single position per bot (no pyramiding)

## Screener
- SIGNAL: Squeeze just released with directional momentum
- SETUP: Currently in squeeze (bands compressed)
- NEUTRAL: No squeeze active
