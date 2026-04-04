"""Shared fixtures for dashboard tests."""
import sys
import os
import pytest
import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture
def trending_up_df():
    """300-bar DataFrame with a clear uptrend — good for QUAD/MOMENTUM buy signals."""
    n = 300
    np.random.seed(42)
    base = 60000 + np.cumsum(np.random.normal(5, 50, n))  # uptrend with noise
    noise = np.random.normal(0, 30, n)
    highs = base + abs(noise) + 50
    lows = base - abs(noise) - 50
    opens = base + np.random.normal(0, 20, n)
    closes = base + np.random.normal(5, 20, n)  # slight bullish bias
    volume = np.random.uniform(100, 1000, n)
    return pd.DataFrame({
        'open': opens, 'high': highs, 'low': lows,
        'close': closes, 'volume': volume
    })


@pytest.fixture
def ranging_df():
    """300-bar DataFrame with a flat/ranging market — good for GRID."""
    n = 300
    np.random.seed(123)
    base = 65000 + np.random.normal(0, 100, n)  # flat with noise
    highs = base + abs(np.random.normal(0, 50, n)) + 30
    lows = base - abs(np.random.normal(0, 50, n)) - 30
    opens = base + np.random.normal(0, 15, n)
    closes = base + np.random.normal(0, 15, n)
    volume = np.random.uniform(100, 500, n)
    return pd.DataFrame({
        'open': opens, 'high': highs, 'low': lows,
        'close': closes, 'volume': volume
    })


@pytest.fixture
def small_df():
    """50-bar DataFrame — too small for most strategies."""
    n = 50
    base = np.linspace(60000, 61000, n)
    return pd.DataFrame({
        'open': base, 'high': base + 50, 'low': base - 50,
        'close': base + 10, 'volume': np.full(n, 500.0)
    })


@pytest.fixture
def sample_bot():
    """A sample DCA bot dict."""
    return {
        'pair': 'BTC-USD',
        'strategy': 'DCA',
        'status': 'RUNNING',
        'allocated_usd': 100.0,
        'current_usd': 75.0,
        'asset_held': 0.001,
        'position_side': 'LONG',
        'avg_entry': 65000.0,
        'total_cost': 65.0,
        'timeframe': '5m',
        'settings': {},
        'stats': {
            'total_trades': 5,
            'winning_trades': 3,
            'losing_trades': 2,
            'stopped_out': 0,
            'total_pnl': 12.50,
            'largest_win': 8.0,
            'largest_loss': -3.0,
            'total_fees_est': 0.5,
            'deposits': 100.0,
            'withdrawals': 0.0,
            'trade_log': []
        },
        'paper': False,
        'dca_state': 'SCANNING',
    }
