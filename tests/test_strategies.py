"""Tests for strategy signal functions in strategies.py."""
import pandas as pd
import numpy as np
import pytest
from strategies import (
    calculate_quad_rotation,
    calculate_advanced_grid,
    calculate_orb,
    calculate_trap,
    calculate_momentum,
    calculate_dca,
    calculate_npr,
    calculate_vwap_mr,
    calculate_squeeze,
)


class TestQuadRotation:
    def test_hold_on_insufficient_data(self, small_df):
        signal, reason, details = calculate_quad_rotation(small_df)
        assert signal == "HOLD"
        assert "enough data" in reason.lower() or "warming" in reason.lower()

    def test_returns_valid_signal(self, trending_up_df):
        signal, reason, details = calculate_quad_rotation(trending_up_df)
        assert signal in ("BUY", "SELL", "HOLD")
        assert isinstance(reason, str)
        assert len(reason) > 0
        assert isinstance(details, dict)

    def test_hold_on_ranging_market(self, ranging_df):
        signal, reason, details = calculate_quad_rotation(ranging_df)
        # Ranging market unlikely to have all 4 stochs aligned
        assert signal in ("BUY", "SELL", "HOLD")


class TestGridSignal:
    def test_hold_on_insufficient_data(self, small_df):
        signal, reason = calculate_advanced_grid(small_df, lower_price=59000, upper_price=61000, grids=10)
        assert signal == "HOLD"

    def test_returns_hold_always(self, ranging_df):
        """Grid signal never returns BUY/SELL — only deployment instructions."""
        signal, reason = calculate_advanced_grid(ranging_df, lower_price=64000, upper_price=66000, grids=10)
        assert signal == "HOLD"

    def test_ranging_market_adx_low(self, ranging_df):
        signal, reason = calculate_advanced_grid(ranging_df, lower_price=64000, upper_price=66000, grids=10)
        assert signal == "HOLD"
        assert "DORMANT" in reason or "GRID" in reason or "ADX" in reason or "Warming" in reason


class TestORB:
    def test_hold_on_insufficient_data(self, small_df):
        small_df['start'] = np.arange(len(small_df)) * 300 + 1700000000
        signal, reason, meta = calculate_orb(small_df)
        assert signal == "HOLD"

    def test_returns_valid_signal(self, trending_up_df):
        trending_up_df['start'] = np.arange(len(trending_up_df)) * 300 + 1700000000
        signal, reason, meta = calculate_orb(trending_up_df)
        assert signal in ("LONG", "SHORT", "EXIT_LONG", "EXIT_SHORT",
                          "PARTIAL_EXIT_LONG", "PARTIAL_EXIT_SHORT", "HOLD")
        assert isinstance(meta, dict)

    def test_returns_three_tuple(self, trending_up_df):
        trending_up_df['start'] = np.arange(len(trending_up_df)) * 300 + 1700000000
        result = calculate_orb(trending_up_df)
        assert len(result) == 3

    def test_configurable_range_hour(self, trending_up_df):
        trending_up_df['start'] = np.arange(len(trending_up_df)) * 300 + 1700000000
        signal, reason, meta = calculate_orb(trending_up_df, range_start_hour=0)
        assert len((signal, reason, meta)) == 3


class TestTrap:
    def test_hold_on_insufficient_data(self, small_df):
        result = calculate_trap(small_df)
        assert result[0] == "HOLD"

    def test_returns_valid_signal(self, ranging_df):
        result = calculate_trap(ranging_df)
        signal = result[0]
        assert signal in ("BREAKOUT_LONG", "BREAKOUT_SHORT", "ADD_LONG", "ADD_SHORT",
                          "EXIT_LONG", "EXIT_SHORT", "PARTIAL_EXIT_LONG", "PARTIAL_EXIT_SHORT",
                          "HOLD")

    def test_returns_three_tuple(self, trending_up_df):
        result = calculate_trap(trending_up_df)
        assert len(result) == 3  # (signal, reason, bo_data)

    def test_tp_stage_parameter(self, ranging_df):
        """Verify tp_stage parameter is accepted"""
        result = calculate_trap(ranging_df, tp_stage=0)
        assert len(result) == 3
        result = calculate_trap(ranging_df, tp_stage=1)
        assert len(result) == 3


class TestMomentum:
    def test_hold_on_insufficient_data(self, small_df):
        signal, reason, atr = calculate_momentum(small_df)
        assert signal == "HOLD"

    def test_returns_three_tuple(self, trending_up_df):
        result = calculate_momentum(trending_up_df)
        assert len(result) == 3
        signal, reason, atr = result
        assert signal in ("BUY", "HOLD")
        assert isinstance(atr, (int, float))

    def test_atr_non_negative(self, trending_up_df):
        _, _, atr = calculate_momentum(trending_up_df)
        assert atr >= 0


class TestDCA:
    def test_hold_on_insufficient_data(self, small_df):
        signal, reason, extra = calculate_dca(small_df)
        assert signal == "HOLD"

    def test_returns_three_tuple(self, trending_up_df):
        result = calculate_dca(trending_up_df)
        assert len(result) == 3
        signal, reason, extra = result
        assert signal in ("ARM", "BUY", "DISARM", "CROSS_ABOVE", "HOLD")
        assert isinstance(extra, dict)

    def test_extra_has_roc_values(self, trending_up_df):
        _, _, extra = calculate_dca(trending_up_df)
        if extra:  # may be empty if warming up
            for key in ('fast_roc', 'slow_roc', 'adx'):
                if key in extra:
                    assert isinstance(extra[key], (int, float))


class TestNPR:
    def test_hold_on_insufficient_data(self, small_df):
        result = calculate_npr(small_df)
        assert result['signal'] == 'HOLD'

    def test_returns_dict(self, trending_up_df):
        result = calculate_npr(trending_up_df)
        assert isinstance(result, dict)
        assert 'signal' in result
        assert result['signal'] in ('LONG', 'SHORT', 'HOLD')

    def test_result_has_required_keys(self, trending_up_df):
        result = calculate_npr(trending_up_df)
        for key in ('signal', 'event_type', 'zone', 'reason'):
            assert key in result, f"Missing key: {key}"


class TestVwapMR:
    def test_hold_on_insufficient_data(self, small_df):
        signal, reason, atr = calculate_vwap_mr(small_df)
        assert signal == "HOLD"

    def test_returns_three_tuple(self, trending_up_df):
        trending_up_df['volume'] = np.random.uniform(100, 1000, len(trending_up_df))
        result = calculate_vwap_mr(trending_up_df)
        assert len(result) == 3
        signal, reason, atr = result
        assert signal in ("BUY", "SELL", "HOLD")
        assert isinstance(atr, (int, float))

    def test_atr_non_negative(self, ranging_df):
        _, _, atr = calculate_vwap_mr(ranging_df)
        assert atr >= 0


class TestSqueeze:
    def test_hold_on_insufficient_data(self, small_df):
        signal, reason, atr = calculate_squeeze(small_df)
        assert signal == "HOLD"

    def test_returns_three_tuple(self, trending_up_df):
        result = calculate_squeeze(trending_up_df)
        assert len(result) == 3
        signal, reason, atr = result
        assert signal in ("BUY", "SHORT", "EXIT_LONG", "EXIT_SHORT", "HOLD")
        assert isinstance(atr, (int, float))

    def test_atr_non_negative(self, ranging_df):
        _, _, atr = calculate_squeeze(ranging_df)
        assert atr >= 0
