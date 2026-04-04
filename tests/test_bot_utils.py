"""Tests for bot_utils.py — snapping, Kelly, trade recording."""
import pytest
import threading
from bot_utils import snap_to_increment, calculate_kelly_pct, BOTS_LOCK


class TestSnapToIncrement:
    def test_basic_snap(self):
        assert snap_to_increment(0.123456, '0.0001') == '0.1234'

    def test_round_down(self):
        """Should always round DOWN, not nearest."""
        assert snap_to_increment(0.99999, '0.01') == '0.99'

    def test_integer_increment(self):
        assert snap_to_increment(123.7, '1') == '123'

    def test_small_increment(self):
        result = snap_to_increment(0.00012345, '0.00000001')
        assert result == '0.00012345'

    def test_zero_value(self):
        assert snap_to_increment(0, '0.01') == '0'

    def test_large_value(self):
        result = snap_to_increment(67543.219, '0.01')
        assert result == '67543.21'

    def test_string_input(self):
        """Should handle string values gracefully."""
        result = snap_to_increment('1.2345', '0.01')
        assert result == '1.23'

    def test_bad_input_fallback(self):
        """Should return str(value) on error."""
        result = snap_to_increment('not_a_number', '0.01')
        assert result == 'not_a_number'

    def test_very_small_increment(self):
        result = snap_to_increment(0.001, '0.000001')
        assert result == '0.001'


class TestBotsLock:
    def test_is_reentrant(self):
        """BOTS_LOCK must be RLock so save_bots works inside locked sections."""
        assert isinstance(BOTS_LOCK, type(threading.RLock()))
        # Verify reentrant acquisition works
        BOTS_LOCK.acquire()
        BOTS_LOCK.acquire()  # Should not deadlock
        BOTS_LOCK.release()
        BOTS_LOCK.release()


class TestKellyCriterion:
    def test_returns_none_without_data(self):
        """Should return None when no stats exist."""
        result = calculate_kelly_pct('NONEXISTENT', 'FAKE-USD')
        assert result is None

    def test_return_type(self):
        """If it returns a value, it should be a float in [0.5, 10.0]."""
        result = calculate_kelly_pct('DCA', 'BTC-USD')
        if result is not None:
            assert 0.5 <= result <= 10.0
            assert isinstance(result, float)
