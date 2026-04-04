"""Tests for input validation."""
import pytest
from validators import validate_start_bot, validate_trade, validate_bracket, validate_trail


class TestValidateStartBot:
    def test_valid_request(self):
        ok, err = validate_start_bot({'pair': 'BTC-USD', 'strategy': 'DCA', 'amount': 50})
        assert ok is True
        assert err is None

    def test_missing_pair(self):
        ok, err = validate_start_bot({'strategy': 'DCA', 'amount': 50})
        assert ok is False
        assert 'pair' in err

    def test_missing_strategy(self):
        ok, err = validate_start_bot({'pair': 'BTC-USD', 'amount': 50})
        assert ok is False
        assert 'strategy' in err

    def test_invalid_strategy(self):
        ok, err = validate_start_bot({'pair': 'BTC-USD', 'strategy': 'FAKE', 'amount': 50})
        assert ok is False
        assert 'Invalid strategy' in err

    def test_negative_amount(self):
        ok, err = validate_start_bot({'pair': 'BTC-USD', 'strategy': 'DCA', 'amount': -10})
        assert ok is False
        assert 'positive' in err

    def test_zero_amount(self):
        ok, err = validate_start_bot({'pair': 'BTC-USD', 'strategy': 'DCA', 'amount': 0})
        assert ok is False

    def test_string_amount(self):
        ok, err = validate_start_bot({'pair': 'BTC-USD', 'strategy': 'DCA', 'amount': 'abc'})
        assert ok is False
        assert 'number' in err

    def test_empty_body(self):
        ok, err = validate_start_bot({})
        assert ok is False

    def test_none_body(self):
        ok, err = validate_start_bot(None)
        assert ok is False

    def test_all_strategies_valid(self):
        for strat in ('QUAD', 'QUAD_SUPER', 'ORB', 'GRID', 'TRAP', 'MOMENTUM', 'DCA', 'NPR', 'VWAP_MR', 'SQUEEZE'):
            ok, err = validate_start_bot({'pair': 'BTC-USD', 'strategy': strat, 'amount': 10})
            assert ok is True, f"{strat} should be valid: {err}"


class TestValidateTrade:
    def test_valid_market_buy(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'action': 'BUY', 'order_type': 'MARKET', 'amount': 10})
        assert ok is True

    def test_valid_limit_sell(self):
        ok, err = validate_trade({'pair': 'ETH-USD', 'action': 'SELL', 'order_type': 'LIMIT', 'amount': 1, 'price': 3500})
        assert ok is True

    def test_limit_without_price(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'action': 'BUY', 'order_type': 'LIMIT', 'amount': 10})
        assert ok is False
        assert 'price' in err

    def test_invalid_action(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'action': 'HOLD', 'order_type': 'MARKET', 'amount': 10})
        assert ok is False

    def test_invalid_order_type(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'action': 'BUY', 'order_type': 'FOK', 'amount': 10})
        assert ok is False


class TestValidateBracket:
    def test_valid_bracket(self):
        ok, err = validate_bracket({
            'pair': 'BTC-USD', 'size': 0.001,
            'entry_price': 65000, 'tp_price': 67000, 'sl_price': 63000
        })
        assert ok is True

    def test_missing_field(self):
        ok, err = validate_bracket({'pair': 'BTC-USD', 'size': 0.001})
        assert ok is False
        assert 'Missing' in err

    def test_negative_size(self):
        ok, err = validate_bracket({
            'pair': 'BTC-USD', 'size': -1,
            'entry_price': 65000, 'tp_price': 67000, 'sl_price': 63000
        })
        assert ok is False


class TestValidateTrail:
    def test_valid_trail(self):
        ok, err = validate_trail({'pair': 'BTC-USD', 'size': 0.001, 'trail_pct': 2.5})
        assert ok is True

    def test_missing_pair(self):
        ok, err = validate_trail({'size': 0.001})
        assert ok is False

    def test_none_body(self):
        ok, err = validate_trail(None)
        assert ok is False
