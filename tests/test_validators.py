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
        ok, err = validate_trade({'pair': 'BTC-USD', 'side': 'BUY', 'order_type': 'MARKET', 'amount': 10})
        assert ok is True

    def test_valid_limit_sell(self):
        ok, err = validate_trade({'pair': 'ETH-USD', 'side': 'SELL', 'order_type': 'LIMIT', 'amount': 1, 'limit_price': 3500})
        assert ok is True

    def test_valid_maker_limit(self):
        ok, err = validate_trade({'pair': 'ETH-USD', 'side': 'BUY', 'order_type': 'MAKER_LIMIT', 'amount': 50})
        assert ok is True

    def test_limit_without_price(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'side': 'BUY', 'order_type': 'LIMIT', 'amount': 10})
        assert ok is False
        assert 'limit_price' in err

    def test_invalid_side(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'side': 'HOLD', 'order_type': 'MARKET', 'amount': 10})
        assert ok is False

    def test_invalid_order_type(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'side': 'BUY', 'order_type': 'FOK', 'amount': 10})
        assert ok is False

    def test_close_valid(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'action': 'CLOSE', 'size': 0.01, 'side': 'BUY', 'type': 'SPOT'})
        assert ok is True

    def test_close_bad_size(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'action': 'CLOSE', 'size': -1})
        assert ok is False
        ok, err = validate_trade({'pair': 'BTC-USD', 'action': 'CLOSE'})
        assert ok is False

    def test_nan_amount_rejected(self):
        ok, err = validate_trade({'pair': 'BTC-USD', 'side': 'BUY', 'order_type': 'MARKET', 'amount': 'nan'})
        assert ok is False


class TestValidateBracket:
    def test_valid_bracket(self):
        ok, err = validate_bracket({
            'pair': 'BTC-USD', 'size': 0.001,
            'entry_price': 65000, 'tp_price': 67000, 'sl_price': 63000
        })
        assert ok is True

    def test_missing_entry(self):
        ok, err = validate_bracket({'pair': 'BTC-USD', 'size': 0.001})
        assert ok is False

    def test_tp_only_is_valid(self):
        ok, err = validate_bracket({'pair': 'BTC-USD', 'size': 0.001, 'entry_price': 65000, 'tp_price': 67000})
        assert ok is True

    def test_requires_tp_or_sl(self):
        ok, err = validate_bracket({'pair': 'BTC-USD', 'size': 0.001, 'entry_price': 65000})
        assert ok is False

    def test_inverted_sl_rejected(self):
        # Long with SL at/above entry = zero/negative stop distance
        ok, err = validate_bracket({'pair': 'BTC-USD', 'size': 0.001, 'entry_price': 65000,
                                    'sl_price': 65000, 'tp_price': 67000})
        assert ok is False

    def test_negative_size(self):
        ok, err = validate_bracket({
            'pair': 'BTC-USD', 'size': -1,
            'entry_price': 65000, 'tp_price': 67000, 'sl_price': 63000
        })
        assert ok is False


class TestValidateTrail:
    def test_valid_trail(self):
        ok, err = validate_trail({'pair': 'BTC-USD', 'size': 0.001, 'pct': 2.5, 'cur_px': 65000, 'side': 'SELL'})
        assert ok is True

    def test_zero_pct_rejected(self):
        ok, err = validate_trail({'pair': 'BTC-USD', 'size': 0.001, 'pct': 0, 'cur_px': 65000})
        assert ok is False

    def test_oversized_pct_rejected(self):
        ok, err = validate_trail({'pair': 'BTC-USD', 'size': 0.001, 'pct': 120, 'cur_px': 65000})
        assert ok is False

    def test_missing_pair(self):
        ok, err = validate_trail({'size': 0.001})
        assert ok is False

    def test_none_body(self):
        ok, err = validate_trail(None)
        assert ok is False
