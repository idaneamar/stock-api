"""
Unit tests for options pricing helpers:
  - net_premium sign classification (CREDIT / DEBIT / EVEN)
  - conservative fill estimate (sell at bid, buy at ask)
"""

import math
import sys
import os

# Allow importing from parent package without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from options.opsp import _mid, _spread_ok


# ---------------------------------------------------------------------------
# Helpers (inline versions matching opsp logic so tests are self-contained)
# ---------------------------------------------------------------------------

_EPSILON = 0.005


def classify_net_premium(net_premium: float) -> str:
    """Return 'CREDIT', 'DEBIT', or 'EVEN'."""
    if abs(net_premium) < _EPSILON:
        return "EVEN"
    return "CREDIT" if net_premium > 0 else "DEBIT"


def conservative_net_premium(
    sp_bid: float,
    lp_ask: float,
    sc_bid: float,
    lc_ask: float,
) -> float:
    """Sell legs at bid, buy legs at ask → most pessimistic fill for a credit trade."""
    return sp_bid - lp_ask + sc_bid - lc_ask


def mid_net_premium(
    sp_bid: float, sp_ask: float,
    lp_bid: float, lp_ask: float,
    sc_bid: float, sc_ask: float,
    lc_bid: float, lc_ask: float,
) -> float:
    sp_mid = _mid(sp_bid, sp_ask)
    lp_mid = _mid(lp_bid, lp_ask)
    sc_mid = _mid(sc_bid, sc_ask)
    lc_mid = _mid(lc_bid, lc_ask)
    return sp_mid - lp_mid + sc_mid - lc_mid


# ---------------------------------------------------------------------------
# Sign classification tests
# ---------------------------------------------------------------------------

class TestClassifyNetPremium:
    def test_positive_is_credit(self):
        assert classify_net_premium(1.20) == "CREDIT"

    def test_negative_is_debit(self):
        assert classify_net_premium(-0.50) == "DEBIT"

    def test_zero_is_even(self):
        assert classify_net_premium(0.0) == "EVEN"

    def test_tiny_positive_below_epsilon_is_even(self):
        assert classify_net_premium(0.001) == "EVEN"

    def test_tiny_negative_above_epsilon_is_even(self):
        assert classify_net_premium(-0.003) == "EVEN"

    def test_exactly_epsilon_is_credit(self):
        assert classify_net_premium(_EPSILON) == "CREDIT"

    def test_large_credit(self):
        assert classify_net_premium(5.00) == "CREDIT"

    def test_large_debit(self):
        assert classify_net_premium(-3.75) == "DEBIT"


# ---------------------------------------------------------------------------
# Conservative estimate tests
# ---------------------------------------------------------------------------

class TestConservativeNetPremium:
    def test_typical_iron_condor_credit(self):
        # Short put: bid=1.50, long put: ask=0.60
        # Short call: bid=1.20, long call: ask=0.45
        result = conservative_net_premium(1.50, 0.60, 1.20, 0.45)
        assert math.isclose(result, 1.65, rel_tol=1e-9)

    def test_conservative_less_than_mid(self):
        """Conservative fill should always be ≤ mid estimate for standard credit spreads."""
        sp_bid, sp_ask = 1.50, 1.60
        lp_bid, lp_ask = 0.55, 0.65
        sc_bid, sc_ask = 1.18, 1.28
        lc_bid, lc_ask = 0.40, 0.50

        conserv = conservative_net_premium(sp_bid, lp_ask, sc_bid, lc_ask)
        mid = mid_net_premium(sp_bid, sp_ask, lp_bid, lp_ask, sc_bid, sc_ask, lc_bid, lc_ask)
        assert conserv <= mid

    def test_symmetric_spreads_conservative_equals_mid_minus_half_spread(self):
        """With symmetric 0.10-wide spreads, conservative = mid − half_spread_sum."""
        sp_bid, sp_ask = 1.45, 1.55
        lp_bid, lp_ask = 0.55, 0.65
        sc_bid, sc_ask = 1.15, 1.25
        lc_bid, lc_ask = 0.45, 0.55

        conserv = conservative_net_premium(sp_bid, lp_ask, sc_bid, lc_ask)
        mid = mid_net_premium(sp_bid, sp_ask, lp_bid, lp_ask, sc_bid, sc_ask, lc_bid, lc_ask)
        # Each spread is 0.10 wide → we give up 0.05 on each of 4 legs = 0.20 total
        assert math.isclose(conserv, mid - 0.20, rel_tol=1e-9)

    def test_debit_scenario(self):
        """Degenerate case where conservative result is negative (DEBIT)."""
        result = conservative_net_premium(0.30, 1.00, 0.20, 0.80)
        assert result < 0

    def test_zero_legs(self):
        result = conservative_net_premium(0.0, 0.0, 0.0, 0.0)
        assert result == 0.0


# ---------------------------------------------------------------------------
# _mid helper tests (from opsp.py)
# ---------------------------------------------------------------------------

class TestMidHelper:
    def test_normal(self):
        assert _mid(1.0, 2.0) == 1.5

    def test_equal_bid_ask(self):
        assert _mid(1.5, 1.5) == 1.5

    def test_nan_bid_returns_nan(self):
        import math
        assert math.isnan(_mid(float("nan"), 2.0))


# ---------------------------------------------------------------------------
# _spread_ok filter tests
# ---------------------------------------------------------------------------

class TestSpreadOk:
    def test_tight_spread_ok(self):
        assert _spread_ok(1.45, 1.55) is True

    def test_zero_mid_not_ok(self):
        assert _spread_ok(0.0, 0.0) is False

    def test_negative_spread_not_ok(self):
        assert _spread_ok(2.0, 1.0) is False


# ---------------------------------------------------------------------------
# Run as script
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
