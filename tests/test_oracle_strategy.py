"""
Unit Tests: Oracle Strategy + Fair Value + Position Sizer + Settlement
=======================================================================
Run: python3 -m pytest tests/test_oracle_strategy.py -v
Or:  python3 tests/test_oracle_strategy.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from engine.fair_value import (
    get_required_magnitude,
    compute_fair_value,
    compute_edge_at_fill,
    estimate_fill_price,
)
from engine.position_sizer import PositionSizer
from engine.settlement_logic import compute_settlement, compute_pnl


def test_timing_thresholds():
    """Continuous monitoring: bigger moves required earlier."""
    # Early window (250s remaining) — needs 0.20% move
    assert get_required_magnitude(250) >= 0.15
    # Mid window (150s remaining) — needs 0.12%
    assert get_required_magnitude(150) >= 0.08
    # Sweet spot (90s remaining) — needs 0.08%
    assert get_required_magnitude(90) <= 0.08
    # Late window (45s remaining) — needs 0.05%
    assert get_required_magnitude(45) <= 0.05
    # Too late (15s remaining) — should return huge number (skip)
    assert get_required_magnitude(15) > 100
    print("  PASS: Timing thresholds")


def test_fair_value_model():
    """Fair value increases with magnitude and timing."""
    # Large move late = very high confidence
    fv_large_late = compute_fair_value(0.15, 60)
    assert fv_large_late >= 0.95, f"Expected >= 0.95, got {fv_large_late}"

    # Small move early = low confidence
    fv_small_early = compute_fair_value(0.05, 250)
    assert fv_small_early <= 0.90, f"Expected <= 0.90, got {fv_small_early}"

    # Tiny move = coin flip
    fv_tiny = compute_fair_value(0.02, 90)
    assert fv_tiny <= 0.60, f"Expected <= 0.60, got {fv_tiny}"

    # Monotonicity: bigger move at same time = higher FV
    fv_08 = compute_fair_value(0.08, 90)
    fv_15 = compute_fair_value(0.15, 90)
    assert fv_15 >= fv_08, f"Expected {fv_15} >= {fv_08}"

    print("  PASS: Fair value model")


def test_edge_at_fill():
    """Edge check: should trade at good prices, skip at bad prices."""
    # Good edge: FV=0.95, fill=$0.10 (buying cheap NO)
    edge = compute_edge_at_fill(0.95, 0.10, min_edge_pct=3.0)
    assert edge.should_trade, f"Expected trade with edge={edge.edge_pct}%"
    assert edge.edge_pct > 50, f"Expected >50% edge, got {edge.edge_pct}"

    # Marginal edge: FV=0.95, fill=$0.90
    edge2 = compute_edge_at_fill(0.95, 0.90, min_edge_pct=3.0)
    assert edge2.edge_pct < 20, f"Expected <20% edge at $0.90, got {edge2.edge_pct}"

    # No edge: FV=0.95, fill=$0.96
    edge3 = compute_edge_at_fill(0.95, 0.96, min_edge_pct=3.0)
    assert not edge3.should_trade, f"Should skip at $0.96 fill"

    # Breakeven should be reasonable (at $0.10 fill, only need ~12% WR)
    assert 0.0 < edge.breakeven_wr < 1.0

    print("  PASS: Edge at fill price")


def test_fill_price_estimation():
    """Fill price derived correctly from book state."""
    # UP trade: buy YES at best ask
    book_up = {"best_bid_yes": 0.80, "best_ask_yes": 0.82}
    fill_up = estimate_fill_price(book_up, "UP")
    assert fill_up == 0.82

    # DOWN trade: buy NO at 1 - best_bid_yes
    book_down = {"best_bid_yes": 0.93, "best_ask_yes": 0.95}
    fill_down = estimate_fill_price(book_down, "DOWN")
    assert abs(fill_down - 0.07) < 0.001, f"Expected ~$0.07, got {fill_down}"

    # No book data
    fill_none = estimate_fill_price({}, "UP")
    assert fill_none is None

    # Garbage book data
    fill_garbage = estimate_fill_price({"best_ask_yes": 0.99}, "UP")
    assert fill_garbage is None  # 0.99 is too close to boundary

    print("  PASS: Fill price estimation")


def test_position_sizer():
    """Position sizing with percentage-based Kelly."""
    config = {
        "bankroll": 1000.0,
        "fractional_kelly": 0.20,
        "min_position_pct": 0.01,
        "max_position_pct": 0.03,
        "max_daily_loss_pct": 0.10,
        "taker_fee_rate": 0.0156,
        "min_order_usd": 1.0,
    }
    sizer = PositionSizer(config)

    # High conviction, cheap fill → should be near max %
    r1 = sizer.compute_size(confidence=0.97, fill_price=0.05, daily_pnl=0.0)
    assert r1.should_trade
    assert r1.size_pct <= 0.03, f"Expected <= 3%, got {r1.size_pct}"
    assert r1.size_usd > 0
    print(f"    High conviction: ${r1.size_usd:.2f} ({r1.size_pct:.1%}), capped by {r1.capped_by}")

    # Moderate conviction → should be in middle
    r2 = sizer.compute_size(confidence=0.85, fill_price=0.52, daily_pnl=0.0)
    assert r2.should_trade
    assert r2.size_pct >= 0.01
    print(f"    Moderate: ${r2.size_usd:.2f} ({r2.size_pct:.1%}), capped by {r2.capped_by}")

    # No edge → should refuse
    r3 = sizer.compute_size(confidence=0.50, fill_price=0.52, daily_pnl=0.0)
    assert not r3.should_trade, f"Should refuse at 50% confidence"
    print(f"    No edge: refused ({r3.reason})")

    # Daily loss limit → should refuse
    r4 = sizer.compute_size(confidence=0.95, fill_price=0.10, daily_pnl=-150.0)
    assert not r4.should_trade
    print(f"    Daily loss: refused ({r4.reason})")

    # Fee estimate should be positive
    assert r1.fee_estimate > 0

    print("  PASS: Position sizer")


def test_settlement_logic():
    """Settlement uses side + outcome (not direction + outcome)."""
    # YES side
    assert compute_settlement("YES", "UP")["won"] is True
    assert compute_settlement("YES", "DOWN")["won"] is False

    # NO side
    assert compute_settlement("NO", "DOWN")["won"] is True
    assert compute_settlement("NO", "UP")["won"] is False

    # Case insensitivity
    assert compute_settlement("yes", "up")["won"] is True
    assert compute_settlement("no", "down")["won"] is True

    print("  PASS: Settlement logic")


def test_pnl_calculation():
    """P&L computed from actual fill price with fee deduction."""
    # Win at $0.10 fill (buying cheap NO)
    pnl_win = compute_pnl(won=True, fill_price=0.10, size_usd=10.0, taker_fee_rate=0.0156)
    assert pnl_win["gross_pnl"] > 0
    assert pnl_win["fee"] > 0
    assert pnl_win["net_pnl"] < pnl_win["gross_pnl"]  # Fee reduces net
    print(f"    Win at $0.10: gross=${pnl_win['gross_pnl']:.2f}, fee=${pnl_win['fee']:.4f}, net=${pnl_win['net_pnl']:.2f}")

    # Loss at $0.10 fill
    pnl_loss = compute_pnl(won=False, fill_price=0.10, size_usd=10.0)
    assert pnl_loss["gross_pnl"] < 0
    print(f"    Loss at $0.10: gross=${pnl_loss['gross_pnl']:.2f}, net=${pnl_loss['net_pnl']:.2f}")

    # Win at $0.52 fill (standard YES trade)
    pnl_yes = compute_pnl(won=True, fill_price=0.52, size_usd=10.0)
    assert pnl_yes["gross_pnl"] > 0
    print(f"    Win at $0.52: gross=${pnl_yes['gross_pnl']:.2f}, shares={pnl_yes['shares']:.2f}")

    print("  PASS: P&L calculation")


def test_scaling_behavior():
    """Position sizing scales linearly with bankroll."""
    for bankroll in [200, 1000, 10000, 50000]:
        config = {
            "bankroll": bankroll,
            "fractional_kelly": 0.20,
            "min_position_pct": 0.01,
            "max_position_pct": 0.03,
            "max_daily_loss_pct": 0.10,
            "taker_fee_rate": 0.0156,
            "min_order_usd": 1.0,
        }
        sizer = PositionSizer(config)
        r = sizer.compute_size(confidence=0.95, fill_price=0.10, daily_pnl=0.0)
        if r.should_trade:
            pct = r.size_usd / bankroll * 100
            print(f"    ${bankroll:>6,}: ${r.size_usd:>8.2f} ({pct:.1f}%)")
            assert 0.8 <= pct <= 3.5, f"Expected 1-3%, got {pct}%"

    print("  PASS: Scaling behavior")


if __name__ == "__main__":
    print("=" * 60)
    print("Oracle Bot v4 Unit Tests")
    print("=" * 60)

    tests = [
        test_timing_thresholds,
        test_fair_value_model,
        test_edge_at_fill,
        test_fill_price_estimation,
        test_position_sizer,
        test_settlement_logic,
        test_pnl_calculation,
        test_scaling_behavior,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    if failed == 0:
        print("ALL TESTS PASSED")
    print("=" * 60)
