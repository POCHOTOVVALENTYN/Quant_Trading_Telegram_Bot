import pytest

from core.pnl.pnl_calculator import PnLCalculator


def test_realized_pnl_long_profit():
    pnl = PnLCalculator.calculate_realized_pnl(
        side="LONG",
        entry_price=100.0,
        exit_price=110.0,
        qty=1.0,
        entry_fee_usd=0.1,
        exit_fee_usd=0.1,
    )

    assert pnl.gross_pnl_usd == pytest.approx(10.0)
    assert pnl.fees_usd == pytest.approx(0.2)
    assert pnl.pnl_usd == pytest.approx(9.8)
    assert pnl.pnl_pct == pytest.approx(9.8)


def test_realized_pnl_long_loss():
    pnl = PnLCalculator.calculate_realized_pnl(
        side="LONG",
        entry_price=100.0,
        exit_price=90.0,
        qty=1.0,
        entry_fee_usd=0.1,
        exit_fee_usd=0.1,
    )

    assert pnl.gross_pnl_usd == pytest.approx(-10.0)
    assert pnl.fees_usd == pytest.approx(0.2)
    assert pnl.pnl_usd == pytest.approx(-10.2)
    assert pnl.pnl_pct == pytest.approx(-10.2)


def test_realized_pnl_short_profit():
    pnl = PnLCalculator.calculate_realized_pnl(
        side="SHORT",
        entry_price=100.0,
        exit_price=90.0,
        qty=1.0,
        entry_fee_usd=0.1,
        exit_fee_usd=0.1,
    )

    assert pnl.gross_pnl_usd == pytest.approx(10.0)
    assert pnl.fees_usd == pytest.approx(0.2)
    assert pnl.pnl_usd == pytest.approx(9.8)
    assert pnl.pnl_pct == pytest.approx(9.8)


def test_realized_pnl_short_loss():
    pnl = PnLCalculator.calculate_realized_pnl(
        side="SHORT",
        entry_price=100.0,
        exit_price=110.0,
        qty=1.0,
        entry_fee_usd=0.1,
        exit_fee_usd=0.1,
    )

    assert pnl.gross_pnl_usd == pytest.approx(-10.0)
    assert pnl.fees_usd == pytest.approx(0.2)
    assert pnl.pnl_usd == pytest.approx(-10.2)
    assert pnl.pnl_pct == pytest.approx(-10.2)


def test_realized_pnl_partial_fill_uses_partial_qty_and_fees():
    pnl = PnLCalculator.calculate_realized_pnl(
        side="LONG",
        entry_price=100.0,
        exit_price=105.0,
        qty=0.4,
        entry_fee_usd=0.04,
        exit_fee_usd=0.042,
    )

    assert pnl.gross_pnl_usd == pytest.approx(2.0)
    assert pnl.fees_usd == pytest.approx(0.082)
    assert pnl.pnl_usd == pytest.approx(1.918)


def test_realized_pnl_invalid_values_return_zero_breakdown():
    pnl = PnLCalculator.calculate_realized_pnl(
        side="LONG",
        entry_price=0.0,
        exit_price=105.0,
        qty=1.0,
        entry_fee_usd=0.1,
        exit_fee_usd=0.1,
    )

    assert pnl.gross_pnl_usd == 0.0
    assert pnl.pnl_usd == 0.0
    assert pnl.pnl_pct == 0.0


def test_estimate_fee_from_notional_and_rate():
    fee = PnLCalculator.estimate_fee(price=25000.0, qty=0.02, fee_rate=0.0005)
    assert fee == pytest.approx(0.25)
