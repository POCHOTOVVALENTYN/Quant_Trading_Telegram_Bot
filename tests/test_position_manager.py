import pytest

from core.position.position_manager import PositionManager, PositionState


def test_position_manager_open_partial_close_and_close():
    opened = PositionManager.open_position(side="LONG", qty=2.0, entry_price=100.0)

    assert opened.action == "OPEN"
    assert opened.state.is_open is True
    assert opened.state.qty == 2.0
    assert opened.state.entry_price == 100.0
    assert opened.state.side == "LONG"

    reduced = PositionManager.partial_close(
        state=opened.state,
        qty=0.5,
        exit_price=110.0,
    )

    assert reduced.action == "PARTIAL_CLOSE"
    assert reduced.closed_qty == 0.5
    assert reduced.remaining_qty == 1.5
    assert reduced.realized_pnl == 5.0
    assert reduced.state.realized_pnl == 5.0
    assert reduced.state.is_open is True

    closed = PositionManager.close_position(
        state=reduced.state,
        exit_price=90.0,
    )

    assert closed.action == "CLOSE"
    assert closed.closed_qty == 1.5
    assert closed.realized_pnl == -15.0
    assert closed.state.is_open is False
    assert closed.state.qty == 0.0
    assert closed.state.realized_pnl == -10.0


def test_position_manager_add_recalculates_average_entry():
    opened = PositionManager.open_position(side="LONG", qty=1.0, entry_price=100.0)
    added = PositionManager.open_position(
        side="BUY",
        qty=1.0,
        entry_price=110.0,
        state=opened.state,
    )

    assert added.action == "ADD"
    assert added.state.qty == 2.0
    assert added.state.entry_price == 105.0


def test_position_manager_flip_creates_new_side_after_full_offset():
    state = PositionState(is_open=True, entry_price=100.0, qty=1.0, side="LONG", realized_pnl=0.0)

    flipped = PositionManager.apply_fill(
        state=state,
        fill_side="SELL",
        fill_qty=1.5,
        fill_price=95.0,
    )

    assert flipped.action == "FLIP"
    assert flipped.closed_qty == 1.0
    assert flipped.realized_pnl == -5.0
    assert flipped.state.is_open is True
    assert flipped.state.side == "SHORT"
    assert flipped.state.qty == 0.5
    assert flipped.state.entry_price == 95.0


def test_position_manager_partial_close_allocates_entry_and_exit_fees():
    opened = PositionManager.open_position(
        side="LONG",
        qty=2.0,
        entry_price=100.0,
        fee_paid_usd=0.2,
    )

    reduced = PositionManager.partial_close(
        state=opened.state,
        qty=0.5,
        exit_price=110.0,
        exit_fee_usd=0.05,
    )

    assert reduced.action == "PARTIAL_CLOSE"
    assert reduced.closed_qty == pytest.approx(0.5)
    assert reduced.fees_usd == pytest.approx(0.10)
    assert reduced.realized_pnl == pytest.approx(4.9)
    assert reduced.state.realized_pnl == pytest.approx(4.9)
    assert reduced.state.open_fees_usd == pytest.approx(0.15)


def test_position_manager_noop_on_zero_close_qty():
    opened = PositionManager.open_position(side="LONG", qty=1.0, entry_price=100.0)

    reduced = PositionManager.partial_close(
        state=opened.state,
        qty=0.0,
        exit_price=110.0,
    )

    assert reduced.action == "NOOP"
    assert reduced.state == opened.state


def test_position_manager_full_close_clears_open_fees():
    opened = PositionManager.open_position(
        side="SHORT",
        qty=1.0,
        entry_price=100.0,
        fee_paid_usd=0.1,
    )

    closed = PositionManager.close_position(
        state=opened.state,
        exit_price=90.0,
        exit_fee_usd=0.09,
    )

    assert closed.action == "CLOSE"
    assert closed.state.is_open is False
    assert closed.state.qty == 0.0
    assert closed.state.open_fees_usd == 0.0
    assert closed.realized_pnl == pytest.approx(9.81)
