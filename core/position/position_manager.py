from dataclasses import dataclass, replace
from typing import Literal, Optional

from core.pnl.pnl_calculator import PnLCalculator


PositionSide = Literal["LONG", "SHORT"]
PositionAction = Literal["OPEN", "ADD", "PARTIAL_CLOSE", "CLOSE", "FLIP", "NOOP"]


@dataclass(frozen=True)
class PositionState:
    is_open: bool = False
    entry_price: float = 0.0
    qty: float = 0.0
    side: Optional[PositionSide] = None
    realized_pnl: float = 0.0
    open_fees_usd: float = 0.0


@dataclass(frozen=True)
class PositionUpdate:
    action: PositionAction
    state: PositionState
    closed_qty: float = 0.0
    remaining_qty: float = 0.0
    realized_pnl: float = 0.0
    average_exit_price: float = 0.0
    fees_usd: float = 0.0


class PositionManager:
    """Tracks position lifecycle from fills instead of scattered ad-hoc mutations."""

    @staticmethod
    def open_position(
        *,
        side: str,
        qty: float,
        entry_price: float,
        state: Optional[PositionState] = None,
        fee_paid_usd: float = 0.0,
    ) -> PositionUpdate:
        return PositionManager.apply_fill(
            state=state or PositionState(),
            fill_side=side,
            fill_qty=qty,
            fill_price=entry_price,
            fill_fee_usd=fee_paid_usd,
        )

    @staticmethod
    def partial_close(
        *,
        state: PositionState,
        qty: float,
        exit_price: float,
        exit_fee_usd: float = 0.0,
    ) -> PositionUpdate:
        if not state.is_open or qty <= 0:
            return PositionUpdate(action="NOOP", state=state)
        close_side = "SELL" if state.side == "LONG" else "BUY"
        return PositionManager.apply_fill(
            state=state,
            fill_side=close_side,
            fill_qty=qty,
            fill_price=exit_price,
            fill_fee_usd=exit_fee_usd,
        )

    @staticmethod
    def close_position(
        *,
        state: PositionState,
        exit_price: float,
        exit_fee_usd: float = 0.0,
    ) -> PositionUpdate:
        if not state.is_open or state.qty <= 0:
            return PositionUpdate(action="NOOP", state=state)
        return PositionManager.partial_close(
            state=state,
            qty=state.qty,
            exit_price=exit_price,
            exit_fee_usd=exit_fee_usd,
        )

    @staticmethod
    def apply_fill(
        *,
        state: PositionState,
        fill_side: str,
        fill_qty: float,
        fill_price: float,
        fill_fee_usd: float = 0.0,
    ) -> PositionUpdate:
        qty = float(fill_qty or 0.0)
        price = float(fill_price or 0.0)
        fee = float(fill_fee_usd or 0.0)
        if qty <= 0 or price <= 0:
            return PositionUpdate(action="NOOP", state=state)

        side = PositionManager._normalize_side(fill_side)
        if not state.is_open or state.qty <= 0 or not state.side:
            next_state = PositionState(
                is_open=True,
                entry_price=price,
                qty=qty,
                side=side,
                realized_pnl=float(state.realized_pnl or 0.0),
                open_fees_usd=fee,
            )
            return PositionUpdate(
                action="OPEN",
                state=next_state,
                remaining_qty=next_state.qty,
                fees_usd=fee,
            )

        if side == state.side:
            total_qty = state.qty + qty
            avg_entry = ((state.entry_price * state.qty) + (price * qty)) / total_qty
            next_state = replace(
                state,
                entry_price=avg_entry,
                qty=total_qty,
                is_open=True,
                open_fees_usd=float(state.open_fees_usd or 0.0) + fee,
            )
            return PositionUpdate(
                action="ADD",
                state=next_state,
                remaining_qty=next_state.qty,
                fees_usd=fee,
            )

        closing_qty = min(state.qty, qty)
        allocated_entry_fee = (float(state.open_fees_usd or 0.0) * closing_qty / state.qty) if state.qty > 1e-12 else 0.0
        closing_fee = fee * (closing_qty / qty) if qty > 1e-12 else 0.0
        pnl = PnLCalculator.calculate_realized_pnl(
            side=state.side,
            entry_price=state.entry_price,
            exit_price=price,
            qty=closing_qty,
            entry_fee_usd=allocated_entry_fee,
            exit_fee_usd=closing_fee,
        )
        remaining = state.qty - closing_qty
        total_realized = float(state.realized_pnl or 0.0) + pnl.pnl_usd
        remaining_open_fees = max(0.0, float(state.open_fees_usd or 0.0) - allocated_entry_fee)

        if remaining > 1e-12:
            next_state = replace(
                state,
                qty=remaining,
                realized_pnl=total_realized,
                is_open=True,
                open_fees_usd=remaining_open_fees,
            )
            return PositionUpdate(
                action="PARTIAL_CLOSE",
                state=next_state,
                closed_qty=closing_qty,
                remaining_qty=remaining,
                realized_pnl=pnl.pnl_usd,
                average_exit_price=price,
                fees_usd=pnl.fees_usd,
            )

        if qty > state.qty:
            flipped_qty = qty - state.qty
            opening_fee = max(0.0, fee - closing_fee)
            next_state = PositionState(
                is_open=True,
                entry_price=price,
                qty=flipped_qty,
                side=side,
                realized_pnl=total_realized,
                open_fees_usd=opening_fee,
            )
            return PositionUpdate(
                action="FLIP",
                state=next_state,
                closed_qty=closing_qty,
                remaining_qty=flipped_qty,
                realized_pnl=pnl.pnl_usd,
                average_exit_price=price,
                fees_usd=pnl.fees_usd,
            )

        next_state = PositionState(
            is_open=False,
            entry_price=0.0,
            qty=0.0,
            side=None,
            realized_pnl=total_realized,
            open_fees_usd=0.0,
        )
        return PositionUpdate(
            action="CLOSE",
            state=next_state,
            closed_qty=closing_qty,
            remaining_qty=0.0,
            realized_pnl=pnl.pnl_usd,
            average_exit_price=price,
            fees_usd=pnl.fees_usd,
        )

    @staticmethod
    def _normalize_side(side: str) -> PositionSide:
        value = str(side or "").upper()
        if value in ("BUY", "LONG"):
            return "LONG"
        if value in ("SELL", "SHORT"):
            return "SHORT"
        raise ValueError(f"Unsupported fill side: {side}")
