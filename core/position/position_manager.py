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
    """Управляет жизненным циклом позиции на основе исполненных ордеров (fills)."""

    @staticmethod
    def apply_fill(
        *,
        state: PositionState,
        fill_side: str,
        fill_qty: float,
        fill_price: float,
        fill_fee_usd: float = 0.0,
    ) -> PositionUpdate:
        """
        Главный метод для обновления состояния позиции на основе нового филла.
        Автоматически определяет: открытие, усреднение, частичное или полное закрытие, или переворот (flip).
        """
        qty = float(fill_qty or 0.0)
        price = float(fill_price or 0.0)
        fee = float(fill_fee_usd or 0.0)
        
        if qty <= 0 or price <= 0:
            return PositionUpdate(action="NOOP", state=state)

        # Нормализация стороны филла
        fill_side_norm: PositionSide = "LONG" if fill_side.upper() in ("BUY", "LONG") else "SHORT"

        # 1. Если позиция закрыта — открываем новую
        if not state.is_open or state.qty <= 1e-12:
            next_state = PositionState(
                is_open=True,
                entry_price=price,
                qty=qty,
                side=fill_side_norm,
                realized_pnl=state.realized_pnl,
                open_fees_usd=fee
            )
            return PositionUpdate(
                action="OPEN",
                state=next_state,
                remaining_qty=qty,
                fees_usd=fee
            )

        # 2. Если сторона совпадает — добавляем к позиции (усреднение)
        if fill_side_norm == state.side:
            total_qty = state.qty + qty
            avg_entry = ((state.entry_price * state.qty) + (price * qty)) / total_qty
            next_state = replace(
                state,
                entry_price=avg_entry,
                qty=total_qty,
                open_fees_usd=state.open_fees_usd + fee
            )
            return PositionUpdate(
                action="ADD",
                state=next_state,
                remaining_qty=total_qty,
                fees_usd=fee
            )

        # 3. Если сторона противоположная — закрываем (частично или полностью)
        closing_qty = min(state.qty, qty)
        # Пропорционально аллоцируем комиссии открытия на закрываемую часть
        allocated_entry_fee = (state.open_fees_usd * closing_qty / state.qty) if state.qty > 1e-12 else 0.0
        
        pnl_breakdown = PnLCalculator.calculate_realized_pnl(
            side=state.side,
            entry_price=state.entry_price,
            exit_price=price,
            qty=closing_qty,
            entry_fee_usd=allocated_entry_fee,
            exit_fee_usd=fee * (closing_qty / qty) if qty > 1e-12 else 0.0
        )

        remaining_qty = state.qty - closing_qty
        new_total_pnl = state.realized_pnl + pnl_breakdown.pnl_usd
        remaining_open_fees = state.open_fees_usd - allocated_entry_fee

        # Частичное закрытие
        if remaining_qty > 1e-12:
            next_state = replace(
                state,
                qty=remaining_qty,
                realized_pnl=new_total_pnl,
                open_fees_usd=max(0.0, remaining_open_fees)
            )
            return PositionUpdate(
                action="PARTIAL_CLOSE",
                state=next_state,
                closed_qty=closing_qty,
                remaining_qty=remaining_qty,
                realized_pnl=pnl_breakdown.pnl_usd,
                average_exit_price=price,
                fees_usd=pnl_breakdown.fees_usd
            )

        # Переворот (Flip) — если филл больше текущей позиции
        if qty > state.qty + 1e-12:
            flipped_qty = qty - state.qty
            opening_fee = fee * (flipped_qty / qty)
            next_state = PositionState(
                is_open=True,
                entry_price=price,
                qty=flipped_qty,
                side=fill_side_norm,
                realized_pnl=new_total_pnl,
                open_fees_usd=opening_fee
            )
            return PositionUpdate(
                action="FLIP",
                state=next_state,
                closed_qty=closing_qty,
                remaining_qty=flipped_qty,
                realized_pnl=pnl_breakdown.pnl_usd,
                average_exit_price=price,
                fees_usd=pnl_breakdown.fees_usd
            )

        # Полное закрытие
        next_state = PositionState(
            is_open=False,
            realized_pnl=new_total_pnl
        )
        return PositionUpdate(
            action="CLOSE",
            state=next_state,
            closed_qty=closing_qty,
            realized_pnl=pnl_breakdown.pnl_usd,
            average_exit_price=price,
            fees_usd=pnl_breakdown.fees_usd
        )

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
        close_side = "SHORT" if state.side == "LONG" else "LONG"
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
