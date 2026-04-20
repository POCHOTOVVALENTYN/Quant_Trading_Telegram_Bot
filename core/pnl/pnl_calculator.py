from dataclasses import dataclass


@dataclass(frozen=True)
class PnLBreakdown:
    side: str
    qty: float
    entry_price: float
    exit_price: float
    gross_pnl_usd: float
    entry_fee_usd: float
    exit_fee_usd: float
    fees_usd: float
    pnl_usd: float
    pnl_pct: float


class PnLCalculator:
    @staticmethod
    def calculate_realized_pnl(
        *,
        side: str,
        entry_price: float,
        exit_price: float,
        qty: float,
        entry_fee_usd: float = 0.0,
        exit_fee_usd: float = 0.0,
    ) -> PnLBreakdown:
        norm_side = str(side or "").upper()
        qty_f = float(qty or 0.0)
        entry_f = float(entry_price or 0.0)
        exit_f = float(exit_price or 0.0)
        entry_fee = float(entry_fee_usd or 0.0)
        exit_fee = float(exit_fee_usd or 0.0)

        if qty_f <= 0 or entry_f <= 0 or exit_f <= 0:
            return PnLBreakdown(
                side=norm_side,
                qty=qty_f,
                entry_price=entry_f,
                exit_price=exit_f,
                gross_pnl_usd=0.0,
                entry_fee_usd=entry_fee,
                exit_fee_usd=exit_fee,
                fees_usd=entry_fee + exit_fee,
                pnl_usd=0.0,
                pnl_pct=0.0,
            )

        if norm_side == "LONG":
            gross = (exit_f - entry_f) * qty_f
        elif norm_side == "SHORT":
            gross = (entry_f - exit_f) * qty_f
        else:
            raise ValueError(f"Unsupported side: {side}")

        fees = entry_fee + exit_fee
        pnl = gross - fees
        entry_notional = abs(entry_f * qty_f)
        pnl_pct = (pnl / entry_notional * 100.0) if entry_notional > 1e-12 else 0.0

        return PnLBreakdown(
            side=norm_side,
            qty=qty_f,
            entry_price=entry_f,
            exit_price=exit_f,
            gross_pnl_usd=gross,
            entry_fee_usd=entry_fee,
            exit_fee_usd=exit_fee,
            fees_usd=fees,
            pnl_usd=pnl,
            pnl_pct=pnl_pct,
        )

    @staticmethod
    def estimate_fee(*, price: float, qty: float, fee_rate: float) -> float:
        price_f = float(price or 0.0)
        qty_f = float(qty or 0.0)
        rate_f = float(fee_rate or 0.0)
        if price_f <= 0 or qty_f <= 0 or rate_f <= 0:
            return 0.0
        return abs(price_f * qty_f) * rate_f
