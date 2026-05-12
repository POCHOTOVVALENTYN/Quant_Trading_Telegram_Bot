from typing import Any, Dict, Optional

from database.session import async_session
from database.models.all_models import ExecutionAuditLog


class AuditTrail:
    """Small reliable persistence helper for execution lifecycle events."""

    def __init__(self, user_id: Optional[int] = None):
        self.user_id = user_id

    async def record(
        self,
        *,
        event_type: str,
        severity: str = "INFO",
        message: Optional[str] = None,
        symbol: Optional[str] = None,
        strategy: Optional[str] = None,
        signal_id: Optional[int] = None,
        position_id: Optional[int] = None,
        order_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            async with async_session() as session:
                session.add(
                    ExecutionAuditLog(
                        user_id=self.user_id,
                        signal_id=signal_id,
                        position_id=position_id,
                        order_id=order_id,
                        symbol=symbol,
                        strategy=strategy,
                        event_type=event_type,
                        severity=severity,
                        message=message,
                        payload=payload or {},
                    )
                )
                await session.commit()
        except Exception:
            # Audit trail must never break trading flow.
            return
