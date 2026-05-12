
import asyncio
import os
from datetime import datetime, timedelta
from sqlalchemy import select, func, desc
from database.session import async_session
from database.models.all_models import Signal, SignalDecisionLog, PnLRecord, Order, Position, ExecutionAuditLog

async def analyze():
    # UTC time range
    now = datetime.utcnow()
    five_hours_ago = now - timedelta(hours=5)
    print(f"📊 Анализ за период: {five_hours_ago.isoformat()} - {now.isoformat()} (UTC)")

    async with async_session() as session:
        # 1. PnL за последние 5 часов
        pnl_stmt = select(PnLRecord).where(PnLRecord.closed_at >= five_hours_ago).order_by(PnLRecord.closed_at.desc())
        pnl_rows = (await session.execute(pnl_stmt)).scalars().all()
        
        total_pnl_usd = sum(r.pnl_usd for r in pnl_rows)
        print(f"\n💰 ЗАВЕРШЕННЫЕ СДЕЛКИ (PnL): {len(pnl_rows)}")
        for r in pnl_rows:
            print(f"  - {r.closed_at.strftime('%H:%M:%S')} | {r.symbol} | PnL: ${r.pnl_usd:.2f} ({r.pnl_pct:.2f}%) | Reason: {r.reason}")
        print(f"💵 ИТОГО PnL: ${total_pnl_usd:.2f}")

        # 2. Активные позиции
        pos_stmt = select(Position).where(Position.status == "OPEN")
        pos_rows = (await session.execute(pos_stmt)).scalars().all()
        print(f"\n📈 ТЕКУЩИЕ ОТКРЫТЫЕ ПОЗИЦИИ: {len(pos_rows)}")
        for p in pos_rows:
            print(f"  - {p.symbol} | {p.side} | Entry: {p.entry_price} | Size: {p.size} | Opened: {p.opened_at.strftime('%H:%M:%S')}")

        # 3. Сигналы (принятые)
        sig_stmt = select(Signal).where(Signal.timestamp >= five_hours_ago).order_by(Signal.timestamp.desc())
        sig_rows = (await session.execute(sig_stmt)).scalars().all()
        print(f"\n📡 ПРИНЯТЫЕ СИГНАЛЫ: {len(sig_rows)}")
        for s in sig_rows:
            print(f"  - {s.timestamp.strftime('%H:%M:%S')} | {s.symbol} | {s.signal_type} | Strategy: {s.strategy} | Status: {s.status}")

        # 4. Пропущенные сигналы (Decision Log)
        logs_stmt = select(SignalDecisionLog).where(SignalDecisionLog.created_at >= five_hours_ago).order_by(SignalDecisionLog.created_at.desc()).limit(20)
        log_rows = (await session.execute(logs_stmt)).scalars().all()
        print(f"\n🔍 АНАЛИЗ РЕШЕНИЙ (последние 20):")
        for l in log_rows:
            outcome = l.outcome or "UNKNOWN"
            print(f"  - {l.created_at.strftime('%H:%M:%S')} | {l.symbol} | {l.direction} | Outcome: {outcome}")

        # 5. Ошибки исполнения (Audit Log)
        audit_stmt = select(ExecutionAuditLog).where(
            ExecutionAuditLog.created_at >= five_hours_ago,
            ExecutionAuditLog.severity.in_(["ERROR", "WARNING", "CRITICAL"])
        ).order_by(ExecutionAuditLog.created_at.desc())
        audit_rows = (await session.execute(audit_stmt)).scalars().all()
        print(f"\n⚠️ ОШИБКИ И ПРЕДУПРЕЖДЕНИЯ:")
        for a in audit_rows:
            print(f"  - {a.created_at.strftime('%H:%M:%S')} | {a.event_type} | {a.severity} | {a.message}")

if __name__ == "__main__":
    asyncio.run(analyze())
