import asyncio
import sys
import os
from sqlalchemy import select
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.session import async_session
from database.models.all_models import Position, PositionStatus, SignalType

async def test_database_persistence():
    print("\n🗄 Тестирование базы данных...")
    
    async with async_session() as session:
        try:
            # 1. Создаем тестовую позицию
            test_pos = Position(
                symbol="TEST/USDT",
                side=SignalType.LONG,
                status=PositionStatus.OPEN,
                entry_price=100.0,
                size=1.0
            )
            session.add(test_pos)
            await session.commit()
            print("✅ Тестовая позиция успешно записана в БД")
            
            # 2. Читаем её обратно
            from sqlalchemy import desc
            stmt = select(Position).where(Position.symbol == "TEST/USDT").order_by(desc(Position.id))
            result = await session.execute(stmt)
            pos = result.scalars().first()
            
            if pos and float(pos.entry_price) == 100.0:
                print(f"✅ Позиция прочитана успешно: ID={pos.id}, Symbol={pos.symbol}")
                
                # 3. Удаляем тест
                await session.delete(pos)
                await session.commit()
                print("✅ Тестовые данные удалены")
            else:
                print(f"❌ Ошибка при чтении позиции. Получено: Symbol={pos.symbol if pos else 'None'}, Price={pos.entry_price if pos else 'None'}")
                
        except Exception as e:
            print(f"❌ Ошибка БД: {e}")
            await session.rollback()

if __name__ == "__main__":
    asyncio.run(test_database_persistence())
