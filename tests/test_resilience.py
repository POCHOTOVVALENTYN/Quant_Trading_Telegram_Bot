import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.execution.engine import ExecutionEngine
from database.models.all_models import Position as PositionModel, PositionStatus, SignalType
from config.settings import settings

class TestReconciliation(unittest.TestCase):
    
    async def run_reconcile_smoke(self):
        print("\n🕵️ Тестирование механизма Реконсила (Синхронизация с биржей)...")
        
        # 1. Mock Exchange
        mock_exchange = AsyncMock()
        # Имитируем одну позицию на бирже (SOL/USDT)
        mock_exchange.fetch_positions_risk.return_value = [
            {"symbol": "SOLUSDT", "contracts": 10.0, "entryPrice": 100.0, "side": "LONG"}
        ]
        mock_exchange.fetch_open_orders.return_value = []
        mock_exchange.request.return_value = {"algoOrders": []}
        
        # 2. Инициализация движка
        engine = ExecutionEngine(exchange_client=mock_exchange, risk_manager=MagicMock())
        engine._prepare_private_ops = AsyncMock()
        
        # 3. Сценарий: В БД пусто, на бирже есть позиция (GHOST POSITION)
        # Мы должны проверить, что реконсил не упадет и корректно обработает "призрака"
        print("🔹 Сценарий 1: Призрак на бирже (позиция есть, в БД нет)")
        
        # Мокаем БД сессию
        mock_session = AsyncMock()
        mock_session.execute.return_value.scalars.return_value.all.return_value = [] # В БД пусто
        
        with patch('database.session.async_session', return_value=mock_session):
            await engine.reconcile_full()
            
        print("✅ Реконсил призрака прошел успешно (бот не упал)")

        # 4. Проверка Retry Logic (_with_time_sync_retry)
        print("\n🔹 Сценарий 2: Проверка отказоустойчивости (Retry при сетевой ошибке)")
        
        fail_mock = AsyncMock()
        # Сначала выбрасывает ошибку, на второй раз успех
        fail_mock.side_effect = [Exception("Binance Timeout"), {"result": "ok"}]
        
        start_time = asyncio.get_event_loop().time()
        res = await engine._with_time_sync_retry(lambda: fail_mock(), ctx="test_retry")
        end_time = asyncio.get_event_loop().time()
        
        if res and res.get("result") == "ok":
            print(f"✅ Retry сработал: успех со второй попытки. Задержка: {end_time - start_time:.2f}с")
        else:
            print("❌ Retry не сработал")

if __name__ == "__main__":
    tester = TestReconciliation()
    asyncio.run(tester.run_reconcile_smoke())
