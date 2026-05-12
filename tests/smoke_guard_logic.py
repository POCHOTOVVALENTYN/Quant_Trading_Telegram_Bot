import asyncio
import logging
import time
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.execution.engine import ExecutionEngine
from config.settings import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smoke_guard")

class TestGuardLogic:
    def __init__(self):
        self.mock_exchange = AsyncMock()
        # Настраиваем возвращаемое значение для асинхронного вызова
        self.mock_exchange.create_order.return_value = {"id": "test_order_123", "average": 59100.0, "status": "closed", "filled": 1.0}
        
        self.mock_risk = MagicMock()
        # Метод должен возвращать float, чтобы сравнения в движке работали
        self.mock_risk.current_r_multiple.side_effect = lambda entry, stop, curr, side: (curr - entry) / abs(entry - stop) if side == "LONG" else (entry - curr) / abs(entry - stop)
        
        self.engine = ExecutionEngine(exchange_client=self.mock_exchange, risk_manager=self.mock_risk)
        self.engine.is_running = True
        self.engine._db_persist_order = AsyncMock()
        self.engine._get_live_position = AsyncMock(return_value=(1.0, "LONG"))
        self.engine._cancel_all_orders = AsyncMock()

    async def test_invalidation_exit(self):
        print("\n" + "-"*30)
        print("🕵️ ТЕСТ: ИНВАЛИДАЦИЯ (EMA50)")
        print("-"*30)
        
        symbol = "BTC/USDT"
        entry_price = 60000.0
        ema50 = 59500.0
        
        # Цена чуть ниже EMA50, но ВНУТРИ буфера 0.5% (59500 * 0.995 = 59202)
        price_in_buffer = 59300.0 
        # Цена ВНЕ буфера
        price_invalid = 59100.0

        test_trade = {
            "symbol": symbol, "signal_type": "LONG", "entry": entry_price,
            "stop": 58000.0, "current_size": 1.0, "opened_at": time.time(),
            "setup_group": "trend", "position_db_id": 1, "invalidation_level": 59500.0
        }
        self.engine.active_trades[symbol] = test_trade
        
        df = pd.DataFrame([{"ema50": ema50}])

        print(f"🔹 Цена {price_in_buffer} (в буфере EMA50={ema50}). Проверка...")
        await self.engine.evaluate_position_management(symbol, price_in_buffer, atr=100.0, df_tf=df)
        if symbol in self.engine.active_trades:
            print("✅ Успех: Позиция НЕ закрыта (буфер работает)")
        else:
            print("❌ Ошибка: Позиция закрыта в буфере!")

        print(f"🔹 Цена {price_invalid} (вне буфера). Проверка...")
        with patch('utils.notifier.send_telegram_msg', AsyncMock()):
            await self.engine.evaluate_position_management(symbol, price_invalid, atr=100.0, df_tf=df)
        
        if symbol not in self.engine.active_trades:
            print("✅ Успех: Позиция закрыта (инвалидация сработала)")
        else:
            print("❌ Ошибка: Позиция все еще открыта!")

    async def test_time_stop(self):
        print("\n" + "-"*30)
        print("🕵️ ТЕСТ: ТАЙМСТОП")
        print("-"*30)
        
        symbol = "ETH/USDT"
        opened_at = time.time() - (40 * 3600) 
        
        test_trade = {
            "symbol": symbol, "signal_type": "LONG", "entry": 3000.0,
            "stop": 2900.0, "initial_stop": 2900.0, "current_size": 1.0,
            "opened_at": opened_at, "setup_group": "trend", "position_db_id": 2,
            "timeframe": "1h"
        }
        self.engine.active_trades[symbol] = test_trade
        
        current_price = 3010.0 
        
        print(f"🔹 Прошло 40 баров, профит минимальный. Проверка...")
        with patch('utils.notifier.send_telegram_msg', AsyncMock()):
            await self.engine.evaluate_position_management(symbol, current_price, atr=50.0)
            
        if symbol not in self.engine.active_trades:
            print("✅ Успех: Позиция закрыта по таймстопу")
        else:
            print("❌ Ошибка: Позиция все еще открыта после 40 баров!")

    async def run_all(self):
        print("🚀 ЗАПУСК РАСШИРЕННЫХ SMOKE-ТЕСТОВ LOGIC GUARD")
        await self.test_invalidation_exit()
        await self.test_time_stop()
        print("\n🏁 ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ")

if __name__ == "__main__":
    tester = TestGuardLogic()
    asyncio.run(tester.run_all())
