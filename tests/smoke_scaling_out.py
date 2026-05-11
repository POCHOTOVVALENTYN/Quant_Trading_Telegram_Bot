import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import time

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.execution.engine import ExecutionEngine
from core.execution.trade_guard import TradeGuard
from config.settings import settings

# Настройка логирования для теста
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smoke_test")

class TestScalingOutSmoke:
    """
    Smoke-тест для проверки логики частичного закрытия (Scaling Out).
    Имитирует движение цены в сторону профита и проверяет реакцию движка.
    """
    
    async def run_test(self):
        print("\n" + "="*50)
        print("🚀 ЗАПУСК SMOKE-ТЕСТА: SCALING OUT & BREAK-EVEN")
        print("="*50)

        # 1. Mock Objects
        mock_exchange = AsyncMock()
        mock_exchange.fetch_ticker.return_value = {"last": 100.0}
        mock_exchange.create_order.return_value = {"id": "test_order_123", "average": 101.5, "status": "closed", "filled": 1.0}
        
        mock_risk = MagicMock()
        # Метод current_r_multiple теперь есть в RiskManager
        mock_risk.current_r_multiple.side_effect = lambda entry, stop, curr, side: (curr - entry) / abs(entry - stop) if side == "LONG" else (entry - curr) / abs(entry - stop)
        
        # 2. Инициализация движка
        engine = ExecutionEngine(exchange_client=mock_exchange, risk_manager=mock_risk)
        engine.is_running = True
        
        symbol = "SOL/USDT"
        entry_price = 100.0
        stop_loss = 98.0 # Риск 2.0 на единицу
        risk_dist = 2.0
        
        # 3. Создаем "активную сделку" в памяти движка
        test_trade = {
            "symbol": symbol,
            "signal_type": "LONG",
            "entry": entry_price,
            "stop": stop_loss,
            "initial_stop": stop_loss,
            "current_size": 10.0,
            "opened_at": time.time() - 3600,
            "setup_group": "trend",
            "tp_levels": [103.0, 105.0, 108.0], # 1.5R, 2.5R, 4.0R
            "tp_hit_indices": [],
            "position_db_id": 999,
            "partial_done": False,
            "be_armed": False,
            "be_moved": False,
            "max_favorable_r": 0.0
        }
        engine.active_trades[symbol] = test_trade
        
        print(f"🟢 Позиция создана: {symbol} LONG @ {entry_price}, SL: {stop_loss}")

        # --- ТЕСТ 1: Цена достигает 1.5R (103.0) ---
        current_price = 103.1
        print(f"\n📈 ШАГ 1: Цена растет до {current_price} (достигнут уровень 1.5R)")
        
        # Подменяем методы, чтобы не лезть в реальную БД
        engine._db_persist_order = AsyncMock()
        engine._get_live_position = AsyncMock(return_value=(10.0, "LONG"))
        
        # Запускаем менеджмент позиции
        with patch('utils.notifier.send_telegram_msg', AsyncMock()):
            await engine.evaluate_position_management(symbol, current_price, atr=1.0, df_tf=None, adx=25.0, cvd_val=0.0)
        
        # Проверки
        if test_trade.get("partial_done"):
            print("✅ Успех: Флаг partial_done установлен")
        else:
            print("❌ Ошибка: Флаг partial_done НЕ установлен")

        if test_trade.get("be_armed"):
            print("✅ Успех: Флаг be_armed установлен (защита в БУ)")
        else:
            print("❌ Ошибка: Флаг be_armed НЕ установлен")
            
        print(f"📊 Текущий размер позиции в памяти: {test_trade['current_size']} (должен быть ~6.7)")
        
        # --- ТЕСТ 2: Проверка на повторное срабатывание ---
        print("\n📈 ШАГ 2: Проверка на отсутствие повторного срабатывания на той же цене")
        mock_exchange.create_order.reset_mock()
        await engine.evaluate_position_management(symbol, current_price, atr=1.0, df_tf=None, adx=25.0, cvd_val=0.0)
        
        if mock_exchange.create_order.called:
            print("❌ Ошибка: Повторный ордер на частичное закрытие!")
        else:
            print("✅ Успех: Повторного срабатывания нет")

        print("\n" + "="*50)
        print("🏁 SMOKE-ТЕСТ ЗАВЕРШЕН")
        print("="*50)

if __name__ == "__main__":
    tester = TestScalingOutSmoke()
    asyncio.run(tester.run_test())
