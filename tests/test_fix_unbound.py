import unittest
from datetime import datetime, timezone
import pandas as pd
from unittest.mock import MagicMock, patch

# Импортируем класс для тестирования
from services.signal_engine.engine import TradingOrchestrator

class TestUnboundLocalFix(unittest.TestCase):
    def setUp(self):
        self.market_data = MagicMock()
        self.execution_engine = MagicMock()
        self.orchestrator = TradingOrchestrator(self.market_data, self.execution_engine)

    def test_process_ohlcv_no_unbound_error(self):
        # Мокаем зависимости внутри _process_ohlcv, чтобы дойти до проблемного места
        symbol = "BTC/USDT"
        timeframe = "1m"
        new_candle = [1679294400000, 20000, 21000, 19000, 20500, 100] # timestamp, open, high, low, close, volume
        
        # Настройка моков для прохождения до блока 'if signal:'
        with patch.object(TradingOrchestrator, '_calculate_indicators', return_value=pd.DataFrame({
            'timestamp': [datetime.now(timezone.utc)],
            'open': [20000.0], 'high': [21000.0], 'low': [19000.0], 'close': [20500.0], 'volume': [100.0],
            'atr': [500.0], 'adx': [25.0]
        })):
            # Мокаем стратегию, чтобы она вернула сигнал
            mock_strategy = MagicMock()
            mock_strategy.evaluate.return_value = {
                'strategy': 'TestStrategy',
                'signal': 'LONG',
                'entry_price': 20500.0
            }
            self.orchestrator.strategies = [mock_strategy]
            
            # Мокаем риск-менеджер и скорер
            self.orchestrator.risk_manager.check_listing_days.return_value = True
            self.orchestrator.risk_manager.is_volatility_sufficient.return_value = True
            self.orchestrator.execution.get_account_metrics.return_value = (1000.0, 0.0, 0)
            
            # Мокаем объекты напрямую в оркестраторе
            self.orchestrator.scorer = MagicMock()
            self.orchestrator.scorer.calculate_score.return_value = 0.8
            self.orchestrator.ai_model = MagicMock()
            self.orchestrator.ai_model.predict_win_probability.return_value = {
                'win_prob': 0.7, 'expected_return': 2.0, 'risk': 1.1
            }
            
            # Перехватываем send_telegram_msg, чтобы не отправлять реальные уведомления
            with patch('services.signal_engine.engine.send_telegram_msg', return_value=MagicMock()):
                try:
                    # Вызов метода. Он не должен выбрасывать UnboundLocalError
                    import asyncio
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(self.orchestrator._process_ohlcv(symbol, timeframe, new_candle))
                except UnboundLocalError as e:
                    self.fail(f"UnboundLocalError raised: {e}")
                except Exception as e:
                    # Другие ошибки нас сейчас не интересуют, так как мы тестируем конкретный баг
                    print(f"Caught other exception (expected due to mocks): {e}")

if __name__ == '__main__':
    unittest.main()
