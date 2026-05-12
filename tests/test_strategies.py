import unittest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.strategies.trend_following import StrategyMATrend
from ai.backtest import calculate_indicators

class TestStrategies(unittest.TestCase):
    
    def test_trend_following_signal(self):
        print("\n🧪 Тестирование StrategyMATrend...")
        
        # 1. Сценарий: Пересечение средних вверх (Golden Cross)
        # Сначала цена падает, чтобы EMA20 была ниже EMA50
        prices = [100 * (0.99**i) for i in range(100)] 
        # Затем цена резко растет, чтобы вызвать пересечение
        prices += [prices[-1] * (1.05**i) for i in range(1, 50)]
        
        df_trend = pd.DataFrame({
            "close": prices,
            "high": [p * 1.005 for p in prices],
            "low": [p * 0.995 for p in prices],
            "volume": [1000 for _ in prices]
        })
        df_trend = calculate_indicators(df_trend)
        
        # Гарантируем, что фильтры пропустят сигнал
        last_idx = df_trend.index[-1]
        prev_idx = df_trend.index[-2]
        
        # 1. Пересечение EMA20 > EMA50
        df_trend.loc[prev_idx, 'ema20'] = 100.0
        df_trend.loc[prev_idx, 'ema50'] = 101.0
        df_trend.loc[last_idx, 'ema20'] = 105.0
        df_trend.loc[last_idx, 'ema50'] = 104.0
        
        # 2. Фильтр тренда (ADX, EMA50/200)
        df_trend.loc[last_idx, 'adx'] = 25.0
        df_trend.loc[last_idx, 'ema200'] = 100.0
        df_trend.loc[last_idx, 'RSI_fast'] = 50.0
        df_trend.loc[last_idx, 'atr'] = 2.0 # Для волатильности
        
        strategy = StrategyMATrend()
        signal = strategy.evaluate(df_trend)
        
        self.assertIsNotNone(signal, "Стратегия не увидела идеальный тренд")
        self.assertEqual(signal['signal'], "LONG")
        print("✅ Сигнал LONG на тренде обнаружен")

        # 2. Сценарий: Флэт (Боковик)
        # Цена колеблется вокруг одного значения
        flat_prices = [100 + (2 * (i % 2 - 0.5)) for i in range(300)]
        df_flat = pd.DataFrame({
            "close": flat_prices,
            "high": [p + 1 for p in flat_prices],
            "low": [p - 1 for p in flat_prices],
            "volume": [500 for _ in flat_prices]
        })
        df_flat = calculate_indicators(df_flat)
        
        signal_flat = strategy.evaluate(df_flat)
        self.assertIsNone(signal_flat, "Стратегия выдала ложный сигнал во флэте")
        print("✅ Флэт успешно проигнорирован")

if __name__ == "__main__":
    unittest.main()
