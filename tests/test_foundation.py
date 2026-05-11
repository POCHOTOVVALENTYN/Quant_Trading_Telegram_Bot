import unittest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import calculate_indicators
from utils.symbol_normalizer import SymbolNormalizer

class TestFoundation(unittest.TestCase):
    
    def test_indicator_calculations(self):
        print("\n🧪 Тестирование расчетов индикаторов...")
        # Создаем искусственный тренд вверх
        data = {
            "close": [100 + i for i in range(100)],
            "high": [101 + i for i in range(100)],
            "low": [99 + i for i in range(100)],
            "volume": [1000 for _ in range(100)]
        }
        df = pd.DataFrame(data)
        df = calculate_indicators(df)
        
        # Проверка EMA
        self.assertIn("ema50", df.columns)
        self.assertIn("ema200", df.columns)
        # На растущем тренде EMA50 должна быть выше EMA200
        self.assertTrue(df["ema50"].iloc[-1] > df["ema200"].iloc[-1])
        
        # Проверка ATR
        self.assertIn("atr", df.columns)
        self.assertTrue(df["atr"].iloc[-1] > 0)
        
        # Проверка RSI
        self.assertIn("RSI", df.columns)
        self.assertTrue(0 <= df["RSI"].iloc[-1] <= 100)
        print("✅ Индикаторы считаются корректно")

    def test_symbol_normalization(self):
        print("\n🧪 Тестирование нормализатора символов...")
        test_cases = [
            ("BTC/USDT", "BTCUSDT"),
            ("sol/usdt", "SOLUSDT"),
            ("ETHUSDT", "ETHUSDT"),
            ("bnb-usdt", "BNBUSDT"),
        ]
        
        for input_sym, expected in test_cases:
            result = SymbolNormalizer.to_binance(input_sym)
            self.assertEqual(result, expected, f"Ошибка нормализации {input_sym}")
            
        print("✅ Нормализатор работает корректно")

if __name__ == "__main__":
    unittest.main()
