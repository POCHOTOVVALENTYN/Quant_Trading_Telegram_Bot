import unittest
import pandas as pd
import numpy as np
import sys
import os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.feature_generator import FeatureGenerator
from ai.backtest import calculate_indicators
# Попробуем импортировать MLWorker если он есть
try:
    from ai.predictor import AIModel
except ImportError:
    AIModel = None

class TestAILogic(unittest.TestCase):
    
    def test_feature_generator_stability(self):
        print("\n🧪 Тестирование FeatureGenerator...")
        # Создаем данные с NaN и нулями для проверки устойчивости
        data = {
            "close": [100.0] * 100,
            "high": [100.0] * 100,
            "low": [100.0] * 100,
            "volume": [0.0] * 100
        }
        df = pd.DataFrame(data)
        df = calculate_indicators(df) # Добавляем EMA, ATR и т.д.
        
        # Добавляем аномальное значение
        df.loc[99, 'volume'] = 1000.0
        df.loc[99, 'high'] = 110.0
        
        features = FeatureGenerator.generate_features(df, funding_rate=0.01)
        
        # Проверки
        self.assertIsInstance(features, dict)
        self.assertIn('atr_ratio', features)
        self.assertIn('rsi', features)
        self.assertIn('funding_rate', features)
        self.assertEqual(features['funding_rate'], 0.01)
        
        # Проверка на NaN (в фичах не должно быть NaN)
        for name, val in features.items():
            self.assertFalse(np.isnan(val), f"Фича {name} содержит NaN")
            
        print(f"✅ Фичи сгенерированы успешно ({len(features)} шт.)")

    def test_ai_prediction_range(self):
        if AIModel is None:
            print("⚠️ AIModel не найден, пропускаю тест предсказания")
            return
            
        print("\n🧪 Тестирование диапазона предсказаний AIModel...")
        model = AIModel()
        
        # Имитируем фичи
        features = {
            'atr_ratio': 1.2,
            'rsi': 65.0,
            'roc_10': 2.5,
            'price_to_sma20': 1.02,
            'volume_ratio': 1.5,
            'funding_rate': 0.0001,
            'orderbook_imbalance': 0.2,
            'body_pos': 0.8
        }
        
        # Предсказание для LONG
        res_long = model.predict_win_probability(features, "LONG")
        self.assertIn('win_prob', res_long)
        self.assertTrue(0.0 <= res_long['win_prob'] <= 1.0)
        
        # Предсказание для SHORT
        res_short = model.predict_win_probability(features, "SHORT")
        self.assertTrue(0.0 <= res_short['win_prob'] <= 1.0)
        
        print(f"✅ Предсказания AI в корректном диапазоне: Long={res_long['win_prob']:.2f}")

if __name__ == "__main__":
    unittest.main()
