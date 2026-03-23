import numpy as np
import logging
from typing import Dict, Any

class AIModel:
    """
    AI Модель для ранжирования сигналов.
    В текущей версии - инференс (предсказание) на основе взвешенных признаков,
    подготовленных для дообучения XGBoost/LightGBM.
    """
    def __init__(self):
        # Веса признаков для Win Probability (балансировка после тестов)
        # Эти коэффициенты будут настраиваться при дообучении модели (Self-learning loop).
        self.weights = {
            'atr_ratio': 0.10,
            'range_relative': 0.05,
            'rsi': 0.1,
            'roc_10': 0.15,
            'price_to_sma20': 0.05,
            'sma20_to_sma50': 0.1,
            'volume_ratio': 0.15,
            'orderbook_imbalance': 0.15,
            'funding_rate': 0.15
        }

    def predict_win_probability(self, features: Dict[str, float], signal_type: str) -> Dict[str, float]:
        """
        Предсказание вероятности успеха сигнала.
        Возвращает: { 'win_prob': 0.0-1.0, 'expected_return': %, 'risk': % }
        """
        if not features:
            return {"win_prob": 0.5, "expected_return": 1.0, "risk": 1.0}

        try:
            score = 0.0
            
            # Объем - ключевой фактор (Баг 5.2)
            score += max(-1.0, min(1.0, (features['volume_ratio'] - 1.0) / 2.0)) * self.weights['volume_ratio']
            
            # Моментум
            if signal_type == "LONG":
                score += (1.0 if features['roc_10'] > 0 else -0.5) * self.weights['roc_10']
                # RSI оптимально для лонга 50-70
                if 50 < features['rsi'] < 75: score += self.weights['rsi']
                elif features['rsi'] > 80: score -= 0.5 * self.weights['rsi'] # Перекупленность
            else:
                score += (1.0 if features['roc_10'] < 0 else -0.5) * self.weights['roc_10']
                # RSI оптимально для шорта 25-50
                if 25 < features['rsi'] < 50: score += self.weights['rsi']
                elif features['rsi'] < 20: score -= 0.5 * self.weights['rsi'] # Перепроданность

            # Трендовость
            trend_align = features['sma20_to_sma50']
            if signal_type == "LONG":
                score += (1.0 if trend_align > 1.0 else -1.0) * self.weights['sma20_to_sma50']
            else:
                score += (1.0 if trend_align < 1.0 else -1.0) * self.weights['sma20_to_sma50']

            # НОВОЕ: Влияние стакана (Imbalance)
            imb = features.get('orderbook_imbalance', 0.0)
            if signal_type == "LONG":
                score += imb * self.weights['orderbook_imbalance']
            else:
                score -= imb * self.weights['orderbook_imbalance']

            # НОВОЕ: Штраф за Funding (Контр-тренд толпы)
            fr = features.get('funding_rate', 0.0)
            if signal_type == "LONG" and fr > 0.01: # Если лонгисты платят много (перегрев)
                score -= 0.5 * self.weights['funding_rate']
            elif signal_type == "SHORT" and fr < -0.01: # Если шортисты платят много
                score -= 0.5 * self.weights['funding_rate']

            # Расчет финальной вероятности (0.0 - 1.0)
            # Базовая вероятность 0.45 + влияние признаков
            win_prob = 0.45 + (score * 0.45) 
            win_prob = max(0.1, min(0.98, win_prob))

            # Расчет мат. ожидания (Expected Return) (Баг L4: убрал *100)
            # При atr_ratio=1 → ~1.5% за сделку (реалистично для крипты)
            expected_return_pct = features['atr_ratio'] * 1.5
            
            # Риск зависит от резкости движений
            risk = 1.0 + (features['range_relative'] * 0.5)

            return {
                "win_prob": round(win_prob, 2),
                "expected_return": round(expected_return_pct, 2),
                "risk": round(risk, 2)
            }
        except Exception as e:
            logging.error(f"Ошибка в AI Model prediction: {e}")
            return {"win_prob": 0.5, "expected_return": 1.5, "risk": 1.0}
