import pandas as pd
import numpy as np
from typing import Dict, Any

class FeatureGenerator:
    """
    Генератор признаков (Features) для AI-модели.
    Превращает сырые данные OHLCV в вектор признаков для оценки вероятности успеха.
    """
    @staticmethod
    def generate_features(df: pd.DataFrame, funding_rate: float = 0.0, orderbook: Dict[str, Any] = None) -> Dict[str, float]:
        if len(df) < 50:
            return {}

        last_row = df.iloc[-1]
        features = {}
        
        # 1. Волатильность (Volatility)
        features['atr_ratio'] = last_row['atr'] / df['atr'].tail(20).mean() if 'atr' in df else 1.0
        features['range_relative'] = (last_row['high'] - last_row['low']) / (df['high'] - df['low']).tail(20).mean()
        
        # 2. Моментум (Momentum)
        features['rsi'] = FeatureGenerator._calculate_rsi(df['close'], 14).iloc[-1]
        features['roc_10'] = ((last_row['close'] - df.iloc[-10]['close']) / df.iloc[-10]['close']) * 100
        
        # 3. Трендовые (Trend)
        sma_20 = df['close'].tail(20).mean()
        sma_50 = df['close'].tail(50).mean()
        features['price_to_sma20'] = last_row['close'] / sma_20
        features['sma20_to_sma50'] = sma_20 / sma_50
        
        # 4. Объем (Volume)
        features['volume_ratio'] = last_row['volume'] / df['volume'].tail(20).mean()
        
        # 5. Новое: Внешние данные (Funding & Orderbook)
        features['funding_rate'] = funding_rate
        
        imbalance = 0.0
        if orderbook and 'bids' in orderbook and 'asks' in orderbook:
            bid_vol = sum([b[1] for b in orderbook['bids'][:10]])
            ask_vol = sum([a[1] for a in orderbook['asks'][:10]])
            if (bid_vol + ask_vol) > 0:
                imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)
        features['orderbook_imbalance'] = imbalance
        
        # 6. Свечные паттерны (упрощенно)
        features['body_pos'] = (last_row['close'] - last_row['low']) / (last_row['high'] - last_row['low'] + 1e-9)
        
        return features

    @staticmethod
    def _calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
