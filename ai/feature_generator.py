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

        # Оптимизация: Берем только последние 100 свечей для расчета признаков (этого достаточно для RSI-14 и ROC-10)
        df_window = df.tail(100)
        last_row = df_window.iloc[-1]
        features = {}
        
        # 1. Волатильность (Volatility) — guard against NaN/zero division
        atr_mean_20 = df_window['atr'].tail(20).mean() if 'atr' in df_window.columns else 0.0
        atr_val = last_row.get('atr', 0.0)
        if pd.isna(atr_mean_20) or atr_mean_20 <= 0 or pd.isna(atr_val):
            features['atr_ratio'] = 1.0
        else:
            features['atr_ratio'] = float(atr_val) / float(atr_mean_20)

        range_mean = (df_window['high'] - df_window['low']).tail(20).mean()
        bar_range = last_row['high'] - last_row['low']
        if pd.isna(range_mean) or range_mean <= 0 or pd.isna(bar_range):
            features['range_relative'] = 1.0
        else:
            features['range_relative'] = float(bar_range) / float(range_mean)
        
        # 2. Моментум (Momentum) — L1: используем готовый RSI из df если есть
        if 'RSI_fast' in df_window.columns:
            features['rsi'] = df_window['RSI_fast'].iloc[-1]
        elif 'RSI' in df_window.columns:
            features['rsi'] = df_window['RSI'].iloc[-1]
        else:
            features['rsi'] = FeatureGenerator._calculate_rsi(df_window['close'], 14).iloc[-1]
        close_10_ago = df_window.iloc[-10]['close'] if len(df_window) >= 10 else last_row['close']
        features['roc_10'] = ((last_row['close'] - close_10_ago) / close_10_ago * 100) if close_10_ago > 0 else 0.0
        
        # 3. Трендовые (Trend)
        sma_20 = df_window['close'].tail(20).mean()
        sma_50 = df_window['close'].tail(50).mean()
        features['price_to_sma20'] = (last_row['close'] / sma_20) if sma_20 > 0 else 1.0
        features['sma20_to_sma50'] = (sma_20 / sma_50) if sma_50 > 0 else 1.0
        
        # 4. Объем (Volume)
        vol_avg_20 = df['volume'].tail(20).mean()
        features['volume_ratio'] = (last_row['volume'] / vol_avg_20) if vol_avg_20 > 0 else 1.0
        
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
