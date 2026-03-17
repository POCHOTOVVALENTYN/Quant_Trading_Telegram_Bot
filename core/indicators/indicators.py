import numpy as np
import pandas as pd

def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """
    Рассчитывает простую скользящую среднюю (SMA).
    
    SMA = average(close, n)
    """
    return series.rolling(window=period).mean()

def calculate_true_range(high: pd.Series, low: pd.Series, prev_close: pd.Series) -> pd.Series:
    """
    Рассчитывает True Range (Истинный диапазон).
    
    TR = max(
        high-low,
        |high-prev_close|,
        |low-prev_close|
    )
    """
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Рассчитывает Average True Range (ATR).
    
    ATR = MovingAverage(TR, n)
    Ожидает df со столбцами 'high', 'low', 'close'
    """
    if 'prev_close' not in df.columns:
        df['prev_close'] = df['close'].shift(1)
        
    df['tr'] = calculate_true_range(df['high'], df['low'], df['prev_close'])
    
    # Для ATR обычно используется скользящее среднее RMA, но по формуле из книги 
    # указана (SMA) или (EMA). Реализуем RMA (по стандарту Уайлдера) или SMA.
    # В книге "Technical Analysis" обычно подразумевается сглаживание по Уайлдеру.
    atr = df['tr'].ewm(alpha=1/period, adjust=False).mean()
    
    return atr
