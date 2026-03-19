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

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Рассчитывает Relative Strength Index (RSI).
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    
    # Чтобы избежать деления на ноль
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)

def calculate_bollinger_bands(series: pd.Series, period: int = 20, std: float = 2.0):
    """
    Рассчитывает Полосы Боллинджера (Bollinger Bands).
    """
    ma = series.rolling(window=period).mean()
    std_dev = series.rolling(window=period).std()
    upper = ma + (std_dev * std)
    lower = ma - (std_dev * std)
    return upper, ma, lower

def calculate_csi(df: pd.DataFrame, atr_period: int = 14) -> pd.Series:
    """
    Cluster Strength Index (CSI) — авторский индикатор из статьи.
    CSI = direction * (0.5 * body_ratio + 0.3 * vol_score + 0.2 * range_z) / ATR
    """
    from scipy.stats import zscore
    
    # 1. Body Ratio (Тело / Диапазон)
    body = (df['close'] - df['open']).abs()
    rng = (df['high'] - df['low']).replace(0, np.nan)
    body_ratio = (body / rng).fillna(0)
    
    # 2. Direction
    direction = np.where(df['close'] > df['open'], 1, -1)
    
    # 3. Vol Score (Текущий объем / Максимальный за 50)
    vol_score = (df['volume'] / df['volume'].rolling(50).max()).fillna(0)
    
    # 4. Range Z-score (Нормализация волатильности)
    range_val = (df['high'] - df['low'])
    # Берём Z-score для последних 100 свечей для стабильности (не всего df)
    range_z = range_val.rolling(100).apply(lambda x: (x.iloc[-1] - x.mean()) / x.std() if x.std() > 0 else 0).fillna(0)
    
    # 5. ATR (Упрощенный для CSI)
    tr = pd.DataFrame({
        'hl': df['high'] - df['low'],
        'hc': (df['high'] - df['close'].shift(1)).abs(),
        'lc': (df['low'] - df['close'].shift(1)).abs()
    }).max(axis=1)
    atr = tr.rolling(atr_period).mean().bfill()
    
    csi = direction * (0.5 * body_ratio + 0.3 * vol_score + 0.2 * range_z) / atr
    return csi.fillna(0)
