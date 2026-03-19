import pandas as pd
from typing import Dict, Any, Optional
from utils.logger import app_logger

class SpreadMomentumStrategy:
    """
    Стратегия из статьи Hubr: Арбитражный импульс между средней ценой и фьючерсом.
    Использует отклонение текущей цены фьючерса от средневзвешенной цены за 5 минут.
    """
    def __init__(self, growth_threshold_pct: float = 0.053):
        # По умолчанию 0.053 для BTC по рекомендации из статьи
        self.growth_threshold_pct = growth_threshold_pct

    async def calculate_signal_strength(self, avg_price: float, current_price: float) -> Optional[Dict[str, Any]]:
        """
        Расчет силы сигнала на основе отклонения.
        avg_price: взвешенная средняя за 5 мин (fetch_avg_price)
        current_price: текущая цена фьючерса
        """
        if avg_price <= 0: return None
        
        # Спред в процентах
        spread_pct = ((current_price - avg_price) / avg_price) * 100
        abs_spread = abs(spread_pct)
        
        # Если отклонение выше порога
        if abs_spread >= self.growth_threshold_pct:
            direction = "LONG" if spread_pct > 0 else "SHORT"
            
            # Шкала силы 1-10 (из статьи)
            # Если порог 0.053, то сила 10 будет при 0.53% (в 10 раз больше порога)
            strength = min(10, int((abs_spread / self.growth_threshold_pct)))
            
            return {
                "symbol": None, # Устанавливается оркестратором
                "signal": direction,
                "spread_pct": spread_pct,
                "strength": strength,
                "strategy": "SPREAD_MOMENTUM"
            }
            
        return None
