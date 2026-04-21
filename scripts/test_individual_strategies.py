import asyncio
import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Добавляем корень проекта в путь поиска модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.backtest import BacktestEngine, calculate_indicators
from core.strategies.strategies import (
    StrategyDonchian, StrategyWRD, StrategyMATrend, StrategyPullback,
    StrategyVolContraction, StrategyWideRangeReversal, StrategyWilliamsR,
    StrategyFundingSqueeze, StrategyRuleOf7,
)

async def download_data(symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    import ccxt.async_support as ccxt
    exchange = ccxt.binance()
    since = exchange.parse8601((datetime.now() - timedelta(days=days)).isoformat())
    
    print(f"📥 Загрузка данных для {symbol} ({timeframe}) за последние {days} дней...")
    all_ohlcv = []
    while since < exchange.milliseconds():
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since)
        if not ohlcv:
            break
        since = ohlcv[-1][0] + 1
        all_ohlcv.extend(ohlcv)
        if len(ohlcv) < 500:
            break
    
    await exchange.close()
    
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

async def main():
    symbol = "BTC/USDT"
    timeframe = "1h"
    days = 60
    
    # Загружаем данные один раз
    raw_df = await download_data(symbol, timeframe, days)
    if raw_df.empty:
        print("❌ Не удалось загрузить данные.")
        return

    # Список стратегий для тестирования
    strategies_to_test = [
        StrategyDonchian(period=20),
        StrategyWRD(atr_multiplier=1.6),
        StrategyVolContraction(lookback=300, contraction_ratio=0.6),
        StrategyMATrend(fast_ma=20, slow_ma=50, global_ma=200),
        StrategyPullback(ma_period=20, global_period=200),
        StrategyWilliamsR(),
        StrategyWideRangeReversal(),
        StrategyFundingSqueeze(),
        StrategyRuleOf7(),
    ]

    results = []

    print("\n" + "="*50)
    print(f"🚀 ЗАПУСК ИНДИВИДУАЛЬНЫХ БЭКТЕСТОВ ({symbol})")
    print("="*50 + "\n")

    for strategy in strategies_to_test:
        strat_name = type(strategy).__name__
        print(f"🔍 Тестирование стратегии: {strat_name}...")
        
        # Создаем движок с одной конкретной стратегией
        engine = BacktestEngine(
            initial_balance=10000.0,
            leverage=10,
            strategies=[strategy]
        )
        
        # Запускаем бэктест
        # Важно: BacktestEngine.run внутри вызывает calculate_indicators
        res = engine.run(raw_df.copy(), symbol=symbol)
        
        if "error" in res:
            print(f"  ❌ Ошибка: {res['error']}")
            continue
            
        # Собираем основные метрики
        results.append({
            "Стратегия": strat_name,
            "Сделок": res.get("total_trades", 0),
            "Win Rate": f"{res.get('win_rate', 0) * 100:.2f}%",
            "Profit USD": f"{res.get('total_pnl', 0):.2f}$",
            "Max Drawdown": f"{res.get('max_drawdown_pct', 0):.2f}%",
            "Sharpe": f"{res.get('sharpe', 0):.2f}",
            "PF": f"{res.get('profit_factor', 0):.2f}"
        })

    # Вывод итоговой таблицы
    if results:
        results_df = pd.DataFrame(results)
        print("\n" + "="*100)
        print("📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ ПО КАЖДОЙ СТРАТЕГИИ")
        print("="*100)
        print(results_df.to_string(index=False))
        print("="*100 + "\n")
    else:
        print("❌ Нет результатов для отображения.")

if __name__ == "__main__":
    asyncio.run(main())
