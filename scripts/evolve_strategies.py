import asyncio
import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta

# Добавляем корень проекта в путь поиска модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.optimizer.optimizer import StrategyOptimizer, OptimizationConfig
from core.strategies.strategies import (
    StrategyDonchian, StrategyWRD, StrategyMATrend, StrategyPullback,
    StrategyVolContraction, StrategyWideRangeReversal, StrategyWilliamsR,
    StrategyFundingSqueeze, StrategyRuleOf7,
    StrategyBollingerMR, StrategyFakeout,
)

async def download_data(symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    import ccxt.async_support as ccxt
    exchange = ccxt.binance()
    since = exchange.parse8601((datetime.now() - timedelta(days=days)).isoformat())
    
    print(f"📥 Загрузка данных для {symbol} ({timeframe}) за последние {days} дней...")
    all_ohlcv = []
    try:
        while since < exchange.milliseconds():
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since)
            if not ohlcv: break
            since = ohlcv[-1][0] + 1
            all_ohlcv.extend(ohlcv)
            if len(ohlcv) < 500: break
    finally:
        await exchange.close()
    
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

async def main():
    symbol = "BTC/USDT"
    timeframe = "1h"
    days = 90  # Для оптимизации берем больше данных
    
    raw_df = await download_data(symbol, timeframe, days)
    if raw_df.empty:
        print("❌ Не удалось загрузить данные.")
        return

    # Настраиваем оптимизатор: 20 вариаций на каждую стратегию
    optimizer = StrategyOptimizer(config=OptimizationConfig(variant_count=20, top_n=1))
    
    strategy_classes = [
        StrategyDonchian, StrategyWRD, StrategyMATrend, StrategyPullback,
        StrategyVolContraction, StrategyWideRangeReversal, StrategyWilliamsR,
        StrategyFundingSqueeze, StrategyRuleOf7,
        StrategyBollingerMR, StrategyFakeout,
    ]

    print("\n" + "="*60)
    print(f"🤖 ЗАПУСК AI STRATEGY OPTIMIZER (Эволюция параметров)")
    print("="*60 + "\n")

    # Запускаем цикл: мутация -> бэктест -> оценка -> отбор
    best_results = optimizer.optimize(
        df=raw_df,
        strategy_classes=strategy_classes
    )

    summary = []
    for name, res in best_results.items():
        summary.append({
            "Стратегия": name,
            "Score": res.score,
            "Profit": f"{res.metrics['profit']:.2f}$",
            "WinRate": f"{res.metrics['winrate']*100:.2f}%",
            "DD": f"{res.metrics['drawdown']:.2f}%",
            "Params": json.dumps(res.parameters['strategy_params'])
        })

    # Вывод результатов
    summary_df = pd.DataFrame(summary)
    print("\n" + "="*140)
    print("🏆 ЛУЧШИЕ ПАРАМЕТРЫ ПОСЛЕ ЭВОЛЮЦИИ")
    print("="*140)
    print(summary_df.to_string(index=False))
    print("="*140 + "\n")

    # Сохранение лучших параметров в файл
    output_path = "data/optimized_parameters.json"
    os.makedirs("data", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({name: res.parameters for name, res in best_results.items()}, f, indent=4)
    print(f"✅ Оптимизированные параметры сохранены в {output_path}")

if __name__ == "__main__":
    asyncio.run(main())
