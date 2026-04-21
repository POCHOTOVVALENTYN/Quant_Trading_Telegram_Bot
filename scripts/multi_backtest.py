import asyncio
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from typing import List

# Добавляем корень проекта в путь поиска модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.backtest import BacktestEngine
from core.strategies.strategies import (
    StrategyDonchian, StrategyWRD, StrategyMATrend, StrategyPullback,
    StrategyVolContraction, StrategyWideRangeReversal, StrategyWilliamsR,
    StrategyFundingSqueeze, StrategyRuleOf7,
)

async def download_data(symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    import ccxt.async_support as ccxt
    exchange = ccxt.binance()
    since = exchange.parse8601((datetime.now() - timedelta(days=days)).isoformat())
    
    print(f"📥 Загрузка {symbol} ({timeframe})...", end="", flush=True)
    all_ohlcv = []
    try:
        while since < exchange.milliseconds():
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since)
            if not ohlcv:
                break
            since = ohlcv[-1][0] + 1
            all_ohlcv.extend(ohlcv)
            if len(ohlcv) < 500:
                break
        print(" OK")
    except Exception as e:
        print(f" ERROR: {e}")
        return pd.DataFrame()
    finally:
        await exchange.close()
    
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

async def main():
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    timeframes = ["15m", "1h", "4h"]
    days = 60
    
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

    all_results = []

    print("\n" + "="*70)
    print(f"🚀 ЗАПУСК МАССОВОГО БЭКТЕСТА ({len(symbols)} монет, {len(timeframes)} таймфрейма)")
    print("="*70 + "\n")

    for symbol in symbols:
        for tf in timeframes:
            raw_df = await download_data(symbol, tf, days)
            if raw_df.empty or len(raw_df) < 250:
                print(f"⚠️ Недостаточно данных для {symbol} {tf}")
                continue

            for strategy in strategies_to_test:
                strat_name = type(strategy).__name__
                
                engine = BacktestEngine(
                    initial_balance=10000.0,
                    leverage=10,
                    strategies=[strategy]
                )
                
                res = engine.run(raw_df.copy(), symbol=symbol)
                
                if "error" in res or res.get("total_trades", 0) == 0:
                    continue
                    
                all_results.append({
                    "Монета": symbol,
                    "ТФ": tf,
                    "Стратегия": strat_name,
                    "Сделок": res.get("total_trades", 0),
                    "Win Rate": res.get("win_rate", 0) * 100,
                    "Profit USD": res.get("total_pnl", 0),
                    "Max DD %": res.get("max_drawdown_pct", 0),
                    "Sharpe": res.get("sharpe", 0),
                    "PF": res.get("profit_factor", 0)
                })

    # Вывод результатов
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        # Сортировка по прибыли
        results_df = results_df.sort_values(by="Profit USD", ascending=False)
        
        print("\n" + "="*110)
        print("📊 ТОП-20 ЛУЧШИХ КОМБИНАЦИЙ (Сортировка по Profit USD)")
        print("="*110)
        print(results_df.head(20).to_string(index=False))
        print("="*110 + "\n")
        
        # Сохранение в CSV для анализа
        results_df.to_csv("multi_backtest_results.csv", index=False)
        print("✅ Все результаты сохранены в multi_backtest_results.csv")
    else:
        print("❌ Нет прибыльных сделок для отображения.")

if __name__ == "__main__":
    asyncio.run(main())
