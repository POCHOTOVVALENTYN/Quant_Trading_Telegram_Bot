import asyncio
import os
import sys
import json
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
    StrategyBollingerMR, StrategyFakeout,
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
    symbols = [
        "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", 
        "ADA/USDT", "DOT/USDT", "LINK/USDT", "NEAR/USDT", 
        "DOGE/USDT", "XRP/USDT", "LTC/USDT", "AVAX/USDT"
    ]
    timeframes = ["15m", "1h", "4h"]
    days = 60
    
    # Загружаем оптимизированные параметры, если они есть
    params_path = "data/optimized_parameters.json"
    optimized_params = {}
    if os.path.exists(params_path):
        with open(params_path, "r") as f:
            optimized_params = json.load(f)
        print(f"✨ Загружены оптимизированные параметры из {params_path}")

    strategies_to_test = []
    
    # Инициализируем стратегии с оптимизированными параметрами
    strat_map = {
        "Donchian": StrategyDonchian,
        "WRD": StrategyWRD,
        "Vol Contraction": StrategyVolContraction,
        "MA Trend": StrategyMATrend,
        "Pullback": StrategyPullback,
        "Williams R": StrategyWilliamsR,
        "WRD Reversal": StrategyWideRangeReversal,
        "Funding Squeeze": StrategyFundingSqueeze,
        "Rule of 7": StrategyRuleOf7,
        "BB Mean Reversion": StrategyBollingerMR,
        "Fakeout": StrategyFakeout,
    }

    for name, cls in strat_map.items():
        if name in optimized_params:
            p = optimized_params[name]
            strategies_to_test.append(cls(**p.get("strategy_params", {})))
        else:
            # Дефолтные параметры если нет оптимизированных
            if name == "Donchian": strategies_to_test.append(cls(period=20))
            elif name == "WRD": strategies_to_test.append(cls(atr_multiplier=1.6))
            elif name == "Vol Contraction": strategies_to_test.append(cls(lookback=300, contraction_ratio=0.6))
            elif name == "MA Trend": strategies_to_test.append(cls(fast_ma=20, slow_ma=50, global_ma=200))
            elif name == "Pullback": strategies_to_test.append(cls(ma_period=20, global_period=200))
            else: strategies_to_test.append(cls())

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
                # Получаем имя стратегии для поиска в оптимизированных параметрах
                display_name = next((k for k, v in strat_map.items() if v == type(strategy)), strat_name)
                
                risk_p = optimized_params.get(display_name, {}).get("risk_params", {})
                
                engine = BacktestEngine(
                    initial_balance=10000.0,
                    leverage=10,
                    strategies=[strategy],
                    sl_multiplier=float(risk_p.get("sl_multiplier", 2.0)),
                    tp_multiplier=float(risk_p.get("tp_multiplier", 3.0))
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
