import asyncio
import os
import sys
import random
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Root path alignment
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.backtest import BacktestEngine, fetch_candles, calculate_indicators
from core.strategies.strategies import (
    StrategyDonchian, StrategyWRD, StrategyMATrend, 
    StrategyPullback, StrategyVolContraction
)

class StrategyEvolver:
    def __init__(self, symbol="BTC/USDT", timeframe="1h", days=60):
        self.symbol = symbol
        self.timeframe = timeframe
        self.days = days
        self.results = []

    def generate_variations(self, strategy_type, base_params: Dict[str, Any], count=10):
        variations = []
        for _ in range(count):
            var = base_params.copy()
            for key, val in var.items():
                if isinstance(val, int):
                    # Randomize int within +/- 50%
                    offset = max(1, int(val * 0.5))
                    var[key] = max(2, val + random.randint(-offset, offset))
                elif isinstance(val, float):
                    # Randomize float within +/- 30%
                    var[key] = round(val * random.uniform(0.7, 1.3), 3)
            variations.append(var)
        return variations

    async def evolve(self):
        print(f"🚀 Starting Evolution Mode for {self.symbol} ({self.days} days)...")
        df = await fetch_candles(self.symbol, self.timeframe, self.days)
        print(f"📊 Downloaded {len(df)} candles.")
        
        # Out-of-Sample split: 70% Train, 30% Test
        split_idx = int(len(df) * 0.7)
        train_df = df.iloc[:split_idx].copy()
        test_df = df.iloc[split_idx:].copy()
        print(f"📈 Train: {len(train_df)} bars | Test: {len(test_df)} bars")

        # Strategies to optimize
        strategy_configs = [
            {"type": StrategyDonchian, "name": "Donchian", "params": {"period": 20}},
            {"type": StrategyWRD, "name": "WRD", "params": {"atr_multiplier": 1.6}},
            {"type": StrategyMATrend, "name": "MATrend", "params": {"fast_ma": 20, "slow_ma": 50, "global_ma": 200}},
            {"type": StrategyPullback, "name": "Pullback", "params": {"ma_period": 20, "global_period": 200}},
        ]

        best_overall = {}

        for config in strategy_configs:
            print(f"\n🧬 Evolving {config['name']}...")
            variations = self.generate_variations(config['type'], config['params'], count=15)
            # Add base variation as control
            variations.append(config['params'])
            
            strat_results = []
            for i, params in enumerate(variations):
                # 1. TRAIN
                strat_instance = config['type'](**params)
                engine_train = BacktestEngine(initial_balance=10000)
                engine_train.strategies = [strat_instance]
                res_train = engine_train.run(train_df.copy())
                
                if res_train.get('total_trades', 0) > 2: # Min trades filter
                    # 2. TEST (Verification)
                    engine_test = BacktestEngine(initial_balance=10000)
                    engine_test.strategies = [strat_instance]
                    res_test = engine_test.run(test_df.copy())
                    
                    metrics = {
                        "params": params,
                        "sharpe_train": res_train.get("sharpe", 0),
                        "sharpe_test": res_test.get("sharpe", 0),
                        "total_trades": res_train.get("total_trades", 0) + res_test.get("total_trades", 0),
                        "final_pnl": res_train.get("total_pnl", 0) + res_test.get("total_pnl", 0)
                    }
                    
                    # Accept only if both positive or test not too bad
                    if metrics["sharpe_train"] > 0:
                        strat_results.append(metrics)
                        print(f"  [{i}] PnL: ${metrics['final_pnl']:.2f} | Sharpe (Tr/Te): {metrics['sharpe_train']:.2f}/{metrics['sharpe_test']:.2f}")
                else:
                    print(f"  [{i}] Not enough trades in Train.")

            # Selection: Sort by Test Sharpe primarily, then Train Sharpe
            strat_results.sort(key=lambda x: (x['sharpe_test'], x['sharpe_train']), reverse=True)
            
            if strat_results:
                best = strat_results[0]
                best_overall[config['name']] = best
                print(f"🏆 Best for {config['name']}: {best['params']} (Test Sharpe: {best['sharpe_test']:.3f})")

        # Save results
        os.makedirs("data", exist_ok=True)
        with open("data/optimized_parameters.json", "w") as f:
            json.dump(best_overall, f, indent=2)
        print(f"\n✅ Evolution complete. Best parameters saved to data/optimized_parameters.json")

if __name__ == "__main__":
    evolver = StrategyEvolver(days=60)
    asyncio.run(evolver.evolve())
