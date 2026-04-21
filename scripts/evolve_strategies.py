"""
Evolution mode CLI: 10 variants per strategy, train/OOS split, top-3 by Sharpe / DD / PF.

Usage (from project root):
    python3 scripts/evolve_strategies.py --symbol BTC/USDT --timeframe 1h --days 90
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Tuple, Type

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.backtest import fetch_candles
from core.optimizer.evolution import EvolutionConfig, StrategyEvolutionRunner
from core.optimizer.mutator import StrategyMutator
from core.strategies.strategies import (
    StrategyDonchian,
    StrategyFundingSqueeze,
    StrategyMATrend,
    StrategyPullback,
    StrategyRuleOf7,
    StrategyVolContraction,
    StrategyWRD,
    StrategyWideRangeReversal,
    StrategyWilliamsR,
)


def _specs() -> List[Tuple[Type, Dict[str, Any]]]:
    return [
        (StrategyDonchian, {"period": 20}),
        (StrategyWRD, {"atr_multiplier": 1.6}),
        (StrategyMATrend, {"fast_ma": 20, "slow_ma": 50, "global_ma": 200}),
        (StrategyPullback, {"ma_period": 20, "global_period": 200}),
        (StrategyVolContraction, {"lookback": 300, "contraction_ratio": 0.6}),
        (StrategyWideRangeReversal, {}),
        (StrategyWilliamsR, {}),
        (StrategyFundingSqueeze, {}),
        (StrategyRuleOf7, {"bars": 7}),
    ]


async def main() -> None:
    parser = argparse.ArgumentParser(description="Strategy evolution (train/OOS, top-3 metrics)")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--timeframe", default="1h")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--variants", type=int, default=10)
    parser.add_argument("--train-ratio", type=float, default=0.7, dest="train_ratio")
    args = parser.parse_args()

    print(f"Evolution: {args.symbol} {args.timeframe} | {args.days} days | {args.variants} variants/strategy")
    df = await fetch_candles(args.symbol, args.timeframe, args.days)
    print(f"Downloaded {len(df)} candles.")

    cfg = EvolutionConfig(
        variant_count=args.variants,
        train_ratio=args.train_ratio,
    )
    runner = StrategyEvolutionRunner(config=cfg)
    all_results: Dict[str, Any] = {}

    for strat_cls, base_params in _specs():
        label = StrategyMutator._strategy_name(strat_cls)
        print(f"\n--- Evolving {label} ---")
        res = runner.evolve_strategy(
            df=df,
            strategy_cls=strat_cls,
            base_strategy_params=base_params,
        )
        all_results[label] = res
        if res.get("error"):
            print(f"  skipped: {res.get('error')}")
            continue
        best = res.get("best_composite_oos") or {}
        print(
            f"  best OOS composite={best.get('composite_oos_score')} | "
            f"replace baseline={res.get('replace_baseline_recommended')}"
        )
        print(f"  baseline OOS score={(res.get('baseline') or {}).get('composite_oos_score')}")

    os.makedirs("data", exist_ok=True)
    out_path = "data/evolution_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nSaved {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
