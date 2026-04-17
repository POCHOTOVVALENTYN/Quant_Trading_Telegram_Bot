"""
Backtest module — evaluate strategies on historical OHLCV data.

Usage (from project root):
    python3 -m ai.backtest --symbol BTC/USDT --timeframe 1h --days 30

This module:
  1. Downloads historical candles via ccxt
  2. Runs all strategies + scoring + AI model on each bar
  3. Simulates SL/TP execution
  4. Outputs per-strategy and aggregate statistics
"""
import asyncio
import argparse
import json
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.strategies.strategies import (
    StrategyDonchian, StrategyWRD, StrategyMATrend, StrategyPullback,
    StrategyVolContraction, StrategyWideRangeReversal, StrategyWilliamsR,
    StrategyFundingSqueeze, StrategyRuleOf7,
)
from core.indicators.indicators import (
    calculate_atr, calculate_rsi, calculate_ema, calculate_adx, calculate_williams_r,
)
from core.strategies.scoring import SignalScorer
from ai.feature_generator import FeatureGenerator
from ai.model import AIModel
from core.risk.risk_manager import RiskManager


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all indicators to a DataFrame (same as orchestrator)."""
    df['ema20'] = calculate_ema(df['close'], 20)
    df['ema50'] = calculate_ema(df['close'], 50)
    df['ema200'] = calculate_ema(df['close'], 200)
    df['atr'] = calculate_atr(df, period=14)
    df['RSI'] = calculate_rsi(df['close'], period=21)
    df['RSI_fast'] = calculate_rsi(df['close'], period=14)

    adx_df = calculate_adx(df, period=14)
    df['adx'] = adx_df['adx']

    df['williams_r'] = calculate_williams_r(df, period=14)
    df['vol_ma20'] = df['volume'].rolling(20).mean()
    df['roc10'] = df['close'].pct_change(10)
    df['funding_rate'] = 0.0

    return df


class BacktestEngine:
    def __init__(
        self,
        initial_balance: float = 10000.0,
        leverage: int = 10,
        sl_multiplier: float = 2.0,
        tp_multiplier: float = 3.0,
        risk_per_trade_pct: float = 0.05,
        score_threshold: float = 0.55,
        ai_threshold: float = 0.55,
    ):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.leverage = leverage
        self.sl_mult = sl_multiplier
        self.tp_mult = tp_multiplier
        self.risk_pct = risk_per_trade_pct
        self.score_threshold = score_threshold
        self.ai_threshold = ai_threshold

        self.strategies = [
            StrategyDonchian(period=20),
            StrategyWRD(atr_multiplier=1.6),
            StrategyVolContraction(lookback=300, contraction_ratio=0.6),
            StrategyMATrend(fast_ma=20, slow_ma=50, global_ma=200),
            StrategyPullback(ma_period=20, global_period=200),
            StrategyWilliamsR(),
            StrategyWideRangeReversal(),
            StrategyFundingSqueeze(),
        ]
        self.scorer = SignalScorer()
        self.ai_model = AIModel()
        self.risk = RiskManager()

        self.trades: List[Dict] = []
        self.equity_curve: List[float] = []
        self.signals_generated = 0
        self.signals_filtered = 0

    def run(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run backtest on OHLCV DataFrame with pre-calculated indicators."""
        if len(df) < 200:
            return {"error": "Need at least 200 bars"}

        df = calculate_indicators(df)
        open_trade: Optional[Dict] = None
        warmup = 200

        for i in range(warmup, len(df)):
            bar = df.iloc[i]
            self.equity_curve.append(self.balance)

            # Check open trade exit
            if open_trade:
                is_long = open_trade["direction"] == "LONG"
                sl = open_trade["sl"]
                tp = open_trade["tp"]

                # SL hit
                if is_long and bar['low'] <= sl:
                    pnl = (sl - open_trade["entry"]) * open_trade["size"]
                    self.balance += pnl
                    open_trade["exit_price"] = sl
                    open_trade["exit_reason"] = "SL"
                    open_trade["pnl"] = pnl
                    open_trade["pnl_pct"] = (sl / open_trade["entry"] - 1) * 100
                    self.trades.append(open_trade)
                    open_trade = None
                elif not is_long and bar['high'] >= sl:
                    pnl = (open_trade["entry"] - sl) * open_trade["size"]
                    self.balance += pnl
                    open_trade["exit_price"] = sl
                    open_trade["exit_reason"] = "SL"
                    open_trade["pnl"] = pnl
                    open_trade["pnl_pct"] = (1 - sl / open_trade["entry"]) * 100
                    self.trades.append(open_trade)
                    open_trade = None

                # TP hit
                elif is_long and bar['high'] >= tp:
                    pnl = (tp - open_trade["entry"]) * open_trade["size"]
                    self.balance += pnl
                    open_trade["exit_price"] = tp
                    open_trade["exit_reason"] = "TP"
                    open_trade["pnl"] = pnl
                    open_trade["pnl_pct"] = (tp / open_trade["entry"] - 1) * 100
                    self.trades.append(open_trade)
                    open_trade = None
                elif not is_long and bar['low'] <= tp:
                    pnl = (open_trade["entry"] - tp) * open_trade["size"]
                    self.balance += pnl
                    open_trade["exit_price"] = tp
                    open_trade["exit_reason"] = "TP"
                    open_trade["pnl"] = pnl
                    open_trade["pnl_pct"] = (1 - tp / open_trade["entry"]) * 100
                    self.trades.append(open_trade)
                    open_trade = None
                else:
                    continue

            if open_trade:
                continue

            # Evaluate strategies on closed bars (up to i, excluding i)
            eval_df = df.iloc[max(0, i - 600):i].copy()
            if len(eval_df) < 60:
                continue

            atr = eval_df['atr'].iloc[-1]
            adx = eval_df['adx'].iloc[-1]
            if pd.isna(atr) or atr <= 0:
                continue

            candidates = []
            for strategy in self.strategies:
                signal = strategy.evaluate(eval_df)
                if not signal:
                    continue

                self.signals_generated += 1

                # ADX filter for trend strategies
                trend_strats = ["MA Trend", "Donchian", "Pullback"]
                if signal['strategy'] in trend_strats and adx < 20:
                    self.signals_filtered += 1
                    continue

                score = self.scorer.calculate_score(eval_df, signal)
                if score < self.score_threshold:
                    self.signals_filtered += 1
                    continue

                features = FeatureGenerator.generate_features(eval_df)
                ai = self.ai_model.predict_win_probability(features, signal['signal'])
                if ai['win_prob'] < self.ai_threshold:
                    self.signals_filtered += 1
                    continue

                candidates.append({
                    "signal": signal,
                    "score": score,
                    "win_prob": ai['win_prob'],
                    "prio": score * ai['win_prob']
                })

            if candidates:
                candidates.sort(key=lambda x: x['prio'], reverse=True)
                top = candidates[0]
                signal = top['signal']
                score = top['score']
                win_prob = top['win_prob']

                # Execute trade
                entry = bar['close']
                direction = signal['signal']
                sl_price = self.risk.calculate_atr_stop(entry, atr, direction, self.sl_mult)

                if direction == "LONG":
                    tp_price = entry + abs(entry - sl_price) * self.tp_mult
                else:
                    tp_price = entry - abs(entry - sl_price) * self.tp_mult

                margin = self.balance * self.risk_pct
                size = (margin * self.leverage) / entry

                open_trade = {
                    "entry": entry,
                    "sl": sl_price,
                    "tp": tp_price,
                    "direction": direction,
                    "strategy": signal['strategy'],
                    "size": size,
                    "score": score,
                    "win_prob": win_prob,
                    "bar_idx": i,
                }

        return self._compile_results()

    def _compile_results(self) -> Dict[str, Any]:
        n_trades = len(self.trades)
        if n_trades == 0:
            return {
                "total_trades": 0,
                "message": "No trades executed",
                "signals_generated": self.signals_generated,
                "signals_filtered": self.signals_filtered,
            }

        wins = [t for t in self.trades if t["pnl"] > 0]
        losses = [t for t in self.trades if t["pnl"] <= 0]
        pnls = [t["pnl"] for t in self.trades]

        # Max drawdown
        peak = self.initial_balance
        max_dd = 0
        running = self.initial_balance
        for t in self.trades:
            running += t["pnl"]
            peak = max(peak, running)
            dd = (peak - running) / peak
            max_dd = max(max_dd, dd)

        # Per-strategy breakdown
        strat_stats = {}
        for t in self.trades:
            name = t["strategy"]
            if name not in strat_stats:
                strat_stats[name] = {"trades": 0, "wins": 0, "total_pnl": 0.0, "pnls": []}
            strat_stats[name]["trades"] += 1
            if t["pnl"] > 0:
                strat_stats[name]["wins"] += 1
            strat_stats[name]["total_pnl"] += t["pnl"]
            strat_stats[name]["pnls"].append(t["pnl"])

        for name, s in strat_stats.items():
            s["win_rate"] = round(s["wins"] / s["trades"], 4) if s["trades"] > 0 else 0
            s["avg_pnl"] = round(np.mean(s["pnls"]), 2)
            s["sharpe"] = round(np.mean(s["pnls"]) / (np.std(s["pnls"]) + 1e-9), 4)
            del s["pnls"]

        avg_win = np.mean([t["pnl"] for t in wins]) if wins else 0
        avg_loss = abs(np.mean([t["pnl"] for t in losses])) if losses else 1

        return {
            "total_trades": n_trades,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / n_trades, 4),
            "total_pnl": round(sum(pnls), 2),
            "final_balance": round(self.balance, 2),
            "return_pct": round((self.balance / self.initial_balance - 1) * 100, 2),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(abs(np.mean([t["pnl"] for t in losses])) if losses else 0, 2),
            "profit_factor": round(avg_win / avg_loss, 4) if avg_loss > 0 else float('inf'),
            "sharpe": round(np.mean(pnls) / (np.std(pnls) + 1e-9), 4),
            "signals_generated": self.signals_generated,
            "signals_filtered": self.signals_filtered,
            "per_strategy": strat_stats,
        }


async def fetch_candles(symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    """Download historical candles via ccxt."""
    import ccxt.pro as ccxtpro

    exchange = ccxtpro.binance({"enableRateLimit": True})
    try:
        since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
        all_candles = []
        limit = 1000

        while True:
            candles = await exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
            if not candles:
                break
            all_candles.extend(candles)
            since = candles[-1][0] + 1
            if len(candles) < limit:
                break
            await asyncio.sleep(0.2)

        df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df
    finally:
        await exchange.close()


async def main():
    parser = argparse.ArgumentParser(description="Backtest trading strategies")
    parser.add_argument("--symbol", default="BTC/USDT", help="Trading pair")
    parser.add_argument("--timeframe", default="1h", help="Candle timeframe")
    parser.add_argument("--days", type=int, default=30, help="History days")
    parser.add_argument("--balance", type=float, default=10000, help="Initial balance USD")
    args = parser.parse_args()

    print(f"\nBacktest: {args.symbol} {args.timeframe} | {args.days} days | ${args.balance}")
    print("Fetching historical data...")

    df = await fetch_candles(args.symbol, args.timeframe, args.days)
    print(f"Downloaded {len(df)} candles")

    engine = BacktestEngine(initial_balance=args.balance)
    results = engine.run(df)

    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"Total trades:     {results.get('total_trades', 0)}")
    print(f"Win rate:         {results.get('win_rate', 0):.1%}")
    print(f"Total PnL:        ${results.get('total_pnl', 0):.2f}")
    print(f"Return:           {results.get('return_pct', 0):.2f}%")
    print(f"Max drawdown:     {results.get('max_drawdown_pct', 0):.2f}%")
    print(f"Profit factor:    {results.get('profit_factor', 0):.2f}")
    print(f"Sharpe:           {results.get('sharpe', 0):.4f}")
    print(f"Signals gen:      {results.get('signals_generated', 0)}")
    print(f"Signals filtered: {results.get('signals_filtered', 0)}")

    print("\nPer Strategy:")
    for name, s in results.get("per_strategy", {}).items():
        print(f"  {name:20s} — {s['trades']} trades, WR {s['win_rate']:.0%}, PnL ${s['total_pnl']:.2f}, Sharpe {s.get('sharpe', 0):.3f}")

    # Save results
    with open("data/backtest_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to data/backtest_results.json")


if __name__ == "__main__":
    asyncio.run(main())
