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
import random
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
from core.pnl.pnl_calculator import PnLCalculator
from config.settings import settings


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
        maker_fee_pct: Optional[float] = None,
        taker_fee_pct: Optional[float] = None,
        entry_slippage_pct: float = 0.0003,
        exit_slippage_pct: float = 0.0005,
    ):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.leverage = leverage
        self.sl_mult = sl_multiplier
        self.tp_mult = tp_multiplier
        self.risk_pct = risk_per_trade_pct
        self.score_threshold = score_threshold
        self.ai_threshold = ai_threshold
        self.maker_fee_pct = float(settings.maker_fee_pct if maker_fee_pct is None else maker_fee_pct)
        self.taker_fee_pct = float(settings.taker_fee_pct if taker_fee_pct is None else taker_fee_pct)
        self.entry_slippage_pct = float(entry_slippage_pct)
        self.exit_slippage_pct = float(exit_slippage_pct)

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

    def _apply_slippage(self, price: float, direction: str, *, is_entry: bool) -> float:
        slip_pct = self.entry_slippage_pct if is_entry else self.exit_slippage_pct
        side = str(direction or "").upper()
        if side == "LONG":
            factor = (1.0 + slip_pct) if is_entry else (1.0 - slip_pct)
        else:
            factor = (1.0 - slip_pct) if is_entry else (1.0 + slip_pct)
        return float(price) * factor

    def _entry_fill_price(self, bar: pd.Series, direction: str) -> float:
        base_price = float(bar["open"])
        slipped = self._apply_slippage(base_price, direction, is_entry=True)
        low = float(bar["low"])
        high = float(bar["high"])
        return min(max(slipped, low), high)

    def _exit_fill_price(self, bar: pd.Series, direction: str, trigger_price: float, reason: str) -> float:
        low = float(bar["low"])
        high = float(bar["high"])
        open_price = float(bar["open"])
        side = str(direction or "").upper()
        trigger = float(trigger_price)

        # Worst-case fill model inside a single OHLC bar:
        # - Stop: allow gap-through beyond trigger against us
        # - TP: require trade-through and then apply adverse slippage
        if reason == "SL":
            if side == "LONG":
                raw_fill = min(trigger, open_price)
            else:
                raw_fill = max(trigger, open_price)
        else:
            if side == "LONG":
                raw_fill = max(trigger, open_price)
            else:
                raw_fill = min(trigger, open_price)

        slipped = self._apply_slippage(raw_fill, direction, is_entry=False)
        return min(max(slipped, low), high)

    def _close_trade(self, trade: Dict[str, Any], exit_price: float, exit_reason: str) -> Dict[str, Any]:
        entry_fee = float(trade.get("entry_fee_usd", 0.0) or 0.0)
        exit_fee = PnLCalculator.estimate_fee(
            price=exit_price,
            qty=float(trade["size"]),
            fee_rate=self.taker_fee_pct,
        )
        pnl = PnLCalculator.calculate_realized_pnl(
            side=str(trade["direction"]),
            entry_price=float(trade["entry"]),
            exit_price=float(exit_price),
            qty=float(trade["size"]),
            entry_fee_usd=entry_fee,
            exit_fee_usd=exit_fee,
        )
        closed = dict(trade)
        closed["exit_price"] = float(exit_price)
        closed["exit_reason"] = exit_reason
        closed["entry_fee_usd"] = entry_fee
        closed["exit_fee_usd"] = exit_fee
        closed["fees_usd"] = pnl.fees_usd
        closed["gross_pnl"] = pnl.gross_pnl_usd
        closed["pnl"] = pnl.pnl_usd
        closed["pnl_pct"] = pnl.pnl_pct
        return closed

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
                stop_hit = bool(is_long and bar['low'] <= sl) or bool((not is_long) and bar['high'] >= sl)
                tp_hit = bool(is_long and bar['high'] >= tp) or bool((not is_long) and bar['low'] <= tp)

                # Conservative intrabar resolution: if both touched inside one bar, assume SL first.
                if stop_hit:
                    exit_price = self._exit_fill_price(bar, open_trade["direction"], sl, "SL")
                    closed = self._close_trade(open_trade, exit_price, "SL")
                    self.balance += closed["pnl"]
                    self.trades.append(closed)
                    open_trade = None
                elif tp_hit:
                    exit_price = self._exit_fill_price(bar, open_trade["direction"], tp, "TP")
                    closed = self._close_trade(open_trade, exit_price, "TP")
                    self.balance += closed["pnl"]
                    self.trades.append(closed)
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

                # ADX filter for trend strategies (Sync with settings)
                trend_strats = ["MA Trend", "Donchian", "Pullback"]
                if signal['strategy'] in trend_strats and adx < settings.regime_adx_trend_min:
                    self.signals_filtered += 1
                    continue
                
                range_strats = ["Williams R", "WRD Reversal"]
                if signal['strategy'] in range_strats and adx > settings.regime_adx_range_max:
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
                entry = self._entry_fill_price(bar, signal['signal'])
                direction = signal['signal']
                sl_price = self.risk.calculate_atr_stop(entry, atr, direction, self.sl_mult)

                if direction == "LONG":
                    tp_price = entry + abs(entry - sl_price) * self.tp_mult
                else:
                    tp_price = entry - abs(entry - sl_price) * self.tp_mult

                margin = self.balance * self.risk_pct
                size = (margin * self.leverage) / entry
                entry_fee = PnLCalculator.estimate_fee(
                    price=entry,
                    qty=size,
                    fee_rate=self.taker_fee_pct,
                )

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
                    "entry_fee_usd": entry_fee,
                }

        return self._compile_results()

    def run_monte_carlo(self, n_simulations: int = 1000) -> Dict[str, Any]:
        """
        Perform Monte Carlo simulation by shuffling trade sequences.
        Helps understand the range of possible outcomes and risk of ruin.
        """
        if not self.trades:
            return {"error": "No trades to simulate"}

        original_trades = [t["pnl"] for t in self.trades]
        final_balances = []
        max_drawdowns = []

        for _ in range(n_simulations):
            shuffled = list(original_trades)
            random.shuffle(shuffled)
            
            balance = self.initial_balance
            peak = balance
            mdd = 0
            
            for pnl in shuffled:
                balance += pnl
                peak = max(peak, balance)
                dd = (peak - balance) / peak
                mdd = max(mdd, dd)
            
            final_balances.append(balance)
            max_drawdowns.append(mdd)

        return {
            "n_simulations": n_simulations,
            "avg_final_balance": float(np.mean(final_balances)),
            "median_final_balance": float(np.median(final_balances)),
            "std_final_balance": float(np.std(final_balances)),
            "avg_max_drawdown_pct": float(np.mean(max_drawdowns)) * 100,
            "max_drawdown_95_percentile": float(np.percentile(max_drawdowns, 95)) * 100,
            "prob_of_negative_return": float(len([b for b in final_balances if b < self.initial_balance]) / n_simulations) * 100
        }

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

    # --- NEW: Monte Carlo ---
    if results.get("total_trades", 0) > 5:
        print("\n Running Monte Carlo Simulation (1000 iterations)...")
        mc_results = engine.run_monte_carlo(1000)
        print("-" * 40)
        print(f"  Avg Final Balance:  ${mc_results['avg_final_balance']:.2f}")
        print(f"  Median Balance:     ${mc_results['median_final_balance']:.2f}")
        print(f"  Avg Max Drawdown:   {mc_results['avg_max_drawdown_pct']:.2f}%")
        print(f"  95% Prob Max DD:    {mc_results['max_drawdown_95_percentile']:.2f}%")
        print(f"  Risk of Ruin/Loss:  {mc_results['prob_of_negative_return']:.1f}%")
        print("-" * 40)
        
        with open("data/monte_carlo_results.json", "w") as f:
            json.dump(mc_results, f, indent=2)

    # --- NEW: Walk-Forward Analysis (Concept) ---
    if args.days >= 60:
        print("\n Running Walk-Forward Analysis (2 Segments)...")
        mid_idx = len(df) // 2
        df_is = df.iloc[:mid_idx]
        df_oos = df.iloc[mid_idx:]
        
        print(f"  IS Period: {len(df_is)} candles | OOS Period: {len(df_oos)} candles")
        
        engine_oos = BacktestEngine(initial_balance=args.balance)
        oos_results = engine_oos.run(df_oos)
        
        print(f"  OOS Return: {oos_results.get('return_pct', 0):.2f}% (vs {results.get('return_pct', 0):.2f}% total)")
        if oos_results.get('return_pct', 0) > 0 and results.get('return_pct', 0) > 0:
            print("  ✅ Strategy shows robustness on OOS data!")
        elif oos_results.get('return_pct', 0) < 0:
            print("  ⚠️ Warning: Strategy failed OOS validation. High risk of over-optimization.")


if __name__ == "__main__":
    asyncio.run(main())
