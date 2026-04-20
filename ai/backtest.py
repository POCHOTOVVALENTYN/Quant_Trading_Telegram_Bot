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
            StrategyRuleOf7(),
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

    async def _resolve_intrabar_conflict(self, symbol: str, start_ts: int, end_ts: int, 
                                         sl: float, tp: float, direction: str) -> str:
        """
        Fetches 1m candles to determine if SL or TP was hit first inside a larger bar.
        Returns 'SL' or 'TP'.
        """
        try:
            # We use the existing fetch_candles helper for 1m data
            # To save time/API, we only fetch for the specific range
            df_1m = await fetch_candles(symbol, "1m", days=1, since_ts=start_ts)
            # Filter to our specific range
            df_range = df_1m[(df_1m['timestamp'] >= start_ts) & (df_1m['timestamp'] <= end_ts)]
            
            is_long = direction == "LONG"
            for _, m_bar in df_range.iterrows():
                m_low, m_high = float(m_bar['low']), float(m_bar['high'])
                
                # Check for SL hit
                sl_hit = (is_long and m_low <= sl) or (not is_long and m_high >= sl)
                # Check for TP hit 
                tp_hit = (is_long and m_high >= tp) or (not is_long and m_low <= tp)

                if sl_hit and tp_hit:
                    # If even on 1m we hit both (rare), assume SL for safety
                    return "SL"
                if sl_hit: return "SL"
                if tp_hit: return "TP"
            
            return "SL" # Fallback
        except Exception as e:
            # If API fails, default to conservative SL
            return "SL"

    async def run(self, df: pd.DataFrame, symbol: str = "BTC/USDT") -> Dict[str, Any]:
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
                entry = open_trade["entry"]
                
                # Dynamic SL (EMA20 Trailing)
                if open_trade.get("trailing_active"):
                    ema20 = bar.get("ema20")
                    if not pd.isna(ema20):
                        # Only move stop in favorable direction
                        if is_long and ema20 > open_trade["sl"]:
                            open_trade["sl"] = ema20
                        elif not is_long and ema20 < open_trade["sl"]:
                            open_trade["sl"] = ema20

                sl = open_trade["sl"]
                
                # Check for hits
                stop_hit = bool(is_long and bar['low'] <= sl) or bool((not is_long) and bar['high'] >= sl)
                
                # Sequential TP targets (40/30/30)
                tp_indices = open_trade.get("tp_indices_hit", [])
                for idx in range(len(open_trade["tp_levels"])):
                    if idx in tp_indices: continue
                    
                    target_px = open_trade["tp_levels"][idx]
                    hit = bool(is_long and bar['high'] >= target_px) or bool((not is_long) and bar['low'] <= target_px)
                    
                    if hit:
                        tp_indices.append(idx)
                        portion = [0.4, 0.3, 0.3][idx]
                        portion_qty = open_trade["original_size"] * portion
                        
                        # Fix amount for the last one or if overflow
                        if idx == 2: portion_qty = open_trade["size"]
                        
                        exit_price = self._exit_fill_price(bar, open_trade["direction"], target_px, f"TP{idx+1}")
                        closed_part = self._close_trade(open_trade, exit_price, f"TP{idx+1}")
                        # In _close_trade, we calculate PnL based on the qty we pass. Let's adjust.
                        real_pnl = closed_part["pnl"] * (portion / (open_trade["size"] / open_trade["original_size"]))
                        
                        self.balance += real_pnl
                        self.trades.append({**closed_part, "pnl": real_pnl, "size": portion_qty})
                        
                        open_trade["size"] -= portion_qty
                        
                        # Trigger BE and Trailing on TP2
                        if idx == 1:
                            open_trade["sl"] = entry # Break Even
                            open_trade["trailing_active"] = True
                            
                        if open_trade["size"] <= 0:
                            open_trade = None
                            break
                
                if not open_trade: continue

                # Check Stop Loss (if not already exited by last TP)
                if stop_hit:
                    exit_price = self._exit_fill_price(bar, open_trade["direction"], sl, "SL")
                    closed = self._close_trade(open_trade, exit_price, "SL")
                    self.balance += closed["pnl"]
                    self.trades.append(closed)
                    open_trade = None
                continue

                # 1m Intrabar resolution for Ambiguous bars (hit SL and any TP)
                any_tp_hit = len(tp_indices) > len(open_trade.get("tp_indices_hit_at_start", []))
                if stop_hit and any_tp_hit:
                    # Resolve conflict: Did SL happen before the first NEW TP of this bar?
                    start_ts = int(bar['timestamp'])
                    end_ts = int(df.iloc[i+1]['timestamp'] - 1) if i+1 < len(df) else start_ts + 3600000
                    # For simplicity, if conflict exists, we prioritize 1m truth.
                    # We skip the complex 1m loop for now and just assume the logic above 
                    # is already better than 'always SL first'. 
                    # But for Stage 3 perfection:
                    first_new_tp = open_trade["tp_levels"][min(tp_indices) if tp_indices else 0]
                    reason = await self._resolve_intrabar_conflict(symbol, start_ts, end_ts, sl, first_new_tp, open_trade["direction"])
                    if reason == "SL":
                        # Hit SL first: discard TPs from this bar
                        exit_price = self._exit_fill_price(bar, open_trade["direction"], sl, "SL")
                        closed = self._close_trade(open_trade, exit_price, "SL")
                        self.balance += closed["pnl"]
                        self.trades.append(closed)
                        open_trade = None
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
                trend_strats = ["MA Trend", "Donchian", "Pullback", "Rule of 7"]
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
                
                # EXECUTE: Professional Scaling Out logic
                entry = self._entry_fill_price(bar, signal['signal'])
                direction = signal['signal']
                
                # StrategyRuleOf7 logic matched: 1.5x / 2.5x / 3.5x tiers
                sl_price = self.risk.calculate_atr_stop(entry, atr, direction, 2.0)
                risk_dist = abs(entry - sl_price)
                
                # Multi-stage targets
                targets = [1.5, 2.5, 4.0] # Professional swing targets
                tp_levels = []
                for mult in targets:
                    tp_px = (entry + risk_dist * mult) if direction == "LONG" else (entry - risk_dist * mult)
                    tp_levels.append(tp_px)

                # Portfolio sizing
                margin = self.balance * self.risk_pct
                size = (margin * self.leverage) / entry
                
                open_trade = {
                    "entry": entry,
                    "sl": sl_price,
                    "tp_levels": tp_levels,
                    "tp_indices_hit": [],
                    "trailing_active": False,
                    "direction": direction,
                    "strategy": signal['strategy'],
                    "original_size": size,
                    "size": size,
                    "score": top['score'],
                    "win_prob": top['win_prob'],
                    "bar_idx": i,
                    "entry_fee_usd": PnLCalculator.estimate_fee(entry, size, self.taker_fee_pct),
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


async def fetch_candles(symbol: str, timeframe: str, days: int = 30, since_ts: Optional[int] = None) -> pd.DataFrame:
    """Download historical candles via ccxt."""
    import ccxt.pro as ccxtpro

    exchange = ccxtpro.binance({"enableRateLimit": True})
    try:
        if since_ts:
            since = since_ts
        else:
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
            
            # If we are doing intrabar resolution, we likely only need one batch
            if since_ts and len(all_candles) >= 500:
                break
                
            await asyncio.sleep(0.1)

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
    results = await engine.run(df, symbol=args.symbol)

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
