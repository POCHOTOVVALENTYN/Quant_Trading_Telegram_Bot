"""
Event-Driven Portfolio Backtest Engine.
Simulates multiple symbols concurrently on a shared balance.
Uses 40/30/30 scaling out and EMA20 trailing.
"""
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.backtest import BacktestEngine, calculate_indicators
from core.pnl.pnl_calculator import PnLCalculator
from config.settings import settings

logger = logging.getLogger(__name__)

class PortfolioBacktester:
    def __init__(self, symbols, initial_balance=10000.0, leverage=10, risk_per_trade=0.03):
        self.symbols = symbols
        self.balance = initial_balance
        self.initial_balance = initial_balance
        self.leverage = leverage
        self.risk_per_trade_pct = risk_per_trade
        
        self.engines = {s: BacktestEngine(initial_balance=initial_balance, leverage=leverage) for s in symbols}
        self.active_trades = {s: [] for s in symbols}
        self.history = []
        self.equity_curve = []
        
    async def run(self, data_map: dict):
        """
        data_map: { 'BTC/USDT': df, 'ETH/USDT': df, ... }
        All DataFrames must have the same timestamps.
        """
        # Ensure indicators are calculated
        for s in self.symbols:
            data_map[s] = calculate_indicators(data_map[s])
            
        # Get common timestamps
        common_ts = sorted(list(set(data_map[self.symbols[0]]['timestamp'])))
        warmup = 200
        
        print(f"Starting Portfolio Backtest on {len(self.symbols)} symbols...")
        
        for ts in common_ts[warmup:]:
            self.equity_curve.append(self.balance)
            
            # 1. Update existing trades for all symbols
            for s in self.symbols:
                df = data_map[s]
                bar_row = df[df['timestamp'] == ts]
                if bar_row.empty: continue
                bar = bar_row.iloc[0]
                
                # We reuse BacktestEngine's logic but handle shared balance
                trades_to_remove = []
                for trade in self.active_trades[s]:
                    # --- EMA20 Trailing ---
                    if trade.get("trailing_active"):
                        ema20 = bar.get("ema20")
                        if not pd.isna(ema20):
                            if trade["direction"] == "LONG" and ema20 > trade["sl"]:
                                trade["sl"] = ema20
                            elif trade["direction"] == "SHORT" and ema20 < trade["sl"]:
                                trade["sl"] = ema20
                    
                    sl = trade["sl"]
                    is_long = trade["direction"] == "LONG"
                    
                    # Check hits
                    low, high = bar['low'], bar['high']
                    stop_hit = (is_long and low <= sl) or (not is_long and high >= sl)
                    
                    # Scaling out check
                    tp_indices = trade.get("tp_indices_hit", [])
                    for idx, target_px in enumerate(trade["tp_levels"]):
                        if idx in tp_indices: continue
                        hit = (is_long and high >= target_px) or (not is_long and low <= target_px)
                        if hit:
                            tp_indices.append(idx)
                            portion = [0.4, 0.3, 0.3][idx]
                            portion_qty = trade["original_size"] * portion
                            if idx == 2: portion_qty = trade["size"]
                            
                            # Realized PnL for this part
                            exit_px = target_px
                            pnl_obj = PnLCalculator.calculate_realized_pnl(
                                side=trade["direction"], entry_price=trade["entry"], exit_price=exit_px, qty=portion_qty,
                                entry_fee_usd=0,
                                exit_fee_usd=PnLCalculator.estimate_fee(price=exit_px, qty=portion_qty, fee_rate=0.0004)
                            )
                            self.balance += pnl_obj.pnl_usd
                            self.history.append({
                                "symbol": s, "type": f"TP{idx+1}", "pnl": pnl_obj.pnl_usd, 
                                "price": exit_px, "ts": ts
                            })
                            trade["size"] -= portion_qty
                            if idx == 1: # Activated BE & Trailing
                                trade["sl"] = trade["entry"]
                                trade["trailing_active"] = True
                    
                    if trade["size"] <= 1e-8:
                        trades_to_remove.append(trade)
                        continue

                    if stop_hit:
                        exit_px = sl
                        pnl_obj = PnLCalculator.calculate_realized_pnl(
                            side=trade["direction"], entry_price=trade["entry"], exit_price=exit_px, qty=trade["size"],
                            entry_fee_usd=0,
                            exit_fee_usd=PnLCalculator.estimate_fee(price=exit_px, qty=trade["size"], fee_rate=0.0005)
                        )
                        self.balance += pnl_obj.pnl_usd
                        self.history.append({
                            "symbol": s, "type": "SL", "pnl": pnl_obj.pnl_usd, "price": exit_px, "ts": ts
                        })
                        trades_to_remove.append(trade)
                
                for t in trades_to_remove:
                    self.active_trades[s].remove(t)

            # 2. Check for NEW signals
            # Limit total open trades
            current_open = sum(len(v) for v in self.active_trades.values())
            if current_open >= 4: # Max 4 concurrent trades in portfolio
                continue
                
            for s in self.symbols:
                if len(self.active_trades[s]) > 0: continue
                
                df = data_map[s]
                idx_list = df.index[df['timestamp'] == ts].tolist()
                if not idx_list: continue
                i = idx_list[0]
                
                eval_df = df.iloc[max(0, i-600):i].copy()
                bar = df.iloc[i]
                atr = eval_df['atr'].iloc[-1]
                adx = eval_df['adx'].iloc[-1]
                
                if pd.isna(atr) or atr <= 0: continue
                
                engine = self.engines[s]
                candidates = []
                for strategy in engine.strategies:
                    sig = strategy.evaluate(eval_df)
                    if not sig: continue
                    
                    # 1. ADX Filter (Sync with settings)
                    trend_strats = ["MA Trend", "Donchian", "Pullback", "Rule of 7"]
                    if sig['strategy'] in trend_strats and adx < 20: continue
                    range_strats = ["Williams R", "WRD Reversal"]
                    if sig['strategy'] in range_strats and adx > 25: continue

                    # 2. Heuristic Score
                    score = engine.scorer.calculate_score(eval_df, sig)
                    if score < 0.58: continue

                    # 3. AI Filter
                    from ai.feature_generator import FeatureGenerator
                    features = FeatureGenerator.generate_features(eval_df)
                    ai_pred = engine.ai_model.predict_win_probability(features, sig['signal'])
                    if ai_pred['win_prob'] < 0.58: continue
                    
                    candidates.append({
                        "sig": sig, "score": score, "win_prob": ai_pred['win_prob'],
                        "prio": score * ai_pred['win_prob']
                    })
                
                if candidates:
                    candidates.sort(key=lambda x: x['prio'], reverse=True)
                    winner = candidates[0]['sig']
                    
                    # Entry logic
                    entry = bar['open']
                    direction = winner['signal']
                    
                    # 4. Dynamic Target Generation (Rule of 7 Excellence)
                    from core.strategies.strategies import StrategyRuleOf7
                    # We need candles at the point of entry
                    t_high = eval_df['high'].iloc[-7:].max()
                    t_low = eval_df['low'].iloc[-7:].min()
                    funding = eval_df.get('funding_rate', pd.Series([0.0]*len(eval_df))).iloc[-1]
                    
                    tg_res = StrategyRuleOf7.calculate_targets(
                        high=t_high, low=t_low, direction=direction, 
                        atr=atr, funding_rate=funding
                    )
                    tp_levels = [tg_res['t1'], tg_res['t2'], tg_res['t3']]
                    
                    # SL based on ATR (2.0x for swing stability)
                    sl = entry - (atr * 2.0) if direction == "LONG" else entry + (atr * 2.0)
                    
                    # Risk: 2% (Standard professional risk)
                    margin = self.balance * 0.02
                    # Ensure minimum trade size
                    size = (margin * self.leverage) / entry
                    
                    self.active_trades[s].append({
                        "entry": entry, "sl": sl, "tp_levels": tp_levels,
                        "direction": direction, "original_size": size, "size": size,
                        "tp_indices_hit": [], "trailing_active": False, "ts_open": ts
                    })
                    print(f"[{datetime.fromtimestamp(ts/1000)}] OPEN {direction} {s} at {entry:.2f}")

        return self._summary()

    def _summary(self):
        total_pnl = self.balance - self.initial_balance
        ret_pct = (total_pnl / self.initial_balance) * 100
        
        # Max Drawdown
        peak = self.initial_balance
        mdd = 0
        for eq in self.equity_curve:
            peak = max(peak, eq)
            mdd = max(mdd, (peak - eq) / peak)
            
        return {
            "final_balance": self.balance,
            "total_pnl": total_pnl,
            "return_pct": ret_pct,
            "max_drawdown": mdd * 100,
            "total_signals": len(self.history)
        }

if __name__ == "__main__":
    from ai.backtest import fetch_candles
    
    async def run_it():
        symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
        data = {}
        for s in symbols:
            data[s] = await fetch_candles(s, "4h", days=180)
            
        pb = PortfolioBacktester(symbols, risk_per_trade=0.03)
        res = await pb.run(data)
        
        print("\n" + "="*40)
        print("PORTFOLIO BACKTEST RESULTS (6 MONTHS)")
        print("="*40)
        print(f"Start Balance:  $10,000")
        print(f"End Balance:    ${res['final_balance']:.2f}")
        print(f"Total PnL:      ${res['total_pnl']:.2f} ({res['return_pct']:.2f}%)")
        print(f"Max Drawdown:   {res['max_drawdown']:.2f}%")
        print("="*40)

    asyncio.run(run_it())
