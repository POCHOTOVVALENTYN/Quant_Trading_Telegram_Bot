import asyncio
import pandas as pd
import numpy as np
import sys
import os
import time
from datetime import datetime, timezone, timedelta

# Add root to sys path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import BacktestEngine, fetch_candles, calculate_indicators
from core.execution.trade_guard import TradeGuard
from core.risk.risk_manager import RiskManager
from config.settings import settings

class Aggressive1HBacktest(BacktestEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.risk_manager = RiskManager()
        settings.trade_mgmt_enabled = True
        self.portfolio_trades = []
        self.score_threshold = 0.58
        self.ai_threshold = 0.58

    async def run_portfolio(self, symbols, days=60):
        print(f"⚡️ 1H AGGRESSIVE: {symbols} | {days} days | AI={self.ai_threshold}")
        
        all_results = {}
        for symbol in symbols:
            print(f"  🔍 {symbol}...")
            try:
                tasks = [
                    fetch_candles(symbol, "1d", days + 30),
                    fetch_candles(symbol, "1h", days),
                    fetch_candles(symbol, "15m", days)
                ]
                df_1d, df_1h, df_15m = await asyncio.gather(*tasks)
                
                self.trades = []
                symbol_res = self._run_single_symbol(symbol, df_1d, df_1h, df_15m)
                
                all_results[symbol] = symbol_res
                self.portfolio_trades.extend(self.trades)
            except Exception as e:
                print(f"    ❌ Error {symbol}: {e}")

        return self._compile_portfolio_results(all_results)

    def _run_single_symbol(self, symbol, df_1d, df_1h, df_15m):
        df_1d = calculate_indicators(df_1d)
        df_1h = calculate_indicators(df_1h)
        df_15m = calculate_indicators(df_15m)

        open_trade = None
        warmup = 200 
        
        for i in range(warmup, len(df_1h)):
            bar_1h = df_1h.iloc[i]
            ts_start = bar_1h['timestamp']
            ts_end = ts_start + 3600 * 1000
            
            if open_trade:
                sub_bars_15m = df_15m[(df_15m['timestamp'] >= ts_start) & (df_15m['timestamp'] < ts_end)]
                exit_triggered = False
                for _, bar_15m in sub_bars_15m.iterrows():
                    current_price = bar_15m['close']
                    is_long = open_trade['direction'] == "LONG"
                    
                    # TP Scaling
                    tp_hit_idx = -1
                    for idx, tp_price in enumerate(open_trade['tp_levels']):
                        if idx in open_trade['tp_hit_indices']: continue
                        hit = (is_long and bar_15m['high'] >= tp_price) or (not is_long and bar_15m['low'] <= tp_price)
                        if hit:
                            tp_hit_idx = idx
                            break
                    if tp_hit_idx != -1:
                        portion = 0.5 if tp_hit_idx == 0 else 1.0
                        qty_to_close = open_trade['size'] * portion
                        closed_part = self._close_trade(open_trade, open_trade['tp_levels'][tp_hit_idx], f"TP{tp_hit_idx+1}")
                        self.balance += (closed_part["pnl"] * (qty_to_close / open_trade['size']))
                        self.trades.append({**closed_part, "pnl": closed_part["pnl"] * (qty_to_close / open_trade['size']), "size": qty_to_close})
                        open_trade['size'] -= qty_to_close
                        open_trade['tp_hit_indices'].append(tp_hit_idx)
                        if tp_hit_idx == 0: open_trade['sl'] = open_trade['entry']
                        if open_trade['size'] <= 0:
                            open_trade = None
                            exit_triggered = True
                            break
                    if not open_trade: break

                    # SL
                    if (is_long and bar_15m['low'] <= open_trade['sl']) or (not is_long and bar_15m['high'] >= open_trade['sl']):
                        closed = self._close_trade(open_trade, open_trade['sl'], "SL")
                        self.balance += closed["pnl"]
                        self.trades.append(closed)
                        open_trade = None
                        exit_triggered = True
                        break

                    # TradeGuard
                    trade_ctx = {
                        "symbol": symbol, "signal_type": open_trade['direction'], "entry": open_trade['entry'],
                        "opened_at": open_trade['opened_at_ts'] / 1000, "setup_group": "trend",
                        "timeframe": "1h", "invalidation_level": open_trade['sl'], "initial_stop": open_trade['initial_stop'],
                        "bars_since_entry": int((bar_15m['timestamp'] - open_trade['opened_at_ts']) / (3600 * 1000))
                    }
                    
                    if TradeGuard.should_time_stop(trade_ctx, current_price):
                        closed = self._close_trade(open_trade, current_price, "TIME_EXIT")
                        self.balance += closed["pnl"]
                        self.trades.append(closed)
                        open_trade = None
                        exit_triggered = True
                        break
                        
                if exit_triggered or open_trade: continue

            # Entry
            bar_1d = df_1d[df_1d['timestamp'] <= ts_start].iloc[-1]
            # Упрощенный фильтр тренда
            bias = 1 if bar_1h['close'] > bar_1h['ema50'] else -1 if bar_1h['close'] < bar_1h['ema50'] else 0
            
            eval_df_1h = df_1h.iloc[max(0, i-200):i].copy()
            candidates = []
            for strategy in self.strategies:
                signal = strategy.evaluate(eval_df_1h)
                if not signal or (bias != 0 and signal['signal'] != ("LONG" if bias == 1 else "SHORT")): continue
                score = self.scorer.calculate_score(eval_df_1h, signal)
                if score < self.score_threshold: continue
                candidates.append({"signal": signal, "score": score})

            if candidates:
                candidates.sort(key=lambda x: x['score'], reverse=True)
                top = candidates[0]['signal']
                entry_price = bar_1h['open']
                sl_price = self.risk.calculate_atr_stop(entry_price, bar_1h['atr'], top['signal'], 2.0)
                tp_levels = [entry_price + abs(entry_price-sl_price)*m if top['signal']=="LONG" else entry_price-abs(entry_price-sl_price)*m for m in [1.5, 3.0]]
                
                size = (self.balance * 0.02 * self.leverage) / entry_price
                open_trade = {
                    "symbol": symbol, "entry": entry_price, "sl": sl_price, "initial_stop": sl_price, 
                    "tp_levels": tp_levels, "tp_hit_indices": [], "direction": top['signal'], "strategy": top['strategy'], 
                    "setup_group": "trend", "size": size, "opened_at_ts": ts_start, "entry_fee_usd": size * entry_price * self.taker_fee_pct
                }

        return self._compile_results()

    def _compile_portfolio_results(self, all_res):
        total_trades = sum(r.get('total_trades', 0) for r in all_res.values())
        total_pnl = sum(r.get('total_pnl', 0) for r in all_res.values())
        return {"total_trades": total_trades, "total_pnl": total_pnl, "final_balance": self.balance, "per_symbol": all_res}

async def main():
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT"]
    engine = Aggressive1HBacktest(initial_balance=1000)
    results = await engine.run_portfolio(symbols, days=60)
    print(f"\nFinal PnL: ${results['total_pnl']:.2f} | Balance: ${results['final_balance']:.2f} | Trades: {results['total_trades']}")
    for s, r in results['per_symbol'].items():
        print(f"  {s}: ${r.get('total_pnl',0):.2f} ({r.get('total_trades',0)} trades)")

if __name__ == "__main__":
    asyncio.run(main())
