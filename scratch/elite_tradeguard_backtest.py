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

class EliteTradeGuardBacktest(BacktestEngine):
    """
    Продвинутый бэктестер с поддержкой мульти-таймфрейма и логики TradeGuard.
    Использует 1H разрешение для проверки условий выхода внутри 4H свечей.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.risk_manager = RiskManager()
        # Включаем менеджмент сделок для теста
        settings.trade_mgmt_enabled = True

    def run_elite(self, symbol, df_1d, df_4h, df_1h):
        print(f"🚀 Запуск элитного бэктеста для {symbol}...")
        
        # Подготовка индикаторов
        df_1d = calculate_indicators(df_1d)
        df_4h = calculate_indicators(df_4h)
        df_1h = calculate_indicators(df_1h)

        open_trade = None
        warmup = 100 
        
        # Итерируемся по 4H барам (Таймфрейм сигналов)
        for i in range(warmup, len(df_4h)):
            bar_4h = df_4h.iloc[i]
            ts_start = bar_4h['timestamp']
            ts_end = ts_start + 4 * 3600 * 1000 # Конец 4H свечи
            
            self.equity_curve.append(self.balance)

            # 1. Управление открытой позицией (Внутри 4H свечи используем 1H данные для точности)
            if open_trade:
                # Находим 1H свечи, которые входят в текущий 4H интервал
                sub_bars_1h = df_1h[(df_1h['timestamp'] >= ts_start) & (df_1h['timestamp'] < ts_end)]
                
                exit_triggered = False
                for _, bar_1h in sub_bars_1h.iterrows():
                    current_price = bar_1h['close']
                    
                    # Рассчитываем текущий R
                    current_r = self.risk_manager.current_r_multiple(
                        open_trade['entry'], 
                        open_trade['initial_stop'], 
                        current_price, 
                        open_trade['direction']
                    )
                    
                    # Имитируем структуру 'trade' для TradeGuard
                    trade_ctx = {
                        "symbol": symbol,
                        "signal_type": open_trade['direction'],
                        "entry": open_trade['entry'],
                        "opened_at": open_trade['opened_at_ts'] / 1000,
                        "setup_group": open_trade['setup_group'],
                        "timeframe": "4h",
                        "invalidation_level": open_trade.get('invalidation_level'),
                        "initial_stop": open_trade['initial_stop'],
                        "bars_since_entry": int((bar_1h['timestamp'] - open_trade['opened_at_ts']) / (4 * 3600 * 1000))
                    }

                    # А) Проверка жесткого SL/TP
                    is_long = open_trade['direction'] == "LONG"
                    if (is_long and bar_1h['low'] <= open_trade['sl']) or (not is_long and bar_1h['high'] >= open_trade['sl']):
                        closed = self._close_trade(open_trade, open_trade['sl'], "SL")
                        self.balance += closed["pnl"]
                        self.trades.append(closed)
                        open_trade = None
                        exit_triggered = True
                        break
                    
                    if (is_long and bar_1h['high'] >= open_trade['tp']) or (not is_long and bar_1h['low'] <= open_trade['tp']):
                        closed = self._close_trade(open_trade, open_trade['tp'], "TP")
                        self.balance += closed["pnl"]
                        self.trades.append(closed)
                        open_trade = None
                        exit_triggered = True
                        break

                    # Б) Проверка TRADEGUARD (Таймстоп и Инвалидация)
                    # Для инвалидации тренда нам нужен DF с индикаторами (1H)
                    eval_df_1h = df_1h[df_1h['timestamp'] <= bar_1h['timestamp']].tail(100)
                    
                    if TradeGuard.should_time_stop(trade_ctx, current_price):
                        closed = self._close_trade(open_trade, current_price, "TIME_EXIT")
                        self.balance += closed["pnl"]
                        self.trades.append(closed)
                        open_trade = None
                        exit_triggered = True
                        print(f"⏰ [TG] Таймстоп на {bar_1h['timestamp']}")
                        break
                        
                    if TradeGuard.should_invalidation_exit(trade_ctx, current_price, eval_df_1h, self.risk_manager):
                        closed = self._close_trade(open_trade, current_price, "INVALIDATION")
                        self.balance += closed["pnl"]
                        self.trades.append(closed)
                        open_trade = None
                        exit_triggered = True
                        print(f"⚠️ [TG] Инвалидация на {bar_1h['timestamp']}")
                        break

                if exit_triggered or open_trade:
                    continue # Если позиция еще открыта или только что закрылась, не ищем новый вход в этом же 4H баре

            # 2. Поиск входа (Глобальный фильтр 1D + Сигнал 4H)
            bar_1d = df_1d[df_1d['timestamp'] <= ts_start].iloc[-1]
            
            # Глобальный фильтр: только по тренду 1D (EMA50)
            bias = 0
            if bar_1d['close'] > bar_1d['ema50']: bias = 1
            elif bar_1d['close'] < bar_1d['ema50']: bias = -1

            eval_df_4h = df_4h.iloc[max(0, i-200):i].copy()
            candidates = []
            for strategy in self.strategies:
                signal = strategy.evaluate(eval_df_4h)
                if not signal: continue
                
                direction = signal['signal']
                if bias == 1 and direction == "SHORT": continue
                if bias == -1 and direction == "LONG": continue

                score = self.scorer.calculate_score(eval_df_4h, signal)
                if score < self.score_threshold: continue

                candidates.append({"signal": signal, "score": score})

            if candidates:
                candidates.sort(key=lambda x: x['score'], reverse=True)
                top = candidates[0]['signal']
                
                entry_price = bar_4h['open']
                direction = top['signal']
                
                # Рассчитываем стоп и профит (2R профит)
                sl_price = self.risk.calculate_atr_stop(entry_price, bar_4h['atr'], direction, 2.0)
                tp_price = entry_price + abs(entry_price - sl_price) * 2.5 if direction == "LONG" else entry_price - abs(entry_price - sl_price) * 2.5
                
                # Риск 2% от депо
                margin = self.balance * 0.02
                size = (margin * self.leverage) / entry_price
                
                open_trade = {
                    "entry": entry_price,
                    "sl": sl_price,
                    "initial_stop": sl_price,
                    "tp": tp_price,
                    "direction": direction,
                    "strategy": top['strategy'],
                    "setup_group": "trend", # Упрощенно для теста
                    "size": size,
                    "opened_at_ts": ts_start,
                    "invalidation_level": sl_price, # Для теста
                    "entry_fee_usd": size * entry_price * self.taker_fee_pct
                }

        return self._compile_results()

async def main():
    symbol = "BTC/USDT"
    days = 60
    
    tasks = [
        fetch_candles(symbol, "1d", days + 50),
        fetch_candles(symbol, "4h", days),
        fetch_candles(symbol, "1h", days)
    ]
    dfs = await asyncio.gather(*tasks)
    
    engine = EliteTradeGuardBacktest(initial_balance=1000)
    results = engine.run_elite(symbol, *dfs)
    
    print("\n" + "="*60)
    print(f"ELITE BACKTEST RESULTS: {symbol} ({days} days)")
    print("="*60)
    print(f"Total trades: {results.get('total_trades', 0)}")
    print(f"Win rate:     {results.get('win_rate', 0):.1%}")
    print(f"Profit Factor: {results.get('profit_factor', 0):.2f}")
    print(f"Return:       {results.get('return_pct', 0):.2f}%")
    print(f"Max Drawdown: {results.get('max_drawdown_pct', 0):.2f}%")
    
    if results.get('total_trades') > 0:
        exit_reasons = {}
        for t in engine.trades:
            r = t.get('exit_reason', 'Unknown')
            exit_reasons[r] = exit_reasons.get(r, 0) + 1
        print("\nExit Reasons Breakdown:")
        for r, count in exit_reasons.items():
            print(f"  {r:15s}: {count}")

if __name__ == "__main__":
    asyncio.run(main())
