import asyncio
import time
from utils.logger import get_execution_logger
from core.risk.risk_manager import RiskManager, TimeExitSystem, PyramidingSystem
from utils.notifier import send_telegram_msg

logger = get_execution_logger()

class ExecutionEngine:
    def __init__(self, exchange_client, risk_manager: RiskManager):
        self.exchange = exchange_client
        self.risk_manager = risk_manager
        self.pyramiding = PyramidingSystem()
        self.time_exit = TimeExitSystem()
        self.active_trades = {}

    async def get_account_metrics(self):
        try:
            balance_data = await self.exchange.fetch_balance()
            total_balance = balance_data.get('USDT', {}).get('total', 0.0)
            positions = await self.exchange.fetch_positions()
            open_positions = [p for p in positions if float(p.get('contracts', 0)) > 0]
            open_positions_count = len(open_positions)
            total_pnl = sum([float(p.get('unrealizedPnl', 0)) for p in positions])
            drawdown = 0.0
            if total_balance > 0:
                drawdown = abs(min(0, total_pnl)) / total_balance
            return total_balance, drawdown, open_positions_count
        except Exception as e:
            logger.error(f"Ошибка получения метрик аккаунта: {e}")
            return 10000.0, 0.0, 0
            
    async def execute_signal(self, signal_data: dict, account_balance: float, current_drawdown: float, open_trades_count: int):
        symbol = signal_data['symbol']
        direction = signal_data['signal']
        entry_price = signal_data['entry_price']
        
        if symbol in self.active_trades:
            await self._check_pyramiding(symbol, entry_price)
            return

        if not self.risk_manager.check_trade_allowed(open_trades_count, current_drawdown):
            logger.warning(f"Риск-менеджер отклонил вход в {symbol}")
            return

        atr_val = signal_data.get('atr', 100.0) 
        stop_price = self.risk_manager.calculate_atr_stop(entry_price, atr_val, direction)
        total_size = self.risk_manager.calculate_position_size(account_balance, entry_price, stop_price)
        
        if total_size <= 0: return
        initial_size = self.pyramiding.get_allocation_amount(total_size, 0)

        try:
            await self.exchange.create_order(
                symbol=symbol, 
                type='market', 
                side='buy' if direction == 'LONG' else 'sell', 
                amount=initial_size
            )
            
            self.active_trades[symbol] = {
                "entry": entry_price,
                "stop": stop_price,
                "stage": 0,
                "opened_at": time.time(),
                "signal_type": direction,
                "total_planned_size": total_size,
                "current_size": initial_size
            }
            
            await send_telegram_msg(
                f"✅ **ВХОД В ПОЗИЦИЮ**\n\n"
                f"🔸 Символ: {symbol}\n"
                f"🔸 Направление: {direction}\n"
                f"💰 Цена: {entry_price:.2f}\n"
                f"🛡 Стоп: {stop_price:.2f}\n"
                f"📊 Объем: 50% от плана"
            )

        except Exception as e:
            logger.error(f"Ошибка при открытии позиции {symbol}: {e}")

    async def update_positions(self, symbol: str, current_price: float, atr: float):
        if symbol not in self.active_trades:
            return

        trade = self.active_trades[symbol]
        
        # 1. Time Exit (15-я стратегия Швагера)
        if self.time_exit.should_exit(trade['opened_at'], time.time(), max_days=5):
            await send_telegram_msg(f"⏳ **ВЫХОД ПО ВРЕМЕНИ**\n\n{symbol} закрыт (5 дней без динамики).")
            await self._close_position(symbol)
            return

        # 2. ATR Trailing Stop (13-я стратегия Швагера)
        new_stop = self.risk_manager.calculate_trailing_stop(
            trade['stop'], current_price, atr, trade['signal_type']
        )
        
        if abs(new_stop - trade['stop']) > (current_price * 0.0001): # Сдвиг только если значим (>0.01%)
            old_stop = trade['stop']
            trade['stop'] = new_stop
            logger.info(f"🛡 [ТРЕЙЛИНГ] Обновлен стоп для {symbol}: {new_stop:.2f}")
            # Уведомление об апе стопа (сильно не спамим, только если сдвиг заметен)
            if abs(new_stop - old_stop) > (atr * 0.1): # Если сдвиг больше 10% от ATR
                await send_telegram_msg(f"🛡 **ТРЕЙЛИНГ-СТОП**\n\n{symbol}: Стоп подтянут до {new_stop:.2f}")

        # 3. Проверка срабатывания стопа
        if (trade['signal_type'] == "LONG" and current_price <= trade['stop']) or \
           (trade['signal_type'] == "SHORT" and current_price >= trade['stop']):
            await send_telegram_msg(f"💥 **СТОП-ЛОСС**\n\nПозиция {symbol} закрыта по стопу ({trade['stop']:.2f}).")
            await self._close_position(symbol)

    async def _check_pyramiding(self, symbol: str, current_price: float):
        trade = self.active_trades[symbol]
        if trade['stage'] >= 2: return

        profit_pct = (current_price - trade['entry']) / trade['entry'] if trade['signal_type'] == "LONG" else (trade['entry'] - current_price) / trade['entry']
        
        if profit_pct > 0.02: 
            next_stage = trade['stage'] + 1
            allocation = self.pyramiding.get_allocation_amount(trade['total_planned_size'], next_stage)
            
            try:
                await self.exchange.create_order(
                    symbol=symbol, 
                    type='market', 
                    side='buy' if trade['signal_type'] == 'LONG' else 'sell', 
                    amount=allocation
                )
                trade['stage'] = next_stage
                trade['current_size'] += allocation
                await send_telegram_msg(
                    f"➕ **ПИРАМИДИНГ (ДОЛИВКА)**\n\n"
                    f"{symbol}: Добавлено в позицию. Этап {next_stage}.\n"
                    f"📊 Текущий объем: {int((trade['current_size']/trade['total_planned_size'])*100)}% от плана."
                )
            except Exception as e:
                logger.error(f"Ошибка пирамидинга {symbol}: {e}")

    async def _close_position(self, symbol: str):
        if symbol not in self.active_trades: return
        trade = self.active_trades[symbol]
        try:
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side='sell' if trade['signal_type'] == 'LONG' else 'buy',
                amount=trade['current_size']
            )
            del self.active_trades[symbol]
        except Exception as e:
            logger.error(f"Ошибка при закрытии {symbol}: {e}")
