import asyncio
import time
import datetime
import traceback
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

from sqlalchemy import select, update
from database.session import async_session
from database.models.all_models import (
    Position as PositionModel, PositionStatus, 
    Order as OrderModel, OrderStatus, 
    SignalType, Signal as SignalModel, 
    PnLRecord as PnLModel
)
from config.settings import settings
from utils.logger import get_execution_logger
from utils.notifier import send_telegram_msg
from core.risk.risk_manager import RiskManager, TimeExitSystem, PyramidingSystem

logger = get_execution_logger()

class ExecutionEngine:
    def __init__(self, exchange_client, risk_manager: RiskManager):
        self.exchange = exchange_client
        if hasattr(self.exchange, 'options'):
            self.exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
        
        self.risk_manager = risk_manager
        self.pyramiding = PyramidingSystem()
        self.time_exit = TimeExitSystem()
        
        self.active_trades: Dict[str, Dict[str, Any]] = {}
        self._symbol_locks: Dict[str, asyncio.Lock] = {}
        
        self._user_order_stream_task: Optional[asyncio.Task] = None
        self._user_position_stream_task: Optional[asyncio.Task] = None
        self._running = True

    def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        if symbol not in self._symbol_locks:
            self._symbol_locks[symbol] = asyncio.Lock()
        return self._symbol_locks[symbol]

    def _norm_sym(self, s: str) -> str:
        """Единый стандарт символа: BTC/USDT"""
        if not s: return ""
        clean = s.split(":")[0].replace("/", "").strip().upper()
        if clean.endswith("USDT") and len(clean) > 4:
            return f"{clean[:-4]}/USDT"
        return clean

    async def start(self):
        """Запуск фоновых задач"""
        if self._user_order_stream_task is None:
            self._user_order_stream_task = asyncio.create_task(self._watch_user_orders_loop())
            logger.info("📡 [EXEC] WebSocket ОРДЕРА: OK")
        
        if self._user_position_stream_task is None:
            self._user_position_stream_task = asyncio.create_task(self._watch_user_positions_loop())
            logger.info("📡 [EXEC] WebSocket ПОЗИЦИИ: OK")

    async def stop(self):
        self._running = False
        for t in [self._user_order_stream_task, self._user_position_stream_task]:
            if t: t.cancel()
        logger.info("🛑 [EXEC] Движок остановлен")

    # --- WEBSOCKET LOOPS ---

    async def _watch_user_orders_loop(self):
        while self._running:
            try:
                orders = await self.exchange.watch_orders()
                for order in orders:
                    await self._handle_order_update(order)
            except Exception as e:
                if self._running:
                    logger.error(f"❌ [WS_ORDERS] Error: {e}")
                    await asyncio.sleep(5)

    async def _watch_user_positions_loop(self):
        while self._running:
            try:
                positions = await self.exchange.watch_positions()
                for pos in positions:
                    symbol = self._norm_sym(pos.get('symbol'))
                    contracts = float(pos.get('contracts', 0) or pos.get('pa', 0) or 0)
                    if abs(contracts) <= 1e-8 and symbol in self.active_trades:
                        logger.info(f"💥 [WS_POS] {symbol} закрыта извне.")
                        await self._close_position(symbol, reason="EXTERNAL")
            except Exception as e:
                if self._running:
                    logger.error(f"❌ [WS_POS] Error: {e}")
                    await asyncio.sleep(5)

    async def _handle_order_update(self, order: dict):
        try:
            status = order.get('status')
            symbol = self._norm_sym(order.get('symbol'))
            ex_id = str(order.get('id'))
            
            if status in ['closed', 'filled']:
                async with async_session() as session:
                    stmt = select(OrderModel).where(OrderModel.exchange_order_id == ex_id)
                    res = await session.execute(stmt)
                    db_order = res.scalar_one_or_none()
                    
                    if db_order:
                        db_order.status = OrderStatus.FILLED
                        if db_order.position_id:
                            pos_stmt = select(PositionModel).where(PositionModel.id == db_order.position_id)
                            pos_res = await session.execute(pos_stmt)
                            db_pos = pos_res.scalar_one_or_none()
                            
                            if db_pos and db_pos.status == PositionStatus.OPEN:
                                ot = db_order.order_type.upper()
                                if any(x in ot for x in ["STOP", "TAKE", "TRAILING"]):
                                    db_pos.status = PositionStatus.CLOSED
                                    db_pos.closed_at = datetime.datetime.utcnow()
                                    logger.info(f"💾 [WS_DB] Позиция {symbol} закрыта по {ot}")
                        await session.commit()
        except Exception as e:
            logger.error(f"Error handling order update: {e}")

    # --- HELPERS ---

    async def _normalize_amount(self, symbol: str, amount: float) -> float:
        try:
            if not self.exchange.markets: await self.exchange.load_markets()
            market = self.exchange.market(symbol)
            step = Decimal(str(market['info']['filters'][1].get('stepSize', '0.001')))
            return float(Decimal(str(amount)).quantize(step, rounding=ROUND_DOWN))
        except: return amount

    async def _normalize_price(self, symbol: str, price: float) -> float:
        try:
            if not self.exchange.markets: await self.exchange.load_markets()
            market = self.exchange.market(symbol)
            tick = Decimal(str(market['info']['filters'][0].get('tickSize', '0.01')))
            return float(Decimal(str(price)).quantize(tick, rounding=ROUND_HALF_UP))
        except: return price

    async def _get_position_mode(self) -> bool:
        """Dual Side (Hedge) mode check"""
        try:
            res = await self.exchange.request('positionSide/dual', 'fapiPrivate', 'GET', {})
            return res.get('dualSidePosition', False)
        except: return False

    async def _cancel_all_orders(self, symbol: str):
        """Отмена всех ордеров по символу (включая Algo)"""
        try:
            # 1. Обычные ордера
            await self.exchange.cancel_all_orders(symbol)
            # 2. Algo ордера (Binance Futures специфично)
            clean_sym = symbol.replace("/", "").split(":")[0]
            await self.exchange.request('allOpenAlgoOrders', 'fapiPrivate', 'DELETE', {'symbol': clean_sym})
        except: pass

    # --- CORE LOGIC ---

    async def _set_protective_orders(self, symbol: str, side: str, amount: float, sl: float, tp: Optional[float] = None) -> Tuple[Optional[str], Optional[str]]:
        """Унифицированная установка SL и TP через Algo API"""
        sl_id, tp_id = None, None
        try:
            is_hedge = await self._get_position_mode()
            clean_sym = symbol.replace("/", "").split(":")[0]
            reduce_side = "SELL" if side.upper() == "BUY" else "BUY"
            
            # Базовые параметры Algo-ордеров
            base_params = {
                "symbol": clean_sym,
                "side": reduce_side,
                "quantity": str(amount),
                "workingType": "MARK_PRICE",
                "reduceOnly": "true",
                "closePosition": "true"
            }
            if is_hedge:
                base_params["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"

            # 1. STOP LOSS
            sl_p = {**base_params, "type": "STOP_MARKET", "triggerPrice": str(await self._normalize_price(symbol, sl))}
            res_sl = await self.exchange.request('algoOrder', 'fapiPrivate', 'POST', sl_p)
            if res_sl and res_sl.get("algoId"):
                sl_id = str(res_sl["algoId"])
                logger.info(f"🛡 [PROTECT] SL установлен для {symbol}: {sl}")

            # 2. TAKE PROFIT (Scale-out: Частичная фиксация)
            if tp:
                tp_ids = []
                # Проверяем, передан ли словарь целей (targets) или просто одно число
                if isinstance(tp, dict):
                    # Ожидаем словарь вида: {"Цель 1": price1, "Цель 2": price2, "Цель 3": price3}
                    # Распределение объемов: 50%, 30%, 20%
                    portions = [0.5, 0.3, 0.2]
                    targets = list(tp.values())
                    
                    remaining_amount = Decimal(str(amount))
                    for i, target_price in enumerate(targets[:3]):
                        part_amt = float((Decimal(str(amount)) * Decimal(str(portions[i]))).quantize(Decimal('0.001'), rounding=ROUND_DOWN))
                        
                        # На последнем шаге отдаем весь остаток во избежание "пыли"
                        if i == 2 or i == len(targets)-1:
                            part_amt = float(remaining_amount)
                            
                        if part_amt <= 0: continue
                        remaining_amount -= Decimal(str(part_amt))
                        
                        tp_p = {**base_params, "type": "TAKE_PROFIT_MARKET", "triggerPrice": str(await self._normalize_price(symbol, target_price)), "quantity": str(part_amt)}
                        res_tp = await self.exchange.request('algoOrder', 'fapiPrivate', 'POST', tp_p)
                        if res_tp and res_tp.get("algoId"):
                            tp_ids.append(str(res_tp["algoId"]))
                            logger.info(f"🎯 [PARTIAL TP {i+1}] {symbol} объем {part_amt} по {target_price}")
                            
                    tp_id = ",".join(tp_ids) # Сохраняем все ID через запятую
                else:
                    # Классический единый TP
                    tp_p = {**base_params, "type": "TAKE_PROFIT_MARKET", "triggerPrice": str(await self._normalize_price(symbol, tp))}
                    res_tp = await self.exchange.request('algoOrder', 'fapiPrivate', 'POST', tp_p)
                    if res_tp and res_tp.get("algoId"):
                        tp_id = str(res_tp["algoId"])
                        logger.info(f"🎯 [PROTECT] TP установлен для {symbol}: {tp}")

        except Exception as e:
            logger.error(f"❌ [PROTECT] Ошибка защиты {symbol}: {e}")
        return sl_id, tp_id

    async def reconcile_full(self):
        """Полная синхронизация: Биржа -> БД -> Memory"""
        try:
            await self.exchange.load_time_difference()
            # Параллельный сбор данных
            tasks = [
                self.exchange.fetch_positions(),
                self.exchange.fetch_open_orders(),
                self.exchange.request('openAlgoOrders', 'fapiPrivate', 'GET', {})
            ]
            pos_data, std_orders, algo_raw = await asyncio.gather(*tasks)
            algo_orders = algo_raw.get("algoOrders", []) if isinstance(algo_raw, dict) else (algo_raw if isinstance(algo_raw, list) else [])

            # Маппинг ордеров биржи
            ex_orders_by_symbol = {}
            for o in (std_orders or []):
                s = self._norm_sym(o.get("symbol"))
                if s not in ex_orders_by_symbol: ex_orders_by_symbol[s] = []
                ex_orders_by_symbol[s].append(o)
            for o in algo_orders:
                s = self._norm_sym(o.get("symbol"))
                if s not in ex_orders_by_symbol: ex_orders_by_symbol[s] = []
                ex_orders_by_symbol[s].append({"id": o.get("algoId"), "type": (o.get("type") or "STOP_MARKET").upper(), "stopPrice": o.get("triggerPrice")})

            # Читаем БД
            async with async_session() as session:
                res = await session.execute(select(PositionModel).where(PositionModel.status == PositionStatus.OPEN))
                db_positions = {self._norm_sym(p.symbol): p for p in res.scalars().all()}

            new_active = {}
            for p in (pos_data or []):
                contracts = float(p.get("contracts", 0) or p.get("pa", 0) or 0)
                symbol = self._norm_sym(p.get("symbol", ""))
                if not symbol or abs(contracts) <= 1e-8: continue

                entry = float(p.get("entryPrice") or 0.0)
                is_long = contracts > 0
                
                # Ищем SL на бирже
                found_sl, sl_id = None, None
                for o in ex_orders_by_symbol.get(symbol, []):
                    if "STOP" in str(o.get("type", "")).upper():
                        found_sl = float(o.get("stopPrice") or 0); sl_id = o.get("id")

                # Синхронизация с БД
                if symbol not in db_positions:
                    # Импортируем "чужую" позицию
                    sl_def = entry * (0.95 if is_long else 1.05)
                    tp_def = entry * (1.10 if is_long else 0.90)
                    dbp = PositionModel(user_id=1, symbol=symbol, side=SignalType.LONG if is_long else SignalType.SHORT,
                                        entry_price=entry, size=abs(contracts), status=PositionStatus.OPEN,
                                        stop_loss=found_sl or sl_def, take_profit=tp_def, opened_at=datetime.datetime.utcnow())
                    async with async_session() as session:
                        session.add(dbp); await session.commit(); await session.refresh(dbp)
                else:
                    dbp = db_positions[symbol]

                # --- RESCUE: Если стопа нет на бирже, ставим его ---
                if not sl_id and dbp.stop_loss:
                    logger.warning(f"🛡 [RESCUE] Восстанавливаю защиту для {symbol}")
                    sl_id, tp_id = await self._set_protective_orders(symbol, "BUY" if is_long else "SELL", abs(contracts), dbp.stop_loss, dbp.take_profit)
                    await send_telegram_msg(
                        f"🛡 **RESCUE: Восстановление защиты**\n\n"
                        f"🔸 Символ: {symbol}\n"
                        f"🛡 SL: {dbp.stop_loss:.4f}\n"
                        f"🎯 TP: {dbp.take_profit:.4f if dbp.take_profit else 'N/A'}\n"
                        f"✅ Защитные ордера успешно выставлены на бирже."
                    )

                new_active[symbol] = {
                    "entry": entry, "stop": found_sl or dbp.stop_loss, "stage": 0, "opened_at": dbp.opened_at.timestamp(),
                    "signal_type": "LONG" if is_long else "SHORT", "current_size": abs(contracts),
                    "position_db_id": dbp.id, "stop_order_id": sl_id
                }

            # Закрываем в БД "призраков"
            for sym, dbp in db_positions.items():
                if sym not in new_active:
                    async with async_session() as session:
                        await session.execute(update(PositionModel).where(PositionModel.id == dbp.id).values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow()))
                        await session.commit()
            
            self.active_trades = new_active
            logger.info(f"✅ [RECONCILE] Активных позиций: {len(self.active_trades)}")
        except Exception as e:
            logger.error(f"❌ Reconcile Error: {e}")

    async def execute_signal(self, signal_data: dict, account_balance: float, drawdown: float, open_count: int):
        symbol = signal_data['symbol']
        direction = signal_data['signal']
        signal_id = signal_data.get("id")

        if not settings.is_trading_enabled or symbol in self.active_trades: return
        if not self.risk_manager.check_trade_allowed(open_count, drawdown): return

        # Idempotency
        async with async_session() as session:
            stmt = update(SignalModel).where(SignalModel.id == signal_id, SignalModel.status == "PENDING").values(status="EXECUTING")
            res = await session.execute(stmt); await session.commit()
            if res.rowcount == 0: return

        try:
            entry_price = float(signal_data['entry_price'])
            stop_price = self.risk_manager.calculate_atr_stop(entry_price, signal_data.get('atr', 100.0), direction)
            lot_size = self.risk_manager.calculate_position_size(account_balance, entry_price, stop_price)
            lot_size = await self._normalize_amount(symbol, lot_size)
            
            if lot_size <= 0: raise Exception("Zero lot size after normalization")

            # 1. Умный Вход (Limit Chasing)
            await self._set_leverage_best_effort(symbol, settings.leverage)
            side = 'buy' if direction.upper() == 'LONG' else 'sell'
            
            entry_exec = entry_price
            order = None
            max_retries = 3
            
            for attempt in range(max_retries):
                try:
                    # Получаем свежий стакан (Orderbook)
                    ob = await self.exchange.fetch_order_book(symbol, limit=5)
                    # Для лонга встаем в лучший Bid, для шорта в лучший Ask
                    best_price = ob['bids'][0][0] if side == 'buy' else ob['asks'][0][0]
                    best_price = await self._normalize_price(symbol, best_price)
                    
                    # Пытаемся выставить Maker ордер (postOnly)
                    logger.info(f"🕸 [LIMIT CHASE] Попытка {attempt+1}: Лимитка {side} {symbol} по {best_price}")
                    temp_order = await self.exchange.create_order(
                        symbol=symbol, type='limit', side=side, amount=lot_size, price=best_price,
                        params={'timeInForce': 'GTX', 'postOnly': True} # GTX = Post Only
                    )
                    
                    # Ждем 3 секунды исполнения
                    await asyncio.sleep(3)
                    
                    # Проверяем статус
                    check = await self.exchange.fetch_order(temp_order['id'], symbol)
                    if check['status'] == 'closed':
                        order = check
                        entry_exec = check['average'] or check['price']
                        break # Успешно вошли по лучшей цене!
                    else:
                        # Отменяем неисполненный лимит и пробуем снова
                        await self.exchange.cancel_order(temp_order['id'], symbol)
                        
                except Exception as e:
                    logger.warning(f"Limit chase error: {e}")
                    
            # Fallback (План Б): Если за 3 попытки не вошли (рынок улетел), бьем по рынку
            if not order:
                logger.warning(f"⚡️ [FALLBACK] Лимитки не сработали, входим Market ордером для {symbol}")
                order = await self.exchange.create_order(symbol, 'market', side, lot_size)
                entry_exec = float(order.get("average") or order.get("price") or entry_price)

            # 2. БД
            async with async_session() as session:
                pos = PositionModel(user_id=1, signal_id=signal_id, symbol=symbol, side=SignalType.LONG if direction.upper() == "LONG" else SignalType.SHORT,
                                    entry_price=entry_exec, size=lot_size, stop_loss=stop_price, take_profit=signal_data.get("take_profit"),
                                    status=PositionStatus.OPEN, opened_at=datetime.datetime.utcnow())
                session.add(pos); await session.commit(); await session.refresh(pos)

            # Передаем словарь целей из RuleOf7
            targets = signal_data.get("targets", signal_data.get("take_profit"))
            sl_id, _ = await self._set_protective_orders(symbol, direction, lot_size, stop_price, targets)

            self.active_trades[symbol] = {
                "entry": entry_exec, "stop": stop_price, "stage": 0, "opened_at": time.time(),
                "signal_type": direction.upper(), "current_size": lot_size, "position_db_id": pos.id, "stop_order_id": sl_id
            }
            await send_telegram_msg(f"✅ **ВХОД: {symbol}** ({direction})\nЦена: {entry_exec:.4f}\nСтоп: {stop_price:.4f}")

        except Exception as e:
            logger.error(f"Entry Error {symbol}: {e}")
            async with async_session() as session:
                await session.execute(update(SignalModel).where(SignalModel.id == signal_id).values(status="FAILED")); await session.commit()

    async def schedule_update_positions(self, symbol: str, current_price: float, atr: float):
        """Вызывается каждую минуту для трейлинга"""
        async with self._get_symbol_lock(symbol):
            if symbol not in self.active_trades: return
            trade = self.active_trades[symbol]
            
            # Трейлинг-стоп
            new_stop = self.risk_manager.calculate_trailing_stop(trade['stop'], current_price, atr, trade['signal_type'])
            if abs(new_stop - trade['stop']) > (current_price * 0.0002):
                logger.info(f"🔄 [TRAILING] Moving {symbol} stop: {trade['stop']} -> {new_stop}")
                # Перевыставляем ордер
                await self._cancel_all_orders(symbol)
                sl_id, _ = await self._set_protective_orders(symbol, trade['signal_type'], trade['current_size'], new_stop)
                trade['stop'] = new_stop; trade['stop_order_id'] = sl_id
                async with async_session() as session:
                    await session.execute(update(PositionModel).where(PositionModel.id == trade["position_db_id"]).values(stop_loss=float(new_stop)))
                    await session.commit()

            # Временной выход
            if self.time_exit.should_exit(trade['opened_at'], time.time(), "1h", current_price, trade['entry'], trade['signal_type']):
                await self._close_position(symbol, reason="TIME")

    async def _close_position(self, symbol: str, reason: str = "AUTO"):
        if symbol not in self.active_trades: return
        trade = self.active_trades[symbol]
        try:
            await self._cancel_all_orders(symbol)
            if reason != "EXTERNAL":
                side = 'sell' if trade['signal_type'] == "LONG" else 'buy'
                await self.exchange.create_order(symbol, 'market', side, trade['current_size'])
            
            async with async_session() as session:
                await session.execute(update(PositionModel).where(PositionModel.id == trade["position_db_id"]).values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow()))
                await session.commit()
            
            del self.active_trades[symbol]
            await send_telegram_msg(f"💰 **ЗАКРЫТО: {symbol}**\nПричина: {reason}")
        except Exception as e: logger.error(f"Error closing {symbol}: {e}")

    async def get_account_metrics(self):
        try:
            balance = await self.exchange.fetch_balance()
            total = balance.get('USDT', {}).get('total', 0.0)
            free = balance.get('USDT', {}).get('free', total)
            pos = await self.exchange.fetch_positions()
            active_p = [p for p in pos if abs(float(p.get('contracts', 0) or p.get('pa', 0) or 0)) > 1e-8]
            pnl = sum([float(p.get('unrealizedPnl', 0)) for p in active_p])
            dd = (abs(min(0, pnl)) / total) if total > 0 else 0.0
            return free, dd, len(active_p)
        except: return 0.0, 0.0, 0

    async def _set_leverage_best_effort(self, symbol: str, leverage: int):
        try: await self.exchange.set_leverage(int(leverage), symbol)
        except: pass
