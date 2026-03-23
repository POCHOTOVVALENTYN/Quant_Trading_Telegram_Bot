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
        
        # K4: Кэш метрик аккаунта (TTL 30с)
        self._metrics_cache: Optional[Tuple[float, float, int]] = None
        self._metrics_cache_ts: float = 0
        self._METRICS_CACHE_TTL: float = 30.0
        self._last_ws_reconcile_ts: float = 0.0
        self._entry_policy_activated: bool = False

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

    def _entry_side_to_order_side(self, side: str) -> str:
        """Нормализует направление входа к BUY/SELL."""
        s = (side or "").upper()
        if s in ("BUY", "LONG"):
            return "BUY"
        if s in ("SELL", "SHORT"):
            return "SELL"
        raise ValueError(f"Unsupported side: {side}")

    async def _with_time_sync_retry(self, op, ctx: str = ""):
        """Единый retry для приватных Binance-запросов при -1021."""
        try:
            return await op()
        except Exception as e:
            if "-1021" not in str(e):
                raise
            try:
                await self.exchange.load_time_difference()
            except Exception:
                pass
            logger.warning(f"⏱ [TIME_SYNC] Retry after -1021 ({ctx})")
            return await op()

    def _classify_protective_order(
        self,
        order_type: str,
        trigger_price: float,
        entry_price: float,
        is_long: bool
    ) -> Optional[str]:
        """
        Возвращает тип защиты:
        - "SL" / "TP" при успешной классификации
        - None, если классифицировать нельзя.
        """
        t = (order_type or "").upper()
        if "TAKE_PROFIT" in t:
            return "TP"
        if "STOP" in t and "TAKE_PROFIT" not in t:
            return "SL"

        # Fallback для случаев, когда биржа не прислала type.
        if trigger_price <= 0 or entry_price <= 0:
            return None
        if is_long:
            return "SL" if trigger_price < entry_price else "TP"
        return "SL" if trigger_price > entry_price else "TP"

    def _pick_better_level(
        self,
        current: Optional[float],
        candidate: float,
        *,
        kind: str,
        is_long: bool
    ) -> float:
        """Выбор наиболее релевантного уровня из нескольких ордеров одного типа."""
        if current is None:
            return candidate
        if kind == "SL":
            # LONG: SL ближе к цене сверху среди уровней ниже entry; SHORT — наоборот.
            return max(current, candidate) if is_long else min(current, candidate)
        # TP: LONG — ближайшая цель сверху; SHORT — ближайшая цель снизу.
        return min(current, candidate) if is_long else max(current, candidate)

    async def _get_live_position(self, symbol: str) -> Tuple[float, Optional[str]]:
        """
        Возвращает актуальный размер позиции и сторону из биржи:
        - size: абсолютный размер (0.0 если позиции нет)
        - side: LONG/SHORT или None
        """
        try:
            positions = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_positions([symbol]),
                ctx=f"fetch_positions({symbol})"
            )
        except Exception:
            positions = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_positions(),
                ctx="fetch_positions(all)"
            )

        for pos in (positions or []):
            if self._norm_sym(pos.get("symbol")) != symbol:
                continue

            raw_contracts = pos.get("contracts", 0)
            if raw_contracts is None:
                raw_contracts = pos.get("pa", 0)

            contracts = float(raw_contracts or 0.0)
            if abs(contracts) <= 1e-8:
                continue

            side = str(pos.get("side", "")).upper()
            if side in ("LONG", "SHORT"):
                return abs(contracts), side
            return abs(contracts), ("LONG" if contracts > 0 else "SHORT")

        return 0.0, None

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
                    now = time.time()
                    if (now - self._last_ws_reconcile_ts) > 30:
                        self._last_ws_reconcile_ts = now
                        await self.reconcile_full()
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
                    now = time.time()
                    if (now - self._last_ws_reconcile_ts) > 30:
                        self._last_ws_reconcile_ts = now
                        await self.reconcile_full()
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
            # M4: Поиск фильтра по filterType вместо хардкод-индекса
            step_size = '0.001'
            for f in market['info'].get('filters', []):
                if f.get('filterType') == 'LOT_SIZE':
                    step_size = f.get('stepSize', '0.001')
                    break
            step = Decimal(str(step_size))
            return float(Decimal(str(amount)).quantize(step, rounding=ROUND_DOWN))
        except Exception as e:
            logger.warning(f"[NORM_AMT] Fallback for {symbol}: {e}")
            return amount

    async def _normalize_price(self, symbol: str, price: float) -> float:
        try:
            if not self.exchange.markets: await self.exchange.load_markets()
            market = self.exchange.market(symbol)
            # M4: Поиск фильтра по filterType
            tick_size = '0.01'
            for f in market['info'].get('filters', []):
                if f.get('filterType') == 'PRICE_FILTER':
                    tick_size = f.get('tickSize', '0.01')
                    break
            tick = Decimal(str(tick_size))
            return float(Decimal(str(price)).quantize(tick, rounding=ROUND_HALF_UP))
        except Exception as e:
            logger.warning(f"[NORM_PRC] Fallback for {symbol}: {e}")
            return price

    async def _get_position_mode(self) -> bool:
        """Dual Side (Hedge) mode check"""
        try:
            res = await self.exchange.request('positionSide/dual', 'fapiPrivate', 'GET', {})
            return res.get('dualSidePosition', False)
        except: return False

    async def _cancel_all_orders(self, symbol: str):
        """Отмена всех ордеров по символу (включая Algo) — K3: с retry и логированием"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 1. Обычные ордера
                await self._with_time_sync_retry(
                    lambda: self.exchange.cancel_all_orders(symbol),
                    ctx=f"cancel_all_orders({symbol})"
                )
                # 2. Algo ордера: читаем список и отменяем по algoId.
                clean_sym = symbol.replace("/", "").split(":")[0]
                algo_raw = await self._with_time_sync_retry(
                    lambda: self.exchange.request('openAlgoOrders', 'fapiPrivate', 'GET', {'symbol': clean_sym}),
                    ctx=f"openAlgoOrders({symbol})"
                )
                algo_orders = algo_raw.get("algoOrders", []) if isinstance(algo_raw, dict) else (algo_raw if isinstance(algo_raw, list) else [])
                for ao in algo_orders:
                    algo_id = ao.get("algoId")
                    if not algo_id:
                        continue
                    try:
                        await self._with_time_sync_retry(
                            lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'DELETE', {'symbol': clean_sym, 'algoId': str(algo_id)}),
                            ctx=f"cancel_algo({symbol}:{algo_id})"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ [CANCEL] Не удалось отменить algo {algo_id} для {symbol}: {e}")
                return  # Успех
            except Exception as e:
                logger.warning(f"⚠️ [CANCEL] Попытка {attempt+1}/{max_retries} для {symbol}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    logger.error(f"❌ [CANCEL] Не удалось отменить ордера {symbol} после {max_retries} попыток: {e}")

    # --- CORE LOGIC ---

    async def _set_protective_orders(self, symbol: str, side: str, amount: float, sl: float, tp: Optional[float] = None) -> Tuple[Optional[str], Optional[str]]:
        """Унифицированная установка SL и TP через Algo API (K6: fix closePosition для partial TP)"""
        sl_id, tp_id = None, None
        try:
            side = self._entry_side_to_order_side(side)
            is_hedge = await self._get_position_mode()
            clean_sym = symbol.replace("/", "").split(":")[0]
            reduce_side = "SELL" if side.upper() == "BUY" else "BUY"

            # 1. STOP LOSS (closePosition=true — закрыть всё)
            sl_p = {
                "symbol": clean_sym,
                "side": reduce_side,
                "algoType": "CONDITIONAL",
                "type": "STOP_MARKET",
                "triggerPrice": str(await self._normalize_price(symbol, sl)),
                "closePosition": "true",
                "workingType": "MARK_PRICE"
            }
            if is_hedge:
                sl_p["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"
                sl_p["reduceOnly"] = "true" # В хедж-режиме может быть полезно, но проверим
            
            res_sl = await self._with_time_sync_retry(
                lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'POST', sl_p),
                ctx=f"set_sl({symbol})"
            )
            sl_id = str(res_sl.get("algoId"))
            logger.info(f"🛡 [PROTECT] SL установлен для {symbol}: {sl}")

            # 2. TAKE PROFIT (Scale-out: Частичная фиксация)
            if tp:
                tp_ids = []
                if isinstance(tp, dict):
                    portions = [0.5, 0.3, 0.2]
                    targets = list(tp.values())
                    remaining_amount = Decimal(str(amount))
                    
                    for i, target_price in enumerate(targets[:3]):
                        part_amt = float((Decimal(str(amount)) * Decimal(str(portions[i]))).quantize(Decimal('0.001'), rounding=ROUND_DOWN))
                        if i == 2 or i == len(targets)-1: part_amt = float(remaining_amount)
                        if part_amt <= 0: continue
                        remaining_amount -= Decimal(str(part_amt))
                        norm_part_amt = await self._normalize_amount(symbol, part_amt)
                        if norm_part_amt <= 0:
                            continue
                        
                        tp_p = {
                            "symbol": clean_sym,
                            "side": reduce_side,
                            "algoType": "CONDITIONAL",
                            "type": "TAKE_PROFIT_MARKET",
                            "triggerPrice": str(await self._normalize_price(symbol, target_price)),
                            "quantity": str(norm_part_amt),
                            "reduceOnly": "true",
                            "workingType": "MARK_PRICE"
                        }
                        if is_hedge: tp_p["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"
                        
                        res_tp = await self._with_time_sync_retry(
                            lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'POST', tp_p),
                            ctx=f"set_partial_tp({symbol})"
                        )
                        if res_tp.get("algoId"):
                            tp_ids.append(str(res_tp["algoId"]))
                            logger.info(f"🎯 [PARTIAL TP {i+1}] {symbol} объем {norm_part_amt} по {target_price}")
                            
                    tp_id = ",".join(tp_ids)
                else:
                    tp_p = {
                        "symbol": clean_sym,
                        "side": reduce_side,
                        "algoType": "CONDITIONAL",
                        "type": "TAKE_PROFIT_MARKET",
                        "triggerPrice": str(await self._normalize_price(symbol, tp)),
                        "closePosition": "true",
                        "workingType": "MARK_PRICE"
                    }
                    if is_hedge: 
                        tp_p["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"
                        tp_p["reduceOnly"] = "true"
                    
                    res_tp = await self._with_time_sync_retry(
                        lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'POST', tp_p),
                        ctx=f"set_tp({symbol})"
                    )
                    tp_id = str(res_tp.get("algoId"))
                    logger.info(f"🎯 [PROTECT] TP установлен для {symbol}: {tp}")

        except Exception as e:
            logger.error(f"❌ [PROTECT] Ошибка защиты {symbol}: {e}")
        return sl_id, tp_id

    async def reconcile_full(self):
        """Полная синхронизация: Биржа -> БД -> Memory"""
        try:
            await self.exchange.load_time_difference()
            pos_data = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_positions(),
                ctx="reconcile.fetch_positions"
            )
            std_orders = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_open_orders(),
                ctx="reconcile.fetch_open_orders"
            )
            algo_raw = await self._with_time_sync_retry(
                lambda: self.exchange.request('openAlgoOrders', 'fapiPrivate', 'GET', {}),
                ctx="reconcile.openAlgoOrders"
            )
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
                ex_orders_by_symbol[s].append({
                    "id": o.get("algoId"),
                    "type": (o.get("type") or "").upper(),
                    "stopPrice": (o.get("triggerPrice") or o.get("stopPrice")),
                })

            # Читаем БД
            async with async_session() as session:
                res = await session.execute(select(PositionModel).where(PositionModel.status == PositionStatus.OPEN))
                db_positions_raw = res.scalars().all()
                db_positions = {self._norm_sym(p.symbol): p for p in db_positions_raw}

            new_active = {}
            for p in (pos_data or []):
                contracts = float(p.get("contracts", 0) or p.get("pa", 0) or 0)
                symbol = self._norm_sym(p.get("symbol", ""))
                if not symbol or abs(contracts) <= 1e-8: continue

                entry = float(p.get("entryPrice") or 0.0)
                # K2: Проверка направления (CCXT возвращает строковое поле side: long/short)
                is_long = p.get("side", "").lower() == "long"
                contracts = abs(contracts) # Гарантируем абсолютный объем для дальнейших расчетов
                
                # Ищем ВСЕ записи в БД для этого символа (Этап: Очистка дублей)
                matching_db = [p_db for p_db in db_positions_raw if self._norm_sym(p_db.symbol) == symbol and p_db.status == PositionStatus.OPEN]
                
                dbp = None
                if not matching_db:
                    # Это позиция открытая вручную или до перезапуска
                    sl_def = entry * (0.95 if is_long else 1.05)
                    tp_def = entry * (1.10 if is_long else 0.90)
                    dbp = PositionModel(
                        user_id=1, symbol=symbol,
                        side=SignalType.LONG if is_long else SignalType.SHORT,
                        size=contracts, entry_price=entry,
                        status=PositionStatus.OPEN,
                        stop_loss=sl_def, take_profit=tp_def,
                        opened_at=datetime.datetime.utcnow()
                    )
                    session.add(dbp); await session.commit(); await session.refresh(dbp)
                else:
                    # Берем самую последнюю запись, остальные гасим (если One-way режим)
                    dbp = matching_db[-1]
                    if len(matching_db) > 1:
                        for extra in matching_db[:-1]: 
                            extra.status = PositionStatus.CLOSED
                            extra.closed_at = datetime.datetime.utcnow()
                        await session.commit()
                    
                    # ПРИНУДИТЕЛЬНО СИНХРОНИЗИРУЕМ ОБЪЕМ ИЗ БИРЖИ В БД
                    if abs(float(dbp.size) - contracts) > 1e-6:
                        logger.info(f"📊 [SYNC] Обновлен объем {symbol}: {dbp.size} -> {contracts}")
                        dbp.size = contracts
                        await session.commit()

                # Ищем SL и TP на бирже (K9: Независимый поиск в ex_orders)
                found_sl, found_tp = None, None
                sl_id, tp_id = None, None
                for o in ex_orders_by_symbol.get(symbol, []):
                    trig = float(o.get("stopPrice") or 0.0)
                    if trig <= 0:
                        continue
                    kind = self._classify_protective_order(
                        str(o.get("type", "")),
                        trig,
                        entry,
                        is_long
                    )
                    if kind == "SL":
                        found_sl = self._pick_better_level(found_sl, trig, kind="SL", is_long=is_long)
                        sl_id = o.get("id")
                    elif kind == "TP":
                        found_tp = self._pick_better_level(found_tp, trig, kind="TP", is_long=is_long)
                        tp_id = o.get("id")

                # РЕАНИМАЦИЯ: Если нет SL ИЛИ нет TP - восстанавливаем недостающее
                if (not sl_id and dbp.stop_loss) or (not tp_id and dbp.take_profit):
                    logger.warning(f"🛡 [RESCUE] Восстанавливаю защиту для {symbol} (SL: {sl_id}, TP: {tp_id})")
                    # Пересоздаем оба для гарантии консистентности (Binance закроет дубли по ID если что)
                    res_sl_id, res_tp_id = await self._set_protective_orders(symbol, "BUY" if is_long else "SELL", contracts, dbp.stop_loss, dbp.take_profit)
                    if res_sl_id: sl_id = res_sl_id; dbp.sl_order_id = sl_id
                    if res_tp_id: tp_id = res_tp_id; dbp.tp_order_id = tp_id
                    await session.commit()
                    
                    await send_telegram_msg(
                        f"🛡 **RESCUE: Восстановление защиты**\n\n"
                        f"🔸 Символ: {symbol}\n"
                        f"🎯 TP: {f'{dbp.take_profit:.4f}' if dbp.take_profit else 'N/A'}\n"
                        f"✅ Защитные ордера успешно выставлены на бирже."
                    )

                # Санити для кеша: для LONG stop < entry, для SHORT stop > entry.
                cache_stop = found_sl or dbp.stop_loss
                if cache_stop:
                    bad_stop = (is_long and float(cache_stop) >= entry) or ((not is_long) and float(cache_stop) <= entry)
                    if bad_stop:
                        fallback_stop = dbp.stop_loss if dbp.stop_loss else (entry * (0.95 if is_long else 1.05))
                        logger.warning(f"⚠️ [RECONCILE] Invalid stop for {symbol}: {cache_stop} vs entry={entry}. Use fallback={fallback_stop}")
                        cache_stop = fallback_stop

                new_active[symbol] = {
                    "entry": entry, 
                    "stop": cache_stop,
                    "take_profit_live": found_tp or dbp.take_profit,
                    "stage": 0, 
                    "opened_at": dbp.opened_at.timestamp(),
                    "signal_type": "LONG" if is_long else "SHORT", 
                    "current_size": contracts,
                    "position_db_id": dbp.id, 
                    "stop_order_id": sl_id,
                    "tp_order_id": tp_id,
                    "initial_stop": float(dbp.stop_loss or cache_stop or 0.0),
                    "be_moved": (float(cache_stop) >= entry) if is_long else (float(cache_stop) <= entry),
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

        if not settings.is_trading_enabled: return
        
        # 1. Быстрая проверка вне лока (оптимизация)
        if symbol in self.active_trades:
            logger.info(f"⏭ [{symbol}] Уже в работе (пре-чек). Пропускаю.")
            return

        # 2. Блокировка по символу для предотвращения гонки сигналов
        async with self._get_symbol_lock(symbol):
            # Повторная проверка внутри лока (Double-Checked Locking)
            if symbol in self.active_trades:
                logger.info(f"⏭ [{symbol}] Уже в работе (лочед-чек). Пропускаю.")
                return

            # Новые правила размера/лимита входов применяем только когда бот полностью "плоский".
            if settings.apply_new_entry_rules_after_flat and not self._entry_policy_activated:
                _, _, live_open_count = await self.get_account_metrics()
                if live_open_count > 0:
                    logger.info(f"⏳ [{symbol}] Новые правила входа активируются после закрытия текущих позиций ({live_open_count} открыто).")
                    return
                self._entry_policy_activated = True
                logger.info("✅ Новые правила входа активированы: max_open_trades=3, margin_per_trade=5%, pyramiding=OFF")

            if not self.risk_manager.check_trade_allowed(open_count, drawdown):
                logger.warning(f"🚫 [{symbol}] Риск-менеджер запретил вход (Drawdown/Limit)")
                return

            # Idempotency (по ID сигнала в БД)
            async with async_session() as session:
                stmt = update(SignalModel).where(SignalModel.id == signal_id, SignalModel.status == "PENDING").values(status="EXECUTING")
                res = await session.execute(stmt); await session.commit()
                if res.rowcount == 0:
                    logger.info(f"⏭ [{symbol}] Сигнал {signal_id} уже обрабатывается или исполнен.")
                    return

            try:
                # 3. Расчет параметров входа
                entry_price = float(signal_data['entry_price'])
                stop_price = self.risk_manager.calculate_atr_stop(entry_price, signal_data.get('atr', 100.0), direction)
                lot_size = self.risk_manager.calculate_position_size(account_balance, entry_price, stop_price)
                lot_size = await self._normalize_amount(symbol, lot_size)
                
                if lot_size <= 0: raise Exception("Zero lot size after normalization")

                # 4. Умный Вход (Limit Chasing)
                await self._set_leverage_best_effort(symbol, settings.leverage)
                side = 'buy' if direction.upper() == 'LONG' else 'sell'
                
                entry_exec = entry_price
                max_retries = 3
                remaining_size = lot_size
                filled_total = 0.0

                for attempt in range(max_retries):
                    if remaining_size <= 0: break
                    try:
                        ob = await self.exchange.fetch_order_book(symbol, limit=5)
                        best_price = ob['bids'][0][0] if side == 'buy' else ob['asks'][0][0]
                        best_price = await self._normalize_price(symbol, best_price)
                        
                        logger.info(f"🕸 [LIMIT CHASE] Попытка {attempt+1}: Лимитка {side} {symbol} по {best_price}")
                        temp_order = await self.exchange.create_order(
                            symbol=symbol, type='limit', side=side, amount=remaining_size, price=best_price,
                            params={'timeInForce': 'GTX', 'postOnly': True}
                        )
                        
                        await asyncio.sleep(3)
                        
                        check = await self.exchange.fetch_order(temp_order['id'], symbol)
                        filled_now = float(check.get('filled', 0.0) or 0.0)
                        
                        if check['status'] == 'closed':
                            entry_exec = float(check.get('average') or check.get('price') or entry_price)
                            filled_total += filled_now
                            remaining_size = 0.0
                            break
                        else:
                            await self.exchange.cancel_order(temp_order['id'], symbol)
                            if filled_now > 0:
                                filled_total += filled_now
                                remaining_size -= filled_now
                                entry_exec = float(check.get('average') or check.get('price') or entry_price)
                            
                    except Exception as e:
                        logger.warning(f"Limit chase error: {e}")
                        
                # 5. Fallback (Market)
                if remaining_size > 0:
                    logger.warning(f"⚡️ [FALLBACK] Добиваем остаток {remaining_size} Market ордером для {symbol}")
                    try:
                        fallback_order = await self.exchange.create_order(symbol, 'market', side, remaining_size)
                        if filled_total == 0:
                            entry_exec = float(fallback_order.get("average") or fallback_order.get("price") or entry_price)
                        filled_total += float(fallback_order.get("filled", remaining_size))
                    except Exception as e:
                        logger.error(f"Market fallback error: {e}")
                        if filled_total == 0: raise e
                
                lot_size = filled_total

                # 6. БД Позиция
                async with async_session() as session:
                    pos = PositionModel(user_id=1, signal_id=signal_id, symbol=symbol, side=SignalType.LONG if direction.upper() == "LONG" else SignalType.SHORT,
                                        entry_price=entry_exec, size=lot_size, stop_loss=stop_price, take_profit=signal_data.get("take_profit"),
                                        status=PositionStatus.OPEN, opened_at=datetime.datetime.utcnow())
                    session.add(pos); await session.commit(); await session.refresh(pos)

                # 7. Защитные ордера
                targets = signal_data.get("targets", signal_data.get("take_profit"))
                sl_id, tp_id = await self._set_protective_orders(symbol, direction, lot_size, stop_price, targets)
                if not sl_id:
                    # Best practice: не оставлять открытую позицию без стопа.
                    emergency_side = 'sell' if direction.upper() == "LONG" else 'buy'
                    await self.exchange.create_order(symbol, 'market', emergency_side, lot_size)
                    async with async_session() as session:
                        await session.execute(
                            update(PositionModel)
                            .where(PositionModel.id == pos.id)
                            .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                        )
                        await session.commit()
                    raise Exception("Protective STOP was not created; position closed by emergency market order")

                # 8. Финализация
                self.active_trades[symbol] = {
                    "entry": entry_exec, "stop": stop_price, "stage": 0, "opened_at": time.time(),
                    "signal_type": direction.upper(), "current_size": lot_size, "position_db_id": pos.id, "stop_order_id": sl_id,
                    "take_profit_live": signal_data.get("targets", signal_data.get("take_profit")),
                    "tp_order_id": tp_id,
                    "initial_stop": stop_price,
                    "be_moved": False,
                    "timeframe": signal_data.get("timeframe", "1h")
                }
                
                # Обновляем сигнал в БД как EXECUTED
                async with async_session() as session:
                    await session.execute(update(SignalModel).where(SignalModel.id == signal_id).values(status="EXECUTED"))
                    await session.commit()

                await send_telegram_msg(f"✅ **ВХОД: {symbol}** ({direction})\nЦена: {entry_exec:.4f}\nСтоп: {stop_price:.4f}")

            except Exception as e:
                logger.error(f"❌ Entry Error {symbol}: {e}")
                async with async_session() as session:
                    await session.execute(update(SignalModel).where(SignalModel.id == signal_id).values(status="FAILED"))
                    await session.commit()

    async def schedule_update_positions(self, symbol: str, current_price: float, atr: float, adx: Optional[float] = None):
        """Вызывается каждую минуту для трейлинга"""
        async with self._get_symbol_lock(symbol):
            if symbol not in self.active_trades: return
            trade = self.active_trades[symbol]
            # Синхронизируем размер из биржи: после partial TP локальный current_size может устареть.
            live_size, _ = await self._get_live_position(symbol)
            if live_size <= 1e-8:
                await self._close_position(symbol, reason="EXTERNAL")
                return
            if abs(live_size - float(trade.get("current_size", 0.0))) > 1e-8:
                trade["current_size"] = live_size
                async with async_session() as session:
                    await session.execute(
                        update(PositionModel)
                        .where(PositionModel.id == trade["position_db_id"])
                        .values(size=float(live_size))
                    )
                    await session.commit()

            # Явный BE-триггер (отдельно от trailing):
            # 1) reached 1R
            # 2) подтверждение: ADX>=20 или breakout >= 0.5 ATR
            if not trade.get("be_moved", False):
                entry = float(trade.get("entry", 0.0) or 0.0)
                initial_stop = float(trade.get("initial_stop", trade.get("stop", 0.0)) or 0.0)
                side = str(trade.get("signal_type", "")).upper()
                risk_distance = abs(entry - initial_stop)
                atr_safe = max(float(atr or 0.0), 0.0)
                breakout_dist = max(atr_safe * 0.5, entry * 0.0015 if entry > 0 else 0.0)
                adx_ok = (adx is not None) and (float(adx) >= 20.0)

                if entry > 0 and risk_distance > 0 and side in ("LONG", "SHORT"):
                    if side == "LONG":
                        reached_1r = current_price >= (entry + risk_distance)
                        breakout_ok = current_price >= (entry + breakout_dist)
                    else:
                        reached_1r = current_price <= (entry - risk_distance)
                        breakout_ok = current_price <= (entry - breakout_dist)

                    if reached_1r and (adx_ok or breakout_ok):
                        be_buffer = entry * 0.0004  # 0.04% буфер на комиссию/проскальзывание
                        be_stop = (entry + be_buffer) if side == "LONG" else (entry - be_buffer)
                        current_stop = float(trade.get("stop", initial_stop) or initial_stop)
                        improves = (side == "LONG" and be_stop > current_stop) or (side == "SHORT" and be_stop < current_stop)

                        if improves:
                            await self._cancel_all_orders(symbol)
                            tp_ref = trade.get("take_profit_live")
                            sl_id, tp_id = await self._set_protective_orders(symbol, side, trade['current_size'], be_stop, tp_ref)
                            if sl_id:
                                trade['stop'] = be_stop
                                trade['stop_order_id'] = sl_id
                                trade['be_moved'] = True
                                if tp_id:
                                    trade['tp_order_id'] = tp_id
                                async with async_session() as session:
                                    await session.execute(
                                        update(PositionModel)
                                        .where(PositionModel.id == trade["position_db_id"])
                                        .values(stop_loss=float(be_stop))
                                    )
                                    await session.commit()
                                await send_telegram_msg(
                                    f"🟡 **BE MOVE: {symbol}**\n"
                                    f"Stop -> {be_stop:.6f}\n"
                                    f"Trigger: 1R + {'ADX' if adx_ok else 'Breakout'}"
                                )
            
            # Трейлинг-стоп
            new_stop = self.risk_manager.calculate_trailing_stop(trade['stop'], current_price, atr, trade['signal_type'])
            if abs(new_stop - trade['stop']) > (current_price * 0.0002):
                logger.info(f"🔄 [TRAILING] Moving {symbol} stop: {trade['stop']} -> {new_stop}")
                # Перевыставляем ордер
                await self._cancel_all_orders(symbol)
                # TP должны оставаться активными после трейлинга.
                tp_ref = trade.get("take_profit_live")
                sl_id, tp_id = await self._set_protective_orders(symbol, trade['signal_type'], trade['current_size'], new_stop, tp_ref)
                trade['stop'] = new_stop; trade['stop_order_id'] = sl_id
                if tp_id:
                    trade['tp_order_id'] = tp_id
                async with async_session() as session:
                    await session.execute(update(PositionModel).where(PositionModel.id == trade["position_db_id"]).values(stop_loss=float(new_stop)))
                    await session.commit()

            # Временной выход (M5: используем реальный ТФ сигнала вместо хардкода "1h")
            trade_tf = trade.get('timeframe', '1h')
            if self.time_exit.should_exit(trade['opened_at'], time.time(), trade_tf, current_price, trade['entry'], trade['signal_type']):
                await self._close_position(symbol, reason="TIME")
                return

            # Пирамидинг (Баг 3.2 — ATR-based пирамидинг Швагера)
            if settings.pyramiding_enabled and self.pyramiding.check_next_entry_allowed(current_price, trade['entry'], atr, trade['signal_type']):
                next_stage = trade['stage'] + 1
                if next_stage < len(self.pyramiding.allocation_pct):
                    balance, _, _ = await self.get_account_metrics()
                    add_size = self.pyramiding.get_allocation_amount(balance, next_stage, current_price)
                    add_size = await self._normalize_amount(symbol, add_size)
                    
                    if add_size > 1e-8:
                        logger.info(f"💎 [PYRAMID] Adding {add_size} to {symbol} (Stage {next_stage})")
                        try:
                            side = 'buy' if trade['signal_type'] == "LONG" else 'sell'
                            await self.exchange.create_order(symbol, 'market', side, add_size)
                            
                            # Обновляем среднюю и объем
                            old_size = trade['current_size']
                            new_size = old_size + add_size
                            new_entry = ((trade['entry'] * old_size) + (current_price * add_size)) / new_size
                            
                            trade['stage'] = next_stage
                            trade['current_size'] = new_size
                            trade['entry'] = new_entry
                            
                            async with async_session() as session:
                                await session.execute(
                                    update(PositionModel)
                                    .where(PositionModel.id == trade["position_db_id"])
                                    .values(size=float(new_size), entry_price=float(new_entry))
                                )
                                await session.commit()
                            
                            # Переставляем стопы
                            await self._cancel_all_orders(symbol)
                            tp_ref = trade.get("take_profit_live")
                            sl_id, tp_id = await self._set_protective_orders(symbol, trade['signal_type'], new_size, trade['stop'], tp_ref)
                            trade['stop_order_id'] = sl_id
                            if tp_id:
                                trade['tp_order_id'] = tp_id
                            
                            await send_telegram_msg(
                                f"💎 **ДОБОР: {symbol}** (Этап {next_stage})\n"
                                f"📈 Новый объем: {new_size:.4f}\n"
                                f"💰 Новая средняя: {new_entry:.4f}"
                            )
                        except Exception as e:
                            logger.error(f"Pyramid error for {symbol}: {e}")

    async def _close_position(self, symbol: str, reason: str = "AUTO"):
        if symbol not in self.active_trades: return
        trade = self.active_trades[symbol]
        try:
            await self._cancel_all_orders(symbol)
            if reason != "EXTERNAL":
                side = 'sell' if trade['signal_type'] == "LONG" else 'buy'
                live_size, live_side = await self._get_live_position(symbol)
                close_amount = live_size if live_size > 1e-8 else trade['current_size']
                # Если направление на бирже не совпадает с локальным кешем, не отправляем рыночный close, чтобы не перевернуть позицию.
                if live_side and live_side != trade['signal_type']:
                    logger.warning(f"⚠️ [CLOSE] Side mismatch for {symbol}: local={trade['signal_type']} live={live_side}. Skip market close.")
                    return
                elif close_amount > 1e-8:
                    await self.exchange.create_order(symbol, 'market', side, close_amount)
            
            async with async_session() as session:
                await session.execute(
                    update(PositionModel)
                    .where(PositionModel.id == trade["position_db_id"])
                    .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                )
                
                # Добавить PnL-запись (Баг 4.3)
                if reason != "EXTERNAL":
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        exit_price = ticker['last']
                        entry = trade['entry']
                        size = trade['current_size']
                        is_long = trade['signal_type'] == "LONG"
                        
                        pnl_usd = (exit_price - entry) * size if is_long else (entry - exit_price) * size
                        pnl_pct = ((exit_price / entry) - 1) * 100 if is_long else ((entry / exit_price) - 1) * 100
                        
                        pnl_rec = PnLModel(
                            user_id=1,
                            symbol=symbol,
                            pnl_usd=pnl_usd,
                            pnl_pct=pnl_pct,
                            reason=reason
                        )
                        session.add(pnl_rec)
                    except Exception as e:
                        logger.warning(f"PnL record error: {e}")
                
                await session.commit()
            
            del self.active_trades[symbol]
            await send_telegram_msg(f"💰 **ЗАКРЫТО: {symbol}**\nПричина: {reason}")
        except Exception as e: logger.error(f"Error closing {symbol}: {e}")

    async def manual_close(self, symbol: str) -> bool:
        """Публичный метод для ручного закрытия из Telegram. (Баг 4.1)"""
        if symbol in self.active_trades:
            await self._close_position(symbol, reason="MANUAL")
            return True
        return False

    async def get_account_metrics(self):
        """K4: Метрики с кэшированием (TTL 30с). При ошибке возвращает последнее известное значение."""
        now = time.time()
        if self._metrics_cache and (now - self._metrics_cache_ts) < self._METRICS_CACHE_TTL:
            return self._metrics_cache
        
        try:
            balance = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_balance(),
                ctx="metrics.fetch_balance"
            )
            total = balance.get('USDT', {}).get('total', 0.0)
            free = balance.get('USDT', {}).get('free', total)
            pos = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_positions(),
                ctx="metrics.fetch_positions"
            )
            active_p = [p for p in pos if abs(float(p.get('contracts', 0) or p.get('pa', 0) or 0)) > 1e-8]
            pnl = sum([float(p.get('unrealizedPnl', 0)) for p in active_p])
            dd = (abs(min(0, pnl)) / total) if total > 0 else 0.0
            result = (free, dd, len(active_p))
            # Обновляем кэш
            self._metrics_cache = result
            self._metrics_cache_ts = now
            return result
        except Exception as e:
            logger.warning(f"⚠️ [METRICS] Ошибка получения метрик: {e}")
            # K4: Возвращаем последнее известное значение вместо нулей
            if self._metrics_cache:
                logger.info("[METRICS] Используем кэшированные метрики")
                return self._metrics_cache
            return 0.0, 0.0, 0

    async def _set_leverage_best_effort(self, symbol: str, leverage: int):
        try: await self.exchange.set_leverage(int(leverage), symbol)
        except: pass
