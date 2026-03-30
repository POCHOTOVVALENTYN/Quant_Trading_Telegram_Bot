import asyncio
import time
import datetime
import traceback
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
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
        self._metrics_lock: asyncio.Lock = asyncio.Lock()
        self._metrics_fail_streak: int = 0
        self._metrics_backoff_until: float = 0.0
        self._balance_cache: Optional[Tuple[float, float]] = None  # (free, total)
        self._balance_cache_ts: float = 0.0
        self._balance_backoff_until: float = 0.0
        self._balance_fail_streak: int = 0
        self._positions_cache: Optional[Tuple[float, int]] = None  # (drawdown, open_count)
        self._positions_cache_ts: float = 0.0
        self._positions_backoff_until: float = 0.0
        self._positions_fail_streak: int = 0
        self._last_ws_reconcile_ts: float = 0.0
        self._entry_policy_activated: bool = False
        self._rescue_cooldown_until: Dict[str, float] = {}
        self._soft_cleanup_last_ts: float = 0.0

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
        max_attempts = 3
        delay = 0.4
        last_err = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await op()
            except Exception as e:
                last_err = e
                if "-1021" not in str(e):
                    raise
                try:
                    await self.exchange.load_time_difference()
                except Exception:
                    pass
                logger.warning(f"⏱ [TIME_SYNC] Retry {attempt}/{max_attempts} after -1021 ({ctx})")
                if attempt < max_attempts:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2.0, 2.0)
        raise last_err

    async def _prepare_private_ops(self, ctx: str = ""):
        """Preflight sync для серии приватных запросов."""
        try:
            await self.exchange.load_time_difference()
        except Exception as e:
            logger.warning(f"⚠️ [TIME_SYNC] Preflight failed ({ctx}): {e}")

    async def _get_reference_price(self, symbol: str) -> Optional[float]:
        """Mark/last цена для safety-check защитных триггеров."""
        try:
            t = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_ticker(symbol),
                ctx=f"fetch_ticker({symbol})"
            )
            px = t.get("mark") or t.get("last") or t.get("close")
            return float(px) if px else None
        except Exception:
            return None

    def _is_valid_stop_side(self, trigger: float, reference: float, side: str) -> bool:
        """
        side = BUY/LONG -> stop ниже рынка
        side = SELL/SHORT -> stop выше рынка
        """
        s = (side or "").upper()
        if s in ("BUY", "LONG"):
            return trigger < reference
        if s in ("SELL", "SHORT"):
            return trigger > reference
        return False

    def _classify_protective_order(
        self,
        order_type: str,
        trigger_price: float,
        entry_price: float,
        is_long: bool,
        db_stop: float = 0.0,
        db_tp: float = 0.0,
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

        if trigger_price <= 0 or entry_price <= 0:
            return None

        # Fallback: type unknown (testnet algo orders). Match by proximity to DB values first.
        if db_stop > 0 and db_tp > 0:
            dist_sl = abs(trigger_price - db_stop)
            dist_tp = abs(trigger_price - db_tp)
            return "SL" if dist_sl <= dist_tp else "TP"

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

    async def _get_live_position(self, symbol: str, preferred_side: Optional[str] = None) -> Tuple[float, Optional[str]]:
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

        norm_pref = (preferred_side or "").upper()
        candidates: List[Tuple[float, str]] = []
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
                candidates.append((abs(contracts), side))
            else:
                candidates.append((abs(contracts), ("LONG" if contracts > 0 else "SHORT")))

        if not candidates:
            return 0.0, None
        if norm_pref in ("LONG", "SHORT"):
            for sz, sd in candidates:
                if sd == norm_pref:
                    return sz, sd
        # fallback: самая крупная позиция по символу
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0]

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
                        try:
                            await self.reconcile_full()
                        except Exception as rec_err:
                            logger.error(f"❌ [WS_ORDERS] reconcile_full failed: {rec_err}")
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
                        try:
                            await self.reconcile_full()
                        except Exception as rec_err:
                            logger.error(f"❌ [WS_POS] reconcile_full failed: {rec_err}")
                    await asyncio.sleep(5)

    async def _handle_order_update(self, order: dict):
        try:
            status = order.get('status')
            symbol = self._norm_sym(order.get('symbol'))
            ex_id = str(order.get('id'))
            
            if status in ['closed', 'filled']:
                is_protective_fill = False
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
                                    is_protective_fill = True
                                    logger.info(f"💾 [WS_DB] Позиция {symbol} закрыта по {ot}")
                        await session.commit()

                if is_protective_fill and symbol in self.active_trades:
                    logger.info(f"🔄 [WS_ORDERS] Удаляю {symbol} из active_trades (SL/TP сработал)")
                    await self._close_position(symbol, reason="EXTERNAL")

                if not is_protective_fill and symbol in self.active_trades:
                    order_type = str(order.get('type', '')).upper()
                    reduce_only = order.get('reduceOnly', False) or order.get('info', {}).get('reduceOnly', False)
                    if reduce_only or any(x in order_type for x in ["STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP"]):
                        live_size, _ = await self._get_live_position(symbol, preferred_side=self.active_trades[symbol].get('signal_type'))
                        if live_size <= 1e-8:
                            logger.info(f"🔄 [WS_ORDERS] Позиция {symbol} полностью закрыта на бирже")
                            await self._close_position(symbol, reason="EXTERNAL")
        except Exception as e:
            logger.error(f"Error handling order update: {e}")

    # --- HELPERS ---

    async def _db_persist_order(
        self,
        *,
        position_id: Optional[int],
        symbol: str,
        exchange_order_id: Optional[str],
        client_order_id: Optional[str],
        order_type: str,
        position_side: str,
        price: Optional[float],
        size: float,
        status: OrderStatus = OrderStatus.FILLED,
    ) -> None:
        """Аудит: биржевые ордера в БД (идемпотентно по exchange/client id)."""
        ex_id = str(exchange_order_id).strip() if exchange_order_id else None
        cl_id = str(client_order_id).strip() if client_order_id else None
        if not ex_id and not cl_id:
            return
        ps = (position_side or "LONG").upper()
        side_enum = SignalType.LONG if ps == "LONG" else SignalType.SHORT
        try:
            async with async_session() as session:
                if ex_id:
                    dup = await session.execute(
                        select(OrderModel.id).where(OrderModel.exchange_order_id == ex_id)
                    )
                    if dup.scalar_one_or_none():
                        return
                if cl_id:
                    dup = await session.execute(
                        select(OrderModel.id).where(OrderModel.client_order_id == cl_id)
                    )
                    if dup.scalar_one_or_none():
                        return
                session.add(
                    OrderModel(
                        user_id=1,
                        position_id=position_id,
                        exchange_order_id=ex_id,
                        client_order_id=cl_id,
                        symbol=symbol,
                        order_type=order_type,
                        side=side_enum,
                        price=float(price or 0.0),
                        size=float(size),
                        status=status,
                    )
                )
                await session.commit()
        except IntegrityError:
            pass
        except Exception as e:
            logger.warning(f"⚠️ [AUDIT] order persist failed ({symbol}): {e}")

    def _position_side_from_entry_side(self, side: str) -> str:
        s = (side or "").upper()
        if s in ("BUY", "LONG"):
            return "LONG"
        if s in ("SELL", "SHORT"):
            return "SHORT"
        return "LONG"

    async def _realized_pnl_from_exchange_trades(
        self, symbol: str, trade: Dict[str, Any]
    ) -> Tuple[float, float]:
        """Сумма realizedPnl по сделкам Binance Futures после открытия позиции."""
        opened_ts = float(trade.get("opened_at", 0) or 0)
        since_ms = max(0, int((opened_ts - 180) * 1000))
        try:
            raw = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_my_trades(symbol, since=since_ms, limit=200),
                ctx=f"audit.fetch_my_trades({symbol})",
            )
        except Exception as e:
            logger.warning(f"⚠️ [AUDIT] fetch_my_trades: {e}")
            return 0.0, 0.0
        total = 0.0
        for t in raw or []:
            info = t.get("info") or {}
            rp = info.get("realizedPnl") or info.get("realizedProfit")
            try:
                total += float(rp or 0)
            except (TypeError, ValueError):
                pass
        entry = float(trade.get("entry", 0) or 0)
        size = float(trade.get("current_size", 0) or 0)
        notional = abs(entry * size) if entry and size else 0.0
        pct = (total / notional * 100.0) if notional > 1e-12 else 0.0
        return total, pct

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

    async def _set_protective_orders(
        self,
        symbol: str,
        side: str,
        amount: float,
        sl: float,
        tp: Optional[float] = None,
        position_id: Optional[int] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Унифицированная установка SL и TP через Algo API (K6: fix closePosition для partial TP)"""
        sl_id, tp_id = None, None
        try:
            side = self._entry_side_to_order_side(side)
            await self._prepare_private_ops(ctx=f"set_protective_orders({symbol})")
            is_hedge = await self._get_position_mode()
            clean_sym = symbol.replace("/", "").split(":")[0]
            reduce_side = "SELL" if side.upper() == "BUY" else "BUY"
            ref_price = await self._get_reference_price(symbol)

            # Dedup перед постановкой новой защиты:
            # удаляем конфликтующие closePosition/GTE ордера того же reduce-side,
            # чтобы избежать -4130 (existing open stop/tp in direction).
            try:
                raw_existing = await self._with_time_sync_retry(
                    lambda: self.exchange.request('openAlgoOrders', 'fapiPrivate', 'GET', {'symbol': clean_sym}),
                    ctx=f"protective_dedup.openAlgoOrders({symbol})"
                )
                existing = raw_existing.get("algoOrders", []) if isinstance(raw_existing, dict) else (raw_existing if isinstance(raw_existing, list) else [])
                for eo in existing:
                    eo_type = str(eo.get("type", "")).upper()
                    algo_id = eo.get("algoId")
                    if not algo_id:
                        continue
                    if ("STOP" in eo_type) or ("TAKE_PROFIT" in eo_type):
                        try:
                            await self._with_time_sync_retry(
                                lambda: self.exchange.request(
                                    'algoOrder', 'fapiPrivate', 'DELETE',
                                    {'symbol': clean_sym, 'algoId': str(algo_id)}
                                ),
                                ctx=f"protective_dedup.cancel({symbol}:{algo_id})"
                            )
                        except Exception as cancel_err:
                            logger.warning(f"⚠️ [PROTECT] Dedup cancel failed {symbol}:{algo_id}: {cancel_err}")
            except Exception as dedup_err:
                logger.warning(f"⚠️ [PROTECT] Dedup precheck failed for {symbol}: {dedup_err}")

            # 1. STOP LOSS (closePosition=true — закрыть всё)
            sl_trigger = await self._normalize_price(symbol, sl)
            if ref_price and not self._is_valid_stop_side(sl_trigger, ref_price, side):
                # Защита от -2021: корректируем триггер чуть дальше от рынка.
                eps = max(ref_price * 0.0015, 1e-8)
                sl_trigger = (ref_price - eps) if side.upper() == "BUY" else (ref_price + eps)
                sl_trigger = await self._normalize_price(symbol, sl_trigger)
                logger.warning(
                    f"⚠️ [PROTECT] Adjust SL trigger for {symbol}: requested={sl:.8f}, ref={ref_price:.8f}, adjusted={sl_trigger:.8f}"
                )
            sl_p = {
                "symbol": clean_sym,
                "side": reduce_side,
                "algoType": "CONDITIONAL",
                "type": "STOP_MARKET",
                "triggerPrice": str(sl_trigger),
                "closePosition": "true",
                "workingType": "MARK_PRICE"
            }
            if is_hedge:
                sl_p["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"
                sl_p["reduceOnly"] = "true" # В хедж-режиме может быть полезно, но проверим
            
            try:
                res_sl = await self._with_time_sync_retry(
                    lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'POST', sl_p),
                    ctx=f"set_sl({symbol})"
                )
                sl_id = str(res_sl.get("algoId"))
                logger.info(f"🛡 [PROTECT] SL установлен для {symbol}: {sl}")
            except Exception as sl_err:
                # Если Binance сообщает, что защитный closePosition уже существует,
                # пробуем подобрать уже выставленный SL и не считаем это фатальной ошибкой.
                err_text = str(sl_err)
                if "-4130" in err_text:
                    try:
                        raw_existing = await self._with_time_sync_retry(
                            lambda: self.exchange.request('openAlgoOrders', 'fapiPrivate', 'GET', {'symbol': clean_sym}),
                            ctx=f"set_sl.recover_openAlgoOrders({symbol})"
                        )
                        existing = raw_existing.get("algoOrders", []) if isinstance(raw_existing, dict) else (raw_existing if isinstance(raw_existing, list) else [])
                        sl_trigger_f = float(sl_trigger)
                        ref_f = float(ref_price) if ref_price is not None else None
                        # Толеранс на округления/нормализацию триггеров.
                        tol = max(abs(sl_trigger_f) * 1e-5, abs(ref_f) * 1e-7 if ref_f else 1e-8)

                        def _close_pos(v) -> bool:
                            vv = str(v).lower().strip()
                            return vv in {"1", "true", "yes", "y"}

                        def _get_trig(eo) -> float:
                            for k in ("stopPrice", "triggerPrice"):
                                try:
                                    v = eo.get(k)
                                    if v is None:
                                        continue
                                    fv = float(v)
                                    if fv > 0:
                                        return fv
                                except Exception:
                                    continue
                            return 0.0

                        entry_side_is_buy = side.upper() == "BUY"
                        correct_side = lambda trig: (trig < ref_f) if (entry_side_is_buy and ref_f is not None) else ((trig > ref_f) if (not entry_side_is_buy and ref_f is not None) else True)

                        stop_like = []
                        for eo in existing:
                            algo_id = eo.get("algoId")
                            if not algo_id:
                                continue
                            type_str = str(eo.get("type", "")).upper()
                            trig = _get_trig(eo)
                            if trig <= 0:
                                continue
                            if not correct_side(trig):
                                continue
                            is_stop = "STOP" in type_str or "STOP_MARKET" in type_str
                            is_close_pos = _close_pos(eo.get("closePosition"))
                            near = abs(trig - sl_trigger_f) <= tol
                            # Приоритет: STOP* + близко к нашему sl_trigger.
                            if is_stop and near:
                                stop_like.append((0, algo_id))
                            elif is_stop and is_close_pos and near:
                                stop_like.append((1, algo_id))
                            elif is_stop and is_close_pos:
                                stop_like.append((2, algo_id))
                            elif near:
                                # Иногда Binance отдаёт тип не содержащий STOP в точности.
                                stop_like.append((3, algo_id))

                        if stop_like:
                            stop_like.sort(key=lambda x: x[0])
                            sl_id = str(stop_like[0][1])
                            logger.warning(f"⚠️ [PROTECT] SL already exists for {symbol}, reuse algoId={sl_id}")
                        else:
                            # Если конкретного SL-кандидата не нашлось — повторно попробуем поставить SL,
                            # но уводим триггер чуть дальше от рынка (уменьшаем шанс на -4130/немедленный триггер).
                            if ref_f is not None:
                                extra = max(ref_f * 0.0005, 1e-8)
                                sl_trigger2 = (sl_trigger_f - extra) if entry_side_is_buy else (sl_trigger_f + extra)
                                sl_trigger2 = await self._normalize_price(symbol, sl_trigger2)
                                if ref_f and self._is_valid_stop_side(sl_trigger2, ref_f, side):
                                    sl_p["triggerPrice"] = str(sl_trigger2)
                                    sl_p2 = dict(sl_p)
                                    res_sl = await self._with_time_sync_retry(
                                        lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'POST', sl_p2),
                                        ctx=f"set_sl.recover_retry({symbol})"
                                    )
                                    sl_id = str(res_sl.get("algoId"))
                                    logger.warning(f"⟳ [PROTECT] SL recover retry success for {symbol}, algoId={sl_id}")
                                else:
                                    logger.error(f"❌ [PROTECT] SL recover retry invalid for {symbol}: {sl_trigger2}")
                                    return None, None
                            else:
                                logger.error(f"❌ [PROTECT] SL missing after -4130 for {symbol}: {sl_err}")
                                return None, None
                    except Exception as rec_err:
                        logger.error(f"❌ [PROTECT] SL recover failed for {symbol}: {rec_err}")
                        return None, None
                else:
                    logger.error(f"❌ [PROTECT] Ошибка SL {symbol}: {sl_err}")
                    return None, None

            if sl_id:
                ps = self._position_side_from_entry_side(side)
                await self._db_persist_order(
                    position_id=position_id,
                    symbol=symbol,
                    exchange_order_id=sl_id,
                    client_order_id=None,
                    order_type="STOP_MARKET_ALGO",
                    position_side=ps,
                    price=float(sl_trigger),
                    size=float(amount),
                    status=OrderStatus.OPEN,
                )

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
                        
                        try:
                            res_tp = await self._with_time_sync_retry(
                                lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'POST', tp_p),
                                ctx=f"set_partial_tp({symbol})"
                            )
                            if res_tp.get("algoId"):
                                aid = str(res_tp["algoId"])
                                tp_ids.append(aid)
                                logger.info(f"🎯 [PARTIAL TP {i+1}] {symbol} объем {norm_part_amt} по {target_price}")
                                ps = self._position_side_from_entry_side(side)
                                tp_px = float(await self._normalize_price(symbol, target_price))
                                await self._db_persist_order(
                                    position_id=position_id,
                                    symbol=symbol,
                                    exchange_order_id=aid,
                                    client_order_id=None,
                                    order_type="TAKE_PROFIT_MARKET_ALGO",
                                    position_side=ps,
                                    price=tp_px,
                                    size=float(norm_part_amt),
                                    status=OrderStatus.OPEN,
                                )
                        except Exception as tp_err:
                            logger.warning(f"⚠️ [PROTECT] Partial TP {i+1} failed for {symbol}: {tp_err}")
                            
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
                    
                    try:
                        res_tp = await self._with_time_sync_retry(
                            lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'POST', tp_p),
                            ctx=f"set_tp({symbol})"
                        )
                        tp_id = str(res_tp.get("algoId"))
                        logger.info(f"🎯 [PROTECT] TP установлен для {symbol}: {tp}")
                        ps = self._position_side_from_entry_side(side)
                        tp_px = float(await self._normalize_price(symbol, tp))
                        await self._db_persist_order(
                            position_id=position_id,
                            symbol=symbol,
                            exchange_order_id=tp_id,
                            client_order_id=None,
                            order_type="TAKE_PROFIT_MARKET_ALGO",
                            position_side=ps,
                            price=tp_px,
                            size=float(amount),
                            status=OrderStatus.OPEN,
                        )
                    except Exception as tp_err:
                        logger.warning(f"⚠️ [PROTECT] TP failed for {symbol}, keep SL active: {tp_err}")

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

            # Единая сессия для всех операций с БД
            async with async_session() as session:
                res = await session.execute(select(PositionModel).where(PositionModel.status == PositionStatus.OPEN))
                db_positions_raw = res.scalars().all()
                db_positions = {self._norm_sym(p.symbol): p for p in db_positions_raw}

                new_active = {}
                for p in (pos_data or []):
                    contracts = float(p.get("contracts", 0) or p.get("pa", 0) or 0)
                    symbol = self._norm_sym(p.get("symbol", ""))
                    if not symbol or abs(contracts) <= 1e-8:
                        continue

                    entry = float(p.get("entryPrice") or 0.0)
                    is_long = p.get("side", "").lower() == "long"
                    contracts = abs(contracts)

                    matching_db = [p_db for p_db in db_positions_raw if self._norm_sym(p_db.symbol) == symbol and p_db.status == PositionStatus.OPEN]

                    dbp = None
                    if not matching_db:
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
                        session.add(dbp)
                        await session.flush()
                    else:
                        dbp = matching_db[-1]
                        if len(matching_db) > 1:
                            for extra in matching_db[:-1]:
                                extra.status = PositionStatus.CLOSED
                                extra.closed_at = datetime.datetime.utcnow()
                            await session.flush()

                        if abs(float(dbp.size) - contracts) > 1e-6:
                            logger.info(f"[SYNC] Volume {symbol}: {dbp.size} -> {contracts}")
                            dbp.size = contracts
                            await session.flush()

                    found_sl, found_tp = None, None
                    sl_id, tp_id = None, None
                    db_sl_val = float(dbp.stop_loss or 0.0)
                    db_tp_val = float(dbp.take_profit or 0.0)
                    for o in ex_orders_by_symbol.get(symbol, []):
                        trig = float(o.get("stopPrice") or 0.0)
                        if trig <= 0:
                            continue
                        kind = self._classify_protective_order(
                            str(o.get("type", "")), trig, entry, is_long,
                            db_stop=db_sl_val, db_tp=db_tp_val,
                        )
                        if kind == "SL":
                            found_sl = self._pick_better_level(found_sl, trig, kind="SL", is_long=is_long)
                            sl_id = o.get("id")
                        elif kind == "TP":
                            found_tp = self._pick_better_level(found_tp, trig, kind="TP", is_long=is_long)
                            tp_id = o.get("id")

                    # RESCUE: восстановление SL если отсутствует на бирже
                    if not sl_id and dbp.stop_loss:
                        now_ts = time.time()
                        if now_ts < self._rescue_cooldown_until.get(symbol, 0.0):
                            logger.warning(f"[RESCUE] Cooldown active for {symbol}")
                        else:
                            live_size, live_side = await self._get_live_position(symbol, preferred_side=("LONG" if is_long else "SHORT"))
                            if live_size <= 0 or (live_side and ((is_long and live_side != "LONG") or ((not is_long) and live_side != "SHORT"))):
                                logger.warning(f"[RESCUE] Skip {symbol}: no live position or side mismatch")
                                continue
                            logger.warning(f"[RESCUE] Restoring protection for {symbol}")
                            safe_stop = dbp.stop_loss
                            if safe_stop:
                                invalid_for_side = (is_long and float(safe_stop) >= entry) or ((not is_long) and float(safe_stop) <= entry)
                                if invalid_for_side:
                                    safe_stop = entry * (0.995 if is_long else 1.005)
                                    logger.warning(f"[RESCUE] Adjusted stop for {symbol}: {dbp.stop_loss} -> {safe_stop}")

                            res_sl_id, res_tp_id = await self._set_protective_orders(
                                symbol, "BUY" if is_long else "SELL", contracts, safe_stop, dbp.take_profit,
                                position_id=dbp.id,
                            )
                            if res_sl_id:
                                sl_id = res_sl_id
                                self._rescue_cooldown_until.pop(symbol, None)
                            else:
                                self._rescue_cooldown_until[symbol] = now_ts + 60
                                logger.warning(f"[RESCUE] Failed for {symbol}. Cooldown 60s.")
                            if res_tp_id:
                                tp_id = res_tp_id

                            if res_sl_id or res_tp_id:
                                await send_telegram_msg(
                                    f"🛟 **ВОССТАНОВЛЕНИЕ ЗАЩИТЫ**\n\n"
                                    f"🔹 Символ: {symbol}\n"
                                    f"🛡 Стоп: {f'{dbp.stop_loss:.4f}' if dbp.stop_loss else 'N/A'}\n"
                                    f"🎯 Тейк: {f'{dbp.take_profit:.4f}' if dbp.take_profit else 'N/A'}"
                                )

                    cache_stop = found_sl or dbp.stop_loss
                    if cache_stop:
                        cache_f = float(cache_stop)
                        wrong_side = (is_long and cache_f >= entry) or ((not is_long) and cache_f <= entry)
                        if wrong_side:
                            be_moved = (is_long and cache_f >= entry) or ((not is_long) and cache_f <= entry)
                            if be_moved and found_sl:
                                pass  # trailing stop crossed BE — this is valid, keep it
                            else:
                                fallback_stop = dbp.stop_loss if dbp.stop_loss else (entry * (0.95 if is_long else 1.05))
                                logger.warning(f"[RECONCILE] Invalid stop for {symbol}: {cache_stop} vs entry={entry}. Fallback={fallback_stop}")
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

                # Закрываем "призраков" (позиции в БД без живой позиции на бирже)
                for sym, dbp in db_positions.items():
                    if sym not in new_active:
                        await session.execute(
                            update(PositionModel)
                            .where(PositionModel.id == dbp.id)
                            .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                        )

                await session.commit()

            # Orphan cleanup: cancel algo/standard orders for symbols with no active position
            orphan_symbols = set(ex_orders_by_symbol.keys()) - set(new_active.keys())
            for orphan_sym in orphan_symbols:
                for o in ex_orders_by_symbol[orphan_sym]:
                    oid = o.get("id")
                    if not oid:
                        continue
                    try:
                        clean = orphan_sym.replace("/", "").split(":")[0]
                        await self._with_time_sync_retry(
                            lambda _oid=oid, _cs=clean: self.exchange.request(
                                'algoOrder', 'fapiPrivate', 'DELETE', {'algoId': str(_oid)}
                            ),
                            ctx=f"orphan_cleanup({orphan_sym}:{oid})"
                        )
                        logger.info(f"🧹 [ORPHAN] Cancelled stale order {oid} for {orphan_sym}")
                    except Exception as e:
                        try:
                            await self._with_time_sync_retry(
                                lambda _oid=oid, _sym=orphan_sym: self.exchange.cancel_order(str(_oid), _sym),
                                ctx=f"orphan_cleanup_std({orphan_sym}:{oid})"
                            )
                            logger.info(f"🧹 [ORPHAN] Cancelled standard order {oid} for {orphan_sym}")
                        except Exception:
                            logger.warning(f"⚠️ [ORPHAN] Could not cancel {oid} for {orphan_sym}: {e}")

            self.active_trades = new_active
            logger.info(f"[RECONCILE] Active positions: {len(self.active_trades)}")
        except Exception as e:
            logger.error(f"Reconcile Error: {e}\n{traceback.format_exc()}")

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
                await self._prepare_private_ops(ctx=f"execute_signal({symbol})")
                # 3. Расчет параметров входа
                entry_price = float(signal_data['entry_price'])
                raw_atr = signal_data.get('atr', 0.0)
                safe_atr = float(raw_atr) if raw_atr and not (isinstance(raw_atr, float) and raw_atr != raw_atr) else 0.0
                if safe_atr <= 0:
                    logger.error(f"[{symbol}] ATR invalid ({raw_atr}), cannot enter — aborting")
                    raise Exception(f"Invalid ATR={raw_atr}, cannot compute stop-loss")
                stop_price = self.risk_manager.calculate_atr_stop(entry_price, safe_atr, direction)
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
                entry_audit: List[Dict[str, Any]] = []

                for attempt in range(max_retries):
                    if remaining_size <= 0: break
                    try:
                        ob = await self._with_time_sync_retry(
                            lambda: self.exchange.fetch_order_book(symbol, limit=5),
                            ctx=f"fetch_order_book({symbol})"
                        )
                        best_price = ob['bids'][0][0] if side == 'buy' else ob['asks'][0][0]
                        best_price = await self._normalize_price(symbol, best_price)
                        
                        logger.info(f"🕸 [LIMIT CHASE] Попытка {attempt+1}: Лимитка {side} {symbol} по {best_price}")
                        temp_order = await self._with_time_sync_retry(
                            lambda: self.exchange.create_order(
                                symbol=symbol, type='limit', side=side, amount=remaining_size, price=best_price,
                                params={'timeInForce': 'GTX', 'postOnly': True}
                            ),
                            ctx=f"create_limit_entry({symbol})"
                        )
                        
                        await asyncio.sleep(3)
                        
                        check = await self._with_time_sync_retry(
                            lambda: self.exchange.fetch_order(temp_order['id'], symbol),
                            ctx=f"fetch_order({symbol}:{temp_order['id']})"
                        )
                        filled_now = float(check.get('filled', 0.0) or 0.0)
                        entry_audit.append({"order": check, "kind": "limit_entry"})
                        
                        if check['status'] == 'closed':
                            entry_exec = float(check.get('average') or check.get('price') or entry_price)
                            filled_total += filled_now
                            remaining_size = 0.0
                            break
                        else:
                            await self._with_time_sync_retry(
                                lambda: self.exchange.cancel_order(temp_order['id'], symbol),
                                ctx=f"cancel_order({symbol}:{temp_order['id']})"
                            )
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
                        fallback_order = await self._with_time_sync_retry(
                            lambda: self.exchange.create_order(symbol, 'market', side, remaining_size),
                            ctx=f"create_market_fallback({symbol})"
                        )
                        entry_audit.append({"order": fallback_order, "kind": "market_entry"})
                        if filled_total == 0:
                            entry_exec = float(fallback_order.get("average") or fallback_order.get("price") or entry_price)
                        filled_total += float(fallback_order.get("filled", remaining_size))
                    except Exception as e:
                        logger.error(f"Market fallback error: {e}")
                        if filled_total == 0: raise e
                
                lot_size = filled_total

                # 6. Пересчёт стопа от ФАКТИЧЕСКОЙ цены входа (а не теоретической из сигнала)
                if abs(entry_exec - entry_price) > entry_price * 0.0001:
                    stop_price = self.risk_manager.calculate_atr_stop(entry_exec, safe_atr, direction)
                    logger.info(f"[{symbol}] SL recalculated for actual fill: entry {entry_price:.6f} → {entry_exec:.6f}, stop → {stop_price:.6f}")

                # 7. БД Позиция
                async with async_session() as session:
                    pos = PositionModel(user_id=1, signal_id=signal_id, symbol=symbol, side=SignalType.LONG if direction.upper() == "LONG" else SignalType.SHORT,
                                        entry_price=entry_exec, size=lot_size, stop_loss=stop_price, take_profit=signal_data.get("take_profit"),
                                        status=PositionStatus.OPEN, opened_at=datetime.datetime.utcnow())
                    session.add(pos); await session.commit(); await session.refresh(pos)

                pos_side = self._position_side_from_entry_side(direction)
                for aud in entry_audit:
                    od = aud.get("order") or {}
                    oid = od.get("id")
                    if oid is None:
                        continue
                    st_raw = str(od.get("status", "")).lower()
                    if st_raw in ("closed", "filled"):
                        ost = OrderStatus.FILLED
                    elif st_raw in ("canceled", "cancelled"):
                        ost = OrderStatus.CANCELED
                    else:
                        ost = OrderStatus.OPEN
                    cid = od.get("clientOrderId")
                    await self._db_persist_order(
                        position_id=pos.id,
                        symbol=symbol,
                        exchange_order_id=str(oid),
                        client_order_id=str(cid) if cid else None,
                        order_type=str(aud.get("kind") or "entry"),
                        position_side=pos_side,
                        price=float(od.get("average") or od.get("price") or 0.0),
                        size=float(od.get("filled") or od.get("amount") or 0.0),
                        status=ost,
                    )

                # 8. Защитные ордера
                targets = signal_data.get("targets") or signal_data.get("take_profit")
                sl_id, tp_id = await self._set_protective_orders(
                    symbol, direction, lot_size, stop_price, targets, position_id=pos.id
                )
                if not sl_id:
                    # Best practice: не оставлять открытую позицию без стопа.
                    emergency_side = 'sell' if direction.upper() == "LONG" else 'buy'
                    em_o = await self._with_time_sync_retry(
                        lambda: self.exchange.create_order(symbol, 'market', emergency_side, lot_size),
                        ctx=f"emergency_close_no_sl({symbol})"
                    )
                    await self._db_persist_order(
                        position_id=pos.id,
                        symbol=symbol,
                        exchange_order_id=str(em_o.get("id")) if em_o and em_o.get("id") is not None else None,
                        client_order_id=None,
                        order_type="MARKET_EMERGENCY_CLOSE",
                        position_side=pos_side,
                        price=float(em_o.get("average") or em_o.get("price") or 0.0),
                        size=float(em_o.get("filled") or lot_size),
                        status=OrderStatus.FILLED,
                    )
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

                _strat = signal_data.get("strategy", "?")
                _tp_val = signal_data.get("take_profit")
                _tp_str = f"{float(_tp_val):.4f}" if _tp_val else "—"
                _dir_emoji = "🟢 LONG" if direction.upper() == "LONG" else "🔴 SHORT"
                await send_telegram_msg(
                    f"✅ **НОВАЯ ПОЗИЦИЯ**\n\n"
                    f"🔹 Символ: `{symbol}`\n"
                    f"📌 Направление: {_dir_emoji}\n"
                    f"📊 Стратегия: {_strat}\n"
                    f"💰 Цена входа: `{entry_exec:.4f}`\n"
                    f"🛡 Стоп-лосс: `{stop_price:.4f}`\n"
                    f"🎯 Тейк-профит: `{_tp_str}`\n"
                    f"📦 Объём: `{lot_size}`"
                )

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
            live_size, _ = await self._get_live_position(symbol, preferred_side=trade.get('signal_type'))
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
                            sl_id, tp_id = await self._set_protective_orders(
                                symbol, side, trade['current_size'], be_stop, tp_ref,
                                position_id=trade.get("position_db_id"),
                            )
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
                                    f"🟡 **БЕЗУБЫТОК: {symbol}**\n\n"
                                    f"🛡 Стоп перенесён на: `{be_stop:.6f}`\n"
                                    f"📈 Триггер: 1R + {'ADX ≥ 20' if adx_ok else 'Пробой'}"
                                )
            
            # Трейлинг-стоп (skip if ATR is invalid to prevent stop jump to current price)
            if atr <= 0:
                return
            new_stop = self.risk_manager.calculate_trailing_stop(trade['stop'], current_price, atr, trade['signal_type'])
            if abs(new_stop - trade['stop']) > (current_price * 0.001):
                logger.info(f"🔄 [TRAILING] Moving {symbol} stop: {trade['stop']} -> {new_stop}")
                # Перевыставляем ордер
                await self._cancel_all_orders(symbol)
                # TP должны оставаться активными после трейлинга.
                tp_ref = trade.get("take_profit_live")
                sl_id, tp_id = await self._set_protective_orders(
                    symbol, trade['signal_type'], trade['current_size'], new_stop, tp_ref,
                    position_id=trade.get("position_db_id"),
                )
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
                            pyr_o = await self.exchange.create_order(symbol, 'market', side, add_size)
                            await self._db_persist_order(
                                position_id=trade.get("position_db_id"),
                                symbol=symbol,
                                exchange_order_id=str(pyr_o.get("id")) if pyr_o and pyr_o.get("id") is not None else None,
                                client_order_id=str(pyr_o.get("clientOrderId")) if pyr_o and pyr_o.get("clientOrderId") else None,
                                order_type="MARKET_PYRAMID_ADD",
                                position_side=str(trade.get("signal_type") or "LONG"),
                                price=float(pyr_o.get("average") or pyr_o.get("price") or 0.0),
                                size=float(pyr_o.get("filled") or add_size),
                                status=OrderStatus.FILLED,
                            )
                            
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
                            sl_id, tp_id = await self._set_protective_orders(
                                symbol, trade['signal_type'], new_size, trade['stop'], tp_ref,
                                position_id=trade.get("position_db_id"),
                            )
                            trade['stop_order_id'] = sl_id
                            if tp_id:
                                trade['tp_order_id'] = tp_id
                            
                            await send_telegram_msg(
                                f"💎 **ПИРАМИДИНГ (ДОБОР)**\n\n"
                                f"🔹 Символ: `{symbol}`\n"
                                f"📊 Этап: {next_stage}\n"
                                f"📦 Новый объём: `{new_size:.4f}`\n"
                                f"💰 Средняя цена: `{new_entry:.4f}`"
                            )
                        except Exception as e:
                            logger.error(f"Pyramid error for {symbol}: {e}")

    async def _close_position(self, symbol: str, reason: str = "AUTO"):
        if symbol not in self.active_trades: return
        trade = self.active_trades[symbol]
        pnl_usd = 0.0
        pnl_pct = 0.0
        try:
            await self._cancel_all_orders(symbol)
            if reason != "EXTERNAL":
                side = 'sell' if trade['signal_type'] == "LONG" else 'buy'
                live_size, live_side = await self._get_live_position(symbol, preferred_side=trade.get('signal_type'))
                close_amount = live_size if live_size > 1e-8 else trade['current_size']
                # Если направление на бирже не совпадает с локальным кешем, не отправляем рыночный close, чтобы не перевернуть позицию.
                if live_side and live_side != trade['signal_type']:
                    logger.warning(f"⚠️ [CLOSE] Side mismatch for {symbol}: local={trade['signal_type']} live={live_side}. Skip market close.")
                    return
                elif close_amount > 1e-8:
                    co = await self.exchange.create_order(symbol, 'market', side, close_amount)
                    ps = str(trade.get("signal_type") or "LONG").upper()
                    await self._db_persist_order(
                        position_id=trade.get("position_db_id"),
                        symbol=symbol,
                        exchange_order_id=str(co.get("id")) if co and co.get("id") is not None else None,
                        client_order_id=str(co.get("clientOrderId")) if co and co.get("clientOrderId") else None,
                        order_type="MARKET_CLOSE",
                        position_side=ps,
                        price=float(co.get("average") or co.get("price") or 0.0),
                        size=float(co.get("filled") or close_amount),
                        status=OrderStatus.FILLED,
                    )
            
            try:
                lev = int(getattr(settings, "leverage", 1) or 1)
            except Exception:
                lev = 1

            async with async_session() as session:
                await session.execute(
                    update(PositionModel)
                    .where(PositionModel.id == trade["position_db_id"])
                    .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                )

                try:
                    if reason == "EXTERNAL":
                        pnl_usd, pnl_pct = await self._realized_pnl_from_exchange_trades(symbol, trade)
                        if abs(pnl_usd) < 1e-12:
                            ticker = await self.exchange.fetch_ticker(symbol)
                            exit_price = float(ticker.get("last") or 0)
                            entry = float(trade["entry"])
                            size = float(trade["current_size"])
                            is_long = trade["signal_type"] == "LONG"
                            if entry > 0 and size > 0 and exit_price > 0:
                                pnl_usd = (exit_price - entry) * size if is_long else (entry - exit_price) * size
                                pnl_pct = ((exit_price / entry) - 1) * 100 if is_long else ((entry / exit_price) - 1) * 100
                    else:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        exit_price = float(ticker["last"])
                        entry = float(trade["entry"])
                        size = float(trade["current_size"])
                        is_long = trade["signal_type"] == "LONG"
                        pnl_usd = (exit_price - entry) * size if is_long else (entry - exit_price) * size
                        pnl_pct = ((exit_price / entry) - 1) * 100 if is_long else ((entry / exit_price) - 1) * 100
                except Exception as e:
                    logger.warning(f"PnL record error: {e}")

                session.add(
                    PnLModel(
                        user_id=1,
                        symbol=symbol,
                        pnl_usd=pnl_usd,
                        pnl_pct=pnl_pct,
                        leverage=lev,
                        reason=reason,
                    )
                )
                try:
                    bal, _, _ = await self.get_account_metrics()
                    self.risk_manager.record_closed_pnl(pnl_usd, bal)
                except Exception:
                    pass

                await session.commit()
            
            _pnl_msg = ""
            _reason_map = {
                "EXTERNAL": "🔄 Биржа (TP/SL)",
                "MANUAL": "🖐 Ручное",
                "TIME": "⏱ Тайм-аут",
                "AUTO": "⚙️ Авто",
            }
            _reason_ru = _reason_map.get(reason, f"⚙️ {reason}")
            try:
                _pnl_emoji = "🟢" if pnl_usd >= 0 else "🔴"
                _pnl_msg = f"\n{_pnl_emoji} Результат: `{pnl_usd:+.2f} USDT ({pnl_pct:+.1f}%)`"
            except Exception:
                pass
            self.active_trades.pop(symbol, None)
            await send_telegram_msg(
                f"💰 **ПОЗИЦИЯ ЗАКРЫТА**\n\n"
                f"🔹 Символ: `{symbol}`\n"
                f"🏁 Причина: {_reason_ru}{_pnl_msg}"
            )
        except Exception as e: logger.error(f"Error closing {symbol}: {e}")

    async def manual_close(self, symbol: str) -> bool:
        """Публичный метод для ручного закрытия из Telegram. (Баг 4.1)"""
        if symbol in self.active_trades:
            await self._close_position(symbol, reason="MANUAL")
            return True
        return False

    async def manual_reduce(self, symbol: str, fraction: float) -> Dict[str, Any]:
        """Частичное ручное закрытие позиции (reduce-only market)."""
        if symbol not in self.active_trades:
            return {"status": "error", "message": "Trade not found"}
        try:
            frac = float(fraction)
        except Exception:
            return {"status": "error", "message": "Invalid fraction"}
        if frac <= 0 or frac >= 1:
            return {"status": "error", "message": "Fraction must be in (0,1)"}

        trade = self.active_trades[symbol]
        side = 'sell' if trade.get('signal_type') == "LONG" else 'buy'

        try:
            live_size, live_side = await self._get_live_position(symbol, preferred_side=trade.get('signal_type'))
            if live_side and live_side != trade.get('signal_type'):
                logger.warning(f"⚠️ [REDUCE] Side mismatch for {symbol}: local={trade.get('signal_type')} live={live_side}")
                return {"status": "error", "message": "Side mismatch with live position"}

            base_size = live_size if live_size > 1e-8 else float(trade.get('current_size') or 0.0)
            if base_size <= 1e-8:
                return {"status": "error", "message": "Position size is too small"}

            reduce_amount = max(base_size * frac, 0.0)
            market = self.exchange.market(symbol) if hasattr(self.exchange, "market") else None
            min_amount = float((market or {}).get("limits", {}).get("amount", {}).get("min") or 0.0)
            if min_amount > 0 and reduce_amount < min_amount:
                return {"status": "error", "message": f"Reduce amount below minimum ({min_amount})"}

            params = {"reduceOnly": True}
            ro = await self._with_time_sync_retry(
                lambda: self.exchange.create_order(symbol, 'market', side, reduce_amount, None, params),
                ctx=f"manual_reduce({symbol})"
            )
            ps = str(trade.get("signal_type") or "LONG")
            await self._db_persist_order(
                position_id=trade.get("position_db_id"),
                symbol=symbol,
                exchange_order_id=str(ro.get("id")) if ro and ro.get("id") is not None else None,
                client_order_id=str(ro.get("clientOrderId")) if ro and ro.get("clientOrderId") else None,
                order_type="MARKET_MANUAL_REDUCE",
                position_side=ps,
                price=float(ro.get("average") or ro.get("price") or 0.0),
                size=float(ro.get("filled") or reduce_amount),
                status=OrderStatus.FILLED,
            )
            await asyncio.sleep(0.25)
            await self.reconcile_full()
            return {
                "status": "success",
                "symbol": symbol,
                "reduced_fraction": frac,
                "requested_amount": reduce_amount
            }
        except Exception as e:
            logger.error(f"❌ [REDUCE] {symbol} failed: {e}")
            return {"status": "error", "message": str(e)}

    async def get_account_metrics(self):
        """K4: Метрики с кэшированием + anti-storm backoff при деградации API."""
        now = time.time()
        # Fast path: свежий кэш.
        if self._metrics_cache and (now - self._metrics_cache_ts) < self._METRICS_CACHE_TTL:
            return self._metrics_cache

        # API в деградации: не штурмим биржу из параллельных задач.
        if now < self._metrics_backoff_until and self._metrics_cache:
            return self._metrics_cache

        # Single-flight: только один concurrent запрос метрик наружу.
        async with self._metrics_lock:
            now = time.time()
            if self._metrics_cache and (now - self._metrics_cache_ts) < self._METRICS_CACHE_TTL:
                return self._metrics_cache
            if now < self._metrics_backoff_until and self._metrics_cache:
                return self._metrics_cache

            try:
                # Component 1: Balance (имеет свой backoff/cache).
                if now < self._balance_backoff_until and self._balance_cache:
                    free, total = self._balance_cache
                else:
                    try:
                        balance = await self._with_time_sync_retry(
                            lambda: self.exchange.fetch_balance(),
                            ctx="metrics.fetch_balance"
                        )
                        total = float(balance.get('USDT', {}).get('total', 0.0) or 0.0)
                        free = float(balance.get('USDT', {}).get('free', total) or total)
                        self._balance_cache = (free, total)
                        self._balance_cache_ts = now
                        self._balance_fail_streak = 0
                        self._balance_backoff_until = 0.0
                    except Exception as be:
                        self._balance_fail_streak = min(self._balance_fail_streak + 1, 8)
                        b_backoff = min(90.0, float(2 ** self._balance_fail_streak))
                        self._balance_backoff_until = now + b_backoff
                        logger.warning(f"⚠️ [METRICS] Balance fetch failed: {be} (backoff {b_backoff:.0f}s)")
                        if self._balance_cache:
                            free, total = self._balance_cache
                        else:
                            raise

                # Component 2: Positions (имеет свой backoff/cache).
                if now < self._positions_backoff_until and self._positions_cache:
                    dd, open_cnt = self._positions_cache
                else:
                    try:
                        pos = await self._with_time_sync_retry(
                            lambda: self.exchange.fetch_positions(),
                            ctx="metrics.fetch_positions"
                        )
                        active_p = [p for p in pos if abs(float(p.get('contracts', 0) or p.get('pa', 0) or 0)) > 1e-8]
                        live_positions_map: Dict[str, Dict[str, Any]] = {}
                        for p in active_p:
                            sym = self._norm_sym(p.get("symbol"))
                            contracts_raw = float(p.get('contracts', 0) or p.get('pa', 0) or 0)
                            if not sym or abs(contracts_raw) <= 1e-8:
                                continue
                            side = str(p.get("side", "")).upper()
                            if side not in ("LONG", "SHORT"):
                                side = "LONG" if contracts_raw > 0 else "SHORT"
                            size = abs(float(contracts_raw))
                            # Если пришло несколько legs по символу, берем крупнейшую.
                            prev = live_positions_map.get(sym)
                            if (not prev) or (size > float(prev.get("size", 0.0) or 0.0)):
                                live_positions_map[sym] = {"size": size, "side": side}

                        await self._soft_cleanup_active_trades(live_positions_map)
                        pnl = sum([float(p.get('unrealizedPnl', 0)) for p in active_p])
                        dd = (abs(min(0, pnl)) / total) if total > 0 else 0.0
                        open_cnt = len(active_p)
                        self._positions_cache = (dd, open_cnt)
                        self._positions_cache_ts = now
                        self._positions_fail_streak = 0
                        self._positions_backoff_until = 0.0
                    except Exception as pe:
                        self._positions_fail_streak = min(self._positions_fail_streak + 1, 8)
                        p_backoff = min(60.0, float(2 ** self._positions_fail_streak))
                        self._positions_backoff_until = now + p_backoff
                        logger.warning(f"⚠️ [METRICS] Positions fetch failed: {pe} (backoff {p_backoff:.0f}s)")
                        if self._positions_cache:
                            dd, open_cnt = self._positions_cache
                        else:
                            raise

                result = (free, dd, int(open_cnt))
                self._metrics_cache = result
                self._metrics_cache_ts = now
                self._metrics_fail_streak = 0
                self._metrics_backoff_until = 0.0
                return result
            except Exception as e:
                self._metrics_fail_streak = min(self._metrics_fail_streak + 1, 8)
                # Экспоненциальный backoff: 2,4,8,...,60 сек.
                backoff = min(60.0, float(2 ** self._metrics_fail_streak))
                self._metrics_backoff_until = now + backoff
                logger.warning(f"⚠️ [METRICS] Ошибка получения метрик: {e} (backoff {backoff:.0f}s)")
                if self._metrics_cache:
                    logger.info("[METRICS] Используем кэшированные метрики")
                    return self._metrics_cache
                return 0.0, 0.0, 0

    async def _set_leverage_best_effort(self, symbol: str, leverage: int):
        try: await self.exchange.set_leverage(int(leverage), symbol)
        except: pass

    async def _soft_cleanup_active_trades(self, live_positions_map: Dict[str, Dict[str, Any]]) -> None:
        """
        Мягкая очистка локального кеша позиций:
        - удаляет из memory позиции, которых уже нет на бирже;
        - синхронизирует размер/сторону для существующих.
        Делает best-effort обновление БД только для явно "призрачных" записей.
        """
        now = time.time()
        if (now - self._soft_cleanup_last_ts) < 5.0:
            return
        self._soft_cleanup_last_ts = now

        if not self.active_trades:
            return

        stale_symbols: List[str] = []
        for symbol in list(self.active_trades.keys()):
            local = self.active_trades.get(symbol) or {}
            live = live_positions_map.get(symbol)
            if not live:
                stale_symbols.append(symbol)
                self.active_trades.pop(symbol, None)
                continue

            live_size = float(live.get("size") or 0.0)
            if live_size > 0 and abs(float(local.get("current_size", 0.0) or 0.0) - live_size) > 1e-8:
                local["current_size"] = live_size

            live_side = str(live.get("side") or "").upper()
            if live_side in ("LONG", "SHORT") and str(local.get("signal_type", "")).upper() != live_side:
                local["signal_type"] = live_side

        if stale_symbols:
            logger.warning(f"🧹 [SOFT_CLEANUP] Removed stale active_trades: {stale_symbols}")
            try:
                async with async_session() as session:
                    for sym in stale_symbols:
                        await session.execute(
                            update(PositionModel)
                            .where(
                                PositionModel.symbol == sym,
                                PositionModel.status == PositionStatus.OPEN
                            )
                            .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                        )
                    await session.commit()
            except Exception as e:
                logger.warning(f"⚠️ [SOFT_CLEANUP] DB close sync failed: {e}")
