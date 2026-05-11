import asyncio
import time
import datetime
import traceback
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
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
from utils.binance_api import BinanceCallPolicy, BinanceRateLimiter, call_with_binance_retry, BinanceUnknownStatusError
from utils.symbol_normalizer import SymbolNormalizer
from core.audit.audit_trail import AuditTrail
from core.risk.risk_manager import RiskManager, TimeExitSystem, PyramidingSystem
from core.position.position_manager import PositionManager, PositionState
from core.pnl.pnl_calculator import PnLCalculator
from core.execution.trade_guard import TradeGuard
try:
    from utils.metrics import (
        trades_opened, trades_closed, pnl_per_trade, entry_failures,
        trade_mgmt_events, trade_mgmt_r_at_exit, trade_mgmt_max_favorable_r,
    )
except ImportError:
    trades_opened = trades_closed = pnl_per_trade = entry_failures = None
    trade_mgmt_events = trade_mgmt_r_at_exit = trade_mgmt_max_favorable_r = None

logger = get_execution_logger()


class EntryExecutionError(Exception):
    """Entry path failure with a stable machine-readable reason for metrics and logs."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


class ExecutionEngine:
    def __init__(self, exchange_client, risk_manager: RiskManager, user_id: Optional[int] = None):
        self.exchange = exchange_client
        self.user_id = user_id if user_id is not None else int(settings.admin_user_ids.split(',')[0]) if settings.admin_user_ids else 1
        self._market_rules_cache = {} # Кэш фильтров
        if hasattr(self.exchange, 'options'):
            self.exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
        self.active_trades: Dict[str, Dict[str, Any]] = {}
        self._trades_lock = asyncio.Lock() # ГЛОБАЛЬНЫЙ ЛОК
        
        self.risk_manager = risk_manager
        self.pyramiding = PyramidingSystem()
        self.time_exit = TimeExitSystem()

        self._symbol_locks: Dict[str, asyncio.Lock] = {}
        
        self._user_order_stream_task: Optional[asyncio.Task] = None
        self._user_position_stream_task: Optional[asyncio.Task] = None
        self._running = True
        
        # K4: Кэш метрик аккаунта (TTL из настроек)
        self._metrics_cache: Optional[Tuple[float, float, int]] = None
        self._metrics_cache_ts: float = 0
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
        self._trade_close_callbacks: list = []
        # WS-петли: последнее успешное событие (для watchdog)
        self._last_ws_orders_event_ts: float = time.time()
        self._last_ws_positions_event_ts: float = time.time()
        # Throttle для _tm_partial_reduce skip-лога: symbol -> last_log_ts
        self._tm_partial_reduce_skip_log_ts: Dict[str, float] = {}
        # Фоновые задачи (periodic reconcile, ws watchdog)
        self._periodic_reconcile_task: Optional[asyncio.Task] = None
        self._binance_limiter = BinanceRateLimiter(max_concurrent=3)
        self.audit_trail = AuditTrail(user_id=self.user_id)

        
    async def _get_market_rules(self, symbol: str) -> dict:
        if symbol in self._market_rules_cache:
            return self._market_rules_cache[symbol]
        
        if not self.exchange.markets: await self.exchange.load_markets()
        market = self.exchange.market(symbol)
        
        rules = {
            'step_size': '0.001', 
            'tick_size': '0.01', 
            'min_notional': '5.0',
            'max_algo': 100
        }
        for f in market['info'].get('filters', []):
            ft = f.get('filterType')
            if ft == 'LOT_SIZE':
                rules['step_size'] = f.get('stepSize', '0.001')
            elif ft == 'PRICE_FILTER':
                rules['tick_size'] = f.get('tickSize', '0.01')
            elif ft == 'MIN_NOTIONAL':
                rules['min_notional'] = f.get('notional', '5.0')
            elif ft == 'MAX_NUM_ALGO_ORDERS':
                rules['max_algo'] = int(f.get('limit', 100))
                
        self._market_rules_cache[symbol] = rules
        return rules

    async def _normalize_price(self, symbol: str, price: float) -> float:
        rules = await self._get_market_rules(symbol)
        tick = Decimal(rules['tick_size'])
        return float(Decimal(str(price)).quantize(tick, rounding=ROUND_HALF_UP))

    def register_trade_close_callback(self, cb):
        """Register a callback(strategy: str, pnl_usd: float) called on every trade close."""
        self._trade_close_callbacks.append(cb)

    def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        if symbol not in self._symbol_locks:
            self._symbol_locks[symbol] = asyncio.Lock()
        return self._symbol_locks[symbol]

    def _record_entry_failure(self, reason: str) -> None:
        try:
            if entry_failures:
                entry_failures.labels(reason=reason).inc()
        except Exception:
            pass

    def _norm_sym(self, s: str) -> str:
        """Единый стандарт символа через SymbolNormalizer."""
        return SymbolNormalizer.normalize(s)

    def _entry_side_to_order_side(self, side: str) -> str:
        """Нормализует направление входа к BUY/SELL."""
        s = (side or "").upper()
        if s in ("BUY", "LONG"):
            return "BUY"
        if s in ("SELL", "SHORT"):
            return "SELL"
        raise ValueError(f"Unsupported side: {side}")

    async def _with_time_sync_retry(self, op, ctx: str = ""):
        """Retry wrapper for Binance private calls: time-sync, transient network and rate-limit safe."""
        try:
            return await call_with_binance_retry(
                op=op,
                exchange=self.exchange,
                limiter=self._binance_limiter,
                policy=BinanceCallPolicy(max_attempts=4, base_delay=0.4, max_delay=3.0, timeout_seconds=30.0),
            )
        except BinanceUnknownStatusError as e:
            logger.error(f"⚠️ [BINANCE_API] UNKNOWN STATUS (503) during {ctx}: {e}")
            logger.info("🔍 Инициирую экстренный reconcile для проверки исполнения...")
            try:
                # Пытаемся синхронизироваться, чтобы понять, открылась ли позиция
                await self.reconcile_full()
            except Exception as sync_err:
                logger.error(f"❌ Ошибка экстренного reconcile: {sync_err}")
            raise
        except Exception as e:
            logger.warning(f"⚠️ [BINANCE_API] {ctx}: {e}")
            raise

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
        client_id: Optional[str] = None,
        position_id: Optional[int] = None,
    ) -> Optional[str]:
        """Classify a protective order as SL or TP.
        """
        # Existing logic unchanged – kept for context.
        # Priority 1: Точное соответствие по префиксу clientOrderId (p{pos_id}_{type}_)
        if client_id:
            cid = str(client_id)
            if "_" in cid:
                parts = cid.split("_")
                # Формат: p{pos_id}_sl_{ts} или p{pos_id}_tp_{ts}
                if len(parts) >= 2 and parts[0].startswith("p"):
                    try:
                        cid_pid = int(parts[0][1:])
                        if position_id and cid_pid == position_id:
                            if parts[1] == "sl": return "SL"
                            if parts[1] == "tp": return "TP"
                    except ValueError:
                        pass

        # Priority 2: Старый формат (asl_{pos_id}, atp_{pos_id}) для обратной совместимости
        if client_id and position_id:
            cid = str(client_id)
            pid_str = f"_{position_id}"
            if pid_str in cid:
                if cid.startswith("asl_"): return "SL"
                if cid.startswith("atp_"): return "TP"
        
        t = (order_type or "").upper()
        if "TAKE_PROFIT" in t:
            return "TP"
        if "STOP" in t and "TAKE_PROFIT" not in t:
            return "SL"
        
        if trigger_price <= 0 or entry_price <= 0:
            return None
        
        # Fallback: match by proximity to DB values first.
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
        """Choose the most appropriate level (SL/TP) among candidates.
        """
        if current is None:
            return candidate
        if kind == "SL":
            # LONG: SL closer to price from above; SHORT – opposite.
            return max(current, candidate) if is_long else min(current, candidate)
        # TP: LONG – nearest above; SHORT – nearest below.
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

        if self._periodic_reconcile_task is None:
            self._periodic_reconcile_task = asyncio.create_task(self._periodic_reconcile_loop())
            logger.info(f"🔄 [EXEC] Периодический reconcile ({settings.reconcile_interval}с): OK")

        self._watchdog_task = asyncio.create_task(self._ws_watchdog_loop())
        logger.info("🛡️ [EXEC] WebSocket Watchdog: OK")

    async def _ws_watchdog_loop(self):
        """Проверка 'живучести' WS-потоков. Если нет событий > 15 мин — перезапуск."""
        WATCHDOG_INTERVAL = 300 # 5 мин
        STALE_THRESHOLD = 900   # 15 мин
        
        while self._running:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            now = time.time()
            
            orders_stale = (now - self._last_ws_orders_event_ts) > STALE_THRESHOLD
            pos_stale = (now - self._last_ws_positions_event_ts) > STALE_THRESHOLD
            
            if orders_stale or pos_stale:
                reason = "ORDERS_STALE" if orders_stale else "POS_STALE"
                logger.warning(f"🛡️ [WATCHDOG] WebSocket поток застыл ({reason}). Перезапуск...")
                
                # Перезапускаем таски
                if self._user_order_stream_task: self._user_order_stream_task.cancel()
                if self._user_position_stream_task: self._user_position_stream_task.cancel()
                
                self._user_order_stream_task = asyncio.create_task(self._watch_user_orders_loop())
                self._user_position_stream_task = asyncio.create_task(self._watch_user_positions_loop())
                
                # Сбрасываем таймеры, чтобы не спамить перезапусками
                self._last_ws_orders_event_ts = now
                self._last_ws_positions_event_ts = now
                
                # Принудительный reconcile для синхронизации после простоя
                try:
                    await self.reconcile_full()
                except Exception: pass

    async def _periodic_reconcile_loop(self):
        """
        Гарантированный периодический reconcile каждые N секунд (из настроек).
        Не зависит от WS-событий — защита от зависших WS-петель.
        """
        await asyncio.sleep(15.0)   # небольшой сдвиг от старта, чтобы не гонять сразу
        while self._running:
            try:
                # Для user-stream'ов отсутствие событий может быть нормой (нет fill/update),
                # поэтому перезапускаем только реально "умершие" задачи.
                for task_attr, restart_coro, name in (
                    ("_user_order_stream_task", self._watch_user_orders_loop, "WS_ORDERS"),
                    ("_user_position_stream_task", self._watch_user_positions_loop, "WS_POS"),
                ):
                    task = getattr(self, task_attr, None)
                    if task is None or task.done():
                        logger.warning(f"⚠️ [WS_WATCHDOG] {name} task is not running — restart")
                        setattr(self, task_attr, asyncio.create_task(restart_coro()))

                await self.reconcile_full()
            except Exception as e:
                logger.warning(f"⚠️ [PERIODIC_RECONCILE] ошибка: {e}")
            await asyncio.sleep(settings.reconcile_interval)

    async def stop(self):
        self._running = False
        for t in [self._user_order_stream_task, self._user_position_stream_task]:
            if t: t.cancel()
        logger.info("🛑 [EXEC] Движок остановлен")

    # --- WEBSOCKET LOOPS ---

    async def _watch_user_orders_loop(self):
        backoff = 5
        while self._running:
            try:
                orders = await self.exchange.watch_orders()
                backoff = 5  # Сброс при успешном получении данных
                self._last_ws_orders_event_ts = time.time()
                self._last_ws_orders_event_ts = time.time()
                for order in orders:
                    await self._handle_order_update(order)
            except Exception as e:
                if self._running:
                    logger.error(f"❌ [WS_ORDERS] Error: {e}. Retry in {backoff}s")
                    now = time.time()
                    if (now - self._last_ws_reconcile_ts) > (settings.reconcile_interval / 2):
                        self._last_ws_reconcile_ts = now
                        try:
                            await self.reconcile_full()
                        except Exception as rec_err:
                            logger.error(f"❌ [WS_ORDERS] reconcile_full failed: {rec_err}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, settings.ws_backoff_max)

    async def _watch_user_positions_loop(self):
        backoff = 5
        while self._running:
            try:
                positions = await self.exchange.watch_positions()
                backoff = 5  # Сброс при успешном получении данных
                self._last_ws_positions_event_ts = time.time()
                for pos in positions:
                    symbol = self._norm_sym(pos.get('symbol'))
                    contracts = float(pos.get('contracts', 0) or pos.get('pa', 0) or 0)
                    if abs(contracts) <= 1e-8 and symbol in self.active_trades:
                        logger.info(f"💥 [WS_POS] {symbol} закрыта извне.")
                        await self._close_position(symbol, reason="EXTERNAL")
            except Exception as e:
                if self._running:
                    logger.error(f"❌ [WS_POS] Error: {e}. Retry in {backoff}s")
                    now = time.time()
                    if (now - self._last_ws_reconcile_ts) > (settings.reconcile_interval / 2):
                        self._last_ws_reconcile_ts = now
                        try:
                            await self.reconcile_full()
                        except Exception as rec_err:
                            logger.error(f"❌ [WS_POS] reconcile_full failed: {rec_err}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, settings.ws_backoff_max)

    async def _handle_order_update(self, order: dict):
        try:
            status_raw = order.get('status')
            status = str(status_raw or '').lower()
            symbol = self._norm_sym(order.get('symbol'))
            ex_id = str(order.get('id'))
            info = order.get('info') or {}
            algo_alt = str(info.get("algoId") or info.get("clientAlgoId") or "").strip()
            protective_fill_price = 0.0
            protective_fill_size = 0.0

            if status in ('canceled', 'cancelled', 'expired', 'rejected'):
                cid_from_event = order.get('clientOrderId') or (info.get('clientOrderId') if isinstance(info, dict) else None)
                async with async_session() as session:
                    async with session.begin():
                        # Собираем все возможные ID
                        id_pool = {str(x).strip() for x in (ex_id, algo_alt, cid_from_event) if x and str(x).strip()}
                        if not id_pool: return
                        
                        stmt = select(OrderModel).where(
                            (OrderModel.exchange_order_id.in_(id_pool)) |
                            (OrderModel.client_order_id.in_(id_pool))
                        )
                        res = await session.execute(stmt)
                        for db_order in res.scalars():
                            db_order.status = OrderStatus.CANCELED
                return

            if status in ('closed', 'filled'):
                self._last_ws_orders_event_ts = time.time()
                is_protective_fill = False
                cid_from_event = order.get('clientOrderId') or (info.get('clientOrderId') if isinstance(info, dict) else None)
                
                async with async_session() as session:
                    async with session.begin():
                        # Поиск по exchange_order_id ИЛИ client_order_id
                        stmt = select(OrderModel).where(
                            (OrderModel.exchange_order_id == ex_id) | 
                            (OrderModel.exchange_order_id == algo_alt) |
                            (OrderModel.client_order_id == cid_from_event)
                        )
                        res = await session.execute(stmt)
                        db_order = res.scalar_one_or_none()

                        if db_order:
                            db_order.status = OrderStatus.FILLED
                            ot = (db_order.order_type or "").upper()
                            if any(x in ot for x in ["STOP", "TAKE", "TRAILING"]):
                                is_protective_fill = True
                                fill_price = float(order.get('average') or order.get('price') or 0.0)
                                fill_size = float(order.get('filled') or order.get('amount') or 0.0)
                                protective_fill_price = fill_price
                                protective_fill_size = fill_size
                                fill_type = "SL" if "STOP" in ot else ("TP" if "TAKE" in ot else "TRAILING")
                                trade = self.active_trades.get(symbol, {})
                                entry = trade.get("entry", 0)
                                pnl_est = 0.0
                                if entry > 0 and fill_price > 0 and fill_size > 0:
                                    is_long = trade.get("signal_type") == "LONG"
                                    pnl_est = (fill_price - entry) * fill_size if is_long else (entry - fill_price) * fill_size
                                logger.info(
                                    f"💾 [WS_FILL] {fill_type} hit for {symbol}: "
                                    f"order_type={ot} fill_price={fill_price:.6f} size={fill_size} | "
                                    f"entry={entry:.6f} est_PnL={pnl_est:+.4f} USDT"
                                )
                                # Check for BE trigger (Target 2 hit)
                                client_id = str(db_order.client_order_id or "")
                                trade = self.active_trades.get(symbol, {})
                                if "_2" in client_id and "atp_" in client_id:
                                    logger.info(f"🚀 [Scaling Out] Target 2 hit for {symbol}. Moving SL to Break Even.")
                                    asyncio.ensure_future(self._move_to_breakeven(symbol, trade))
                                    trade["trailing_active"] = True
                                    trade["trailing_source"] = "EMA20"

                if is_protective_fill and symbol in self.active_trades:
                    trade = self.active_trades.get(symbol)
                    live_size, _ = await self._get_live_position(symbol, preferred_side=trade.get('signal_type'))
                    if live_size <= 1e-8:
                        logger.info(f"🔄 [WS_ORDERS] Closing {symbol} from active_trades (SL/TP triggered)")
                        await self._close_position(symbol, reason="EXTERNAL")
                    else:
                        position_update = PositionManager.partial_close(
                            state=self._trade_position_state(trade),
                            qty=protective_fill_size,
                            exit_price=protective_fill_price or float(trade.get("entry") or 0.0),
                            exit_fee_usd=self._extract_order_fee_usd(order, "protective_fill"),
                        )
                        self._apply_position_update_to_trade(trade, position_update)
                        async with async_session() as session:
                            async with session.begin():
                                await session.execute(
                                    update(PositionModel)
                                    .where(PositionModel.id == trade["position_db_id"])
                                    .values(
                                        size=float(live_size),
                                        realized_pnl=float(position_update.state.realized_pnl),
                                    )
                                )
                        trade["current_size"] = float(live_size)
                        logger.info(
                            f"📉 [WS_ORDERS] Partial protective fill for {symbol}: "
                            f"closed={position_update.closed_qty:.6f} remaining={live_size:.6f} "
                            f"realized={position_update.realized_pnl:+.4f}"
                        )

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
        session: Optional[AsyncSession] = None,
    ) -> None:
        """Аудит: биржевые ордера в БД (идемпотентно по exchange/client id)."""
        ex_id = str(exchange_order_id).strip() if exchange_order_id else None
        cl_id = str(client_order_id).strip() if client_order_id else None
        if not ex_id and not cl_id:
            return
        ps = (position_side or "LONG").upper()
        side_enum = SignalType.LONG if ps == "LONG" else SignalType.SHORT
        
        async def _work(sess: AsyncSession):
            if ex_id:
                dup = await sess.execute(
                    select(OrderModel.id).where(OrderModel.exchange_order_id == ex_id)
                )
                if dup.scalar_one_or_none():
                    return
            if cl_id:
                dup = await sess.execute(
                    select(OrderModel.id).where(OrderModel.client_order_id == cl_id)
                )
                if dup.scalar_one_or_none():
                    return
            sess.add(
                OrderModel(
                    user_id=self.user_id,
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

        try:
            if session:
                await _work(session)
            else:
                async with async_session() as sess:
                    async with sess.begin():
                        await _work(sess)
        except IntegrityError:
            pass
        except Exception as e:
            logger.warning(f"⚠️ [AUDIT] order persist failed ({symbol}): {e}")

    async def _audit_event(
        self,
        *,
        event_type: str,
        severity: str = "INFO",
        message: Optional[str] = None,
        symbol: Optional[str] = None,
        strategy: Optional[str] = None,
        signal_id: Optional[int] = None,
        position_id: Optional[int] = None,
        order_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        await self.audit_trail.record(
            event_type=event_type,
            severity=severity,
            message=message,
            symbol=symbol,
            strategy=strategy,
            signal_id=signal_id,
            position_id=position_id,
            order_id=order_id,
            payload=payload,
        )

    def _position_side_from_entry_side(self, side: str) -> str:
        s = (side or "").upper()
        if s in ("BUY", "LONG"):
            return "LONG"
        if s in ("SELL", "SHORT"):
            return "SHORT"
        return "LONG"

    def _trade_position_state(self, trade: Dict[str, Any]) -> PositionState:
        side = trade.get("signal_type")
        qty = float(trade.get("current_size", 0.0) or 0.0)
        return PositionState(
            is_open=qty > 1e-12 and bool(side),
            entry_price=float(trade.get("entry", 0.0) or 0.0),
            qty=qty,
            side=str(side).upper() if side else None,
            realized_pnl=float(trade.get("realized_pnl", 0.0) or 0.0),
            open_fees_usd=float(trade.get("open_fees_usd", 0.0) or 0.0),
        )

    def _apply_position_update_to_trade(self, trade: Dict[str, Any], update_result) -> Dict[str, Any]:
        state = update_result.state
        trade["current_size"] = float(state.qty)
        trade["realized_pnl"] = float(state.realized_pnl)
        trade["position_is_open"] = bool(state.is_open)
        trade["open_fees_usd"] = float(state.open_fees_usd)
        if state.is_open and state.side:
            trade["entry"] = float(state.entry_price)
            trade["signal_type"] = str(state.side)
        return trade

    async def _find_open_position_guard(self, symbol: str) -> Optional[str]:
        local_trade = self.active_trades.get(symbol)
        if local_trade:
            if local_trade.get("stage") == "PENDING_EXECUTION":
                return "active_trades_pending_execution"
            if local_trade.get("position_is_open", False):
                return "active_trades_open_position"
            if float(local_trade.get("current_size", 0.0) or 0.0) > 1e-8:
                return "active_trades_nonzero_size"

        try:
            async with async_session() as session:
                res = await session.execute(
                    select(PositionModel.id)
                    .where(
                        PositionModel.symbol == symbol,
                        PositionModel.status == PositionStatus.OPEN,
                    )
                    .limit(1)
                )
                if res.scalar_one_or_none() is not None:
                    return "db_open_position"
        except Exception as e:
            logger.debug(f"[GUARD] DB open-position check failed for {symbol}: {e}")

        try:
            live_size, _ = await self._get_live_position(symbol)
            if live_size > 1e-8:
                return "exchange_live_position"
        except Exception as e:
            logger.debug(f"[GUARD] Exchange open-position check failed for {symbol}: {e}")

        return None

    def _estimate_fee_rate(self, order_kind: str = "") -> float:
        kind = str(order_kind or "").lower()
        if "limit" in kind or "maker" in kind:
            return float(getattr(settings, "maker_fee_pct", 0.0) or 0.0)
        return float(getattr(settings, "taker_fee_pct", 0.0) or 0.0)

    def _extract_order_fee_usd(self, order: Optional[Dict[str, Any]], order_kind: str = "") -> float:
        if not order:
            return 0.0

        fee = order.get("fee")
        if isinstance(fee, dict):
            try:
                return abs(float(fee.get("cost", 0.0) or 0.0))
            except (TypeError, ValueError):
                pass

        fees = order.get("fees")
        if isinstance(fees, list):
            total = 0.0
            seen = False
            for item in fees:
                if not isinstance(item, dict):
                    continue
                try:
                    total += abs(float(item.get("cost", 0.0) or 0.0))
                    seen = True
                except (TypeError, ValueError):
                    continue
            if seen:
                return total

        info = order.get("info") or {}
        for key in ("commission", "cumQuoteCommission", "executedCommission"):
            try:
                val = info.get(key)
                if val is not None:
                    return abs(float(val))
            except (TypeError, ValueError):
                continue

        price = float(order.get("average") or order.get("price") or 0.0)
        qty = float(order.get("filled") or order.get("amount") or 0.0)
        return PnLCalculator.estimate_fee(
            price=price,
            qty=qty,
            fee_rate=self._estimate_fee_rate(order_kind),
        )

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
        total_fees = 0.0
        for t in raw or []:
            info = t.get("info") or {}
            rp = info.get("realizedPnl") or info.get("realizedProfit")
            
            # Binance realizedPnl already includes fees, but let's log fees separately for audit
            fee = t.get("fee", {})
            if fee:
                total_fees += float(fee.get("cost", 0.0))
                
            try:
                total += float(rp or 0)
            except (TypeError, ValueError):
                pass
        
        entry = float(trade.get("entry", 0) or 0)
        size = float(trade.get("current_size", 0) or 0)
        notional = abs(entry * size) if entry and size else 0.0
        pct = (total / notional * 100.0) if notional > 1e-12 else 0.0
        return total, pct

    async def _persist_pnl_reconciled_close(self, snap: Dict[str, Any]) -> None:
        """P&L при закрытии позиции только в БД (нет живой позиции на бирже)."""
        sym = self._norm_sym(str(snap.get("symbol") or ""))
        if not sym:
            return
        side_raw = snap.get("side")
        if hasattr(side_raw, "value"):
            side_str = str(side_raw.value)
        else:
            side_str = str(side_raw or "LONG").split(".")[-1]
        trade = {
            "opened_at": float(snap.get("opened_at_ts") or time.time()),
            "entry": float(snap.get("entry_price") or 0),
            "current_size": float(snap.get("size") or 0),
            "signal_type": side_str,
            "open_fees_usd": 0.0,
        }
        pnl_usd, pnl_pct = await self._realized_pnl_from_exchange_trades(sym, trade)
        if abs(pnl_usd) < 1e-12 and trade["entry"] > 0 and trade["current_size"] > 0:
            try:
                ticker = await self.exchange.fetch_ticker(sym)
                exit_price = float(ticker.get("last") or 0)
                if exit_price > 0:
                    fee_rate = float(getattr(settings, "taker_fee_pct", 0.0) or 0.0)
                    est_entry_fee = PnLCalculator.estimate_fee(
                        price=trade["entry"],
                        qty=trade["current_size"],
                        fee_rate=fee_rate,
                    )
                    est_exit_fee = PnLCalculator.estimate_fee(
                        price=exit_price,
                        qty=trade["current_size"],
                        fee_rate=fee_rate,
                    )
                    pnl = PnLCalculator.calculate_realized_pnl(
                        side=side_str,
                        entry_price=trade["entry"],
                        exit_price=exit_price,
                        qty=trade["current_size"],
                        entry_fee_usd=est_entry_fee,
                        exit_fee_usd=est_exit_fee,
                    )
                    pnl_usd = pnl.pnl_usd
                    pnl_pct = pnl.pnl_pct
            except Exception:
                pass
        try:
            lev = int(getattr(settings, "leverage", 1) or 1)
        except Exception:
            lev = 1
        try:
            async with async_session() as session:
                async with session.begin():
                    session.add(
                        PnLModel(
                            user_id=self.user_id,
                            symbol=sym,
                            pnl_usd=pnl_usd,
                            pnl_pct=pnl_pct,
                            leverage=lev,
                            reason="RECONCILE_CLOSE",
                        )
                    )
            try:
                bal, _, _ = await self.get_account_metrics()
                await self.risk_manager.record_closed_pnl(pnl_usd, bal)
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"⚠️ [AUDIT] RECONCILE_CLOSE PnL failed {sym}: {e}")

    async def _db_mark_order_canceled(self, exchange_order_id: str) -> None:
        oid = str(exchange_order_id).strip()
        if not oid:
            return
        try:
            async with async_session() as session:
                async with session.begin():
                    r = await session.execute(
                        select(OrderModel).where(OrderModel.exchange_order_id == oid)
                    )
                    row = r.scalar_one_or_none()
                    if row:
                        row.status = OrderStatus.CANCELED
        except Exception as e:
            logger.debug(f"[AUDIT] mark canceled {oid}: {e}")

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
                clean_sym = SymbolNormalizer.to_binance(symbol)
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
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        position_id: Optional[int] = None,
        session: Optional[AsyncSession] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Унифицированная установка SL и TP через Algo API."""
        sl_id, tp_id = None, None
        try:
            side = self._entry_side_to_order_side(side)
            await self._prepare_private_ops(ctx=f"set_protective_orders({symbol})")
            is_hedge = await self._get_position_mode()
            # Подготовка символа для Binance API
            clean_sym = SymbolNormalizer.to_binance(symbol)
            reduce_side = "SELL" if side.upper() == "BUY" else "BUY"
            rules = await self._get_market_rules(symbol)
            max_algo = rules.get('max_algo', 100)
            
            # Быстрая проверка лимита алго-ордеров
            try:
                open_orders = await self._with_time_sync_retry(
                    lambda: self.exchange.fetch_open_orders(symbol),
                    ctx=f"check_max_algo({symbol})"
                )
                algo_types = ["STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"]
                current_algo_count = sum(1 for o in open_orders if str(o.get('type','')).upper() in algo_types)
                if current_algo_count >= max_algo:
                    logger.warning(f"⚠️ [{symbol}] Лимит алго-ордеров исчерпан ({current_algo_count}/{max_algo}). SL/TP пропущены!")
                    return None, None
            except Exception as e:
                logger.warning(f"⚠️ [{symbol}] Не удалось проверить лимит алго-ордеров: {e}")

            ref_price = await self._get_reference_price(symbol)

            # 1. STOP LOSS
            if sl:
                sl_trigger = await self._normalize_price(symbol, sl)
                if ref_price and not self._is_valid_stop_side(sl_trigger, ref_price, side):
                    eps = max(ref_price * 0.0015, 1e-8)
                    sl_trigger = (ref_price - eps) if side.upper() == "BUY" else (ref_price + eps)
                    sl_trigger = await self._normalize_price(symbol, sl_trigger)

                ts_ms = int(time.time() * 1000)
                sl_cid = f"p{position_id}_sl_{ts_ms}" if position_id else None
                
                sl_p = {
                    "symbol": clean_sym,
                    "side": reduce_side,
                    "algoType": "CONDITIONAL",
                    "type": "STOP_MARKET",
                    "triggerPrice": str(sl_trigger),
                    "closePosition": "true",
                    "workingType": "MARK_PRICE",
                    "newClientStrategyId": sl_cid if sl_cid else None
                }
                if is_hedge:
                    sl_p["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"
                    sl_p["reduceOnly"] = "true"

                sl_p["stopPrice"] = str(sl_trigger)
                try:
                    res_sl = await self._with_time_sync_retry(
                        lambda: self.exchange.fapiPrivatePostAlgoOrder(sl_p),
                        ctx=f"create_algo_sl({symbol})"
                    )
                    sl_id = str(res_sl.get("algoId") or res_sl.get("orderId"))
                    logger.info(f"🛡 [PROTECT] SL установлен для {symbol}: {sl}")
                    ps = self._position_side_from_entry_side(side)
                    await self._db_persist_order(
                        position_id=position_id,
                        symbol=symbol,
                        exchange_order_id=sl_id,
                        client_order_id=sl_cid,
                        order_type="STOP_MARKET_ALGO",
                        position_side=ps,
                        price=float(sl_trigger),
                        size=float(amount),
                        status=OrderStatus.OPEN,
                        session=session,
                    )
                except Exception as sl_err:
                    logger.warning(f"⚠️ [PROTECT] SL creation failed for {symbol}: {sl_err}")

            # 2. TAKE PROFIT
            if tp:
                tp_trigger = await self._normalize_price(symbol, tp)
                ts_ms = int(time.time() * 1000)
                tp_cid = f"p{position_id}_tp_{ts_ms}" if position_id else None
                tp_p = {
                    "symbol": clean_sym,
                    "side": reduce_side,
                    "algoType": "CONDITIONAL",
                    "type": "TAKE_PROFIT_MARKET",
                    "triggerPrice": str(tp_trigger),
                    "closePosition": "true",
                    "workingType": "MARK_PRICE",
                    "newClientStrategyId": tp_cid if tp_cid else None
                }
                if is_hedge:
                    tp_p["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"
                    tp_p["reduceOnly"] = "true"

                tp_p["stopPrice"] = str(tp_trigger)
                try:
                    res_tp = await self._with_time_sync_retry(
                        lambda: self.exchange.fapiPrivatePostAlgoOrder(tp_p),
                        ctx=f"create_algo_tp({symbol})"
                    )
                    tp_id = str(res_tp.get("algoId") or res_tp.get("orderId"))
                    logger.info(f"🎯 [PROTECT] TP установлен для {symbol}: {tp}")
                    ps = self._position_side_from_entry_side(side)
                    await self._db_persist_order(
                        position_id=position_id,
                        symbol=symbol,
                        exchange_order_id=tp_id,
                        client_order_id=tp_cid,
                        order_type="TAKE_PROFIT_MARKET_ALGO",
                        position_side=ps,
                        price=float(tp_trigger),
                        size=float(amount),
                        status=OrderStatus.OPEN,
                        session=session,
                    )
                except Exception as tp_err:
                    logger.warning(f"⚠️ [PROTECT] TP failed for {symbol}: {tp_err}")

        except Exception as e:
            logger.error(f"❌ [PROTECT] Ошибка защиты {symbol}: {e}")
        return sl_id, tp_id

    async def _cancel_extra_algo_orders(self, symbol: str, keep_ids: List[str]) -> None:
        """Cancel any open algo orders for *symbol* that are NOT in *keep_ids*.
        This helps to remove leftover test orders (e.g., old SL/TP) after a rescue.
        """
        try:
            # Fetch all open algo orders for the symbol.
            res = await self.exchange.request('openAlgoOrders', 'fapiPrivate', 'GET', {'symbol': SymbolNormalizer.to_binance(symbol)})
            raw_orders = res.get('algoOrders', []) if isinstance(res, dict) else (res if isinstance(res, list) else [])
            for o in raw_orders:
                oid = str(o.get('algoId') or o.get('id') or "").strip()
                if oid and oid not in keep_ids:
                    try:
                        await self.exchange.cancel_order(oid, symbol)
                        logger.info(f"🧹 [CLEANUP] Canceled stale algo order {oid} for {symbol}")
                    except Exception as e:
                        logger.warning(f"⚠️ [CLEANUP] Failed to cancel stale algo order {oid} for {symbol}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ [CLEANUP] Error retrieving algo orders for {symbol}: {e}")

    async def _reconcile_orders_for_position(
        self, 
        symbol: str, 
        position_db_id: int, 
        entry_price: float, 
        is_long: bool,
        db_sl_val: float, 
        db_tp_val: float,
        exchange_orders: List[dict]
    ) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[float]]:
        """
        Унифицированный поиск SL и TP среди биржевых ордеров.
        Возвращает: (sl_id, tp_id, found_sl_price, found_tp_price)
        """
        found_sl, found_tp = None, None
        sl_id, tp_id = None, None
        
        for o in exchange_orders:
            trig = float(o.get("stopPrice") or 0.0)
            if trig <= 0:
                continue
            
            kind = self._classify_protective_order(
                str(o.get("type", "")), 
                trig, 
                entry_price, 
                is_long,
                db_stop=db_sl_val, 
                db_tp=db_tp_val,
                client_id=o.get("client_id") or o.get("client_order_id"),
                position_id=position_db_id
            )
            
            if kind == "SL":
                found_sl = self._pick_better_level(found_sl, trig, kind="SL", is_long=is_long)
                sl_id = str(o.get("id"))
            elif kind == "TP":
                found_tp = self._pick_better_level(found_tp, trig, kind="TP", is_long=is_long)
                tp_id = str(o.get("id"))
                
        return sl_id, tp_id, found_sl, found_tp

    async def reconcile_full(self):
        """Полная синхронизация: Биржа -> БД -> Memory"""
        try:
            await self._prepare_private_ops(ctx="reconcile_full")
            pos_data = await self._with_time_sync_retry(
                lambda: self.exchange.fetch_positions_risk(),
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
                    "client_id": o.get("clientStrategyId"),
                    "type": (o.get("type") or "").upper(),
                    "stopPrice": (o.get("triggerPrice") or o.get("stopPrice")),
                })

            # Единая сессия для всех операций с БД
            ghost_snaps: List[Dict[str, Any]] = []
            async with async_session() as session:
                async with session.begin():
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
                                user_id=self.user_id, symbol=symbol,
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
                        
                        # Унифицированная сверка ордеров защиты
                        db_sl_val = float(dbp.stop_loss or 0.0)
                        db_tp_val = float(dbp.take_profit or 0.0)
                        sl_id, tp_id, found_sl, found_tp = await self._reconcile_orders_for_position(
                            symbol=symbol,
                            position_db_id=dbp.id,
                            entry_price=entry,
                            is_long=is_long,
                            db_sl_val=db_sl_val,
                            db_tp_val=db_tp_val,
                            exchange_orders=ex_orders_by_symbol.get(symbol, [])
                        )

                        # RESCUE: восстановление недостающих ордеров защиты
                        if (not sl_id and dbp.stop_loss) or (not tp_id and dbp.take_profit):
                            now_ts = time.time()
                            if now_ts < self._rescue_cooldown_until.get(symbol, 0.0):
                                logger.warning(f"[RESCUE] Cooldown active for {symbol}")
                            else:
                                live_size, live_side = await self._get_live_position(symbol, preferred_side=("LONG" if is_long else "SHORT"))
                                if live_size <= 0 or (live_side and ((is_long and live_side != "LONG") or ((not is_long) and live_side != "SHORT"))):
                                    logger.warning(f"[RESCUE] Skip {symbol}: no live position or side mismatch")
                                    await self._cancel_extra_algo_orders(symbol, [])
                                    continue
                                logger.warning(f"[RESCUE] Restoring protection for {symbol}")

                                safe_stop = dbp.stop_loss
                                if safe_stop:
                                    invalid_for_side = (is_long and float(safe_stop) >= entry) or ((not is_long) and float(safe_stop) <= entry)
                                    if invalid_for_side:
                                        safe_stop = entry * (0.995 if is_long else 1.005)
                                        logger.warning(f"[RESCUE] Adjusted stop for {symbol}: {dbp.stop_loss} -> {safe_stop}")

                                if not sl_id and safe_stop:
                                    res_sl_id, _ = await self._set_protective_orders(
                                        symbol, "BUY" if is_long else "SELL", contracts, safe_stop, None, position_id=dbp.id, session=session
                                    )
                                    if res_sl_id:
                                        sl_id = res_sl_id
                                        self._rescue_cooldown_until.pop(symbol, None)
                                    else:
                                        self._rescue_cooldown_until[symbol] = now_ts + settings.rescue_cooldown
                                        logger.warning(f"[RESCUE] Failed to set SL for {symbol}. Cooldown {settings.rescue_cooldown}s.")

                                if not tp_id and dbp.take_profit:
                                    _, res_tp_id = await self._set_protective_orders(
                                        symbol, "BUY" if is_long else "SELL", contracts, None, dbp.take_profit, position_id=dbp.id, session=session
                                    )
                                    if res_tp_id:
                                        tp_id = res_tp_id
                                        self._rescue_cooldown_until.pop(symbol, None)
                                    else:
                                        self._rescue_cooldown_until[symbol] = now_ts + settings.rescue_cooldown
                                        logger.warning(f"[RESCUE] Failed to set TP for {symbol}. Cooldown {settings.rescue_cooldown}s.")

                                if sl_id or tp_id:
                                    msg = f"🛟 **ВОССТАНОВЛЕНИЕ ЗАЩИТЫ**\n\n" \
                                          f"🔹 Символ: {symbol}\n" \
                                          f"🛡 Стоп: {f'{dbp.stop_loss:.4f}' if dbp.stop_loss else 'N/A'}\n" \
                                          f"🎯 Тейк: {f'{dbp.take_profit:.4f}' if dbp.take_profit else 'N/A'}\n\n"
                                    if sl_id: msg += f"🆔 SL ID: `{sl_id}`\n"
                                    if tp_id: msg += f"🆔 TP ID: `{tp_id}`"
                                    await send_telegram_msg(msg)
                                else:
                                    self._rescue_cooldown_until[symbol] = now_ts + settings.rescue_cooldown
                        
                        await self._cancel_extra_algo_orders(symbol, [str(sl_id) for sl_id in [sl_id, tp_id] if sl_id])
                        
                        cache_stop = found_sl or dbp.stop_loss
                        new_active[symbol] = {
                            "entry": entry,
                            "stop": cache_stop,
                            "take_profit_live": found_tp or dbp.take_profit,
                            "stage": prev_trade.get("stage", 0) if (prev_trade := self.active_trades.get(symbol, {})) else 0,
                            "opened_at": dbp.opened_at.timestamp() if dbp.opened_at else time.time(),
                            "signal_type": "LONG" if is_long else "SHORT",
                            "current_size": contracts,
                            "position_db_id": dbp.id,
                            "stop_order_id": sl_id,
                            "tp_order_id": tp_id,
                            "position_is_open": True,
                            "realized_pnl": prev_trade.get("realized_pnl", 0.0) if prev_trade else 0.0,
                            "open_fees_usd": prev_trade.get("open_fees_usd", 0.0) if prev_trade else 0.0,
                            "initial_stop": float(dbp.stop_loss or cache_stop or 0.0),
                            "be_moved": (float(cache_stop) >= entry) if is_long else (float(cache_stop) <= entry),
                            "strategy": prev_trade.get("strategy", "unknown") if prev_trade else "unknown",
                            "timeframe": prev_trade.get("timeframe", "1h") if prev_trade else "1h",
                        }

                    async with self._trades_lock:
                        self.active_trades = new_active

                    for sym, dbp in db_positions.items():
                        if sym not in new_active:
                            ghost_snaps.append({
                                "symbol": dbp.symbol,
                                "entry_price": float(dbp.entry_price or 0),
                                "size": float(dbp.size or 0),
                                "side": dbp.side,
                                "opened_at_ts": dbp.opened_at.timestamp() if dbp.opened_at else time.time(),
                            })
                            await session.execute(
                                update(PositionModel)
                                .where(PositionModel.id == dbp.id)
                                .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                            )



            for snap in ghost_snaps:
                await self._persist_pnl_reconciled_close(snap)

            # Orphan cleanup: cancel algo/standard orders for symbols with no active position
            orphan_symbols = set(ex_orders_by_symbol.keys()) - set(new_active.keys())
            for orphan_sym in orphan_symbols:
                for o in ex_orders_by_symbol[orphan_sym]:
                    oid = o.get("id")
                    if not oid:
                        continue
                    try:
                        clean = SymbolNormalizer.to_binance(orphan_sym)
                        await self._with_time_sync_retry(
                            lambda _oid=oid, _cs=clean: self.exchange.request(
                                'algoOrder', 'fapiPrivate', 'DELETE', {'algoId': str(_oid)}
                            ),
                            ctx=f"orphan_cleanup({orphan_sym}:{oid})"
                        )
                        logger.info(f"🧹 [ORPHAN] Cancelled stale order {oid} for {orphan_sym}")
                        await self._db_mark_order_canceled(str(oid))
                    except Exception as e:
                        try:
                            await self._with_time_sync_retry(
                                lambda _oid=oid, _sym=orphan_sym: self.exchange.cancel_order(str(_oid), _sym),
                                ctx=f"orphan_cleanup_std({orphan_sym}:{oid})"
                            )
                            logger.info(f"🧹 [ORPHAN] Cancelled standard order {oid} for {orphan_sym}")
                            await self._db_mark_order_canceled(str(oid))
                        except Exception:
                            logger.warning(f"⚠️ [ORPHAN] Could not cancel {oid} for {orphan_sym}: {e}")

            self.active_trades = new_active

            # Cleanup stale PENDING/EXECUTING signals older than 10 minutes
            try:
                async with async_session() as session:
                    async with session.begin():
                        cutoff = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
                        stale_result = await session.execute(
                            update(SignalModel)
                            .where(
                                SignalModel.status.in_(["PENDING", "EXECUTING"]),
                                SignalModel.timestamp < cutoff
                            )
                            .values(status="EXPIRED")
                        )
                        if stale_result.rowcount > 0:
                            logger.info(f"🧹 [RECONCILE] Expired {stale_result.rowcount} stale signals")
            except Exception as sig_err:
                logger.debug(f"Signal cleanup error: {sig_err}")

            logger.info(f"[RECONCILE] Active positions: {len(self.active_trades)} | Orphan orders cleaned: {len(orphan_symbols)}")
        except Exception as e:
            logger.error(f"Reconcile Error: {e}\n{traceback.format_exc()}")

    async def execute_signal(self, signal_data: dict, account_balance: float, drawdown: float, open_count: int):
        # Нормализуем символ перед началом обработки
        symbol = SymbolNormalizer.normalize(signal_data['symbol'])
        direction = signal_data['signal']
        signal_id = signal_data.get("id")

        if not settings.is_trading_enabled: return

        # 1. Быстрая проверка вне лока (оптимизация)
        guard_reason = await self._find_open_position_guard(symbol)
        if guard_reason:
            await self._audit_event(
                event_type="entry_blocked",
                symbol=symbol,
                strategy=signal_data.get("strategy"),
                signal_id=signal_id,
                message=f"Entry blocked before order placement: {guard_reason}",
                payload={"guard_reason": guard_reason, "direction": direction},
            )
            logger.info(f"⏭ [{symbol}] Вход заблокирован до размещения ордера: {guard_reason}")
            return

        # 2. Блокировка по символу для предотвращения гонки сигналов
        async with self._get_symbol_lock(symbol):
            # Повторная проверка внутри лока (Double-Checked Locking)
            guard_reason = await self._find_open_position_guard(symbol)
            if guard_reason:
                await self._audit_event(
                    event_type="entry_blocked",
                    symbol=symbol,
                    strategy=signal_data.get("strategy"),
                    signal_id=signal_id,
                    message=f"Entry blocked inside symbol lock: {guard_reason}",
                    payload={"guard_reason": guard_reason, "direction": direction},
                )
                logger.info(f"⏭ [{symbol}] Вход заблокирован внутри лока: {guard_reason}")
                return
                
            # Резервируем монету, чтобы параллельные тики по другим стратегиям не вошли сюда
            self.active_trades[symbol] = {"stage": "PENDING_EXECUTION", "ts": time.time()}

            # Новые правила размера/лимита входов применяем только когда бот полностью "плоский".
            if settings.apply_new_entry_rules_after_flat and not self._entry_policy_activated:
                _, _, live_open_count = await self.get_account_metrics()
                if live_open_count > 0:
                    logger.info(f"⏳ [{symbol}] Новые правила входа активируются после закрытия текущих позиций ({live_open_count} открыто).")
                    del self.active_trades[symbol]
                    return
                self._entry_policy_activated = True
                logger.info("✅ Новые правила входа активированы: max_open_trades=3, margin_per_trade=5%, pyramiding=OFF")

            if not self.risk_manager.check_trade_allowed(open_count, drawdown):
                logger.warning(f"🚫 [{symbol}] Риск-менеджер запретил вход (Drawdown/Limit)")
                del self.active_trades[symbol]
                return

            # Idempotency (по ID сигнала в БД)
            async with async_session() as session:
                async with session.begin():
                    stmt = update(SignalModel).where(SignalModel.id == signal_id, SignalModel.status == "PENDING").values(status="EXECUTING")
                    res = await session.execute(stmt)
                if res.rowcount == 0:
                    logger.info(f"⏭ [{symbol}] Сигнал {signal_id} уже обрабатывается или исполнен.")
                    del self.active_trades[symbol]
                    return

            try:
                await self._audit_event(
                    event_type="entry_started",
                    symbol=symbol,
                    strategy=signal_data.get("strategy"),
                    signal_id=signal_id,
                    message="Signal accepted for execution",
                    payload={
                        "direction": direction,
                        "entry_price": signal_data.get("entry_price"),
                        "timeframe": signal_data.get("timeframe"),
                    },
                )
                await self._prepare_private_ops(ctx=f"execute_signal({symbol})")
                # 3. Расчет параметров входа
                entry_price = float(signal_data.get('entry_price', 0))
                raw_atr = signal_data.get('atr', 0.0)
                safe_atr = float(raw_atr) if raw_atr and not (isinstance(raw_atr, float) and raw_atr != raw_atr) else 0.0
                if safe_atr <= 0:
                    logger.error(f"[{symbol}] ATR invalid ({raw_atr}), cannot enter — aborting")
                    raise Exception(f"Invalid ATR={raw_atr}, cannot compute stop-loss")
                stop_price = self.risk_manager.calculate_atr_stop(entry_price, safe_atr, direction)

                market_info_data = None
                try:
                    market_obj = self.exchange.market(symbol) if hasattr(self.exchange, "market") else None
                    if market_obj:
                        market_info_data = market_obj
                except Exception:
                    pass
                market_ctx_data = signal_data.get("market_context")
                ml_prob = signal_data.get("ml_prob", 0.5)

                size_check = self.risk_manager.assess_trade_feasibility(
                    account_balance, entry_price, stop_price,
                    market_info=market_info_data,
                    market_context=market_ctx_data,
                    ml_prob=ml_prob,
                )
                if not size_check.get("feasible", False):
                    raise EntryExecutionError(
                        size_check.get("reason", "size_infeasible"),
                        f"Size infeasible: {size_check.get('reason', 'unknown')}"
                    )
                lot_size = await self._normalize_amount(symbol, float(size_check["position_size"]))

                if lot_size <= 0:
                    raise EntryExecutionError("normalized_to_zero", "Zero lot size after normalization")

                # 3.5 Проверка MIN_NOTIONAL
                rules = await self._get_market_rules(symbol)
                min_notional = float(rules.get('min_notional', 5.0))
                current_notional = lot_size * entry_price
                if current_notional < min_notional:
                    logger.warning(f"🚫 [{symbol}] Ордер отклонен: Notional {current_notional:.2f} < Min {min_notional}")
                    raise EntryExecutionError("min_notional_fail", f"Notional {current_notional:.2f} < {min_notional}")

                # 4. Умный Вход (Limit Chasing)
                await self._set_leverage_best_effort(symbol, settings.leverage)
                side = 'buy' if direction.upper() == 'LONG' else 'sell'
                
                entry_exec = entry_price
                max_retries = 3
                remaining_size = lot_size
                filled_total = 0.0
                entry_audit: List[Dict[str, Any]] = []
                saw_no_book = False

                for attempt in range(max_retries):
                    if remaining_size <= 0: break
                    try:
                        ob = await self._with_time_sync_retry(
                            lambda: self.exchange.fetch_order_book(symbol, limit=5),
                            ctx=f"fetch_order_book({symbol})"
                        )
                        if not ob or not ob.get('bids') or not ob.get('asks'):
                            saw_no_book = True
                            logger.warning(f"Limit chase error: no book")
                            break
                        best_price = ob['bids'][0][0] if side == 'buy' else ob['asks'][0][0]
                        best_price = await self._normalize_price(symbol, best_price)
                        
                        logger.info(f"🕸 [LIMIT CHASE] Попытка {attempt+1}: Лимитка {side} {symbol} по {best_price}")
                        
                        temp_order = await self._with_time_sync_retry(
                            lambda: self.exchange.create_order(
                                symbol=symbol, type='limit', side=side, amount=remaining_size, price=best_price,
                                params={'timeInForce': 'GTX', 'postOnly': True} if settings.use_post_only else {}
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
                        if "no book" in str(e).lower():
                            saw_no_book = True
                            break
                        
                # 5. Fallback (Market)
                if remaining_size > 0:
                    if saw_no_book:
                        logger.warning(f"⚡️ [FALLBACK] No book for {symbol}, switching straight to market entry")
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
                        if filled_total == 0:
                            raise EntryExecutionError("market_fallback_failed", f"Market fallback error: {e}")
                
                lot_size = filled_total
                if lot_size <= 0:
                    raise EntryExecutionError("entry_not_filled", "Entry did not fill any size")

                # 6. Пересчёт стопа от ФАКТИЧЕСКОЙ цены входа (а не теоретической из сигнала)
                if abs(entry_exec - entry_price) > entry_price * 0.0001:
                    stop_price = self.risk_manager.calculate_atr_stop(entry_exec, safe_atr, direction)
                    logger.info(f"[{symbol}] SL recalculated for actual fill: entry {entry_price:.6f} → {entry_exec:.6f}, stop → {stop_price:.6f}")

                position_update = PositionManager.open_position(
                    side=direction,
                    qty=lot_size,
                    entry_price=entry_exec,
                    fee_paid_usd=sum(
                        self._extract_order_fee_usd(aud.get("order") or {}, str(aud.get("kind") or ""))
                        for aud in entry_audit
                    ),
                )
                position_state = position_update.state

                # 7. БД Позиция и Ордера (Транзакционно)
                async with async_session() as session:
                    async with session.begin():
                        pos = PositionModel(
                            user_id=self.user_id,
                            signal_id=signal_id,
                            symbol=symbol,
                            side=SignalType.LONG if direction.upper() == "LONG" else SignalType.SHORT,
                            entry_price=position_state.entry_price,
                            size=position_state.qty,
                            stop_loss=stop_price,
                            take_profit=signal_data.get("take_profit"),
                            status=PositionStatus.OPEN,
                            opened_at=datetime.datetime.utcnow()
                        )
                        session.add(pos)
                        await session.flush()  # Получаем pos.id

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
                                session=session,
                            )

                await self._audit_event(
                    event_type="position_opened",
                    symbol=symbol,
                    strategy=signal_data.get("strategy"),
                    signal_id=signal_id,
                    position_id=pos.id,
                    message="Position opened after entry fill",
                    payload={
                        "entry_price": position_state.entry_price,
                        "qty": position_state.qty,
                        "side": position_state.side,
                        "stop_loss": stop_price,
                        "take_profit": signal_data.get("take_profit"),
                    },
                )

                # 8. Защитные ордера
                targets = signal_data.get("targets") or signal_data.get("take_profit")
                # Мы не включаем постановку защиты в ту же транзакцию, т.к. это внешние вызовы API,
                # но передаем сессию для записи ордеров защиты если нужно (опционально)
                sl_id, tp_id = await self._set_protective_orders(
                    symbol, direction, lot_size, stop_price, targets, position_id=pos.id
                )
                if not sl_id:
                    # Best practice: не оставлять открытую позицию без стопа.
                    logger.error(
                        f"❌ [PROTECT] Missing STOP after entry for {symbol}: "
                        f"direction={direction} entry={entry_exec:.6f} stop={stop_price:.6f} "
                        f"tp={targets} size={lot_size}"
                    )
                    emergency_side = 'sell' if direction.upper() == "LONG" else 'buy'
                    em_o = await self._with_time_sync_retry(
                        lambda: self.exchange.create_order(symbol, 'market', emergency_side, lot_size),
                        ctx=f"emergency_close_no_sl({symbol})"
                    )
                    async with async_session() as session:
                        async with session.begin():
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
                                session=session,
                            )
                            await session.execute(
                                update(PositionModel)
                                .where(PositionModel.id == pos.id)
                                .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                            )
                    raise EntryExecutionError(
                        "protective_stop_missing",
                        "Protective STOP was not created; position closed by emergency market order"
                    )
                await self._audit_event(
                    event_type="protective_orders_set",
                    symbol=symbol,
                    strategy=signal_data.get("strategy"),
                    signal_id=signal_id,
                    position_id=pos.id,
                    message="Protective orders linked to position",
                    payload={"sl_id": sl_id, "tp_id": tp_id},
                )

                # 8. Финализация
                self.active_trades[symbol] = {
                    "entry": position_state.entry_price, "stop": stop_price, "stage": 0, "opened_at": time.time(),
                    "signal_type": position_state.side, "current_size": position_state.qty, "position_db_id": pos.id, "stop_order_id": sl_id,
                    "take_profit_live": signal_data.get("targets", signal_data.get("take_profit")),
                    "tp_order_id": tp_id,
                    "initial_stop": stop_price,
                    "be_moved": False,
                    "position_is_open": position_state.is_open,
                    "realized_pnl": position_state.realized_pnl,
                    "open_fees_usd": position_state.open_fees_usd,
                    "timeframe": signal_data.get("timeframe", "1h"),
                    "strategy": signal_data.get("strategy", "unknown"),
                    # Trade management context
                    "setup_group": signal_data.get("setup_group", "trend"),
                    "breakout_level": signal_data.get("breakout_level"),
                    "invalidation_level": signal_data.get("invalidation_level"),
                    "ma_at_entry": signal_data.get("ma_at_entry", {}),
                    "be_armed": False,
                    "partial_done": False,
                    "max_favorable_r": 0.0,
                    "max_adverse_r": 0.0,
                    "bars_since_entry": 0,
                    "last_mgmt_bar_ts": 0.0,
                }
                
                # Обновляем сигнал в БД как EXECUTED
                async with async_session() as session:
                    async with session.begin():
                        await session.execute(update(SignalModel).where(SignalModel.id == signal_id).values(status="EXECUTED"))
                _strat = signal_data.get("strategy", "?")
                _tp_val = signal_data.get("take_profit")
                _tp_str = f"{float(_tp_val):.4f}" if _tp_val else "—"
                _dir_emoji = "🟢 LONG" if direction.upper() == "LONG" else "🔴 SHORT"

                _sl_dist_pct = abs(entry_exec - stop_price) / entry_exec * 100 if entry_exec > 0 else 0
                _risk_usdt = abs(entry_exec - stop_price) * lot_size

                pct_slip = ((entry_exec - entry_price) / entry_price) * 100 if entry_price > 0 else 0
                if direction.upper() == 'SHORT':
                    pct_slip = -pct_slip

                try:
                    from utils.metrics import order_execution_slippage
                    order_execution_slippage.observe(pct_slip)
                except Exception:
                    pass

                if trades_opened:
                    trades_opened.labels(symbol=symbol, direction=direction.upper()).inc()
                logger.info(
                    f"✅ [ENTRY] {symbol} {direction.upper()} | "
                    f"strategy={_strat} entry={entry_exec:.6f} "
                    f"SL={stop_price:.6f} ({_sl_dist_pct:.2f}%) "
                    f"TP={_tp_str} size={lot_size} ATR={safe_atr:.6f} | "
                    f"risk={_risk_usdt:.4f} USDT SL_order={sl_id} TP_order={tp_id} "
                    f"balance={account_balance:.2f}"
                )

                await send_telegram_msg(
                    f"✅ **НОВАЯ ПОЗИЦИЯ**\n\n"
                    f"🔹 Символ: `{symbol}`\n"
                    f"📌 Направление: {_dir_emoji}\n"
                    f"📊 Стратегия: {_strat}\n"
                    f"💰 Цена входа: `{entry_exec:.4f}`\n"
                    f"🛡 Стоп-лосс: `{stop_price:.4f}` ({_sl_dist_pct:.2f}%)\n"
                    f"🎯 Тейк-профит: `{_tp_str}`\n"
                    f"📦 Объём: `{lot_size}` | Риск: `{_risk_usdt:.2f}` USDT"
                )

            except Exception as e:
                # Если мы зафейлили вход, освобождаем монету
                if symbol in self.active_trades and self.active_trades[symbol].get("stage") == "PENDING_EXECUTION":
                    del self.active_trades[symbol]
                
                reason = e.reason if isinstance(e, EntryExecutionError) else "unexpected_entry_error"
                self._record_entry_failure(reason)
                logger.error(f"❌ Entry Error {symbol} [{reason}]: {e}", exc_info=True)
                await self._audit_event(
                    event_type="entry_failed",
                    severity="ERROR",
                    symbol=symbol,
                    strategy=signal_data.get("strategy"),
                    signal_id=signal_id,
                    message=f"Entry failed: {reason}",
                    payload={"reason": reason, "error": str(e)[:300]},
                )

                # Помечаем сигнал как FAILED в БД
                async with async_session() as session:
                    async with session.begin():
                        await session.execute(update(SignalModel).where(SignalModel.id == signal_id).values(status="FAILED", comment=str(e)[:200]))

                try:
                    await send_telegram_msg(
                        f"❌ **ОШИБКА ВХОДА**\n\n"
                        f"🔹 Символ: `{symbol}`\n"
                        f"📊 Стратегия: {signal_data.get('strategy', 'N/A')}\n"
                        f"⚠️ Причина: `{reason}: {str(e)[:180]}`"
                    )
                except Exception:
                    pass
                raise e

    async def schedule_update_positions(self, symbol: str, current_price: float, atr: float, adx: Optional[float] = None, df_tf=None, cvd_val: float = 0.0):
        """Вызывается каждую минуту для трейлинга и trade management."""
        async with self._get_symbol_lock(symbol):
            async with self._trades_lock: # Блокируем чтение
                if symbol not in self.active_trades: return
                trade = self.active_trades[symbol]
                trade['current_price'] = current_price
                live_size, _ = await self._get_live_position(symbol, preferred_side=trade.get('signal_type'))
                if live_size <= 1e-8:
                    await self._close_position(symbol, reason="EXTERNAL")
                    return
                if abs(live_size - float(trade.get("current_size", 0.0))) > 1e-8:
                    trade["current_size"] = live_size
                    async with async_session() as session:
                        async with session.begin():
                            await session.execute(
                                update(PositionModel)
                                .where(PositionModel.id == trade["position_db_id"])
                                .values(size=float(live_size))
                            )
                            
            # Trade management cycle (invalidation, time stop, partial); стоп — ниже, unified pipeline
            try:
                await self.evaluate_position_management(symbol, current_price, atr, adx=adx, df_tf=df_tf, cvd_val=cvd_val)
            except Exception as e:
                logger.debug(f"[TM] {symbol}: management cycle error: {e}")
            if symbol not in self.active_trades:
                return
            trade = self.active_trades[symbol]

            # Единый расчёт и одно обновление защитного стопа за тик (ATR + confirmed + BE).
            try:
                desired_stop, stop_src = self._compute_unified_desired_stop(
                    trade, current_price, atr, adx, df_tf
                )
                await self._apply_unified_protective_stop(
                    symbol, trade, desired_stop, stop_src, current_price
                )
            except Exception as e:
                logger.warning(f"[UNIFIED_STOP] {symbol}: {e}")

            if symbol not in self.active_trades:
                return
            trade = self.active_trades[symbol]

            # Legacy TimeExitSystem: только если TM выключен или явно включён параллельный режим.
            trade_tf = trade.get("timeframe", "1h")
            if not settings.trade_mgmt_enabled:
                if self.time_exit.should_exit(trade["opened_at"], time.time(), trade_tf, current_price, trade["entry"], trade["signal_type"]):
                    await self._close_position(symbol, reason="TIME")
                    return
            elif getattr(settings, "legacy_time_exit_system_enabled", False):
                if self.time_exit.should_exit(trade["opened_at"], time.time(), trade_tf, current_price, trade["entry"], trade["signal_type"]):
                    await self._close_position(symbol, reason="TIME")
                    return

            if atr <= 0:
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
                            fill_size = float(pyr_o.get("filled") or add_size)
                            fill_price = float(pyr_o.get("average") or pyr_o.get("price") or current_price)
                            position_update = PositionManager.open_position(
                                side=trade["signal_type"],
                                qty=fill_size,
                                entry_price=fill_price,
                                state=self._trade_position_state(trade),
                                fee_paid_usd=self._extract_order_fee_usd(pyr_o, "market_pyramid_add"),
                            )
                            new_size = float(position_update.state.qty)
                            new_entry = float(position_update.state.entry_price)

                            trade['stage'] = next_stage
                            self._apply_position_update_to_trade(trade, position_update)

                            async with async_session() as session:
                                async with session.begin():
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
                                        session=session,
                                    )
                                    await session.execute(
                                        update(PositionModel)
                                        .where(PositionModel.id == trade["position_db_id"])
                                        .values(size=float(new_size), entry_price=float(new_entry))
                                    )
                            
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

    # ─── Trade Management (loss minimization) ───────────────────────────

    def _tm_record(self, action: str) -> None:
        try:
            if trade_mgmt_events:
                trade_mgmt_events.labels(action=action).inc()
        except Exception:
            pass

    @staticmethod
    def _better_stop_for_side(is_long: bool, a: float, b: float) -> float:
        """Return the more protective stop: higher for LONG, lower for SHORT."""
        if is_long:
            return max(a, b)
        return min(a, b)

    def _compute_unified_desired_stop(
        self,
        trade: Dict[str, Any],
        current_price: float,
        atr: float,
        adx: Optional[float],
        df_tf: Any,
    ) -> Tuple[float, str]:
        """
        Single computation for protective stop: ATR trailing (2.5x) + optional confirmed trail (2.0x)
        + break-even when 1R and confirmation. No exchange calls.
        """
        side = str(trade.get("signal_type", "")).upper()
        is_long = side == "LONG"
        current_stop = float(trade.get("stop") or 0)
        entry = float(trade.get("entry") or 0)
        initial_stop = float(trade.get("initial_stop", trade.get("stop")) or 0)

        if entry <= 0 or current_stop <= 0:
            return current_stop, "unchanged"

        atr_safe = float(atr or 0.0)
        if atr_safe <= 0:
            best = current_stop
            src = "unchanged"
        else:
            best = self.risk_manager.calculate_trailing_stop(
                current_stop, current_price, atr_safe, side, multiplier=2.5
            )
            src = "atr_trail"

            current_r = self.risk_manager.current_r_multiple(entry, initial_stop, current_price, side)
            if (
                settings.confirmed_trailing_enabled
                and trade.get("be_armed")
                and current_r >= float(settings.confirmed_trailing_min_r)
                and df_tf is not None
                and not getattr(df_tf, "empty", True)
                and len(df_tf) >= 2
            ):
                last_bar = df_tf.iloc[-2]
                bar_ts = last_bar.get("timestamp", 0)
                try:
                    bar_ts = float(bar_ts.timestamp()) if hasattr(bar_ts, "timestamp") else float(bar_ts)
                except Exception:
                    bar_ts = 0
                if bar_ts > float(trade.get("last_mgmt_bar_ts", 0)):
                    bar_dict = {
                        "open": float(last_bar.get("open", 0)),
                        "close": float(last_bar.get("close", 0)),
                        "high": float(last_bar.get("high", 0)),
                        "low": float(last_bar.get("low", 0)),
                    }
                    if self.risk_manager.favorable_bar_confirmed(bar_dict, side):
                        trail_conf = self.risk_manager.calculate_trailing_stop(
                            current_stop, current_price, atr_safe, side, multiplier=2.0
                        )
                        best = self._better_stop_for_side(is_long, best, trail_conf)
                        src = "confirmed_trail"
                    trade["last_mgmt_bar_ts"] = bar_ts

        if not trade.get("be_moved", False):
            risk_distance = abs(entry - initial_stop)
            atr_for_breakout = max(atr_safe, 0.0)
            breakout_dist = max(atr_for_breakout * 0.5, entry * 0.0015 if entry > 0 else 0.0)
            adx_ok = (adx is not None) and (float(adx) >= 20.0)
            if entry > 0 and risk_distance > 0 and side in ("LONG", "SHORT"):
                if side == "LONG":
                    reached_1r = current_price >= (entry + risk_distance)
                    breakout_ok = current_price >= (entry + breakout_dist)
                else:
                    reached_1r = current_price <= (entry - risk_distance)
                    breakout_ok = current_price <= (entry - breakout_dist)
                if reached_1r and (adx_ok or breakout_ok):
                    be_stop = float(
                        RiskManager.break_even_price(entry, side, float(getattr(settings, "be_buffer_pct", 0.0004) or 0.0004))
                    )
                    new_best = self._better_stop_for_side(is_long, best, be_stop)
                    if abs(new_best - best) > 1e-12:
                        best = new_best
                        src = "break_even" if src == "unchanged" else f"{src}+be"
                        trade["be_moved"] = True

        # --- Professional EMA20 Trailing (Scaling Out Phase) ---
        if trade.get("trailing_active") and df_tf is not None and not getattr(df_tf, "empty", True) and len(df_tf) >= 20:
            try:
                # Use pandas to calculate EMA20 accurately
                ema_series = df_tf['close'].ewm(span=20, adjust=False).mean()
                ema20 = float(ema_series.iloc[-1])
                # We normalize it for check consistency
                ema20_norm = float(Decimal(str(ema20)).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP))
                
                new_best = self._better_stop_for_side(is_long, best, ema20_norm)
                if abs(new_best - best) > 1e-12:
                    best = new_best
                    src = "ema20_trail"
            except Exception as e:
                logger.error(f"Error calculating EMA trailing: {e}")

        return best, src

    async def _apply_unified_protective_stop(
        self,
        symbol: str,
        trade: Dict[str, Any],
        desired_stop: float,
        source: str,
        current_price: float,
    ) -> None:
        """At most one cancel+replace per tick if the desired stop improves enough."""
        if symbol not in self.active_trades:
            return
        side = str(trade.get("signal_type", "")).upper()
        is_long = side == "LONG"
        current_stop = float(trade.get("stop") or 0)
        if current_stop <= 0:
            return

        threshold = max(current_price * 0.001, 1e-12)
        delta = abs(desired_stop - current_stop)
        if delta <= threshold:
            return

        improves = (is_long and desired_stop > current_stop) or ((not is_long) and desired_stop < current_stop)
        if not improves:
            return

        be_before = bool(trade.get("be_moved", False))
        is_be = "break_even" in source or "+be" in source

        logger.info(
            f"🎯 [UNIFIED_STOP] {symbol} {side}: {current_stop:.6f} -> {desired_stop:.6f} "
            f"(src={source}, Δ={delta:.6f})"
        )
        await self._cancel_all_orders(symbol)
        tp_ref = trade.get("take_profit_live")
        sl_id, tp_id = await self._set_protective_orders(
            symbol, side, trade["current_size"], desired_stop, tp_ref,
            position_id=trade.get("position_db_id"),
        )
        if not sl_id:
            logger.warning(f"⚠️ [UNIFIED_STOP] {symbol}: protective SL not placed at {desired_stop:.6f}")
            return

        trade["stop"] = desired_stop
        trade["stop_order_id"] = sl_id
        if tp_id:
            trade["tp_order_id"] = tp_id

        if is_be and not be_before:
            trade["be_moved"] = True
        elif not trade.get("be_moved"):
            # Трейлинг мог подтянуть стоп выше/ниже чистого BE — считаем безубыток достигнутым.
            entry_px = float(trade.get("entry") or 0)
            if entry_px > 0:
                buf = float(getattr(settings, "be_buffer_pct", 0.0004) or 0.0004)
                be_px = float(RiskManager.break_even_price(entry_px, side, buffer_pct=buf))
                if is_long and desired_stop >= be_px - 1e-10:
                    trade["be_moved"] = True
                elif (not is_long) and desired_stop <= be_px + 1e-10:
                    trade["be_moved"] = True

        async with async_session() as session:
            async with session.begin():
                await session.execute(
                    update(PositionModel)
                    .where(PositionModel.id == trade["position_db_id"])
                    .values(stop_loss=float(desired_stop))
                )

                if "confirmed" in source:
                    self._tm_record("confirmed_trailing")

                order_type_tag = "SL_TRAILING_UPDATE"
                if is_be and not be_before:
                    order_type_tag = "SL_BREAKEVEN_MOVE"
                elif "confirmed" in source:
                    order_type_tag = "SL_TRAILING_CONFIRMED"

                await self._db_persist_order(
                    position_id=trade.get("position_db_id"),
                    symbol=symbol,
                    exchange_order_id=str(sl_id) if sl_id else None,
                    client_order_id=None,
                    order_type=order_type_tag,
                    position_side=side,
                    price=float(desired_stop),
                    size=float(trade["current_size"]),
                    status=OrderStatus.OPEN,
                    session=session,
                )

        if is_be and not be_before:
            await send_telegram_msg(
                f"🟡 **БЕЗУБЫТОК: {symbol}**\n\n"
                f"🛡 Стоп: `{desired_stop:.6f}`\n"
                f"📊 Источник: unified ({source})\n"
                f"💹 Цена: `{current_price:.6f}`"
            )

    def _tm_bars_since_entry(self, trade: dict, timeframe: str) -> int:
        from core.strategies.strategies import get_timeframe_seconds
        opened = trade.get("opened_at", 0)
        if not opened:
            return 0
        tf_sec = get_timeframe_seconds(timeframe)
        if tf_sec <= 0:
            return 0
        return int((time.time() - opened) / tf_sec)



    async def _tm_partial_reduce(self, symbol: str, trade: dict, current_r: float) -> bool:
        """Reduce position by partial_fraction if partial trigger is met."""
        if not settings.partial_enabled:
            return False
        if trade.get("partial_done"):
            return False
        if current_r < settings.partial_trigger_r:
            return False

        frac = settings.partial_fraction
        side = 'sell' if trade.get('signal_type') == "LONG" else 'buy'
        live_size, _ = await self._get_live_position(symbol, preferred_side=trade.get('signal_type'))
        base_size = live_size if live_size > 1e-8 else float(trade.get('current_size', 0))
        reduce_amount = base_size * frac
        if reduce_amount <= 0:
            return False

        try:
            market = self.exchange.market(symbol) if hasattr(self.exchange, "market") else None
            min_amount = float((market or {}).get("limits", {}).get("amount", {}).get("min") or 0)
            if min_amount > 0 and reduce_amount < min_amount:
                _now = time.time()
                if (_now - self._tm_partial_reduce_skip_log_ts.get(symbol, 0.0)) > 60.0:
                    logger.debug(f"[TM] {symbol}: partial reduce {reduce_amount} < min_amount {min_amount}, skip (throttled)")
                    self._tm_partial_reduce_skip_log_ts[symbol] = _now
                return False

            params = {"reduceOnly": True}
            ro = await self._with_time_sync_retry(
                lambda: self.exchange.create_order(symbol, 'market', side, reduce_amount, None, params),
                ctx=f"tm_partial_reduce({symbol})"
            )
            await self._db_persist_order(
                position_id=trade.get("position_db_id"),
                symbol=symbol,
                exchange_order_id=str(ro.get("id")) if ro and ro.get("id") is not None else None,
                client_order_id=str(ro.get("clientOrderId")) if ro and ro.get("clientOrderId") else None,
                order_type="MARKET_PARTIAL_REDUCE_TM",
                position_side=str(trade.get("signal_type") or "LONG"),
                price=float(ro.get("average") or ro.get("price") or 0.0),
                size=float(ro.get("filled") or reduce_amount),
                status=OrderStatus.FILLED,
            )
            fill_size = float(ro.get("filled") or reduce_amount)
            fill_price = float(ro.get("average") or ro.get("price") or trade.get("entry") or 0.0)
            position_update = PositionManager.partial_close(
                state=self._trade_position_state(trade),
                qty=fill_size,
                exit_price=fill_price,
                exit_fee_usd=self._extract_order_fee_usd(ro, "market_partial_reduce_tm"),
            )
            self._apply_position_update_to_trade(trade, position_update)
            new_size = float(position_update.state.qty)
            trade["partial_done"] = True
            async with async_session() as session:
                async with session.begin():
                    await session.execute(
                        update(PositionModel)
                        .where(PositionModel.id == trade["position_db_id"])
                        .values(
                            size=float(max(0.0, new_size)),
                            realized_pnl=float(position_update.state.realized_pnl),
                        )
                    )

            self._tm_record("partial_reduce")
            await self._audit_event(
                event_type="position_partial_close",
                symbol=symbol,
                strategy=trade.get("strategy"),
                position_id=trade.get("position_db_id"),
                message="Trade management partial reduce executed",
                payload={
                    "closed_qty": fill_size,
                    "remaining_qty": new_size,
                    "realized_pnl": position_update.realized_pnl,
                    "fees_usd": position_update.fees_usd,
                    "current_r": current_r,
                },
            )
            
            logger.info(
                f"🔻 [TM-PARTIAL] {symbol}: reduced {frac*100:.0f}% at {current_r:.2f}R "
                f"(closed {reduce_amount:.6f}, remaining {new_size:.6f})"
            )
            await send_telegram_msg(
                f"🔻 **ЧАСТИЧНОЕ ЗАКРЫТИЕ**\n\n"
                f"🔹 Символ: `{symbol}`\n"
                f"📊 R-множитель: `{current_r:.2f}R`\n"
                f"📦 Закрыто: `{frac*100:.0f}%` ({reduce_amount:.6f})\n"
                f"📦 Осталось: `{new_size:.6f}`"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error in TM partial reduce for {symbol}: {e}")
            return False

    async def _move_to_breakeven(self, symbol: str, trade: dict):
        """Moves STOP LOSS to entry price to eliminate risk."""
        try:
            entry = trade.get("entry")
            if not entry: return
            
            be_price = entry
            await self._update_stop_order_only(symbol, trade, be_price)
            
            logger.info(f"✅ [PROTECT] {symbol} moved to BREAK EVEN at {be_price}")
            await send_telegram_msg(f"🛡 **BREAK EVEN**\n`{symbol}` стоп переставлен в безубыток ({be_price})")
        except Exception as e:
            logger.error(f"Error moving to BE for {symbol}: {e}")
            return False

    async def evaluate_position_management(
        self,
        symbol: str,
        current_price: float,
        atr: float,
        adx: "Optional[float]" = None,
        df_tf: "Optional[pd.DataFrame]" = None,
        cvd_val: float = 0.0,
    ) -> None:
        """
        Main trade management cycle: called every minute before unified stop update.
        Handles invalidation exit, TM time stop, BE arming, partial reduce, vol/CVD heuristics.
        ATR / confirmed / BE stop placement: `_compute_unified_desired_stop` + `_apply_unified_protective_stop`.
        """
        if not getattr(settings, "trade_mgmt_enabled", True):
            return
        if symbol not in self.active_trades:
            return

        trade = self.active_trades[symbol]
        entry = float(trade.get("entry", 0))
        initial_stop = float(trade.get("initial_stop", trade.get("stop", 0)))
        side = str(trade.get("signal_type", "")).upper()

        if entry <= 0 or initial_stop <= 0:
            return

        current_r = self.risk_manager.current_r_multiple(entry, initial_stop, current_price, side)
        trade["max_favorable_r"] = max(float(trade.get("max_favorable_r", 0)), current_r)
        trade["max_adverse_r"] = min(float(trade.get("max_adverse_r", 0)), current_r)

        trade["max_adverse_r"] = min(float(trade.get("max_adverse_r", 0)), current_r)



        # 1. Setup invalidation exit
        if TradeGuard.should_invalidation_exit(trade, current_price, df_tf, self.risk_manager):
            self._tm_record("invalidation_exit")
            if trade_mgmt_r_at_exit:
                trade_mgmt_r_at_exit.observe(current_r)
            if trade_mgmt_max_favorable_r:
                trade_mgmt_max_favorable_r.observe(float(trade.get("max_favorable_r", 0)))
            logger.info(
                f"⚠️ [TM-INVALIDATION] {symbol} {side}: setup invalidated at {current_r:.2f}R, closing"
            )
            await send_telegram_msg(
                f"⚠️ **ИНВАЛИДАЦИЯ СЕТАПА**\n\n"
                f"🔹 Символ: `{symbol}`\n"
                f"📊 R: `{current_r:.2f}` | Группа: `{trade.get('setup_group')}`\n"
                f"🔴 Позиция закрыта досрочно"
            )
            await self._close_position(symbol, reason="INVALIDATION")
            return

        # 2. Time stop
        tf = trade.get("timeframe", "1h")
        tf_sec = 3600
        if tf.endswith("m"): tf_sec = int(tf[:-1]) * 60
        elif tf.endswith("h"): tf_sec = int(tf[:-1]) * 3600
        elif tf.endswith("d"): tf_sec = int(tf[:-1]) * 86400
        opened = float(trade.get("opened_at", time.time()))
        trade["bars_since_entry"] = int((time.time() - opened) / tf_sec) if tf_sec > 0 else 0
        
        if TradeGuard.should_time_stop(trade, current_price):
            self._tm_record("time_stop")
            if trade_mgmt_r_at_exit:
                trade_mgmt_r_at_exit.observe(current_r)
            if trade_mgmt_max_favorable_r:
                trade_mgmt_max_favorable_r.observe(float(trade.get("max_favorable_r", 0)))
            logger.info(
                f"⏰ [TM-TIMESTOP] {symbol} {side}: bars={trade.get('bars_since_entry', 0)}, "
                f"R={current_r:.2f}, closing"
            )
            await send_telegram_msg(
                f"⏰ **ТАЙМСТОП**\n\n"
                f"🔹 Символ: `{symbol}`\n"
                f"📊 Баров: `{trade.get('bars_since_entry', 0)}` | R: `{current_r:.2f}`\n"
                f"🔴 Позиция закрыта по таймауту"
            )
            await self._close_position(symbol, reason="TIME_MGMT")
            return

        # 3. Break-even (arm flag, actual BE move happens in schedule_update_positions)
        if TradeGuard.should_arm_break_even(trade, current_r, adx):
            if not trade.get("be_armed"):
                trade["be_armed"] = True
                self._tm_record("be_move")
                logger.info(f"🔰 [TM-BE-ARM] {symbol} {side}: armed BE at {current_r:.2f}R")

        # 3.1 CVD-based BE (Advanced Phase 4)
        # If CVD shows strong reversal (delta > 0.7 against us) and we are in profit, protect at BE
        is_in_profit = (current_price > entry) if side == "LONG" else (current_price < entry)
        if not trade.get("be_armed") and is_in_profit:
            cvd_threshold = 0.7
            should_cvd_be = (side == "LONG" and cvd_val < -cvd_threshold) or (side == "SHORT" and cvd_val > cvd_threshold)
            if should_cvd_be:
                trade["be_armed"] = True
                self._tm_record("be_cvd")
                logger.info(f"🔰 [TM-BE-CVD] {symbol} {side}: armed BE due to CVD reversal ({cvd_val:.2f})")

        # 3.2 Volspike Exhaustion Exit (Advanced Phase 4)
        if df_tf is not None and not df_tf.empty and current_r >= 1.0:
            last_vol = float(df_tf.iloc[-1].get("volume", 0))
            avg_vol = float(df_tf["volume"].tail(20).mean())
            if avg_vol > 0 and last_vol > avg_vol * 5.0: # 5x volume spike
                logger.info(f"🚀 [TM-VOLSPIKE] {symbol} {side}: volume exhaustion {last_vol/avg_vol:.1f}x avg, closing at {current_r:.2f}R")
                await send_telegram_msg(f"🚀 **VOLSPIKE EXIT**\n`{symbol}`: Volume spike {last_vol/avg_vol:.1f}x -> profit locked")
                await self._close_position(symbol, reason="VOLSPIKE")
                return

        # 4. Partial reduction (Scaling Out)
        if TradeGuard.should_partial_reduce(trade, current_r):
            fraction = getattr(settings, "partial_fraction", 0.33)
            await self._partially_close_position(symbol, fraction=fraction, reason="PARTIAL_TP")
            
            # Автоматически переводим в безубыток после частичной фиксации для защиты прибыли
            if not trade.get("be_armed") and not trade.get("be_moved"):
                trade["be_armed"] = True
                logger.info(f"🔰 [TM-AUTO-BE] {symbol}: arming BE after partial reduction")

        # Confirmed trailing + ATR trail + BE: см. _compute_unified_desired_stop / schedule_update_positions

    async def _update_stop_order_only(self, symbol: str, trade: dict, new_price: float):
        """Cancels ONLY the current Stop-Loss and places a new one at new_price."""
        try:
            sl_id = trade.get("stop_order_id")
            clean_sym = SymbolNormalizer.to_binance(symbol)
            
            # 1. Cancel old SL if exists
            if sl_id:
                try:
                    await self._with_time_sync_retry(
                        lambda: self.exchange.request('algoOrder', 'fapiPrivate', 'DELETE', {'symbol': clean_sym, 'algoId': str(sl_id)}),
                        ctx=f"cancel_sl_only({symbol})"
                    )
                except Exception as e:
                    # If already canceled or hit, ignore
                    if "-4130" not in str(e):
                        logger.warning(f"Failed to cancel SL {sl_id} for {symbol}: {e}")

            # 2. Place new SL
            side = trade.get("signal_type")
            lot_size, _ = await self._get_live_position(symbol, preferred_side=side)
            if lot_size <= 0: return

            new_price_norm = await self._normalize_price(symbol, new_price)
            new_sl_id, _ = await self._set_protective_orders(
                symbol, side, lot_size, new_price_norm, tp=None, position_id=trade.get("position_db_id")
            )
            
            if new_sl_id:
                trade["stop_order_id"] = new_sl_id
                trade["stop"] = new_price_norm
                logger.debug(f"[TRAIL_UPDATE] {symbol} SL updated to {new_price_norm}")
        except Exception as e:
            logger.error(f"Error in _update_stop_order_only for {symbol}: {e}")

    async def _partially_close_position(self, symbol: str, fraction: float = 0.5, reason: str = "PARTIAL_TP"):
        """Частичное закрытие позиции (Scaling Out)"""
        if symbol not in self.active_trades: return
        trade = self.active_trades[symbol]
        
        try:
            live_size, live_side = await self._get_live_position(symbol, preferred_side=trade.get('signal_type'))
            base_amount = live_size if live_size > 1e-8 else trade['current_size']
            close_amount = base_amount * fraction
            
            if close_amount < 1e-8:
                logger.warning(f"⚠️ [PARTIAL] {symbol}: amount too small to close ({close_amount})")
                return

            side = 'sell' if trade['signal_type'] == "LONG" else 'buy'
            
            # Отправляем рыночный ордер на часть объема
            try:
                co = await self.exchange.create_order(symbol, 'market', side, close_amount, None, {"reduceOnly": True})
            except Exception as _ce:
                logger.error(f"❌ [PARTIAL] Error creating order for {symbol}: {_ce}")
                return

            exit_price = float(co.get("average") or co.get("price") or 0.0)
            if exit_price <= 0:
                ticker = await self.exchange.fetch_ticker(symbol)
                exit_price = float(ticker["last"])

            # Считаем реализованный профит по этой части
            entry = float(trade["entry"])
            is_long = trade["signal_type"] == "LONG"
            pnl_usd = (exit_price - entry) * close_amount if is_long else (entry - exit_price) * close_amount
            
            # Обновляем состояние сделки
            trade["realized_pnl"] = float((trade.get("realized_pnl") or 0.0) + pnl_usd)
            trade["current_size"] = float(trade["current_size"]) - close_amount
            trade["partial_done"] = True
            
            # Запись в БД
            async with async_session() as session:
                async with session.begin():
                    await self._db_persist_order(
                        position_id=trade.get("position_db_id"),
                        symbol=symbol,
                        exchange_order_id=str(co.get("id")),
                        order_type="PARTIAL_CLOSE",
                        position_side=str(trade["signal_type"]),
                        price=exit_price,
                        size=close_amount,
                        status=OrderStatus.FILLED,
                    )
            
            logger.info(f"💰 [TM-PARTIAL] {symbol} {trade['signal_type']}: closed {fraction*100:.0f}% at {exit_price}. PnL: ${pnl_usd:.2f}")
            await send_telegram_msg(
                f"💰 **ЧАСТИЧНАЯ ФИКСАЦИЯ (Scaling Out)**\n\n"
                f"🔹 Символ: `{symbol}`\n"
                f"📊 Фиксация: `{fraction*100:.0f}%` позиции\n"
                f"📈 Цена: `{exit_price}` | PnL: `{pnl_usd:+.2f} USDT`"
            )
            
        except Exception as e:
            logger.error(f"❌ [PARTIAL] Critical error for {symbol}: {e}")

    async def _close_position(self, symbol: str, reason: str = "AUTO"):
        if symbol not in self.active_trades: return
        trade = self.active_trades[symbol]
        pnl_usd = 0.0
        pnl_pct = 0.0
        exit_price_hint = 0.0
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
                    # -4164: если нотионал < $5 биржа требует reduceOnly=True
                    try:
                        co = await self.exchange.create_order(symbol, 'market', side, close_amount)
                    except Exception as _ce:
                        _ce_str = str(_ce)
                        if "-4164" in _ce_str or "notional must be no smaller" in _ce_str:
                            logger.warning(
                                f"⚠️ [CLOSE] {symbol}: notional too small, retrying with reduceOnly=True"
                            )
                            co = await self.exchange.create_order(
                                symbol, 'market', side, close_amount,
                                None, {"reduceOnly": True}
                            )
                        else:
                            raise
                    ps = str(trade.get("signal_type") or "LONG").upper()

                    exit_price_hint = float(co.get("average") or co.get("price") or 0.0)
            
            try:
                lev = int(getattr(settings, "leverage", 1) or 1)
            except Exception:
                lev = 1

            # Подготовка данных для PnL ДО начала транзакции
            exit_price = exit_price_hint
            if reason == "EXTERNAL":
                pnl_usd, pnl_pct = await self._realized_pnl_from_exchange_trades(symbol, trade)
                if abs(pnl_usd) < 1e-12:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        exit_price = float(ticker.get("last") or 0)
                        entry = float(trade["entry"])
                        size = float(trade["current_size"])
                        is_long = trade["signal_type"] == "LONG"
                        if entry > 0 and size > 0 and exit_price > 0:
                            pnl_usd = (exit_price - entry) * size if is_long else (entry - exit_price) * size
                            pnl_pct = ((exit_price / entry) - 1) * 100 if is_long else ((entry / exit_price) - 1) * 100
                    except Exception as e:
                        logger.warning(f"Ticker fetch failed for external close PnL: {e}")
            else:
                if exit_price <= 0:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        exit_price = float(ticker["last"])
                    except Exception as e:
                        logger.warning(f"Ticker fetch failed for close PnL: {e}")

                try:
                    position_update = PositionManager.close_position(
                        state=self._trade_position_state(trade),
                        exit_price=exit_price,
                        exit_fee_usd=self._extract_order_fee_usd(co, "market_close") if reason != "EXTERNAL" and close_amount > 1e-8 else 0.0,
                    )
                    pnl_usd = float(position_update.realized_pnl)
                    pnl_pct = PnLCalculator.calculate_realized_pnl(
                        side=str(trade["signal_type"]),
                        entry_price=float(trade["entry"]),
                        exit_price=exit_price,
                        qty=float(trade["current_size"]),
                        entry_fee_usd=float(trade.get("open_fees_usd", 0.0) or 0.0),
                        exit_fee_usd=self._extract_order_fee_usd(co, "market_close") if reason != "EXTERNAL" and close_amount > 1e-8 else 0.0,
                    ).pnl_pct
                except Exception as e:
                    logger.warning(f"PnL calculation error: {e}")

            final_realized = float((trade.get("realized_pnl") or 0.0) + pnl_usd)

            async with async_session() as session:
                async with session.begin():
                    # 1. Запись закрывающего ордера (если он был)
                    if reason != "EXTERNAL" and close_amount > 1e-8:
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
                            session=session,
                        )

                    # 2. Обновление статуса позиции
                    await session.execute(
                        update(PositionModel)
                        .where(PositionModel.id == trade["position_db_id"])
                        .values(
                            status=PositionStatus.CLOSED, 
                            closed_at=datetime.datetime.utcnow(),
                            realized_pnl=final_realized
                        )
                    )

                    # 3. Запись PnL
                    session.add(
                        PnLModel(
                            user_id=self.user_id,
                            symbol=symbol,
                            pnl_usd=pnl_usd,
                            pnl_pct=pnl_pct,
                            leverage=lev,
                            reason=reason,
                        )
                    )
                    
                    try:
                        bal, _, _ = await self.get_account_metrics()
                        await self.risk_manager.record_closed_pnl(pnl_usd, bal)
                    except Exception:
                        pass

            closed_size = float(trade.get("current_size", 0.0) or 0.0)
            trade["realized_pnl"] = float((trade.get("realized_pnl") or 0.0) + pnl_usd)
            trade["current_size"] = 0.0
            trade["position_is_open"] = False
            trade["open_fees_usd"] = 0.0
            await self._audit_event(
                event_type="position_closed",
                symbol=symbol,
                strategy=trade.get("strategy"),
                position_id=trade.get("position_db_id"),
                message=f"Position closed: {reason}",
                payload={
                    "reason": reason,
                    "pnl_usd": pnl_usd,
                    "pnl_pct": pnl_pct,
                    "entry_price": trade.get("entry"),
                    "size": closed_size,
                },
            )

            _pnl_msg = ""
            _reason_map = {
                "EXTERNAL": "🔄 Биржа (TP/SL)",
                "MANUAL": "🖐 Ручное",
                "TIME": "⏱ Тайм-аут",
                "TIME_MGMT": "⏰ Тайм-стоп (TM)",
                "INVALIDATION": "⚠️ Инвалидация сетапа",
                "AUTO": "⚙️ Авто",
                "RECONCILE_CLOSE": "🔃 Реконсил",
            }
            _reason_ru = _reason_map.get(reason, f"⚙️ {reason}")
            try:
                _pnl_emoji = "🟢" if pnl_usd >= 0 else "🔴"
                _pnl_msg = f"\n{_pnl_emoji} Результат: `{pnl_usd:+.2f} USDT ({pnl_pct:+.1f}%)`"
            except Exception:
                pass

            hold_time_str = ""
            try:
                opened = trade.get("opened_at", 0)
                if opened:
                    hold_secs = time.time() - opened
                    hold_mins = hold_secs / 60
                    if hold_mins >= 60:
                        hold_time_str = f"\n⏱ Время: {hold_mins / 60:.1f}ч"
                    else:
                        hold_time_str = f"\n⏱ Время: {hold_mins:.0f}мин"
            except Exception:
                pass

            max_fav_r = float(trade.get("max_favorable_r", 0))
            if trade_mgmt_max_favorable_r and max_fav_r != 0:
                trade_mgmt_max_favorable_r.observe(max_fav_r)
            logger.info(
                f"💰 [CLOSE] {symbol} {trade.get('signal_type')} | "
                f"reason={reason} PnL={pnl_usd:+.4f} USDT ({pnl_pct:+.2f}%) | "
                f"entry={trade.get('entry', 0):.6f} stop={trade.get('stop', 0):.6f} "
                f"initial_stop={trade.get('initial_stop', 'N/A')} "
                f"BE_moved={trade.get('be_moved', False)} "
                f"max_R={max_fav_r:.2f} bars={trade.get('bars_since_entry', 0)} "
                f"size={trade.get('current_size', 0)} "
                f"strategy={trade.get('strategy', 'unknown')}"
            )

            if trades_closed:
                trades_closed.labels(symbol=symbol, reason=reason).inc()
            if pnl_per_trade:
                pnl_per_trade.observe(pnl_usd)

            strategy_name = trade.get("strategy", "unknown")
            for cb in self._trade_close_callbacks:
                try:
                    if asyncio.iscoroutinefunction(cb):
                        await cb(strategy_name, pnl_usd)
                    else:
                        cb(strategy_name, pnl_usd)
                except Exception as cb_err:
                    logger.debug(f"Trade close callback error: {cb_err}")

            self._tm_partial_reduce_skip_log_ts.pop(symbol, None) # Устраняем утечку памяти (Этап 1)
            self.active_trades.pop(symbol, None)

            await send_telegram_msg(
                f"💰 **ПОЗИЦИЯ ЗАКРЫТА**\n\n"
                f"🔹 Символ: `{symbol}`\n"
                f"🏁 Причина: {_reason_ru}{_pnl_msg}{hold_time_str}"
            )
        except Exception as e: logger.error(f"Error closing {symbol}: {e}")

    async def manual_close(self, symbol: str) -> bool:
        """Публичный метод для ручного закрытия из Telegram. (Баг 4.1)"""
        symbol = SymbolNormalizer.normalize(symbol)
        logger.info(f"🚨 [MANUAL] Запрос на закрытие: {symbol}")
        if symbol in self.active_trades:
            await self._close_position(symbol, reason="MANUAL")
            return True
        return False

    async def manual_reduce(self, symbol: str, fraction: float) -> Dict[str, Any]:
        """Частичное ручное закрытие позиции (reduce-only market)."""
        symbol = SymbolNormalizer.normalize(symbol)
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
        if self._metrics_cache and (now - self._metrics_cache_ts) < settings.metrics_cache_ttl:
            return self._metrics_cache

        # API в деградации: не штурмим биржу из параллельных задач.
        if now < self._metrics_backoff_until and self._metrics_cache:
            return self._metrics_cache

        # Single-flight: только один concurrent запрос метрик наружу.
        async with self._metrics_lock:
            now = time.time()
            if self._metrics_cache and (now - self._metrics_cache_ts) < settings.metrics_cache_ttl:
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

        async with self._trades_lock:
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
                        async with session.begin():
                            for sym in stale_symbols:
                                await session.execute(
                                    update(PositionModel)
                                    .where(
                                        PositionModel.symbol == sym,
                                        PositionModel.status == PositionStatus.OPEN
                                    )
                                    .values(status=PositionStatus.CLOSED, closed_at=datetime.datetime.utcnow())
                                )
                except Exception as e:
                    logger.warning(f"⚠️ [SOFT_CLEANUP] DB close sync failed: {e}")
