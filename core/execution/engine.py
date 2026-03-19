import asyncio
import time
import datetime
from utils.logger import get_execution_logger
from core.risk.risk_manager import RiskManager, TimeExitSystem, PyramidingSystem
from utils.notifier import send_telegram_msg
from database.session import async_session
from config.settings import settings

logger = get_execution_logger()

class ExecutionEngine:
    def __init__(self, exchange_client, risk_manager: RiskManager):
        self.exchange = exchange_client
        self.risk_manager = risk_manager
        self.pyramiding = PyramidingSystem()
        self.time_exit = TimeExitSystem()
        self.active_trades = {}
        self._symbol_locks: dict[str, asyncio.Lock] = {}
        self._position_update_tasks: dict[str, asyncio.Task] = {}

    def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        lock = self._symbol_locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self._symbol_locks[symbol] = lock
        return lock

    async def _normalize_amount(self, symbol: str, amount: float) -> float:
        """
        Приводим количество к биржевым ограничениям (precision/min amount).
        CCXT сам умеет amount_to_precision, но рынки должны быть загружены.
        """
        if amount <= 0:
            return 0.0

        try:
            if not getattr(self.exchange, "markets", None):
                await self.exchange.load_markets()
            amt_str = self.exchange.amount_to_precision(symbol, amount)
            amt = float(amt_str)
        except Exception:
            amt = float(amount)

        # Best-effort min amount check
        try:
            market = self.exchange.market(symbol)
            limits = (market or {}).get("limits") or {}
            min_amt = ((limits.get("amount") or {}).get("min")) or 0.0
            if min_amt and amt < float(min_amt):
                return 0.0
        except Exception:
            pass

        return amt

    async def _normalize_price(self, symbol: str, price: float) -> float:
        if price <= 0:
            return 0.0
        try:
            if not getattr(self.exchange, "markets", None):
                await self.exchange.load_markets()
            px_str = self.exchange.price_to_precision(symbol, price)
            return float(px_str)
        except Exception:
            return float(price)

    async def _set_leverage_best_effort(self, symbol: str, leverage: int):
        if not leverage:
            return
        try:
            # Not all exchanges / ccxt versions support this universally
            if hasattr(self.exchange, "set_leverage"):
                await self.exchange.set_leverage(int(leverage), symbol)
        except Exception as e:
            logger.error(f"Не удалось установить leverage={leverage} для {symbol}: {e}")

    async def _place_protective_orders(self, symbol: str, side: str, size: float, stop_price: float, take_profit: float | None):
        """
        Ставим reduce-only SL/TP на бирже.
        Для Binance Futures через CCXT обычно используются STOP_MARKET / TAKE_PROFIT_MARKET + params.stopPrice.
        """
        protective: dict[str, str] = {}
        try:
            reduce_side = "sell" if side == "buy" else "buy"
            stop_price = await self._normalize_price(symbol, stop_price)
            if take_profit:
                take_profit = await self._normalize_price(symbol, take_profit)

            sl_order = await self.exchange.create_order(
                symbol=symbol,
                type="STOP_MARKET",
                side=reduce_side,
                amount=size,
                params={
                    "stopPrice": stop_price,
                    "reduceOnly": True,
                    "workingType": "MARK_PRICE",
                },
            )
            if sl_order and sl_order.get("id"):
                protective["stop_order_id"] = sl_order["id"]

            if take_profit:
                tp_order = await self.exchange.create_order(
                    symbol=symbol,
                    type="TAKE_PROFIT_MARKET",
                    side=reduce_side,
                    amount=size,
                    params={
                        "stopPrice": take_profit,
                        "reduceOnly": True,
                        "workingType": "MARK_PRICE",
                    },
                )
                if tp_order and tp_order.get("id"):
                    protective["tp_order_id"] = tp_order["id"]
        except Exception as e:
            logger.error(f"Ошибка выставления SL/TP на бирже для {symbol}: {e}")
            # Не падаем: internal stop всё равно отработает (хуже, но лучше чем crash)
        return protective

    @staticmethod
    def _make_client_order_id(position_id: int | None, kind: str, signal_id: int | None = None) -> str:
        """
        Детерминированный clientOrderId для строгого связывания ордеров с позицией.
        Binance имеет ограничения по длине (держим <= 32).
        """
        if position_id:
            return f"qt-p{position_id}-{kind}"[:32]
        if signal_id:
            return f"qt-s{signal_id}-{kind}"[:32]
        return f"qt-{kind}"[:32]

    async def _create_order_with_client_id(
        self,
        *,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: float | None = None,
        client_order_id: str | None = None,
        params: dict | None = None,
    ):
        """
        Wrapper над CCXT create_order с пробросом clientOrderId.
        Для Binance Futures обычно нужен newClientOrderId.
        """
        p = dict(params or {})
        if client_order_id:
            p.setdefault("newClientOrderId", client_order_id)
            p.setdefault("clientOrderId", client_order_id)
        
        # Для совместимости с Algo API тестнета используем чистый ID монеты (напр. ETHUSDT)
        try:
            market = self.exchange.market(symbol)
            clean_symbol = market['id']
        except Exception:
            # Fallback к ручной очистке
            clean_symbol = symbol.replace("/", "").split(":")[0]
            
        return await self.exchange.create_order(symbol=clean_symbol, type=type, side=side, amount=amount, price=price, params=p)

    async def _cancel_order_safe(self, symbol: str, order_id: str | None):
        if not order_id:
            return
        try:
            await self.exchange.cancel_order(order_id, symbol)
            # Best-effort sync to DB
            try:
                from sqlalchemy import update
                from database.models.all_models import Order as OrderModel, OrderStatus
                async with async_session() as session:
                    await session.execute(
                        update(OrderModel)
                        .where(OrderModel.exchange_order_id == str(order_id))
                        .values(status=OrderStatus.CANCELED)
                    )
                    await session.commit()
            except Exception:
                pass
        except Exception:
            return

    async def reconcile_from_exchange(self):
        """
        Восстановление `active_trades` из фактических позиций биржи.
        Это критично после рестарта: in-memory состояние теряется, а позиции остаются.
        """
        try:
            if not getattr(self.exchange, "markets", None):
                await self.exchange.load_markets()
            positions = await self.exchange.fetch_positions()
        except Exception as e:
            logger.error(f"Ошибка reconcile: не удалось получить позиции: {e}")
            return

        restored = 0
        for p in positions or []:
            try:
                contracts = float(p.get("contracts", 0) or 0)
                if abs(contracts) <= 0:
                    continue

                symbol = p.get("symbol")
                if not symbol:
                    continue

                entry = float(p.get("entryPrice") or p.get("entry_price") or 0.0)
                if entry <= 0:
                    continue

                side = "LONG" if contracts > 0 else "SHORT"

                # Пробуем восстановить stop из биржи, но unified fields не всегда есть.
                # В любом случае internal stop будет рассчитан заново при первом ATR update.
                self.active_trades[symbol] = {
                    "entry": entry,
                    "stop": entry * (0.99 if side == "LONG" else 1.01),
                    "stage": 0,
                    "opened_at": time.time(),
                    "signal_type": side,
                    "total_planned_size": abs(contracts),
                    "current_size": abs(contracts),
                    "reconciled": True,
                }

                # Best-effort sync to DB: ensure open Position row exists
                try:
                    from sqlalchemy import select
                    from database.models.all_models import Position as PositionModel, PositionStatus
                    async with async_session() as session:
                        q = (
                            select(PositionModel)
                            .where(PositionModel.symbol == symbol)
                            .where(PositionModel.status == PositionStatus.OPEN)
                            .limit(1)
                        )
                        r = await session.execute(q)
                        existing = r.scalar_one_or_none()
                        if not existing:
                            side = "LONG" if contracts > 0 else "SHORT"
                            pos = PositionModel(
                                user_id=1,
                                signal_id=None,
                                symbol=symbol,
                                side=SignalType.LONG if side == "LONG" else SignalType.SHORT,
                                entry_price=entry,
                                size=abs(contracts),
                                stop_loss=None,
                                take_profit=None,
                                status=PositionStatus.OPEN,
                                opened_at=datetime.datetime.utcnow(),
                            )
                            session.add(pos)
                            await session.commit()
                            self.active_trades[symbol]["position_db_id"] = pos.id
                        else:
                            self.active_trades[symbol]["position_db_id"] = existing.id
                except Exception:
                    pass

                restored += 1
            except Exception:
                continue

        if restored:
            logger.info(f"Reconcile: восстановлено позиций: {restored}")

    async def reconcile_full(self):
        """
        Полный контур учета:
        - Биржа -> БД: позиции (открыты/закрыты), PnL best-effort
        - Биржа -> БД: статусы ордеров
        - БД -> memory: восстановление active_trades + привязка SL/TP order ids
        """
        try:
            if not getattr(self.exchange, "markets", None):
                await self.exchange.load_markets()
        except Exception:
            pass

        # 1) Ордеры: биржа -> БД (истина = биржа)
        try:
            from sqlalchemy import select, update
            from sqlalchemy.exc import IntegrityError
            from database.models.all_models import Order as OrderModel, OrderStatus

            try:
                ex_open = await self.exchange.fetch_open_orders()
            except Exception:
                ex_open = []
            ex_open_by_id = {str(o.get("id")): o for o in (ex_open or []) if o.get("id")}

            async with async_session() as session:
                q = select(OrderModel).where(OrderModel.status.in_([OrderStatus.PENDING, OrderStatus.OPEN]))
                r = await session.execute(q)
                open_orders = r.scalars().all()

            def _extract_client_id(exo: dict) -> str | None:
                if not exo:
                    return None
                cid = exo.get("clientOrderId")
                if cid:
                    return str(cid)
                info = exo.get("info") or {}
                for k in ("clientOrderId", "newClientOrderId"):
                    if info.get(k):
                        return str(info.get(k))
                return None

            def _infer_order_type(exo: dict) -> str:
                t = (exo.get("type") or "").lower()
                # ccxt unified order type might be 'stop_market'/'take_profit_market'/'market'
                if "stop" in t:
                    return "stop_market"
                if "take" in t:
                    return "take_profit_market"
                if t == "market":
                    return "market"
                return t or "unknown"

            def _map_status(ex_status: str | None) -> OrderStatus | None:
                if ex_status in ("open",):
                    return OrderStatus.OPEN
                if ex_status in ("closed",):
                    return OrderStatus.FILLED
                if ex_status in ("canceled", "cancelled"):
                    return OrderStatus.CANCELED
                if ex_status in ("rejected", "expired"):
                    return OrderStatus.REJECTED
                return None

            # 1a) Upsert open orders from exchange into DB (strictly)
            # This ensures we don't miss orders created outside our process.
            try:
                from database.models.all_models import SignalType
                for exo in (ex_open or []):
                    try:
                        ex_id = exo.get("id")
                        symbol = exo.get("symbol")
                        if not ex_id or not symbol:
                            continue
                        cid = _extract_client_id(exo)
                        status = _map_status(exo.get("status") or "open") or OrderStatus.OPEN

                        # Try to derive position_id from cid pattern qt-p{position_id}-...
                        position_id = None
                        if cid and cid.startswith("qt-p"):
                            try:
                                # qt-p123-sl
                                pos_part = cid.split("-", 2)[0]  # qt-p123
                                position_id = int(pos_part.replace("qt-p", ""))
                            except Exception:
                                position_id = None

                        side_enum = SignalType.LONG
                        try:
                            s = (exo.get("side") or "").lower()
                            # For our model: LONG/SHORT represents position direction, not order buy/sell.
                            # Best-effort: keep LONG; actual linkage comes via position_id anyway.
                            if s:
                                side_enum = SignalType.LONG
                        except Exception:
                            pass

                        price = exo.get("price")
                        if price is None:
                            # conditional orders often have stopPrice in info
                            price = (exo.get("stopPrice") or (exo.get("info") or {}).get("stopPrice"))
                        amount = exo.get("amount") or 0.0

                        async with async_session() as session:
                            row = OrderModel(
                                user_id=1,
                                exchange_order_id=str(ex_id),
                                client_order_id=str(cid) if cid else None,
                                position_id=position_id,
                                symbol=symbol,
                                order_type=_infer_order_type(exo),
                                side=side_enum,
                                price=float(price) if price is not None else None,
                                size=float(amount),
                                status=status,
                            )
                            session.add(row)
                            try:
                                await session.commit()
                            except IntegrityError:
                                await session.rollback()
                                # Update existing row by exchange_order_id OR client_order_id
                                stmt = (
                                    update(OrderModel)
                                    .where(
                                        (OrderModel.exchange_order_id == str(ex_id))
                                        | ((OrderModel.client_order_id == str(cid)) if cid else False)
                                    )
                                    .values(
                                        status=status,
                                        symbol=symbol,
                                        position_id=position_id,
                                        price=float(price) if price is not None else None,
                                        size=float(amount),
                                    )
                                )
                                await session.execute(stmt)
                                await session.commit()
                    except Exception:
                        continue
            except Exception as e:
                logger.error(f"Ошибка reconcile_full: upsert open orders: {e}")

            # 1b) Update statuses for DB open orders using exchange open map / fetch_order fallback
            for o in open_orders:
                try:
                    if not o.exchange_order_id:
                        continue
                    exo = ex_open_by_id.get(str(o.exchange_order_id))
                    if exo is not None:
                        ex_status = (exo or {}).get("status") or "open"
                    else:
                        ex_order = await self.exchange.fetch_order(str(o.exchange_order_id), o.symbol)
                        ex_status = (ex_order or {}).get("status")
                    new_status = _map_status(ex_status)

                    if new_status and new_status != o.status:
                        async with async_session() as session:
                            await session.execute(
                                update(OrderModel)
                                .where(OrderModel.id == o.id)
                                .values(status=new_status)
                            )
                            await session.commit()
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"Ошибка reconcile_full: обновление ордеров: {e}")

        # 2) Синхронизация позиций: биржа <-> БД
        exchange_positions: dict[str, dict] = {}
        try:
            pos = await self.exchange.fetch_positions()
            for p in pos or []:
                try:
                    contracts = float(p.get("contracts", 0) or 0)
                    symbol = p.get("symbol")
                    if not symbol:
                        continue
                    if abs(contracts) <= 0:
                        continue
                    exchange_positions[symbol] = {
                        "contracts": contracts,
                        "entry": float(p.get("entryPrice") or p.get("entry_price") or 0.0),
                    }
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"Ошибка reconcile_full: fetch_positions: {e}")

        # 2a) Fetch ALL open orders to find orphans (SL/TP)
        ex_open_all = []
        try:
            ex_open_all = await self.exchange.fetch_open_orders()
        except Exception as e:
            logger.error(f"Ошибка fetch_open_orders при синхронизации: {e}")

        # Map symbol -> [Orders]
        ex_orders_by_symbol = {}
        for o in (ex_open_all or []):
            sym = o.get("symbol")
            if sym not in ex_orders_by_symbol:
                ex_orders_by_symbol[sym] = []
            ex_orders_by_symbol[sym].append(o)

        try:
            from sqlalchemy import select, update
            from database.models.all_models import Position as PositionModel, PositionStatus
            async with async_session() as session:
                q = select(PositionModel).where(PositionModel.status == PositionStatus.OPEN)
                r = await session.execute(q)
                db_open_positions = r.scalars().all()

            # Приводим все символы из БД к чистому виду (без :USDT) для маппинга
            db_open_by_symbol = {p.symbol.split(":")[0]: p for p in db_open_positions}

            # Закрываем в БД те, которых уже нет на бирже
            for sym, dbp in db_open_by_symbol.items():
                if sym not in exchange_positions:
                    # Если на бирже позиции нет, пробуем отменить open reduce-only ордера (best-effort)
                    try:
                        from database.models.all_models import Order as OrderModel, OrderStatus
                        async with async_session() as session:
                            qord = (
                                select(OrderModel)
                                .where(OrderModel.symbol == sym)
                                .where(OrderModel.status == OrderStatus.OPEN)
                            )
                            rord = await session.execute(qord)
                            for o in rord.scalars().all():
                                if o.exchange_order_id:
                                    await self._cancel_order_safe(sym, str(o.exchange_order_id))
                    except Exception:
                        pass

                    # best-effort: попытка оценить realized pnl по истории сделок после открытия
                    realized = None
                    try:
                        since_ms = int(dbp.opened_at.timestamp() * 1000) if dbp.opened_at else None
                        my_trades = await self.exchange.fetch_my_trades(sym, since=since_ms, limit=50) if since_ms else await self.exchange.fetch_my_trades(sym, limit=50)
                        pnl_sum = 0.0
                        for t in my_trades or []:
                            pnl_sum += float(t.get("info", {}).get("realizedPnl", 0.0) or 0.0)
                        realized = pnl_sum
                    except Exception:
                        realized = None

                    async with async_session() as session:
                        await session.execute(
                            update(PositionModel)
                            .where(PositionModel.id == dbp.id)
                            .values(
                                status=PositionStatus.CLOSED,
                                closed_at=datetime.datetime.utcnow(),
                                realized_pnl=float(realized) if realized is not None else dbp.realized_pnl,
                            )
                        )
                        await session.commit()

            # 2c) Обновляем существующие в БД позиции, если там NULL в SL/TP (best-effort)
            for sym, dbp in db_open_by_symbol.items():
                if sym in exchange_positions:
                    found_sl = None
                    found_tp = None
                    for o in ex_orders_by_symbol.get(sym, []):
                        ot = (o.get("info", {}).get("type") or o.get("type", "")).upper()
                        if "STOP" in ot: found_sl = o.get("stopPrice") or o.get("price")
                        if "TAKE" in ot: found_tp = o.get("stopPrice") or o.get("price")
                    
                    if (found_sl or found_tp) and (dbp.stop_loss is None or dbp.take_profit is None):
                        async with async_session() as session:
                             await session.execute(
                                 update(PositionModel)
                                 .where(PositionModel.id == dbp.id)
                                 .values(
                                     stop_loss=float(found_sl) if (found_sl and dbp.stop_loss is None) else dbp.stop_loss,
                                     take_profit=float(found_tp) if (found_tp and dbp.take_profit is None) else dbp.take_profit,
                                 )
                             )
                             await session.commit()
                    
                    # 2d) ЕСЛИ СТОПОВ ВСЕ ЕЩЕ НЕТ (ни на бирже, ни в БД) - ВЫСТАВЛЯЕМ ДЕФОЛТНЫЙ
                    if not found_sl and not dbp.stop_loss:
                        try:
                            side = "sell" if (exchange_positions.get(sym, {}).get("contracts", 0) > 0) else "buy"
                            entry = float(exchange_positions.get(sym, {}).get("entry", 0))
                            # Дефолтный стоп 1.5% от входа
                            sp = entry * 0.985 if side == "sell" else entry * 1.015
                            sp_norm = await self._normalize_price(sym, sp)
                            amount = float(abs(exchange_positions.get(sym, {}).get("contracts", 0)))
                            
                            logger.info(f"🛡 [AUTORESCUE] Выставляю дефолтный SL для {sym} (1.5%): {sp_norm}")
                            
                            # Переходим на самый защищенный тип: STOP (Limit Stop)
                            # Это обходит блокировку "Algo Order API"
                            try:
                                clean_symbol = sym.replace("/", "").split(":")[0]
                                limit_price = sp_norm * 0.999 if side == "sell" else sp_norm * 1.001
                                limit_price = await self._normalize_price(sym, limit_price)
                                amount = float(abs(exchange_positions.get(sym, {}).get("contracts", 0)))
                                
                                raw_params = {
                                    "symbol": clean_symbol,
                                    "side": side.upper(),
                                    "type": "STOP",
                                    "quantity": str(amount),
                                    "price": str(limit_price),
                                    "stopPrice": str(sp_norm),
                                    "timeInForce": "GTC",
                                    "reduceOnly": "true",
                                    "workingType": "MARK_PRICE"
                                }
                                await self.exchange.fapiPrivatePostOrder(raw_params)
                                # Сразу пишем в БД
                                async with async_session() as session:
                                    await session.execute(update(PositionModel).where(PositionModel.id == dbp.id).values(stop_loss=float(sp_norm)))
                                    await session.commit()
                            except Exception as raw_e:
                                logger.error(f"❌ FINAL RAW API Error для {sym}: {raw_e}")
                        except Exception as e:
                            logger.error(f"❌ Ошибка AUTORESCUE SL для {sym}: {e}")

            # Создаем в БД позиции, которые есть на бирже, но нет в БД
            for raw_sym, ep in exchange_positions.items():
                sym = raw_sym.split(":")[0]
                if sym in db_open_by_symbol:
                    continue
                async with async_session() as session:
                    # Попытка найти SL/TP среди ордеров на бирже для этого символа
                    found_sl = None
                    found_tp = None
                    # Ищем и по сырому, и по чистому (на всякий случай)
                    for o in ex_orders_by_symbol.get(sym, []) + ex_orders_by_symbol.get(raw_sym, []):
                        ot = (o.get("info", {}).get("type") or o.get("type", "")).upper()
                        if "STOP" in ot: found_sl = o.get("stopPrice") or o.get("price")
                        if "TAKE" in ot: found_tp = o.get("stopPrice") or o.get("price")

                    pos_row = PositionModel(
                        user_id=1,
                        signal_id=None,
                        symbol=sym, # Пишем в базу ЧИСТЫЙ символ
                        entry_price=float(ep.get("entry") or 0.0),
                        size=float(abs(ep.get("contracts") or 0.0)),
                        stop_loss=float(found_sl) if found_sl else None,
                        take_profit=float(found_tp) if found_tp else None,
                        status=PositionStatus.OPEN,
                        opened_at=datetime.datetime.utcnow(),
                    )
                    session.add(pos_row)
                    await session.commit()
        except Exception as e:
            logger.error(f"Ошибка reconcile_full: синхронизация позиций: {e}")

        # 3) Восстанавливаем active_trades строго из БД:
        # Position OPEN + связанные Order OPEN по position_id (SL/TP)
        try:
            from sqlalchemy import select
            from database.models.all_models import Position as PositionModel, PositionStatus, Order as OrderModel, OrderStatus

            async with async_session() as session:
                qpos = select(PositionModel).where(PositionModel.status == PositionStatus.OPEN)
                rpos = await session.execute(qpos)
                open_pos = rpos.scalars().all()

            new_active: dict[str, dict] = {}
            for p in open_pos:
                try:
                    symbol = p.symbol
                    ex = exchange_positions.get(symbol)
                    side = (p.side.value if getattr(p, "side", None) is not None else None) or ("LONG" if (ex and float(ex.get("contracts", 0)) > 0) else "SHORT")
                    size = float(abs(ex.get("contracts"))) if ex and ex.get("contracts") is not None else float(p.size)

                    # SL/TP ордера — строго по position_id
                    stop_id = None
                    tp_id = None
                    try:
                        async with async_session() as session:
                            qord = (
                                select(OrderModel)
                                .where(OrderModel.status == OrderStatus.OPEN)
                                .where(OrderModel.position_id == p.id)
                                .where(OrderModel.order_type.in_(["stop_market", "take_profit_market"]))
                            )
                            rord = await session.execute(qord)
                            for o in rord.scalars().all():
                                if o.order_type == "stop_market" and not stop_id:
                                    stop_id = o.exchange_order_id
                                elif o.order_type == "take_profit_market" and not tp_id:
                                    tp_id = o.exchange_order_id
                    except Exception:
                        pass

                    new_active[symbol] = {
                        "entry": float(p.entry_price),
                        "stop": float(p.stop_loss) if p.stop_loss else float(p.entry_price) * 0.99,
                        "stage": 0,
                        "opened_at": time.time(),
                        "signal_type": side,
                        "total_planned_size": size,
                        "current_size": size,
                        "position_db_id": p.id,
                        "stop_order_id": stop_id,
                        "tp_order_id": tp_id,
                        "timeframe": "1h", # Fallback for reconciled positions
                        "reconciled": True,
                    }
                except Exception:
                    continue

            self.active_trades = new_active
        except Exception as e:
            logger.error(f"Ошибка reconcile_full: восстановление active_trades: {e}")

    async def get_account_metrics(self):
        try:
            balance_data = await self.exchange.fetch_balance()
            total_balance = balance_data.get('USDT', {}).get('total', 0.0)
            free_balance = balance_data.get('USDT', {}).get('free', total_balance)
            positions = await self.exchange.fetch_positions()
            open_positions = [p for p in positions if abs(float(p.get('contracts', 0) or 0)) > 0]
            open_positions_count = len(open_positions)
            total_pnl = sum([float(p.get('unrealizedPnl', 0)) for p in positions])
            drawdown = 0.0
            if total_balance > 0:
                drawdown = abs(min(0, total_pnl)) / total_balance
            return free_balance, drawdown, open_positions_count
        except Exception as e:
            logger.error(f"Ошибка получения метрик аккаунта: {e}")
            return 10000.0, 0.0, 0
            
    async def execute_signal(self, signal_data: dict, account_balance: float, current_drawdown: float, open_trades_count: int):
        symbol = signal_data['symbol']
        direction = signal_data['signal']
        entry_price = signal_data['entry_price']

        if not settings.is_trading_enabled:
            await self._update_signal_status(signal_data.get("id"), "REJECTED")
            return

        async with self._get_symbol_lock(symbol):
            if symbol in self.active_trades:
                await self._check_pyramiding(symbol, entry_price)
                return

            # Idempotency: переводим PENDING -> EXECUTING атомарно. Если уже обработан — выходим.
            signal_id = signal_data.get("id")
            if signal_id:
                try:
                    from sqlalchemy import update
                    from database.models.all_models import Signal as SignalModel
                    async with async_session() as session:
                        stmt = (
                            update(SignalModel)
                            .where(SignalModel.id == signal_id)
                            .where(SignalModel.status == "PENDING")
                            .values(status="EXECUTING")
                        )
                        res = await session.execute(stmt)
                        await session.commit()
                        if getattr(res, "rowcount", 0) == 0:
                            return
                except Exception as e:
                    logger.error(f"Ошибка идемпотентности сигнала {signal_id}: {e}")

            if not self.risk_manager.check_trade_allowed(open_trades_count, current_drawdown):
                logger.warning(f"🛑 Риск-менеджер отклонил вход в {symbol}. Причина: Лимит позиций или просадка.")
                await self._update_signal_status(signal_id, "REJECTED")
                return

            atr_val = signal_data.get('atr', 100.0)
            stop_price = self.risk_manager.calculate_atr_stop(entry_price, atr_val, direction)
            total_size_risk = self.risk_manager.calculate_position_size(account_balance, entry_price, stop_price)

            # Cap by available margin * leverage (best-effort). For USDT-m futures:
            # max_notional ≈ free_usdt * leverage * 0.95
            lev = int(settings.leverage or 1)
            max_notional_usdt = float(account_balance) * float(lev) * 0.95
            total_size_cap = max_notional_usdt / float(entry_price) if entry_price > 0 else 0.0
            total_size = min(total_size_risk, total_size_cap) if total_size_cap > 0 else total_size_risk

            if total_size <= 0:
                logger.warning(f"⚠️ Нулевой объем позиции для {symbol}. Проверьте баланс и уровень стоп-лосса.")
                await self._update_signal_status(signal_id, "EXPIRED")
                return

            initial_size = self.pyramiding.get_allocation_amount(total_size, 0)
            initial_size = await self._normalize_amount(symbol, initial_size)
            if initial_size <= 0:
                logger.warning(f"⚠️ Объем {symbol} меньше minQty после округления/лимитов.")
                await self._update_signal_status(signal_id, "REJECTED")
                return

            try:
                side = 'buy' if direction == 'LONG' else 'sell'
                await self._set_leverage_best_effort(symbol, int(settings.leverage))
                entry_cid = self._make_client_order_id(None, "entry", signal_id=signal_id)
                order = await self._create_order_with_client_id(
                    symbol=symbol,
                    type='market',
                    side=side,
                    amount=initial_size,
                    client_order_id=entry_cid,
                )

                # На всякий: если биржа вернула среднюю цену/филлы, используем её как entry
                try:
                    entry_exec = float(order.get("average") or order.get("price") or entry_price)
                except Exception:
                    entry_exec = entry_price

                tp_price = signal_data.get("take_profit")
                try:
                    tp_price = float(tp_price) if tp_price is not None else None
                except Exception:
                    tp_price = None

                # Persist: Order + Position in DB (best-effort, but should not block trading)
                position_db_id = None
                try:
                    from database.models.all_models import Order as OrderModel, OrderStatus, SignalType, Position as PositionModel, PositionStatus
                    async with async_session() as session:
                        # position row first -> gives position_id for strict clientOrderId linkage
                        pos_row = PositionModel(
                            user_id=1,
                            signal_id=signal_id,
                            symbol=symbol,
                            side=SignalType.LONG if direction == "LONG" else SignalType.SHORT,
                            entry_price=float(entry_exec),
                            size=float(initial_size),
                            stop_loss=float(stop_price),
                            take_profit=float(tp_price) if tp_price else None,
                            status=PositionStatus.OPEN,
                            opened_at=datetime.datetime.utcnow(),
                        )
                        session.add(pos_row)
                        await session.flush()
                        position_db_id = pos_row.id

                        # entry order row (linked)
                        entry_order_row = OrderModel(
                            user_id=1,
                            exchange_order_id=order.get("id"),
                            client_order_id=entry_cid,
                            position_id=position_db_id,
                            symbol=symbol,
                            order_type="market",
                            side=SignalType.LONG if direction == "LONG" else SignalType.SHORT,
                            price=float(entry_exec),
                            size=float(initial_size),
                            status=OrderStatus.FILLED,
                        )
                        session.add(entry_order_row)
                        await session.commit()
                except Exception as e:
                    logger.error(f"Ошибка записи Order/Position в БД для {symbol}: {e}")

                # Place protective orders using position_id-based clientOrderId (strict linkage)
                protective: dict[str, str] = {}
                sl_cid = self._make_client_order_id(position_db_id, "sl", signal_id=signal_id)
                tp_cid = self._make_client_order_id(position_db_id, "tp", signal_id=signal_id)
                reduce_side = "sell" if side == "buy" else "buy"

                try:
                    sp = await self._normalize_price(symbol, stop_price)
                    sl_order = await self._create_order_with_client_id(
                        symbol=symbol,
                        type="STOP",
                        side=reduce_side,
                        amount=initial_size,
                        price=await self._normalize_price(symbol, sp * 0.999 if side == "sell" else sp * 1.001),
                        client_order_id=sl_cid,
                        params={"stopPrice": sp, "reduceOnly": True, "workingType": "MARK_PRICE"},
                    )
                    if sl_order and sl_order.get("id"):
                        protective["stop_order_id"] = sl_order["id"]
                except Exception as e:
                    logger.error(f"Ошибка выставления SL (strict) для {symbol}: {e}")

                if tp_price:
                    try:
                        tp_norm = await self._normalize_price(symbol, float(tp_price))
                        tp_order = await self._create_order_with_client_id(
                            symbol=symbol,
                            type="TAKE_PROFIT",
                            side=reduce_side,
                            amount=initial_size,
                            price=tp_norm,
                            client_order_id=tp_cid,
                            params={"stopPrice": tp_norm, "reduceOnly": True, "workingType": "MARK_PRICE"},
                        )
                        if tp_order and tp_order.get("id"):
                            protective["tp_order_id"] = tp_order["id"]
                    except Exception as e:
                        logger.error(f"Ошибка выставления TP (strict) для {symbol}: {e}")

                # Persist protective orders with strict linkage (best-effort)
                try:
                    from database.models.all_models import Order as OrderModel, OrderStatus, SignalType
                    async with async_session() as session:
                        if protective.get("stop_order_id"):
                            session.add(
                                OrderModel(
                                    user_id=1,
                                    exchange_order_id=protective.get("stop_order_id"),
                                    client_order_id=sl_cid,
                                    position_id=position_db_id,
                                    symbol=symbol,
                                    order_type="stop_market",
                                    side=SignalType.LONG if direction == "LONG" else SignalType.SHORT,
                                    price=float(stop_price),
                                    size=float(initial_size),
                                    status=OrderStatus.OPEN,
                                )
                            )
                        if protective.get("tp_order_id"):
                            session.add(
                                OrderModel(
                                    user_id=1,
                                    exchange_order_id=protective.get("tp_order_id"),
                                    client_order_id=tp_cid,
                                    position_id=position_db_id,
                                    symbol=symbol,
                                    order_type="take_profit_market",
                                    side=SignalType.LONG if direction == "LONG" else SignalType.SHORT,
                                    price=float(tp_price) if tp_price else None,
                                    size=float(initial_size),
                                    status=OrderStatus.OPEN,
                                )
                            )
                        await session.commit()
                except Exception as e:
                    logger.error(f"Ошибка записи protective orders в БД для {symbol}: {e}")

                self.active_trades[symbol] = {
                    "entry": entry_exec,
                    "stop": stop_price,
                    "stage": 0,
                    "opened_at": time.time(),
                    "signal_type": direction,
                    "total_planned_size": total_size,
                    "current_size": initial_size,
                    "signal_id": signal_id,
                    "position_db_id": position_db_id,
                    "timeframe": signal_data.get("timeframe", "1h"),
                    **protective,
                }

                # Обновляем статус в БД
                async with async_session() as session:
                    from sqlalchemy import update
                    from database.models.all_models import Signal as SignalModel
                    stmt = update(SignalModel).where(SignalModel.id == signal_id).values(status="EXECUTED")
                    await session.execute(stmt)
                    await session.commit()

                await send_telegram_msg(
                    f"✅ **ВХОД В ПОЗИЦИЮ**\n\n"
                    f"🔸 Символ: {symbol}\n"
                    f"🔸 Направление: {direction}\n"
                    f"💰 Цена: {entry_exec:.4f}\n"
                    f"🛡 Стоп: {stop_price:.4f}\n"
                    f"📊 Объем: {initial_size}\n"
                    f"🧷 Биржевой SL: {'✅' if protective.get('stop_order_id') else '❌'} | "
                    f"TP: {'✅' if protective.get('tp_order_id') else '❌'}"
                )

            except Exception as e:
                logger.error(f"Ошибка при открытии позиции {symbol}: {e}")
                # Обновляем статус на FAILED в БД
                async with async_session() as session:
                    from sqlalchemy import update
                    from database.models.all_models import Signal as SignalModel
                    stmt = update(SignalModel).where(SignalModel.id == signal_id).values(status="FAILED")
                    await session.execute(stmt)
                    await session.commit()

                await send_telegram_msg(f"❌ **ОШИБКА ВХОДА**\n\n{symbol}: Не удалось открыть позицию. Ошибка биржи: {str(e)[:140]}...")

    async def _update_signal_status(self, signal_id: int, status: str):
        if not signal_id: return
        try:
            from sqlalchemy import update
            from database.models.all_models import Signal as SignalModel
            async with async_session() as session:
                stmt = update(SignalModel).where(SignalModel.id == signal_id).values(status=status)
                await session.execute(stmt)
                await session.commit()
        except Exception as e:
            logger.error(f"Ошибка обновления статуса сигнала {signal_id}: {e}")

    async def update_positions(self, symbol: str, current_price: float, atr: float):
        async with self._get_symbol_lock(symbol):
            if symbol not in self.active_trades:
                return

            trade = self.active_trades[symbol]
        
            # 1. Time Exit (15-я стратегия Швагера)
            if self.time_exit.should_exit(
                trade['opened_at'], 
                time.time(), 
                trade.get('timeframe', '1h'), 
                current_price, 
                trade['entry'], 
                trade['signal_type']
            ):
                await self._close_position(symbol, reason="TIME")
                return

            # 2. ATR Trailing Stop (13-я стратегия Швагера)
            new_stop = self.risk_manager.calculate_trailing_stop(
                trade['stop'], current_price, atr, trade['signal_type']
            )
        
            if abs(new_stop - trade['stop']) > (current_price * 0.0001): # Сдвиг только если значим (>0.01%)
                old_stop = trade['stop']
                trade['stop'] = new_stop
                logger.info(f"🛡 [ТРЕЙЛИНГ] Обновлен стоп для {symbol}: {new_stop:.4f}")

                # Обновляем биржевой SL (cancel+replace), best-effort
                stop_order_id = trade.get("stop_order_id")
                await self._cancel_order_safe(symbol, stop_order_id)
                try:
                    reduce_side = "sell" if trade["signal_type"] == "LONG" else "buy"
                    new_stop = await self._normalize_price(symbol, new_stop)
                    sl_order = await self.exchange.create_order(
                        symbol=symbol,
                        type="STOP_MARKET",
                        side=reduce_side,
                        amount=trade["current_size"],
                        params={
                            "stopPrice": new_stop,
                            "reduceOnly": True,
                            "workingType": "MARK_PRICE",
                        },
                    )
                    if sl_order and sl_order.get("id"):
                        trade["stop_order_id"] = sl_order["id"]
                        # Sync to DB: new protective order row
                        try:
                            from database.models.all_models import Order as OrderModel, OrderStatus, SignalType
                            async with async_session() as session:
                                session.add(
                                    OrderModel(
                                        user_id=1,
                                        exchange_order_id=trade["stop_order_id"],
                                        symbol=symbol,
                                        order_type="stop_market",
                                        side=SignalType.LONG if trade["signal_type"] == "LONG" else SignalType.SHORT,
                                        price=float(new_stop),
                                        size=float(trade["current_size"]),
                                        status=OrderStatus.OPEN,
                                    )
                                )
                                await session.commit()
                        except Exception:
                            pass

                    # Update position stop in DB (best-effort)
                    pos_id = trade.get("position_db_id")
                    if pos_id:
                        from sqlalchemy import update
                        from database.models.all_models import Position as PositionModel
                        async with async_session() as session:
                            await session.execute(
                                update(PositionModel).where(PositionModel.id == pos_id).values(stop_loss=float(new_stop))
                            )
                            await session.commit()
                except Exception as e:
                    logger.error(f"Ошибка обновления биржевого SL {symbol}: {e}")

                # Уведомление об апе стопа (сильно не спамим, только если сдвиг заметен)
                if abs(new_stop - old_stop) > (atr * 0.1): # Если сдвиг больше 10% от ATR
                    await send_telegram_msg(f"🛡 **ТРЕЙЛИНГ-СТОП**\n\n{symbol}: Стоп подтянут до {new_stop:.4f}")

            # 3. Проверка срабатывания стопа (internal safety net)
            if (trade['signal_type'] == "LONG" and current_price <= trade['stop']) or \
               (trade['signal_type'] == "SHORT" and current_price >= trade['stop']):
                await self._close_position(symbol, reason="STOP_LOSS")

    def schedule_update_positions(self, symbol: str, current_price: float, atr: float):
        """
        Дебаунс: не даём накапливаться конкурентным update_positions задачам по одному символу.
        """
        t = self._position_update_tasks.get(symbol)
        if t and not t.done():
            return
        self._position_update_tasks[symbol] = asyncio.create_task(self.update_positions(symbol, current_price, atr))

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

    async def _close_position(self, symbol: str, reason: str = "AUTO"):
        if symbol not in self.active_trades: return
        trade = self.active_trades[symbol]
        
        # Получаем текущую цену (упрощенно из последнего апдейта или через exchange)
        # Для уведомления нам нужен PnL
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            exit_price = ticker['last']
            
            pnl_pct_calc = (exit_price - trade['entry']) / trade['entry'] if trade['signal_type'] == "LONG" else (trade['entry'] - exit_price) / trade['entry']
            pnl_pct_calc *= 100
            
            # Перед закрытием отменяем reduce-only SL/TP (best-effort)
            await self._cancel_order_safe(symbol, trade.get("stop_order_id"))
            await self._cancel_order_safe(symbol, trade.get("tp_order_id"))

            # Закрываем позицию на бирже
            order = await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side='sell' if trade['signal_type'] == 'LONG' else 'buy',
                amount=trade['current_size']
            )

            # Persist close order (best-effort)
            try:
                from database.models.all_models import Order as OrderModel, OrderStatus, SignalType
                async with async_session() as session:
                    session.add(
                        OrderModel(
                            user_id=1,
                            exchange_order_id=order.get("id"),
                            symbol=symbol,
                            order_type="market_close",
                            side=SignalType.LONG if trade["signal_type"] == "LONG" else SignalType.SHORT,
                            price=float(exit_price),
                            size=float(trade["current_size"]),
                            status=OrderStatus.FILLED,
                        )
                    )
                    await session.commit()
            except Exception:
                pass
            
            # 🎯 Этап 2: Получаем РЕАЛЬНЫЙ P&L из истории торгов (ждем немного для обработки)
            await asyncio.sleep(2)
            real_pnl_usd = 0.0
            try:
                # Берем последние сделки по инструменту
                my_trades = await self.exchange.fetch_my_trades(symbol, limit=5)
                # Фильтруем сделки, которые относятся к нашему ордеру закрытия
                order_id = order.get('id')
                relevant_trades = [t for t in my_trades if t.get('order') == order_id]
                
                if relevant_trades:
                    # Суммируем реализованный PnL (для Binance Futures это поле 'realizedPnl' в info сделки)
                    for t in relevant_trades:
                        real_pnl_usd += float(t.get('info', {}).get('realizedPnl', 0.0))
                else:
                    # Fallback на расчетный, если история еще не обновилась
                    real_pnl_usd = float(pnl_pct_calc * trade['current_size'] * trade['entry'] / 100)
            except Exception as e:
                logger.error(f"Ошибка получения реального PnL для {symbol}: {e}")
                real_pnl_usd = float(pnl_pct_calc * trade['current_size'] * trade['entry'] / 100)

            reason_map = {
                "TIME": "⏳ ВЫХОД ПО ВРЕМЕНИ",
                "STOP_LOSS": "💥 СТОП-ЛОСС",
                "MANUAL": "👤 РУЧНОЕ ЗАКРЫТИЕ",
                "AUTO": "🤖 АВТОМАТИЧЕСКОЕ ЗАКРЫТИЕ"
            }
            
            pnl_emoji = "🟢" if real_pnl_usd > 0 else "🔴"
            
            await send_telegram_msg(
                f"💰 **ПОЗИЦИЯ ЗАКРЫТА**\n\n"
                f"🔹 Символ: {symbol}\n"
                f"🔹 Причина: {reason_map.get(reason, reason)}\n"
                f"💰 Цена выхода: {exit_price:.2f}\n"
                f"📊 Фактический PnL: {pnl_emoji} {real_pnl_usd:+.4f} USDT\n"
                f"📍 Статистика синхронизирована с биржей."
            )

            # Сохраняем в БД статистику
            from database.models.all_models import PnLRecord as PnLModel
            async with async_session() as session:
                new_record = PnLModel(
                    symbol=symbol,
                    pnl_usd=real_pnl_usd,
                    pnl_pct=float(pnl_pct_calc), # Оставляем расчетный % для ориентира
                    reason=reason,
                    user_id=1
                )
                session.add(new_record)
                await session.flush()

                # Update Position row if we have it
                try:
                    from sqlalchemy import update
                    from database.models.all_models import Position as PositionModel, PositionStatus
                    pos_id = trade.get("position_db_id")
                    if pos_id:
                        await session.execute(
                            update(PositionModel)
                            .where(PositionModel.id == pos_id)
                            .values(
                                status=PositionStatus.CLOSED,
                                closed_at=datetime.datetime.utcnow(),
                                realized_pnl=float(real_pnl_usd),
                            )
                        )
                except Exception:
                    pass

                await session.commit()
            
            del self.active_trades[symbol]
            logger.info(f"✅ Позиция {symbol} закрыта ({reason})")
        except Exception as e:
            logger.error(f"Ошибка при закрытии {symbol}: {e}")
            await send_telegram_msg(f"❌ **ОШИБКА ЗАКРЫТИЯ**\n\n{symbol}: {str(e)[:100]}")

    async def manual_close(self, symbol: str):
        """Публичный метод для ручного закрытия из Telegram"""
        if symbol in self.active_trades:
            await self._close_position(symbol, reason="MANUAL")
            return True
        return False
