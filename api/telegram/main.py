import logging
import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, TypeHandler, CallbackQueryHandler
from telegram.error import BadRequest
import time
import httpx
from collections import defaultdict
from config.settings import settings

ENGINE_URL = "http://trading-engine:8000" # URL внутри Docker сети

BTN_AUTOTRADE_SETTINGS = "⚙️ Настройки автоторговли"
BTN_API_SETTINGS = "⚙️ Настройки API"
BTN_TOGGLE = "🔄 Вкл/Выкл"
BTN_ACTIVE = "💼 Активные позиции"
BTN_HISTORY = "📜 История сделок"
BTN_STATS = "📈 Статистика"
BTN_SIGNALS = "📉 Сигналы"
BTN_STRATEGIES = "📚 Стратегии"
BTN_FAQ = "❓ FAQ"
BTN_HOME = "🏠 Главное меню"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

_action_cooldowns: dict[str, float] = {}
_ACTION_COOLDOWN_BY_TYPE = {
    "refresh": 0.8,
    "reduce25": 2.5,
    "reduce50": 2.5,
    "close": 2.5,
}
_ACTION_COOLDOWN_DEFAULT = 1.2
_ACTION_COOLDOWN_MAX_KEYS = 2000
_ACTION_ADAPTIVE_MAX_MULTIPLIER = 3.0
_ACTION_ADAPTIVE_STEP = 0.35
_ACTION_SPAM_DECAY_SECONDS = 12.0
_action_spam_score: dict[str, int] = {}
_action_spam_last_ts: dict[str, float] = {}


async def _load_runtime_settings(client: httpx.AsyncClient) -> dict:
    try:
        response = await client.get(f"{ENGINE_URL}/api/v1/runtime-settings", timeout=5.0)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            data["_runtime_status"] = "OK"
            return data
    except Exception as e:
        logging.warning(f"Runtime settings endpoint unavailable, fallback to local defaults: {e}")

    # Fallback: чтобы меню "Настройки" работало даже если новый REST endpoint ещё не доступен.
    return {
        "pyramiding_enabled": settings.pyramiding_enabled,
        "per_trade_margin_pct": settings.per_trade_margin_pct,
        "max_open_trades": settings.max_open_trades,
        "leverage": settings.leverage,
        "apply_after_flat": settings.apply_new_entry_rules_after_flat,
        "_runtime_status": "fallback",
    }


async def _safe_answer_callback(query, text: str | None = None, show_alert: bool = False):
    """
    Безопасный answerCallbackQuery:
    - не роняет handler, если callback уже протух/некорректен.
    """
    try:
        await query.answer(text=text, show_alert=show_alert)
    except BadRequest as e:
        logging.warning(f"Callback answer skipped: {e}")
    except Exception as e:
        logging.warning(f"Callback answer failed: {e}")


async def _get_json_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict | None = None,
    timeout: float = 8.0,
    retries: int = 1,
) -> dict:
    last_exc = None
    for attempt in range(retries + 1):
        try:
            response = await client.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, dict) else {}
        except Exception as e:
            last_exc = e
            if attempt >= retries:
                raise
            await asyncio.sleep(0.35 * (attempt + 1))
    raise last_exc


def _cooldown_key(user_id: int, action: str, symbol_raw: str) -> str:
    return f"{user_id}:{action}:{symbol_raw}"


def _cleanup_cooldown_map(now_ts: float):
    # Ленивая уборка: удаляем устаревшие ключи и защищаемся от бесконечного роста словаря.
    expired_keys = [k for k, until in _action_cooldowns.items() if float(until or 0.0) <= now_ts]
    for k in expired_keys:
        _action_cooldowns.pop(k, None)
    stale_spam = [k for k, ts in _action_spam_last_ts.items() if (now_ts - float(ts or 0.0)) > (_ACTION_SPAM_DECAY_SECONDS * 4.0)]
    for k in stale_spam:
        _action_spam_last_ts.pop(k, None)
        _action_spam_score.pop(k, None)
    if len(_action_cooldowns) > _ACTION_COOLDOWN_MAX_KEYS:
        # На всякий случай жёсткая очистка, если поток callback слишком большой.
        _action_cooldowns.clear()
        _action_spam_score.clear()
        _action_spam_last_ts.clear()


def _is_action_on_cooldown(user_id: int, action: str, symbol_raw: str) -> bool:
    now = time.time()
    _cleanup_cooldown_map(now)
    key = _cooldown_key(user_id, action, symbol_raw)
    base_cd = float(_ACTION_COOLDOWN_BY_TYPE.get(action, _ACTION_COOLDOWN_DEFAULT))
    until = float(_action_cooldowns.get(key, 0.0) or 0.0)

    score = int(_action_spam_score.get(key, 0) or 0)
    last_ts = float(_action_spam_last_ts.get(key, now) or now)
    if now - last_ts > _ACTION_SPAM_DECAY_SECONDS:
        score = max(0, score - 2)

    if now < until:
        score = min(score + 1, 8)
        _action_spam_score[key] = score
        _action_spam_last_ts[key] = now
        return True

    # Если спама нет, постепенно возвращаемся к базовому cooldown.
    if score > 0:
        score -= 1
    _action_spam_score[key] = score
    _action_spam_last_ts[key] = now

    adaptive_multiplier = min(_ACTION_ADAPTIVE_MAX_MULTIPLIER, 1.0 + (_ACTION_ADAPTIVE_STEP * score))
    _action_cooldowns[key] = now + (base_cd * adaptive_multiplier)
    return False


def _spam_level_badge(user_id: int, action: str, symbol_raw: str) -> str:
    """Возвращает компактный индикатор anti-spam уровня: lvl N/5."""
    key = _cooldown_key(user_id, action, symbol_raw)
    score = int(_action_spam_score.get(key, 0) or 0)
    lvl = max(1, min(5, 1 + (score // 2)))
    return f"lvl {lvl}/5"


def _build_settings_menu_text(runtime: dict) -> str:
    leverage_val = runtime.get("leverage", settings.leverage)
    side = runtime.get('allowed_position_side', getattr(settings, 'allowed_position_side', 'BOTH'))
    pos_usdt = runtime.get('position_size_usdt', getattr(settings, 'position_size_usdt', 0.0))
    return (
        "⚙️ **НАСТРОЙКИ АВТОТОРГОВЛИ**\n\n"
        f"🔄 Торговля: {'✅ ВКЛ' if runtime.get('is_trading_enabled', settings.is_trading_enabled) else '❌ ВЫКЛ'}\n"
        f"↕️ Направление: `{side}`\n"
        f"⚡ Плечо: `{leverage_val}x`\n"
        f"📂 Макс. сделок: `{runtime.get('max_open_trades', settings.max_open_trades)}`\n"
        f"💰 Маржа: `{runtime.get('per_trade_margin_pct', settings.per_trade_margin_pct)*100:.1f}%`\n"
        f"💵 Объём: `{pos_usdt:.0f} USDT` {'(авто)' if pos_usdt == 0 else ''}\n"
        f"🎯 TP: `{runtime.get('tp_pct', settings.tp_pct)*100:.2f}%`\n"
        f"🛡 SL: Long `{settings.sl_long_pct*100:.1f}%` / Short `{settings.sl_short_pct*100:.1f}%`\n"
        f"💎 Пирамидинг: {'✅' if runtime.get('pyramiding_enabled', settings.pyramiding_enabled) else '❌'}\n"
        f"⏱ Expiry: `{runtime.get('signal_expiry_seconds', settings.signal_expiry_seconds)}с`\n\n"
        "👇 Выберите раздел для настройки:"
    )


def _build_settings_menu_keyboard(runtime: dict, presets: list) -> InlineKeyboardMarkup:
    is_on = runtime.get('is_trading_enabled', settings.is_trading_enabled)
    pyr_on = runtime.get('pyramiding_enabled', False)
    buttons = [
        [InlineKeyboardButton(
            f"{'🟢' if is_on else '🔴'} Торговля: {'ВКЛ' if is_on else 'ВЫКЛ'}",
            callback_data="rt_toggle_trading",
        )],
        [InlineKeyboardButton("⚡ Плечо", callback_data="menu_leverage"),
         InlineKeyboardButton("💰 Маржа", callback_data="menu_margin")],
        [InlineKeyboardButton("📂 Позиции", callback_data="menu_positions"),
         InlineKeyboardButton("🎯 TP / SL", callback_data="menu_tp_sl")],
        [InlineKeyboardButton("↕️ Направление", callback_data="menu_side"),
         InlineKeyboardButton("💵 Объём", callback_data="menu_volume")],
        [InlineKeyboardButton(
            f"💎 Пирамидинг: {'ВКЛ' if pyr_on else 'ВЫКЛ'}",
            callback_data="rt_toggle_pyramiding",
        )],
        [InlineKeyboardButton("⏱ Expiry сигналов", callback_data="menu_expiry")],
    ]
    if presets:
        buttons.append([InlineKeyboardButton("🎯 Пресеты риска", callback_data="menu_presets")])
    return InlineKeyboardMarkup(buttons)


async def _render_settings_message(message_target, edit: bool = False):
    timeout = httpx.Timeout(connect=2.5, read=8.0, write=5.0, pool=3.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        presets = []
        try:
            presets_resp = await client.get(f"{ENGINE_URL}/api/v1/presets", timeout=5.0)
            presets_resp.raise_for_status()
            raw_presets = presets_resp.json()
            if isinstance(raw_presets, list):
                presets = raw_presets
        except Exception as e:
            logging.warning(f"Presets endpoint unavailable, showing runtime controls only: {e}")
        runtime = await _load_runtime_settings(client)

    text = _build_settings_menu_text(runtime)
    keyboard = _build_settings_menu_keyboard(runtime, presets)
    if edit:
        await message_target.edit_message_text(text, reply_markup=keyboard, parse_mode='Markdown')
    else:
        await message_target.reply_text(text, reply_markup=keyboard, parse_mode='Markdown')


def _build_main_menu_markup() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(BTN_AUTOTRADE_SETTINGS), KeyboardButton(BTN_API_SETTINGS)],
        [KeyboardButton(BTN_TOGGLE), KeyboardButton(BTN_ACTIVE)],
        [KeyboardButton(BTN_HISTORY), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_SIGNALS), KeyboardButton(BTN_STRATEGIES)],
        [KeyboardButton(BTN_FAQ)],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def _load_runtime_settings_for_menu() -> dict:
    timeout = httpx.Timeout(connect=2.5, read=8.0, write=5.0, pool=3.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await _load_runtime_settings(client)


async def _render_main_menu(message_target):
    await message_target.reply_text(
        "🤖 **Algo Quant Bot**\n\nВыберите раздел для управления ботом:",
        reply_markup=_build_main_menu_markup(),
        parse_mode='Markdown',
    )


async def show_api_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with httpx.AsyncClient() as client:
            ex = await client.get(f"{ENGINE_URL}/api/v1/exchange/check", timeout=6.0)
            st = await client.get(f"{ENGINE_URL}/api/v1/status", timeout=6.0)
            ex_data = ex.json() if ex.status_code == 200 else {"status": "error", "message": ex.text}
            st_data = st.json() if st.status_code == 200 else {"status": "error", "message": st.text}
            is_ok = ex_data.get("status") == "success"
            mode = "Testnet" if st_data.get("testnet", settings.testnet) else "Real"
            msg = (
                "⚙️ **НАСТРОЙКИ API**\n\n"
                f"🔗 Биржа: `Binance Futures`\n"
                f"🧪 Режим: `{mode}`\n"
                f"📡 Соединение: {'✅ OK' if is_ok else '❌ Ошибка'}\n"
                f"📝 Детали: `{(ex_data.get('message') or 'n/a')[:140]}`\n\n"
                "Для смены ключей используйте `.env` и перезапуск контейнеров."
            )
            await update.message.reply_text(msg, parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка проверки API: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Этап 14-15: Главное меню
    """
    user_id = update.effective_user.id
    allowed_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]
    
    if user_id not in allowed_ids:
        await update.message.reply_text("⛔️ Доступ запрещен. Вы не являетесь администратором.")
        return

    await _render_main_menu(update.message)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{ENGINE_URL}/api/v1/status", timeout=5.0)
            data = response.json()
            
            if data["status"] == "running":
                msg = (
                    "🟢 Трейдинг-движок запущен\n"
                    f"💰 Баланс: {data['balance']:.2f} USDT\n"
                    f"📉 Просадка: {data['drawdown']}\n"
                    f"📂 Открыто сделок: {data['open_trades']}\n"
                    f"🔗 Режим: {'Testnet' if data['testnet'] else 'Real'}"
                )
            else:
                msg = f"🟡 Статус системы: {data['status']}"
            await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"🔴 Ошибка связи с движком: {e}")

async def connect_exchange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔑 **Настройка API-ключей**\n\n"
        "Для подключения биржи отредактируйте файл `.env` и перезапустите контейнеры.\n\n"
        "⚠️ Никогда не отправляйте ключи в чат!",
        parse_mode='Markdown'
    )


def _risk_tag_for_trade(is_long: bool, entry: float, stop: float, current_price: float | None) -> str:
    """Оценка близости цены к стопу для UX-индикатора."""
    try:
        if current_price is None:
            return "⚪️ N/A"
        cp = float(current_price)
        if entry <= 0 or stop <= 0 or cp <= 0 or abs(entry - stop) <= 1e-9:
            return "⚪️ N/A"
        if is_long:
            progress = (cp - stop) / (entry - stop)
        else:
            progress = (stop - cp) / (stop - entry)
        if progress <= 0.3:
            return "🔴 Высокий"
        if progress <= 0.8:
            return "🟡 Средний"
        return "🟢 Низкий"
    except Exception:
        return "⚪️ N/A"


def _sorted_positions_by_risk(trades: dict) -> list[tuple[int, str, dict, str]]:
    enriched = []
    for symbol, info in trades.items():
        is_lg = info.get('signal_type') == "LONG"
        curr_p = info.get('current_price')
        entry = float(info.get('entry', 0.0) or 0.0)
        stop = float(info.get('stop', 0.0) or 0.0)
        risk_tag = _risk_tag_for_trade(is_lg, entry, stop, curr_p)
        risk_rank = {"🔴 Высокий": 0, "🟡 Средний": 1, "🟢 Низкий": 2, "⚪️ N/A": 3}.get(risk_tag, 3)
        enriched.append((risk_rank, symbol, info, risk_tag))
    enriched.sort(key=lambda x: (x[0], x[1]))
    return enriched


def _build_positions_list_view(trades: dict) -> tuple[str, InlineKeyboardMarkup]:
    enriched = _sorted_positions_by_risk(trades)
    list_lines = ["📌 **СПИСОК ПОЗИЦИЙ (приоритет по риску):**"]
    keyboard_rows = []
    for _, symbol, info, risk_tag in enriched:
        side_emoji = "🟢 LONG" if info.get('signal_type') == "LONG" else "🔴 SHORT"
        pnl_usd = float(info.get('pnl_usd', 0.0) or 0.0)
        pnl_badge = "🟢" if pnl_usd >= 0 else "🔴"
        list_lines.append(f"• `{symbol}` · {side_emoji} · {risk_tag} · {pnl_badge} `{pnl_usd:+.2f}$`")
        sraw = symbol.replace("/", "_")
        keyboard_rows.append([
            InlineKeyboardButton(f"ℹ️ {symbol}", callback_data=f"pos_view_{sraw}"),
            InlineKeyboardButton("❌", callback_data=f"close_{sraw}"),
        ])
    return "\n".join(list_lines), InlineKeyboardMarkup(keyboard_rows)


def _build_position_details_view(symbol: str, info: dict) -> str:
    is_lg = info.get('signal_type') == "LONG"
    side_emoji = "🟢 LONG" if is_lg else "🔴 SHORT"
    curr_p = info.get('current_price')
    curr_p_str = "🔍 ожидание..." if curr_p is None else f"`{float(curr_p):.6f}`"

    pnl_usd = float(info.get('pnl_usd', 0.0) or 0.0)
    pnl_pct = float(info.get('pnl_pct', 0.0) or 0.0)
    if curr_p is not None:
        pnl_emoji = "🟢" if pnl_usd >= 0 else "🔴"
        pnl_str = f"{pnl_emoji} `{pnl_usd:+.2f} USDT / {pnl_pct:+.2f}%`"
    else:
        pnl_str = "⏳ `расчет...`"

    entry = float(info.get('entry', 0.0) or 0.0)
    stop = float(info.get('stop', 0.0) or 0.0)
    risk_tag = _risk_tag_for_trade(is_lg, entry, stop, curr_p)

    opened_for = time.time() - float(info.get('opened_at', time.time()))
    if opened_for < 3600:
        opened_ago = f"{int(opened_for // 60)}м"
    elif opened_for < 86400:
        opened_ago = f"{int(opened_for // 3600)}ч {int((opened_for % 3600) // 60)}м"
    else:
        opened_ago = f"{int(opened_for // 86400)}д {int((opened_for % 86400) // 3600)}ч"

    return (
        f"🔹 **{symbol}**\n"
        f"📌 {side_emoji}\n"
        f"💰 Вход: `{entry:.6f}`\n"
        f"📈 Текущая: {curr_p_str}\n"
        f"📊 Объем: `{float(info.get('current_size', 0.0) or 0.0):.4f}`\n"
        f"🛡 Стоп: `{stop:.6f}`\n"
        f"📉 PnL: {pnl_str}\n"
        f"🚨 Риск до стопа: {risk_tag}\n"
        f"⏱ В позиции: `{opened_ago}`"
    )


async def show_active_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with httpx.AsyncClient() as client:
            data = await _get_json_with_retry(client, f"{ENGINE_URL}/api/v1/trades", timeout=8.0, retries=1)
            logging.info(f"🕵️‍♂️ DEBUG: Получено данных от движка: {data}")
            trades = data.get("trades", {})

            if not trades:
                await update.message.reply_text("📂 **Активных позиций на данный момент нет.**", parse_mode='Markdown')
                return

            # Портфельная сводка до списка карточек.
            total_pnl_usd = 0.0
            total_pnl_pct = 0.0
            counted = 0
            for _, info in trades.items():
                cp = info.get('current_price')
                if cp is None:
                    continue
                total_pnl_usd += float(info.get('pnl_usd', 0.0) or 0.0)
                total_pnl_pct += float(info.get('pnl_pct', 0.0) or 0.0)
                counted += 1

            sum_emoji = "🟢" if total_pnl_usd >= 0 else "🔴"
            avg_pnl_pct = (total_pnl_pct / counted) if counted > 0 else 0.0
            summary_msg = (
                f"💼 **ОТКРЫТЫЕ ПОЗИЦИИ: {len(trades)}**\n"
                f"{sum_emoji} Суммарный PnL: `{total_pnl_usd:+.2f} USDT`\n"
                f"📊 Средний PnL: `{avg_pnl_pct:+.2f}%`\n"
                f"🧮 Рассчитано по позициям: `{counted}/{len(trades)}`"
            )
            await update.message.reply_text(summary_msg, parse_mode='Markdown')

            list_text, list_kb = _build_positions_list_view(trades)
            await update.message.reply_text(list_text, parse_mode='Markdown', reply_markup=list_kb)

    except Exception as e:
        logging.error(f"Ошибка получения позиций: {e!r}")
        await update.message.reply_text("❌ Не удалось связаться с движком торгов.")


async def show_trade_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with httpx.AsyncClient() as client:
            data = await _get_json_with_retry(
                client,
                f"{ENGINE_URL}/api/v1/history",
                params={"limit": 20},
                timeout=8.0,
                retries=1,
            )
            items = data.get("items", [])
            if not items:
                await update.message.reply_text("📜 История сделок пуста.")
                return

            await update.message.reply_text(f"📜 **ИСТОРИЯ СДЕЛОК ({len(items)}):**", parse_mode='Markdown')
            for it in items:
                pnl_usd = float(it.get("pnl_usd", 0.0) or 0.0)
                pnl_pct = float(it.get("pnl_pct", 0.0) or 0.0)
                emoji = "🟢" if pnl_usd >= 0 else "🔴"
                ts = (it.get("closed_at") or "").replace("T", " ")[:19] if it.get("closed_at") else "N/A"
                reason = str(it.get('reason', 'AUTO') or 'AUTO').upper()
                reason_map = {
                    "STOP": "🛑 стоп",
                    "TAKE": "🎯 тейк",
                    "MANUAL": "🖐 ручное",
                    "TIME": "⏱ тайм-аут",
                    "EXTERNAL": "🔄 внешнее",
                    "AUTO": "⚙️ авто",
                }
                reason_ru = reason_map.get(reason, f"⚙️ {reason.lower()}")
                msg = (
                    f"🧾 **{it.get('symbol', 'N/A')}**\n"
                    f"{emoji} Результат: `{pnl_usd:+.2f} USDT / {pnl_pct:+.2f}%`\n"
                    f"🏁 Закрытие: {reason_ru}\n"
                    f"🕒 Время: `{ts} UTC`"
                )
                await update.message.reply_text(msg, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Ошибка истории сделок: {e!r}")
        await update.message.reply_text("❌ Не удалось получить историю сделок.")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    allowed_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]
    if user_id not in allowed_ids:
        return

    text = update.message.text
    logging.info(f"📩 ПОЛУЧЕНО СООБЩЕНИЕ: '{text}' от {user_id}")
    if text == BTN_ACTIVE:
        await show_active_positions(update, context)
    elif text == BTN_TOGGLE:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/toggle", timeout=5.0)
                res = response.json()
                st = "✅ ВКЛ" if res.get("is_enabled") else "❌ ВЫКЛ"
                await update.message.reply_text(f"🔄 Автоторговля: {st}")
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка переключения: {e}")
    elif text in {"⚙ Настройки", BTN_AUTOTRADE_SETTINGS}:
        try:
            await _render_settings_message(update.message, edit=False)
        except Exception as e:
            logging.error(f"Ошибка настроек: {e}")
            await update.message.reply_text("❌ Ошибка при получении настроек.")
    elif text == BTN_API_SETTINGS:
        await show_api_settings(update, context)
    elif text == BTN_SIGNALS:
        from database.session import async_session
        from database.models.all_models import Signal
        from sqlalchemy import select
        
        try:
            from datetime import datetime, timedelta
            async with async_session() as session:
                # Показываем 5 свежих сигналов за последние 6 часов
                six_hours_ago = datetime.utcnow() - timedelta(hours=6)
                query = select(Signal).where(Signal.timestamp >= six_hours_ago).order_by(Signal.timestamp.desc()).limit(5)
                result = await session.execute(query)
                signals = result.scalars().all()
                
                if not signals:
                    await update.message.reply_text("📭 Актуальных сигналов за последние 6 часов нет. Бот мониторит рынок...")
                    return

                await update.message.reply_text(f"📉 **ПОСЛЕДНИЕ СИГНАЛЫ ({len(signals)}):**", parse_mode='Markdown')

                status_map = {
                    "PENDING": "⌛️ В ОЖИДАНИИ",
                    "EXECUTED": "✅ В ПОЗИЦИИ",
                    "FAILED": "❌ ОШИБКА",
                    "REJECTED": "🛑 ОТКЛОНЕН",
                    "EXPIRED": "⏱ ИСТЕК"
                }

                def fmt_p(val):
                    if val is None: return "N/A"
                    return f"{val:.4f}" if val < 1.0 else f"{val:.2f}"

                for last_signal in signals:
                    status_ru = status_map.get(last_signal.status, "🕒 ОБРАБОТКА")
                    signal_type_ru = "🟢 LONG" if last_signal.signal_type == "LONG" else "🔴 SHORT"

                    msg = (
                        f"🚀 **СИГНАЛ: {last_signal.strategy}**\n\n"
                        f"🔸 **Символ:** {last_signal.symbol}\n"
                        f"🔸 **Направление:** {signal_type_ru}\n\n"
                        f"💰 **Цена входа:** {fmt_p(last_signal.entry_price)}\n"
                        f"🛡 **Stop Loss:** {fmt_p(last_signal.stop_loss)}\n"
                        f"🎯 **Take Profit:** {fmt_p(last_signal.take_profit)}\n\n"
                        f"🕒 **Время (UTC):** {last_signal.timestamp.strftime('%H:%M:%S')}\n\n"
                        f"🤖 **AI ВЕРДИКТ:**\n"
                        f"📈 **Вероятность успеха:** {int((last_signal.win_prob or 0.5) * 100)}%\n"
                        f"💰 **Ож. доходность:** {last_signal.expected_return or '0.0'}%\n"
                        f"⚠️ **Уровень риска:** {last_signal.risk or '1.0'}\n"
                        f"📊 **AI Score:** {last_signal.confidence or 0.6:.2f}\n\n"
                        f"ℹ️ **Статус:** {status_ru}\n"
                        f"───────────────────"
                    )
                    await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logging.error(f"Ошибка получения сигналов из БД: {e}")
            await update.message.reply_text("❌ Ошибка при обращении к базе данных.")
    elif text == BTN_STRATEGIES:
        strategy_text = (
            "📖 **ТОРГОВАЯ СИСТЕМА (Швагер + AI)**\n\n"
            "📈 **8 СТРАТЕГИЙ (4 группы):**\n"
            "🔹 *Пробой:* Donchian (1.15x), WRD (1.05x), Vol Contraction (1.05x)\n"
            "🔹 *Тренд:* MA Trend (1.10x), Pullback (1.10x)\n"
            "🔹 *Разворот:* Williams R, WRD Reversal\n"
            "🔹 *Крипто:* Funding Squeeze\n\n"
            "🛡 **РИСК-МЕНЕДЖМЕНТ:**\n"
            "• Стоп-лосс: ATR × 2.0 (мин. 0.5%)\n"
            "• Трейлинг: ATR × 2.5 (мин. 0.3%)\n"
            "• Безубыток: при 1R + ADX/Пробой\n"
            "• Тайм-аут: 48 свечей без прогресса\n"
            "• Макс. дневная просадка: 5%\n\n"
            "🤖 **ИИ-ФИЛЬТРАЦИЯ:**\n"
            "• Score > 0.55 (взвешенный по стратегии)\n"
            "• AI Win Prob > 0.55\n"
            "• Внешний AI: Groq → Grok → Gemini → OpenRouter\n"
            "• Листинг > 100 дней\n"
            "• ADX ≥ 20 (для трендовых)\n"
            "• Корреляция: макс. 2 в группе"
        )
        await update.message.reply_text(strategy_text, parse_mode='Markdown')
    elif text == BTN_FAQ:
        faq_text = (
            "❓ **FAQ: ПОЛНОЕ ОПИСАНИЕ БОТА**\n\n"
            "1) **УПРАВЛЕНИЕ**\n"
            "• `🔄 Вкл/Выкл` — мгновенно включает/выключает автоторговлю.\n"
            "• `💼 Активные позиции` — список позиций, карточки, refresh, reduce, ручное закрытие.\n"
            "• `📜 История сделок` — последние закрытия с причиной и PnL.\n\n"
            "2) **НАСТРОЙКИ АВТОТОРГОВЛИ**\n"
            "• `⚡ Плечо` — runtime-плечо (без рестарта).\n"
            "• `💰 Маржа` — доля капитала на сделку (%).\n"
            "• `📂 Позиции` — лимит одновременных сделок.\n"
            "• `🎯 TP / SL` — TP runtime; SL берётся из риск-логики (ATR/направление).\n"
            "• `↕️ Направление` — LONG / SHORT / BOTH.\n"
            "• `💵 Объём` — фикс USDT на сделку (0 = авто по марже).\n"
            "• `💎 Пирамидинг` — разрешение поэтапного добора.\n"
            "• `⏱ Expiry` — защита от старых сигналов.\n\n"
            "3) **РИСК-ЛОГИКА**\n"
            "• Стоп-лосс: ATR-логика + минимальная дистанция.\n"
            "• Трейлинг: подтяжка только в сторону уменьшения риска.\n"
            "• Безубыток: перенос SL при 1R + подтверждение.\n"
            "• Daily halt: остановка входов при дневной просадке.\n"
            "• Корреляционный фильтр: ограничение однотипных позиций в кластерах.\n\n"
            "4) **СИГНАЛЫ И AI**\n"
            "• Сигнал проходит скоринг и AI-фильтр перед входом.\n"
            "• В карточке сигнала: стратегия, вход, SL/TP, win probability, score.\n"
            "• Внешний AI работает каскадом по доступным провайдерам.\n\n"
            "5) **ПОЧЕМУ НЕТ ВХОДА**\n"
            "• лимит позиций,\n"
            "• выключена торговля,\n"
            "• слабый score/win_prob,\n"
            "• сигнал устарел,\n"
            "• фильтр листинга/корреляции/funding,\n"
            "• достигнут дневной стоп по просадке.\n\n"
            "6) **ПРИЧИНЫ ЗАКРЫТИЯ ПОЗИЦИИ**\n"
            "• `🔄 Биржа (TP/SL)` — защитный ордер сработал на бирже.\n"
            "• `🖐 Ручное` — закрытие из Telegram.\n"
            "• `⏱ Тайм-аут` — time-exit.\n"
            "• `⚙️ Авто` — внутренняя логика управления.\n\n"
            "7) **СИНХРОНИЗАЦИЯ**\n"
            "• Бот регулярно синхронизирует биржу, БД и память (`reconcile`).\n"
            "• Если позиция закрыта внешне, она должна исчезнуть из активных.\n"
            "• В разделе позиций показываются только актуальные данные после синхронизации."
        )
        await update.message.reply_text(faq_text, parse_mode='Markdown')
    elif text == BTN_STATS:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{ENGINE_URL}/api/v1/stats", timeout=5.0)
                data = response.json()
                daily = data.get("daily", {})
                
                msg = (
                    "📊 **СТАТИСТИКА ТОРГОВЛИ**\n\n"
                    "📅 **За последние 24 часа:**\n"
                    f"• Закрыто сделок: {daily.get('trades_count', 0)}\n"
                    f"• Прибыль/Убыток: {'🟢' if daily.get('pnl_usd', 0) >= 0 else '🔴'} "
                    f"{daily.get('pnl_usd', 0):+.2f} USDT ({daily.get('avg_pct', 0):+.2f}%)\n\n"
                    "📅 *Статистика за 7 и 30 дней будет доступна после накопления данных.*"
                )
                
                keyboard = [[InlineKeyboardButton("♻️ Сбросить статистику", callback_data="reset_stats")]]
                await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception as e:
            logging.error(f"Ошибка статистики: {e}")
            await update.message.reply_text("❌ Ошибка при получении статистики.")

    elif text in {"📜 История", BTN_HISTORY}:
        await show_trade_history(update, context)
    elif text == BTN_HOME:
        await _render_main_menu(update.message)
    else:
        await update.message.reply_text("Команда не распознана. Нажмите кнопку из меню 👇", reply_markup=_build_main_menu_markup())
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await _safe_answer_callback(query)
    user_id = update.effective_user.id if update and update.effective_user else 0

    if query.data.startswith("pos_view_") or query.data.startswith("pos_nav_") or query.data == "pos_back_list":
        symbol_raw = ""
        if query.data.startswith("pos_view_"):
            symbol_raw = query.data.replace("pos_view_", "")
        elif query.data.startswith("pos_nav_"):
            symbol_raw = query.data.replace("pos_nav_", "")
        symbol = symbol_raw.replace("_", "/") if symbol_raw else ""
        try:
            async with httpx.AsyncClient() as client:
                data = await _get_json_with_retry(client, f"{ENGINE_URL}/api/v1/trades", timeout=8.0, retries=1)
                trades = data.get("trades", {})
                if not trades:
                    await query.edit_message_text("📂 **Активных позиций на данный момент нет.**", parse_mode='Markdown')
                    return

                if query.data == "pos_back_list":
                    list_text, list_kb = _build_positions_list_view(trades)
                    await query.edit_message_text(list_text, parse_mode='Markdown', reply_markup=list_kb)
                    return

                ordered = [sym for _, sym, _, _ in _sorted_positions_by_risk(trades)]
                if symbol not in trades:
                    list_text, list_kb = _build_positions_list_view(trades)
                    await query.edit_message_text(
                        f"⚪️ Позиция `{symbol}` уже закрыта.\n\n{list_text}",
                        parse_mode='Markdown',
                        reply_markup=list_kb
                    )
                    return

                idx = ordered.index(symbol) if symbol in ordered else 0
                prev_symbol = ordered[idx - 1]
                next_symbol = ordered[(idx + 1) % len(ordered)]
                cur_raw = symbol.replace("/", "_")
                details_text = _build_position_details_view(symbol, trades[symbol])
                details_text = f"{details_text}\n\n🧭 `{idx + 1}/{len(ordered)}`"
                keyboard = [
                    [
                        InlineKeyboardButton("◀️", callback_data=f"pos_nav_{prev_symbol.replace('/', '_')}"),
                        InlineKeyboardButton("📋 К списку", callback_data="pos_back_list"),
                        InlineKeyboardButton("▶️", callback_data=f"pos_nav_{next_symbol.replace('/', '_')}"),
                    ],
                    [
                        InlineKeyboardButton("🔄 Обновить", callback_data=f"pos_refresh_{cur_raw}"),
                        InlineKeyboardButton("🧯 Сократить 25%", callback_data=f"pos_reduce25_{cur_raw}"),
                        InlineKeyboardButton("🧯 Сократить 50%", callback_data=f"pos_reduce50_{cur_raw}"),
                    ],
                    [InlineKeyboardButton("❌ Закрыть позицию", callback_data=f"close_{cur_raw}")]
                ]
                await query.edit_message_text(
                    details_text,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка загрузки позиции `{symbol}`: {e}", parse_mode='Markdown')

    elif query.data.startswith("pos_refresh_") or query.data.startswith("pos_reduce25_") or query.data.startswith("pos_reduce50_"):
        if query.data.startswith("pos_refresh_"):
            symbol_raw = query.data.replace("pos_refresh_", "")
            action = "refresh"
        elif query.data.startswith("pos_reduce25_"):
            symbol_raw = query.data.replace("pos_reduce25_", "")
            action = "reduce25"
        else:
            symbol_raw = query.data.replace("pos_reduce50_", "")
            action = "reduce50"

        if _is_action_on_cooldown(user_id, action, symbol_raw):
            lvl = _spam_level_badge(user_id, action, symbol_raw)
            if action == "refresh":
                wait_txt = "чуть-чуть"
            else:
                wait_txt = "2-3 сек"
            await _safe_answer_callback(query, f"⏱ Слишком часто, подожди {wait_txt} · {lvl}", show_alert=False)
            return

        symbol = symbol_raw.replace("_", "/")
        try:
            async with httpx.AsyncClient() as client:
                if action in {"reduce25", "reduce50"}:
                    fraction = 0.25 if action == "reduce25" else 0.50
                    r = await client.post(
                        f"{ENGINE_URL}/api/v1/trades/reduce/{symbol_raw}",
                        params={"fraction": fraction},
                        timeout=10.0
                    )
                    res = r.json()
                    if res.get("status") == "success":
                        await _safe_answer_callback(query, f"✅ Сокращение {int(fraction*100)}% отправлено")
                    else:
                        await _safe_answer_callback(
                            query,
                            f"❌ Сокращение не выполнено: {str(res.get('message', 'unknown error'))[:80]}",
                            show_alert=True
                        )

                data = await _get_json_with_retry(client, f"{ENGINE_URL}/api/v1/trades", timeout=8.0, retries=1)
                trades = data.get("trades", {})
                if not trades:
                    await query.edit_message_text("📂 **Активных позиций на данный момент нет.**", parse_mode='Markdown')
                    return
                ordered = [sym for _, sym, _, _ in _sorted_positions_by_risk(trades)]
                if symbol not in trades:
                    list_text, list_kb = _build_positions_list_view(trades)
                    await query.edit_message_text(
                        f"⚪️ Позиция `{symbol}` уже закрыта.\n\n{list_text}",
                        parse_mode='Markdown',
                        reply_markup=list_kb
                    )
                    return

                idx = ordered.index(symbol) if symbol in ordered else 0
                prev_symbol = ordered[idx - 1]
                next_symbol = ordered[(idx + 1) % len(ordered)]
                cur_raw = symbol.replace("/", "_")
                details_text = _build_position_details_view(symbol, trades[symbol])
                details_text = f"{details_text}\n\n🧭 `{idx + 1}/{len(ordered)}`"
                keyboard = [
                    [
                        InlineKeyboardButton("◀️", callback_data=f"pos_nav_{prev_symbol.replace('/', '_')}"),
                        InlineKeyboardButton("📋 К списку", callback_data="pos_back_list"),
                        InlineKeyboardButton("▶️", callback_data=f"pos_nav_{next_symbol.replace('/', '_')}"),
                    ],
                    [
                        InlineKeyboardButton("🔄 Обновить", callback_data=f"pos_refresh_{cur_raw}"),
                        InlineKeyboardButton("🧯 Сократить 25%", callback_data=f"pos_reduce25_{cur_raw}"),
                        InlineKeyboardButton("🧯 Сократить 50%", callback_data=f"pos_reduce50_{cur_raw}"),
                    ],
                    [InlineKeyboardButton("❌ Закрыть позицию", callback_data=f"close_{cur_raw}")]
                ]
                await query.edit_message_text(
                    details_text,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка Pro-действия для `{symbol}`: {e}", parse_mode='Markdown')

    elif query.data.startswith("close_"):
        symbol_raw = query.data.replace("close_", "")
        if _is_action_on_cooldown(user_id, "close", symbol_raw):
            lvl = _spam_level_badge(user_id, "close", symbol_raw)
            await _safe_answer_callback(query, f"⏱ Закрытие уже отправлено, подожди 2-3 сек... · {lvl}", show_alert=False)
            return
        symbol = symbol_raw.replace("_", "/")
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/trades/close/{symbol_raw}", timeout=10.0)
                res = response.json()
                
                if res.get("status") == "success":
                    await query.edit_message_text(f"✅ Позиция **{symbol}** успешно закрыта вручную.", parse_mode='Markdown')
                else:
                    await query.edit_message_text(f"❌ Ошибка закрытия **{symbol}**: {res.get('message')}")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка связи с движком: {e}")

    elif query.data == "menu_leverage":
        runtime = await _load_runtime_settings_for_menu()
        lev = runtime.get("leverage", settings.leverage)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{'✅ ' if lev == v else ''}{v}x", callback_data=f"rt_leverage_{v}") for v in [5, 10, 15]],
            [InlineKeyboardButton(f"{'✅ ' if lev == v else ''}{v}x", callback_data=f"rt_leverage_{v}") for v in [20, 25, 50]],
            [InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")],
        ])
        await query.edit_message_text(
            f"⚡ **НАСТРОЙКА ПЛЕЧА**\n\nТекущее плечо: `{lev}x`\n\nВыберите значение:",
            reply_markup=kb, parse_mode='Markdown'
        )

    elif query.data == "menu_margin":
        runtime = await _load_runtime_settings_for_menu()
        margin = runtime.get("per_trade_margin_pct", settings.per_trade_margin_pct)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➖ 1%", callback_data="rt_margin_dec"),
             InlineKeyboardButton(f"💰 {margin*100:.1f}%", callback_data="noop"),
             InlineKeyboardButton("➕ 1%", callback_data="rt_margin_inc")],
            [InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")],
        ])
        await query.edit_message_text(
            f"💰 **МАРЖА НА СДЕЛКУ**\n\nТекущая маржа: `{margin*100:.1f}%`\n\nИзменяйте кнопками ±1%:",
            reply_markup=kb, parse_mode='Markdown'
        )

    elif query.data == "menu_positions":
        runtime = await _load_runtime_settings_for_menu()
        max_t = runtime.get("max_open_trades", settings.max_open_trades)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➖ 1", callback_data="rt_open_trades_dec"),
             InlineKeyboardButton(f"📂 {max_t}", callback_data="noop"),
             InlineKeyboardButton("➕ 1", callback_data="rt_open_trades_inc")],
            [InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")],
        ])
        await query.edit_message_text(
            f"📂 **МАКС. ОТКРЫТЫХ ПОЗИЦИЙ**\n\nТекущий лимит: `{max_t}`\n\nИзменяйте кнопками:",
            reply_markup=kb, parse_mode='Markdown'
        )

    elif query.data == "menu_tp_sl":
        runtime = await _load_runtime_settings_for_menu()
        tp = runtime.get("tp_pct", settings.tp_pct)
        sl_l = settings.sl_long_pct
        sl_s = settings.sl_short_pct
        sl_corr = settings.sl_correction_enabled
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎯 TP -0.1%", callback_data="rt_tp_dec"),
             InlineKeyboardButton("🎯 TP +0.1%", callback_data="rt_tp_inc")],
            [InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")],
        ])
        await query.edit_message_text(
            f"🎯 **ТЕЙК-ПРОФИТ / СТОП-ЛОСС**\n\n"
            f"🎯 Тейк-профит: `{tp*100:.2f}%`\n"
            f"🟢 SL Long: `{sl_l*100:.1f}%`\n"
            f"🔴 SL Short: `{sl_s*100:.1f}%`\n"
            f"🛡 Коррекция SL: {'✅ Вкл' if sl_corr else '❌ Выкл'}\n\n"
            f"Изменяйте TP кнопками:",
            reply_markup=kb, parse_mode='Markdown'
        )

    elif query.data == "menu_side":
        runtime = await _load_runtime_settings_for_menu()
        side = runtime.get('allowed_position_side', getattr(settings, 'allowed_position_side', 'BOTH'))
        _mark = lambda v: "✅ " if side.upper() == v else ""
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{_mark('LONG')}🟢 LONG", callback_data="rt_side_long"),
             InlineKeyboardButton(f"{_mark('SHORT')}🔴 SHORT", callback_data="rt_side_short"),
             InlineKeyboardButton(f"{_mark('BOTH')}↕️ BOTH", callback_data="rt_side_both")],
            [InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")],
        ])
        await query.edit_message_text(
            f"↕️ **НАПРАВЛЕНИЕ ТОРГОВЛИ**\n\nТекущее: `{side}`\n\nВыберите:",
            reply_markup=kb, parse_mode='Markdown'
        )

    elif query.data == "menu_volume":
        runtime = await _load_runtime_settings_for_menu()
        pos_usdt = runtime.get('position_size_usdt', getattr(settings, 'position_size_usdt', 0.0)) or 0.0
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➖ 1$", callback_data="rt_pos_usdt_dec"),
             InlineKeyboardButton(f"💵 {pos_usdt:.0f}$", callback_data="noop"),
             InlineKeyboardButton("➕ 1$", callback_data="rt_pos_usdt_inc")],
            [InlineKeyboardButton("➖ 5$", callback_data="rt_pos_usdt_dec5"),
             InlineKeyboardButton("➕ 5$", callback_data="rt_pos_usdt_inc5")],
            [InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")],
        ])
        await query.edit_message_text(
            f"💵 **ОБЪЁМ ПОЗИЦИИ**\n\nТекущий: `{pos_usdt:.0f} USDT` {'(авто)' if pos_usdt == 0 else ''}\n\n"
            f"0 = рассчитывается автоматически по марже",
            reply_markup=kb, parse_mode='Markdown'
        )

    elif query.data == "menu_expiry":
        runtime = await _load_runtime_settings_for_menu()
        exp = runtime.get("signal_expiry_seconds", settings.signal_expiry_seconds)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➖ 10с", callback_data="rt_expiry_dec"),
             InlineKeyboardButton(f"⏱ {exp}с", callback_data="noop"),
             InlineKeyboardButton("➕ 10с", callback_data="rt_expiry_inc")],
            [InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")],
        ])
        await query.edit_message_text(
            f"⏱ **СРОК ДЕЙСТВИЯ СИГНАЛА**\n\nТекущий: `{exp}с`\n\n"
            f"Сигналы старше этого времени игнорируются.",
            reply_markup=kb, parse_mode='Markdown'
        )

    elif query.data == "menu_presets":
        try:
            async with httpx.AsyncClient() as client:
                presets_resp = await client.get(f"{ENGINE_URL}/api/v1/presets", timeout=5.0)
                presets_resp.raise_for_status()
                presets = presets_resp.json() if isinstance(presets_resp.json(), list) else []
            btns = []
            for p in presets:
                label = f"✅ {p['name']}" if p.get('is_active') else f"🔘 {p['name']}"
                btns.append([InlineKeyboardButton(label, callback_data=f"apply_preset_{p['name']}")])
            btns.append([InlineKeyboardButton("◀️ Назад", callback_data="menu_back_settings")])
            await query.edit_message_text(
                "🎯 **ПРЕСЕТЫ РИСКА**\n\nВыберите готовый профиль:",
                reply_markup=InlineKeyboardMarkup(btns), parse_mode='Markdown'
            )
        except Exception as e:
            await query.edit_message_text(f"❌ Не удалось загрузить пресеты: {e}")

    elif query.data == "menu_back_settings":
        await _render_settings_message(query, edit=True)

    elif query.data == "noop":
        pass

    elif query.data == "rt_toggle_trading":
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/toggle", timeout=5.0)
                res = response.json()
                st = "✅ ВКЛ" if res.get("is_enabled") else "❌ ВЫКЛ"
                await _safe_answer_callback(query, f"🔄 Торговля: {st}")
                await _render_settings_message(query, edit=True)
        except Exception as e:
            await _safe_answer_callback(query, f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data.startswith("apply_preset_"):
        preset_name = query.data.replace("apply_preset_", "")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/presets/apply/{preset_name}", timeout=5.0)
                res = response.json()
                if res.get("status") == "success":
                    await _safe_answer_callback(query, f"✅ Пресет {preset_name} активирован!")
                    await _render_settings_message(query, edit=True)
                else:
                    await _safe_answer_callback(query, f"❌ Ошибка: {res.get('message')}", show_alert=True)
        except Exception as e:
            await _safe_answer_callback(query, f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data == "rt_toggle_pyramiding":
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/runtime-settings/pyramiding/toggle", timeout=5.0)
                res = response.json()
                if res.get("status") == "success":
                    await _safe_answer_callback(query, "✅ Пирамидинг обновлён")
                    await _render_settings_message(query, edit=True)
                else:
                    await _safe_answer_callback(query, "❌ Ошибка обновления пирамидинга", show_alert=True)
        except Exception as e:
            await _safe_answer_callback(query, f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data in {"rt_margin_dec", "rt_margin_inc", "rt_open_trades_dec", "rt_open_trades_inc", "rt_pos_usdt_dec", "rt_pos_usdt_inc", "rt_pos_usdt_dec5", "rt_pos_usdt_inc5", "rt_tp_dec", "rt_tp_inc", "rt_expiry_dec", "rt_expiry_inc"}:
        try:
            async with httpx.AsyncClient() as client:
                runtime = await _load_runtime_settings(client)
                if query.data.startswith("rt_margin"):
                    cur = float(runtime.get("per_trade_margin_pct", settings.per_trade_margin_pct))
                    new_val = cur - 0.01 if query.data.endswith("dec") else cur + 0.01
                    response = await client.post(
                        f"{ENGINE_URL}/api/v1/runtime-settings/per-trade-margin",
                        params={"value": new_val},
                        timeout=5.0
                    )
                    res = response.json()
                    if res.get("status") == "success":
                        await _safe_answer_callback(query, f"✅ Маржа: {res.get('per_trade_margin_pct', new_val)*100:.1f}%")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _safe_answer_callback(query, "❌ Не удалось обновить маржу", show_alert=True)
                elif query.data.startswith("rt_open_trades"):
                    cur = int(runtime.get("max_open_trades", settings.max_open_trades))
                    new_val = cur - 1 if query.data.endswith("dec") else cur + 1
                    response = await client.post(
                        f"{ENGINE_URL}/api/v1/runtime-settings/max-open-trades",
                        params={"value": new_val},
                        timeout=5.0
                    )
                    res = response.json()
                    if res.get("status") == "success":
                        await _safe_answer_callback(query, f"✅ Макс. сделок: {res.get('max_open_trades', new_val)}")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _safe_answer_callback(query, "❌ Не удалось обновить лимит сделок", show_alert=True)
                elif query.data.startswith("rt_pos_usdt"):
                    cur = float(runtime.get("position_size_usdt", getattr(settings, "position_size_usdt", 0.0)) or 0.0)
                    if query.data.endswith("dec5"):
                        step = -5.0
                    elif query.data.endswith("inc5"):
                        step = 5.0
                    elif query.data.endswith("dec"):
                        step = -1.0
                    else:
                        step = 1.0
                    new_val = max(0.0, min(100000.0, cur + step))
                    response = await client.post(
                        f"{ENGINE_URL}/api/v1/runtime-settings/position-size-usdt",
                        params={"value": new_val},
                        timeout=5.0
                    )
                    res = response.json()
                    if res.get("status") == "success":
                        await _safe_answer_callback(query, f"✅ Объём: {res.get('position_size_usdt', new_val):.2f} USDT")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _safe_answer_callback(query, "❌ Не удалось обновить объём", show_alert=True)
                elif query.data.startswith("rt_tp"):
                    cur = float(runtime.get("tp_pct", settings.tp_pct) or settings.tp_pct)
                    step = 0.001
                    new_val = cur - step if query.data.endswith("dec") else cur + step
                    response = await client.post(
                        f"{ENGINE_URL}/api/v1/runtime-settings/tp-pct",
                        params={"value": new_val},
                        timeout=5.0
                    )
                    res = response.json()
                    if res.get("status") == "success":
                        await _safe_answer_callback(query, f"✅ TP: {res.get('tp_pct', new_val)*100:.2f}%")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _safe_answer_callback(query, "❌ Не удалось обновить TP", show_alert=True)
                else:
                    cur = int(runtime.get("signal_expiry_seconds", settings.signal_expiry_seconds))
                    new_val = cur - 10 if query.data.endswith("dec") else cur + 10
                    response = await client.post(
                        f"{ENGINE_URL}/api/v1/runtime-settings/signal-expiry",
                        params={"value": new_val},
                        timeout=5.0
                    )
                    res = response.json()
                    if res.get("status") == "success":
                        await _safe_answer_callback(query, f"✅ Expiry: {res.get('signal_expiry_seconds', new_val)}с")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _safe_answer_callback(query, "❌ Не удалось обновить expiry", show_alert=True)
        except Exception as e:
            await _safe_answer_callback(query, f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data in {"rt_side_long", "rt_side_short", "rt_side_both"}:
        value = query.data.replace("rt_side_", "").upper()
        if value == "BOTH":
            value = "BOTH"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{ENGINE_URL}/api/v1/runtime-settings/allowed-side",
                    params={"value": value},
                    timeout=5.0
                )
                res = response.json()
                if res.get("status") == "success":
                    await _safe_answer_callback(query, f"✅ Тип позиции: {res.get('allowed_position_side', value)}")
                    await _render_settings_message(query, edit=True)
                else:
                    await _safe_answer_callback(query, f"❌ {res.get('message', 'Не удалось обновить тип позиции')}", show_alert=True)
        except Exception as e:
            await _safe_answer_callback(query, f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data.startswith("rt_leverage_"):
        try:
            leverage_value = int(query.data.replace("rt_leverage_", ""))
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{ENGINE_URL}/api/v1/runtime-settings/leverage",
                    params={"value": leverage_value},
                    timeout=8.0,
                )
                res = response.json()
                if res.get("status") == "success":
                    await _safe_answer_callback(query, f"✅ Плечо: {res.get('leverage', leverage_value)}x")
                    await _render_settings_message(query, edit=True)
                else:
                    await _safe_answer_callback(query, f"❌ {res.get('message', 'Не удалось обновить плечо')}", show_alert=True)
        except Exception as e:
            await _safe_answer_callback(query, f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data == "reset_stats":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟡 Binance", callback_data="reset_stats_binance")],
            [InlineKeyboardButton("🌐 Все биржи", callback_data="reset_stats_all")],
            [InlineKeyboardButton("❌ Отмена", callback_data="reset_stats_cancel")],
        ])
        await query.edit_message_text(
            "⚠️ **Сброс статистики**\n\nВыберите биржу, для которой нужно сбросить статистику.",
            reply_markup=kb,
            parse_mode='Markdown'
        )
    elif query.data in {"reset_stats_binance", "reset_stats_all"}:
        scope = "binance" if query.data.endswith("binance") else "all"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/stats/reset", params={"scope": scope}, timeout=8.0)
                res = response.json()
                if res.get("status") == "success":
                    await query.edit_message_text(f"✅ Статистика успешно сброшена (`{scope}`).", parse_mode='Markdown')
                else:
                    await query.edit_message_text(f"❌ Ошибка сброса: {res.get('message')}")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка связи с движком: {e}")
    elif query.data == "reset_stats_cancel":
        await query.edit_message_text("❎ Сброс статистики отменён.")

# ======= ANTI-SPAM & RATE LIMITING (Этап 18) =======
# Хранилище: {user_id: [timestamp1, timestamp2, ...]}
user_message_times = defaultdict(list)
RATE_LIMIT_MESSAGES = 10      # Максимум 10 сообщений
RATE_LIMIT_WINDOW = 5.0      # за 5 секунд

async def rate_limit_middleware(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Проверяет, не спамит ли пользователь.
    В python-telegram-bot v20+ это можно сделать через TypeHandler(Update, ...).
    """
    # Если апдейт не содержит сообщения от пользователя, пропускаем
    if not update.effective_user or not update.effective_message:
        return
        
    user_id = update.effective_user.id
    current_time = time.time()
    
    # Очищаем старые метки времени
    user_message_times[user_id] = [
        ts for ts in user_message_times[user_id] 
        if current_time - ts < RATE_LIMIT_WINDOW
    ]
    
    if len(user_message_times[user_id]) >= RATE_LIMIT_MESSAGES:
        logging.warning(f"Пользователь {user_id} заблокирован за спам.")
        await update.effective_message.reply_text("⛔️ Слишком много запросов. Подождите пару секунд.")
        # Прерываем обработку (raise DropUpdate в реальном приложении)
        raise ApplicationHandlerStop()
        
    user_message_times[user_id].append(current_time)
# ==================================================

def run_bot():
    app = ApplicationBuilder().token(settings.telegram_bot_token.get_secret_value()).build()

    # Исключение для остановки хэндлеров
    global ApplicationHandlerStop
    from telegram.ext import ApplicationHandlerStop

    # Добавляем Rate Limit Middleware с высшим приоритетом (-1)
    app.add_handler(TypeHandler(Update, rate_limit_middleware), group=-1)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("connect_exchange", connect_exchange))
    app.add_handler(CommandHandler("start_trading", status)) # mock
    app.add_handler(CommandHandler("stop_trading", status)) # mock
    
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(CallbackQueryHandler(callback_handler))

    logging.info("Telegram Bot is polling...")
    app.run_polling()

if __name__ == "__main__":
    run_bot()
