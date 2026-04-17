import logging
import asyncio
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, TypeHandler, CallbackQueryHandler
from telegram.error import BadRequest
import time
import httpx
from contextlib import asynccontextmanager

# Global client for preventing TIME_WAIT exhaustion
global_client = httpx.AsyncClient(
    timeout=httpx.Timeout(connect=3.0, read=10.0, write=5.0, pool=3.0),
    headers={"X-API-Key": settings.internal_api_key}
)

@asynccontextmanager
async def get_http_client(*args, **kwargs):
    # Ignore kwargs like timeout here, let global client handle it
    yield global_client

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

from utils.logger import app_logger as logger

# _log = logging.getLogger(__name__) # Use logger instead

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id if update.effective_user else 0
        allowed_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]
        if user_id not in allowed_ids:
            if update.callback_query:
                await _safe_answer_callback(update.callback_query, "⛔️ Нет доступа", show_alert=True)
            elif update.message:
                await update.message.reply_text("⛔️ Доступ запрещен. Вы не являетесь администратором.")
            return
        return await func(update, context)
    return wrapper

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
    async with get_http_client() as client:
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
        [KeyboardButton(BTN_FAQ), KeyboardButton(BTN_HOME)],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def _load_runtime_settings_for_menu() -> dict:
    timeout = httpx.Timeout(connect=2.5, read=8.0, write=5.0, pool=3.0)
    async with get_http_client() as client:
        return await _load_runtime_settings(client)


async def _render_main_menu(message_target):
    await message_target.reply_text(
        "🤖 **Algo Quant Bot**\n\nВыберите раздел для управления ботом:",
        reply_markup=_build_main_menu_markup(),
        parse_mode='Markdown',
    )


def _build_faq_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎛 Управление", callback_data="faq_controls"),
         InlineKeyboardButton("⚙️ Настройки", callback_data="faq_settings")],
        [InlineKeyboardButton("🛡 Риск-логика", callback_data="faq_risk"),
         InlineKeyboardButton("🤖 Сигналы и AI", callback_data="faq_ai")],
        [InlineKeyboardButton("🚫 Почему нет входа", callback_data="faq_no_entry"),
         InlineKeyboardButton("🏁 Причины закрытия", callback_data="faq_close_reasons")],
        [InlineKeyboardButton("🔄 Синхронизация", callback_data="faq_sync")],
    ])


def _faq_text_by_section(section: str) -> str:
    if section == "controls":
        return (
            "🎛 **FAQ: УПРАВЛЕНИЕ**\n\n"
            "• `🔄 Вкл/Выкл` — включает/выключает автоторговлю сразу.\n"
            "• `💼 Активные позиции` — список позиций + действия (обновить/сократить/закрыть).\n"
            "• `📜 История сделок` — последние закрытые сделки с причиной и PnL.\n"
            "• `📈 Статистика` — агрегаты по закрытым сделкам и сброс статистики.\n"
            "• `📉 Сигналы` — свежие сигналы из БД со статусом.\n"
            "• `📚 Стратегии` — краткая методология бота."
        )
    if section == "settings":
        return (
            "⚙️ **FAQ: НАСТРОЙКИ**\n\n"
            "• `⚡ Плечо` — runtime-плечо без рестарта.\n"
            "• `💰 Маржа` — доля капитала на 1 сделку (процент).\n"
            "• `📂 Позиции` — лимит одновременных сделок.\n"
            "• `🎯 TP/SL` — TP runtime, SL рассчитывается риск-движком.\n"
            "• `↕️ Направление` — LONG / SHORT / BOTH.\n"
            "• `💵 Объём` — фикс USDT на сделку (0 = авто-расчёт).\n"
            "• `💎 Пирамидинг` — доп. входы по правилам.\n"
            "• `⏱ Expiry` — защита от устаревших сигналов."
        )
    if section == "risk":
        return (
            "🛡 **FAQ: РИСК-ЛОГИКА**\n\n"
            "• Стоп-лосс: ATR-расчёт + минимальная дистанция.\n"
            "• Трейлинг: стоп двигается только в сторону снижения риска.\n"
            "• Безубыток: перенос стопа при 1R + подтверждение (ADX/пробой).\n"
            "• Daily halt: блок входов при превышении дневного лимита просадки.\n"
            "• Корреляционный фильтр: ограничивает одинаковые направления внутри кластеров.\n"
            "• Листинг-фильтр: отсекает слишком новые инструменты."
        )
    if section == "ai":
        return (
            "🤖 **FAQ: СИГНАЛЫ И AI**\n\n"
            "• Сигнал проходит технический скоринг и AI-фильтр.\n"
            "• Минимальные пороги: score и win probability.\n"
            "• Внешний AI работает каскадом по доступным провайдерам.\n"
            "• Если провайдер недоступен/в лимите — используется следующий.\n"
            "• Решения AI логируются для последующей аналитики/обучения."
        )
    if section == "no_entry":
        return (
            "🚫 **FAQ: ПОЧЕМУ НЕТ ВХОДА**\n\n"
            "• достигнут лимит позиций,\n"
            "• автоторговля выключена,\n"
            "• сигнал устарел,\n"
            "• score/win_prob ниже порога,\n"
            "• фильтр листинга/корреляции/funding не пройден,\n"
            "• активирован дневной стоп по просадке,\n"
            "• биржа отклонила ордер (правила/лимиты инструмента)."
        )
    if section == "close_reasons":
        return (
            "🏁 **FAQ: ПРИЧИНЫ ЗАКРЫТИЯ**\n\n"
            "• `🔄 Биржа (TP/SL)` — сработал защитный ордер на бирже.\n"
            "• `🖐 Ручное` — закрытие через Telegram-кнопку.\n"
            "• `⏱ Тайм-аут` — выход по time-exit.\n"
            "• `⚙️ Авто` — закрытие внутренней логикой бота."
        )
    if section == "sync":
        return (
            "🔄 **FAQ: СИНХРОНИЗАЦИЯ**\n\n"
            "• Есть регулярный reconcile: биржа ↔ БД ↔ память.\n"
            "• `/api/v1/trades` перед выдачей делает принудительную синхронизацию.\n"
            "• Локальный кеш `active_trades` мягко очищается по live-позициям.\n"
            "• Если позиция закрыта внешне, она должна исчезнуть из списка активных.\n"
            "• При сетевых сбоях возможны краткие задержки обновления, затем данные выравниваются."
        )
    return "❓ Раздел FAQ не найден."


@admin_only
async def show_api_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with get_http_client() as client:
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

@admin_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Этап 14-15: Главное меню
    """
    await _render_main_menu(update.message)

@admin_only
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with get_http_client() as client:
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

@admin_only
async def connect_exchange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔑 **Настройка API-ключей**\n\n"
        "Для подключения биржи отредактируйте файл `.env` и перезапустите контейнеры.\n\n"
        "⚠️ Никогда не отправляйте ключи в чат!",
        parse_mode='Markdown'
    )


@admin_only
async def toggle_trading(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command = update.message.text.split()[0] if update.message.text else ""
    want_enabled = command == "/start_trading"
    try:
        async with get_http_client() as client:
            resp = await client.get(f"{ENGINE_URL}/api/v1/status", timeout=5.0)
            status_data = resp.json()
            is_running = status_data.get("status") == "running"
            if want_enabled == is_running:
                st = "уже включена" if is_running else "уже выключена"
                await update.message.reply_text(f"ℹ️ Автоторговля {st}.")
                return
            resp = await client.post(f"{ENGINE_URL}/api/v1/toggle", timeout=5.0)
            res = resp.json()
            st = "✅ ВКЛ" if res.get("is_enabled") else "❌ ВЫКЛ"
            await update.message.reply_text(f"🔄 Автоторговля: {st}")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка переключения: {e}")


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


def _build_positions_list_view(trades: dict, page: int = 1, page_size: int = 5) -> tuple[str, InlineKeyboardMarkup]:
    enriched = _sorted_positions_by_risk(trades)
    total_positions = len(enriched)
    total_pages = (total_positions + page_size - 1) // page_size
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    paged_items = enriched[start_idx:end_idx]
    
    list_lines = [f"📌 **СПИСОК ПОЗИЦИЙ (Страница {page}/{total_pages}):**"]
    keyboard_rows = []
    
    for _, symbol, info, risk_tag in paged_items:
        side_emoji = "🟢 LONG" if info.get('signal_type') == "LONG" else "🔴 SHORT"
        pnl_usd = float(info.get('pnl_usd', 0.0) or 0.0)
        pnl_badge = "🟢" if pnl_usd >= 0 else "🔴"
        list_lines.append(f"• `{symbol}` · {side_emoji} · {risk_tag} · {pnl_badge} `{pnl_usd:+.2f}$`")
        sraw = symbol.replace("/", "_")
        keyboard_rows.append([
            InlineKeyboardButton(f"ℹ️ {symbol}", callback_data=f"pos_view_{sraw}"),
            InlineKeyboardButton("❌", callback_data=f"close_{sraw}"),
        ])
    
    # Navigation buttons
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"pos_page_{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"pos_page_{page+1}"))
    
    if nav_buttons:
        keyboard_rows.append(nav_buttons)
        
    keyboard_rows.append([InlineKeyboardButton("🔄 Обновить", callback_data="pos_page_1")])
    
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


@admin_only
async def show_active_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with get_http_client() as client:
            data = await _get_json_with_retry(client, f"{ENGINE_URL}/api/v1/trades", timeout=15.0, retries=2)
            trades = data.get("trades", {})

            if not trades:
                await update.message.reply_text("📂 **Активных позиций на данный момент нет.**", parse_mode='Markdown')
                return

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

            list_text, list_kb = _build_positions_list_view(trades, page=1)
            await update.message.reply_text(list_text, parse_mode='Markdown', reply_markup=list_kb)

    except httpx.TimeoutException:
        logging.error("Таймаут при запросе активных позиций к движку")
        await update.message.reply_text("⏱ Движок не ответил вовремя (таймаут). Попробуйте через 10 сек.")
    except httpx.ConnectError:
        logging.error("Нет подключения к движку торгов")
        await update.message.reply_text("❌ Нет подключения к движку торгов. Убедитесь, что trading-engine запущен.")
    except Exception as e:
        logging.error(f"Ошибка получения позиций: {e!r}")
        await update.message.reply_text(f"❌ Ошибка загрузки позиций:\n`{_escape_md(str(e)[:150])}`", parse_mode='Markdown')


_HISTORY_REASON_MAP = {
    "STOP": "🛑 стоп",
    "STOP_LOSS": "🛑 стоп-лосс",
    "TAKE": "🎯 тейк",
    "TAKE_PROFIT": "🎯 тейк-профит",
    "TRAILING_STOP": "📐 трейлинг",
    "MANUAL": "🖐 ручное",
    "TIME": "⏱ тайм-аут",
    "EXTERNAL": "🔄 биржа (TP/SL)",
    "AUTO": "⚙️ авто",
    "RECONCILE_CLOSE": "🔃 реконсил",
    "BREAKEVEN": "🔰 безубыток",
    "DAILY_DRAWDOWN": "📉 дневная просадка",
}


def _escape_md(text: str) -> str:
    """Escape Markdown v1 special chars in dynamic text to prevent BadRequest."""
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


def _format_history_pct(pnl_pct: float, pnl_usd: float, notional_usd: float | None = None) -> tuple[str, bool]:
    """
    Format pnl_pct for human-readable history cards.
    Extremely large percentages are usually caused by tiny position notionals,
    so we cap display and mark it as small-notional.
    """
    pct = float(pnl_pct or 0.0)
    abs_pct = abs(pct)
    abs_usd = abs(float(pnl_usd or 0.0))
    abs_notional = abs(float(notional_usd or 0.0))

    # Prefer explicit notional when provided by backend.
    # Fallback to pnl_usd heuristic for compatibility with older payloads.
    small_notional = (abs_notional > 0 and abs_notional <= 25.0) or (abs_usd <= 200.0)
    if abs_pct >= 300.0 and small_notional:
        sign = "+" if pct >= 0 else "-"
        return f"{sign}299.99%*", True

    return f"{pct:+.2f}%", False


@admin_only
async def show_trade_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with get_http_client() as client:
            data = await _get_json_with_retry(
                client,
                f"{ENGINE_URL}/api/v1/history",
                params={"limit": 20},
                timeout=10.0,
                retries=2,
            )
            items = data.get("items", [])
            if not items:
                await update.message.reply_text("📜 История сделок пуста.")
                return

            lines = [f"📜 **ИСТОРИЯ СДЕЛОК ({len(items)}):**\n"]
            has_small_notional_mark = False
            for it in items:
                pnl_usd = float(it.get("pnl_usd", 0.0) or 0.0)
                pnl_pct = float(it.get("pnl_pct", 0.0) or 0.0)
                notional_usd = float(it.get("notional_usd", 0.0) or 0.0)
                pnl_pct_text, pct_marked = _format_history_pct(pnl_pct, pnl_usd, notional_usd)
                if pct_marked:
                    has_small_notional_mark = True
                emoji = "🟢" if pnl_usd >= 0 else "🔴"
                ts = (it.get("closed_at") or "").replace("T", " ")[:19] if it.get("closed_at") else "N/A"
                reason = str(it.get('reason', 'AUTO') or 'AUTO').upper()
                reason_ru = _HISTORY_REASON_MAP.get(reason, f"⚙️ {_escape_md(reason.lower())}")
                symbol = _escape_md(str(it.get('symbol', 'N/A')))
                notional_text = f"{notional_usd:.2f}" if notional_usd > 0 else "N/A"
                lines.append(
                    f"🧾 **{symbol}**\n"
                    f"{emoji} `{pnl_usd:+.2f} USDT / {pnl_pct_text}`\n"
                    f"🧮 Notional: `{notional_text} USDT`\n"
                    f"🏁 {reason_ru} · 🕒 `{ts}`\n───"
                )

            if has_small_notional_mark:
                lines.append(
                    "\nℹ️ `*` Процент ограничен для very small-notional позиций "
                    "(ориентируйтесь в первую очередь на `USDT PnL`)."
                )

            full_text = "\n".join(lines)
            if len(full_text) > 4000:
                for i in range(0, len(full_text), 4000):
                    chunk = full_text[i:i + 4000]
                    await update.message.reply_text(chunk, parse_mode='Markdown')
            else:
                await update.message.reply_text(full_text, parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Ошибка истории сделок: {e!r}")
        await update.message.reply_text(f"❌ Не удалось получить историю сделок.\n`{_escape_md(str(e)[:120])}`", parse_mode='Markdown')

@admin_only
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    logger.info(f"📩 ПОЛУЧЕНО СООБЩЕНИЕ: '{text}' от {user_id}")
    if text == BTN_ACTIVE:
        await show_active_positions(update, context)
    elif text == BTN_TOGGLE:
        try:
            async with get_http_client() as client:
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
        try:
            async with get_http_client() as client:
                resp = await client.get(f"{ENGINE_URL}/api/v1/signals?limit=5", timeout=5.0)
                resp.raise_for_status()
                signals = resp.json()

            if not signals:
                await update.message.reply_text("📭 Актуальных сигналов за последние 6 часов нет.")
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
                v = float(val)
                return f"{v:.4f}" if v < 1.0 else f"{v:.2f}"

            for s in signals:
                sig_side = str(s.get("signal_type", "")).upper()
                signal_type_ru = "🟢 LONG" if sig_side == "LONG" else "🔴 SHORT"
                status_raw = s.get("status", "UNKNOWN")
                status_ru = status_map.get(status_raw, f"❓ {status_raw}")
                
                win_p = s.get("win_prob", 0.0) or 0.0
                conf = s.get("confidence", 0.0) or 0.0
                ts = str(s.get("timestamp", "N/A"))[:19].replace("T", " ")

                msg = (
                    f"🚀 **СИГНАЛ: {s.get('strategy', 'Unknown')}**\n\n"
                    f"🔸 **Символ:** {s.get('symbol', 'N/A')}\n"
                    f"🔸 **Направление:** {signal_type_ru}\n\n"
                    f"💰 **Цена входа:** {fmt_p(s.get('entry_price'))}\n"
                    f"🛡 **Stop Loss:** {fmt_p(s.get('stop_loss', 0))}\n"
                    f"🎯 **Take Profit:** {fmt_p(s.get('take_profit', 0))}\n\n"
                    f"🕒 **Время (UTC):** {ts}\n\n"
                    f"🤖 **AI ВЕРДИКТ:**\n"
                    f"📈 **Вероятность успеха:** {int(win_p * 100)}%\n"
                    f"💰 **Ож. доходность:** {s.get('expected_return', '0.0')}%\n"
                    f"⚠️ **Уровень риска:** {s.get('risk', '1.0')}\n"
                    f"📊 **AI Score:** {conf:.2f}\n\n"
                    f"ℹ️ **Статус:** {status_ru}\n"
                    f"📁 **Источник:** {s.get('source', 'unknown')}\n"
                    f"───────────────────"
                )
                await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logging.error(f"Ошибка получения сигналов из REST API: {e}")
            await update.message.reply_text("❌ Ошибка при обращении к торговому движку.")
    elif text == BTN_FAQ:
        await update.message.reply_text(
            "❓ **FAQ БОТА**\n\nВыберите раздел:",
            parse_mode='Markdown',
            reply_markup=_build_faq_menu_keyboard()
        )
    elif text == BTN_STATS:
        try:
            async with get_http_client() as client:
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
@admin_only
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id if update.effective_user else 0
    _answered = False

    async def _answer_once(text: str | None = None, show_alert: bool = False):
        nonlocal _answered
        if not _answered:
            _answered = True
            await _safe_answer_callback(query, text=text, show_alert=show_alert)

    if query.data.startswith("pos_page_"):
        await _answer_once()
        try:
            page = int(query.data.replace("pos_page_", ""))
        except: page = 1
        try:
            async with get_http_client() as client:
                data = await _get_json_with_retry(client, f"{ENGINE_URL}/api/v1/trades", timeout=8.0, retries=1)
                trades = data.get("trades", {})
                if not trades:
                    await query.edit_message_text("📂 **Активных позиций на данный момент нет.**", parse_mode='Markdown')
                    return
                list_text, list_kb = _build_positions_list_view(trades, page=page)
                await query.edit_message_text(list_text, parse_mode='Markdown', reply_markup=list_kb)
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка пагинации: {e}")
        return

    if query.data.startswith("pos_view_") or query.data.startswith("pos_nav_") or query.data == "pos_back_list":
        await _answer_once()
        symbol_raw = ""
        if query.data.startswith("pos_view_"):
            symbol_raw = query.data.replace("pos_view_", "")
        elif query.data.startswith("pos_nav_"):
            symbol_raw = query.data.replace("pos_nav_", "")
        symbol = symbol_raw.replace("_", "/") if symbol_raw else ""
        try:
            async with get_http_client() as client:
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
            await _answer_once(f"⏱ Слишком часто, подожди {wait_txt} · {lvl}")
            return

        symbol = symbol_raw.replace("_", "/")
        try:
            async with get_http_client() as client:
                if action in {"reduce25", "reduce50"}:
                    fraction = 0.25 if action == "reduce25" else 0.50
                    r = await client.post(
                        f"{ENGINE_URL}/api/v1/trades/reduce/{symbol_raw}",
                        params={"fraction": fraction},
                        timeout=10.0
                    )
                    res = r.json()
                    if res.get("status") == "success":
                        await _answer_once(f"✅ Сокращение {int(fraction*100)}% отправлено")
                    else:
                        await _answer_once(
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
            await _answer_once(f"⏱ Закрытие уже отправлено, подожди 2-3 сек... · {lvl}")
            return
        await _answer_once()
        symbol = symbol_raw.replace("_", "/")
        
        try:
            async with get_http_client() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/trades/close/{symbol_raw}", timeout=10.0)
                res = response.json()
                
                if res.get("status") == "success":
                    await query.edit_message_text(f"✅ Позиция **{symbol}** успешно закрыта вручную.", parse_mode='Markdown')
                else:
                    await query.edit_message_text(f"❌ Ошибка закрытия **{symbol}**: {res.get('message')}")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка связи с движком: {e}")

    elif query.data == "menu_leverage":
        await _answer_once()
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
        await _answer_once()
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
        await _answer_once()
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
        await _answer_once()
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
        await _answer_once()
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
        await _answer_once()
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
        await _answer_once()
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
        await _answer_once()
        try:
            async with get_http_client() as client:
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
        await _answer_once()
        await _render_settings_message(query, edit=True)

    elif query.data == "noop":
        await _answer_once()

    elif query.data == "rt_toggle_trading":
        try:
            async with get_http_client() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/toggle", timeout=5.0)
                res = response.json()
                st = "✅ ВКЛ" if res.get("is_enabled") else "❌ ВЫКЛ"
                await _answer_once(f"🔄 Торговля: {st}")
                await _render_settings_message(query, edit=True)
        except Exception as e:
            await _answer_once(f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data.startswith("apply_preset_"):
        from urllib.parse import quote
        preset_name = query.data.replace("apply_preset_", "")
        try:
            async with get_http_client() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/presets/apply/{quote(preset_name, safe='')}", timeout=5.0)
                res = response.json()
                if res.get("status") == "success":
                    await _answer_once(f"✅ Пресет {preset_name} активирован!")
                    await _render_settings_message(query, edit=True)
                else:
                    await _answer_once(f"❌ Ошибка: {res.get('message')}", show_alert=True)
        except Exception as e:
            await _answer_once(f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data == "rt_toggle_pyramiding":
        try:
            async with get_http_client() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/runtime-settings/pyramiding/toggle", timeout=5.0)
                res = response.json()
                if res.get("status") == "success":
                    await _answer_once("✅ Пирамидинг обновлён")
                    await _render_settings_message(query, edit=True)
                else:
                    await _answer_once("❌ Ошибка обновления пирамидинга", show_alert=True)
        except Exception as e:
            await _answer_once(f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data in {"rt_margin_dec", "rt_margin_inc", "rt_open_trades_dec", "rt_open_trades_inc", "rt_pos_usdt_dec", "rt_pos_usdt_inc", "rt_pos_usdt_dec5", "rt_pos_usdt_inc5", "rt_tp_dec", "rt_tp_inc", "rt_expiry_dec", "rt_expiry_inc"}:
        try:
            async with get_http_client() as client:
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
                        await _answer_once(f"✅ Маржа: {res.get('per_trade_margin_pct', new_val)*100:.1f}%")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _answer_once("❌ Не удалось обновить маржу", show_alert=True)
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
                        await _answer_once(f"✅ Макс. сделок: {res.get('max_open_trades', new_val)}")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _answer_once("❌ Не удалось обновить лимит сделок", show_alert=True)
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
                        await _answer_once(f"✅ Объём: {res.get('position_size_usdt', new_val):.2f} USDT")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _answer_once("❌ Не удалось обновить объём", show_alert=True)
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
                        await _answer_once(f"✅ TP: {res.get('tp_pct', new_val)*100:.2f}%")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _answer_once("❌ Не удалось обновить TP", show_alert=True)
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
                        await _answer_once(f"✅ Expiry: {res.get('signal_expiry_seconds', new_val)}с")
                        await _render_settings_message(query, edit=True)
                    else:
                        await _answer_once("❌ Не удалось обновить expiry", show_alert=True)
        except Exception as e:
            await _answer_once(f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data in {"rt_side_long", "rt_side_short", "rt_side_both"}:
        value = query.data.replace("rt_side_", "").upper()
        if value == "BOTH":
            value = "BOTH"
        try:
            async with get_http_client() as client:
                response = await client.post(
                    f"{ENGINE_URL}/api/v1/runtime-settings/allowed-side",
                    params={"value": value},
                    timeout=5.0
                )
                res = response.json()
                if res.get("status") == "success":
                    await _answer_once(f"✅ Тип позиции: {res.get('allowed_position_side', value)}")
                    await _render_settings_message(query, edit=True)
                else:
                    await _answer_once(f"❌ {res.get('message', 'Не удалось обновить тип позиции')}", show_alert=True)
        except Exception as e:
            await _answer_once(f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data.startswith("rt_leverage_"):
        try:
            leverage_value = int(query.data.replace("rt_leverage_", ""))
            async with get_http_client() as client:
                response = await client.post(
                    f"{ENGINE_URL}/api/v1/runtime-settings/leverage",
                    params={"value": leverage_value},
                    timeout=8.0,
                )
                res = response.json()
                if res.get("status") == "success":
                    await _answer_once(f"✅ Плечо: {res.get('leverage', leverage_value)}x")
                    await _render_settings_message(query, edit=True)
                else:
                    await _answer_once(f"❌ {res.get('message', 'Не удалось обновить плечо')}", show_alert=True)
        except Exception as e:
            await _answer_once(f"❌ Ошибка связи: {e}", show_alert=True)

    elif query.data == "reset_stats":
        await _answer_once()
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
        await _answer_once()
        scope = "binance" if query.data.endswith("binance") else "all"
        try:
            async with get_http_client() as client:
                response = await client.post(f"{ENGINE_URL}/api/v1/stats/reset", params={"scope": scope}, timeout=8.0)
                res = response.json()
                if res.get("status") == "success":
                    await query.edit_message_text(f"✅ Статистика успешно сброшена (`{scope}`).", parse_mode='Markdown')
                else:
                    await query.edit_message_text(f"❌ Ошибка сброса: {res.get('message')}")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка связи с движком: {e}")
    elif query.data == "reset_stats_cancel":
        await _answer_once()
        await query.edit_message_text("❎ Сброс статистики отменён.")
    elif query.data == "faq_back":
        await _answer_once()
        await query.edit_message_text(
            "❓ **FAQ БОТА**\n\nВыберите раздел:",
            parse_mode='Markdown',
            reply_markup=_build_faq_menu_keyboard()
        )
    elif query.data.startswith("faq_"):
        await _answer_once()
        faq_key = query.data.replace("faq_", "")
        if faq_key not in {"controls", "settings", "risk", "ai", "no_entry", "close_reasons", "sync"}:
            await query.edit_message_text(
                "❓ **FAQ БОТА**\n\nВыберите раздел:",
                parse_mode='Markdown',
                reply_markup=_build_faq_menu_keyboard()
            )
            return
        faq_text = _faq_text_by_section(faq_key)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад к FAQ", callback_data="faq_back")]
        ])
        await query.edit_message_text(faq_text, parse_mode='Markdown', reply_markup=kb)

    else:
        await _answer_once()
        logging.warning(f"Unmatched callback_data: {query.data!r} from user {user_id}")

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


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный перехватчик ошибок Telegram handlers."""
    logger.exception("Unhandled Telegram handler error", exc=context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("⚠️ Внутренняя ошибка обработчика. Попробуйте ещё раз через пару секунд.")
    except Exception:
        pass

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
    app.add_handler(CommandHandler("start_trading", toggle_trading))
    app.add_handler(CommandHandler("stop_trading", toggle_trading))
    
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(global_error_handler)

    logger.info("Telegram Bot is polling...")
    app.run_polling()

if __name__ == "__main__":
    run_bot()
