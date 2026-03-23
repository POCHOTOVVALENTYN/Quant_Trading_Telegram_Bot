import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, TypeHandler, CallbackQueryHandler
from telegram.error import BadRequest
import time
import httpx
from collections import defaultdict
from config.settings import settings

ENGINE_URL = "http://trading-engine:8000" # URL внутри Docker сети

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


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


def _build_settings_menu_text(runtime: dict) -> str:
    runtime_status = runtime.get("_runtime_status", "fallback")
    leverage_val = runtime.get("leverage", settings.leverage)
    return (
        "⚙️ **ТЕКУЩИЕ НАСТРОЙКИ**\n\n"
        "🛑 **Stop Loss:**\n"
        f"🟢 Long: {settings.sl_long_pct*100:.1f}%\n"
        f"🔴 Short: {settings.sl_short_pct*100:.1f}%\n"
        f"🛡 Коррекция SL (0.1%): {'✅ Вкл' if settings.sl_correction_enabled else '❌ Выкл'}\n\n"
        "📊 **Runtime-параметры (без рестарта):**\n"
        f"🔌 REST runtime: `{runtime_status}`\n"
        f"⚡ Плечо: {leverage_val}x\n"
        f"📂 Макс. сделок: {runtime.get('max_open_trades', settings.max_open_trades)}\n"
        f"💰 Маржа на сделку: {runtime.get('per_trade_margin_pct', settings.per_trade_margin_pct)*100:.1f}%\n"
        f"💎 Пирамидинг: {'✅ Вкл' if runtime.get('pyramiding_enabled', settings.pyramiding_enabled) else '❌ Выкл'}\n\n"
        "🎯 **Пресеты риска:**"
    )


def _build_settings_menu_keyboard(runtime: dict, presets: list) -> InlineKeyboardMarkup:
    buttons = []
    # Отдельные кнопки runtime-настроек
    buttons.append([
        InlineKeyboardButton(
            f"💎 Пирамидинг: {'ON' if runtime.get('pyramiding_enabled', False) else 'OFF'}",
            callback_data="rt_toggle_pyramiding",
        )
    ])
    buttons.append([
        InlineKeyboardButton("💰 Маржа -1%", callback_data="rt_margin_dec"),
        InlineKeyboardButton("💰 Маржа +1%", callback_data="rt_margin_inc"),
    ])
    buttons.append([
        InlineKeyboardButton("📂 Сделки -1", callback_data="rt_open_trades_dec"),
        InlineKeyboardButton("📂 Сделки +1", callback_data="rt_open_trades_inc"),
    ])
    buttons.append([
        InlineKeyboardButton("⚡ 10x", callback_data="rt_leverage_10"),
        InlineKeyboardButton("⚡ 15x", callback_data="rt_leverage_15"),
        InlineKeyboardButton("⚡ 20x", callback_data="rt_leverage_20"),
    ])
    buttons.append([
        InlineKeyboardButton("⚡ 25x", callback_data="rt_leverage_25"),
        InlineKeyboardButton("⚡ 50x", callback_data="rt_leverage_50"),
    ])
    # Ниже — существующие пресеты
    for p in presets:
        label = f"✅ {p['name']}" if p['is_active'] else p['name']
        buttons.append([InlineKeyboardButton(label, callback_data=f"apply_preset_{p['name']}")])
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Этап 14-15: Главное меню
    """
    user_id = update.effective_user.id
    allowed_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]
    
    if user_id not in allowed_ids:
        await update.message.reply_text("⛔️ Доступ запрещен. Вы не являетесь администратором.")
        return

    keyboard = [
        [KeyboardButton("💼 Активные позиции"), KeyboardButton("📈 Статистика")],
        [KeyboardButton("📉 Сигналы"), KeyboardButton("📜 История")],
        [KeyboardButton("📚 Стратегии"), KeyboardButton("⚙ Настройки")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("Добро пожаловать в Algo Quant Bot! Выберите действие:", reply_markup=reply_markup)

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
    await update.message.reply_text("Please provide your Binance API keys (Use secure config or settings menu!).")

async def show_active_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{ENGINE_URL}/api/v1/trades", timeout=8.0)
            data = response.json()
            logging.info(f"🕵️‍♂️ DEBUG: Получено данных от движка: {data}")
            trades = data.get("trades", {})

            if not trades:
                await update.message.reply_text("📂 **Активных позиций на данный момент нет.**", parse_mode='Markdown')
                return

            await update.message.reply_text(f"💼 **ВАШИ ОТКРЫТЫЕ ПОЗИЦИИ ({len(trades)}):**", parse_mode='Markdown')

            for symbol, info in trades.items():
                is_lg = info['signal_type'] == "LONG"
                side_emoji = "🟢 LONG" if is_lg else "🔴 SHORT"
                
                curr_p = info.get('current_price')
                curr_p_str = f"`{curr_p:+.4f}`" if curr_p else "🔍 Ожидаем..."
                
                pnl_usd = info.get('pnl_usd', 0.0)
                pnl_pct = info.get('pnl_pct', 0.0)
                
                if curr_p:
                    pnl_emoji = "🟢" if pnl_usd >= 0 else "🔴"
                    pnl_str = f"{pnl_emoji} **PnL: {pnl_usd:+.2f} USDT ({pnl_pct:+.2f}%)**"
                else:
                    pnl_str = "⏳ **PnL: расчет...**"

                msg = (
                    f"🔹 **{symbol}** ({side_emoji})\n"
                    f"💰 Вход: `{info['entry']:.4f}`\n"
                    f"📈 Тек. цена: {curr_p_str}\n"
                    f"📊 Объем: `{info['current_size']}`\n"
                    f"🛡 Стоп: `{info['stop']:.4f}`\n"
                    f"{pnl_str}\n"
                    f"⏱ Открыта: {time.strftime('%H:%M:%S', time.gmtime(time.time() - info['opened_at']))} назад"
                )

                keyboard = [[InlineKeyboardButton("❌ Закрыть позицию", callback_data=f"close_{symbol.replace('/', '_')}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)

                await update.message.reply_text(msg, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Ошибка получения позиций: {e}")
        await update.message.reply_text("❌ Не удалось связаться с движком торгов.")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    allowed_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]
    if user_id not in allowed_ids:
        return

    text = update.message.text
    logging.info(f"📩 ПОЛУЧЕНО СООБЩЕНИЕ: '{text}' от {user_id}")
    if text == "💼 Активные позиции":
        await show_active_positions(update, context)
    elif text == "📈 Начать торговлю":
        await update.message.reply_text("Система автоматической торговли активирована.")
    elif text == "⚙ Настройки":
        try:
            await _render_settings_message(update.message, edit=False)
        except Exception as e:
            logging.error(f"Ошибка настроек: {e}")
            await update.message.reply_text("❌ Ошибка при получении настроек.")
    elif text == "📉 Сигналы":
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
    elif text == "📚 Стратегии":
        strategy_text = (
            "📖 **МЕТОДОЛОГИЯ БОТА (Schwager v2 + AI)**\n\n"
            "🛡 **РИСК-МЕНЕДЖМЕНТ (Осторожный)**\n"
            "• **Начальный вход:** 15% от плана ( Stage 0 ).\n"
            "• **Пирамидинг:** Доливка по 5% (4 этапа) при росте цены на +1.5 ATR.\n"
            "• **Stop Loss:** Раздельный (L/S) + Коррекция 0.1%.\n"
            "• **ATR Trailing Stop:** Подтягивается в профит.\n"
            "• **Time Exit:** 5 дней без движения.\n\n"
            "📈 **СИГНАЛЬНЫЕ СТРАТЕГИИ (10)**\n"
            "• *Volatility, Trend, Breakout, Pattern*.\n\n"
            "🤖 **ИНТЕЛЛЕКТУАЛЬНЫЙ СЛОЙ (AI Layer)**\n"
            "• **Signal Scorer:** Фильтр (Score > 0.65).\n"
            "• **AI Filter:** Win Prob > 60%.\n"
            "• **Фильтр листинга:** > 100 дней.\n"
        )
        await update.message.reply_text(strategy_text, parse_mode='Markdown')
    elif text == "📈 Статистика":
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

    elif text == "📜 История":
        await update.message.reply_text("Последние сделки: ...")
    else:
        await update.message.reply_text("Команда не распознана.")
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await _safe_answer_callback(query)
    
    if query.data.startswith("close_"):
        symbol_raw = query.data.replace("close_", "")
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

    elif query.data in {"rt_margin_dec", "rt_margin_inc", "rt_open_trades_dec", "rt_open_trades_inc"}:
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
                else:
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
        await _safe_answer_callback(query, "Эта функция будет реализована в следующем обновлении БД.", show_alert=True)

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
