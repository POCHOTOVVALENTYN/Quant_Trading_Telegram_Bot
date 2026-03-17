import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, TypeHandler
import time
import httpx
from collections import defaultdict
from config.settings import settings

ENGINE_URL = "http://trading-engine:8000" # URL внутри Docker сети

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

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
        [KeyboardButton("📊 Портфель"), KeyboardButton("📈 Начать торговлю")],
        [KeyboardButton("📉 Сигналы"), KeyboardButton("📜 История")],
        [KeyboardButton("⚙ Настройки")]
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

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    allowed_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]
    if user_id not in allowed_ids:
        return

    text = update.message.text
    if text == "📊 Портфель":
        await status(update, context)
    elif text == "📈 Начать торговлю":
        await update.message.reply_text("Система автоматической торговли активирована.")
    elif text == "⚙ Настройки":
        await update.message.reply_text("Риск на сделку: 2%\nСтратегия: WRD + Rule of 7")
    elif text == "📉 Сигналы":
        from database.session import async_session
        from database.models.all_models import Signal
        from sqlalchemy import select
        
        try:
            async with async_session() as session:
                query = select(Signal).order_by(Signal.timestamp.desc()).limit(1)
                result = await session.execute(query)
                last_signal = result.scalar_one_or_none()
                
                if not last_signal:
                    await update.message.reply_text("📭 Активных сигналов пока нет. Ждем выполнения условий по ансамблю стратегий (Schwager v2 + AI)...")
                    return
保障

                # Перевод сигналов для удобства пользователя
                signal_type_ru = "🟢 LONG" if last_signal.signal_type == "LONG" else "🔴 SHORT"
                
                msg = (
                    f"🚀 **СИГНАЛ: {last_signal.strategy}**\n\n"
                    f"🔸 **Символ:** {last_signal.symbol}\n"
                    f"🔸 **Направление:** {signal_type_ru}\n\n"
                    f"💰 **Вход:** {last_signal.entry_price:.2f}\n"
                    f"🕒 **Время:** {last_signal.timestamp.strftime('%H:%M:%S')}\n\n"
                    f"🤖 **AI ВЕРДИКТ:**\n"
                    f"📈 **Вероятность успеха:** {int(last_signal.win_prob * 100)}%\n"
                    f"💰 **Ож. доходность:** {last_signal.expected_return}%\n"
                    f"⚠️ **Уровень риска:** {last_signal.risk}\n"
                    f"📊 **AI Score:** {last_signal.confidence:.2f}\n\n"
                    f"✅ **Статус:** ТОРГУЕМ"
                )
                await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logging.error(f"Ошибка получения сигналов из БД: {e}")
            await update.message.reply_text("❌ Ошибка при обращении к базе данных.")
    elif text == "📜 История":
        await update.message.reply_text("Последние сделки: ...")
    else:
        await update.message.reply_text("Команда не распознана.")

# ======= ANTI-SPAM & RATE LIMITING (Этап 18) =======
# Хранилище: {user_id: [timestamp1, timestamp2, ...]}
user_message_times = defaultdict(list)
RATE_LIMIT_MESSAGES = 5      # Максимум 5 сообщений
RATE_LIMIT_WINDOW = 10.0     # за 10 секунд

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

    logging.info("Telegram Bot is polling...")
    app.run_polling()

if __name__ == "__main__":
    run_bot()
