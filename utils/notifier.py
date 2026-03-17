import httpx
import logging
from config.settings import settings

logger = logging.getLogger(__name__)

async def send_telegram_msg(text: str):
    """
    Отправка мгновенного уведомления администратору в Telegram.
    """
    token = settings.telegram_bot_token.get_secret_value()
    admin_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]
    
    async with httpx.AsyncClient() as client:
        for admin_id in admin_ids:
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                payload = {
                    "chat_id": admin_id,
                    "text": text,
                    "parse_mode": "Markdown"
                }
                await client.post(url, json=payload, timeout=5.0)
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления в Telegram: {e}")
