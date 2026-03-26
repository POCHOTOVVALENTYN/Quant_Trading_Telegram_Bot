import asyncio
import httpx
import logging
from config.settings import settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 2
RETRY_DELAY = 2.0


async def send_telegram_msg(text: str):
    """
    Отправка мгновенного уведомления администратору в Telegram.
    Retry: 2 попытки с 2-сек задержкой между ними.
    """
    token = settings.telegram_bot_token.get_secret_value()
    admin_ids = [int(i.strip()) for i in settings.admin_user_ids.split(",") if i.strip()]

    async with httpx.AsyncClient() as client:
        for admin_id in admin_ids:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": admin_id,
                "text": text,
                "parse_mode": "Markdown"
            }
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = await client.post(url, json=payload, timeout=8.0)
                    if resp.status_code == 200:
                        break
                    # Markdown parse errors — retry without parse_mode
                    if resp.status_code == 400 and "can't parse entities" in resp.text:
                        payload_plain = {**payload, "parse_mode": None}
                        payload_plain.pop("parse_mode")
                        await client.post(url, json=payload_plain, timeout=8.0)
                        break
                    logger.warning(f"Telegram HTTP {resp.status_code} (attempt {attempt}): {resp.text[:120]}")
                except Exception as e:
                    logger.warning(f"Telegram send error (attempt {attempt}/{MAX_RETRIES}): {type(e).__name__}: {e}")
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
