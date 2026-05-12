import asyncio
import redis.asyncio as aioredis
import json
from config.settings import settings

async def test():
    print(f"Connecting to: {settings.redis_url}")
    try:
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        await r.ping()
        print("PING SUCCESSFUL")
        await r.publish("test_channel", json.dumps({"test": "ok"}))
        print("PUBLISH SUCCESSFUL")
        await r.close()
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    asyncio.run(test())
