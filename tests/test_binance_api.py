import asyncio

import pytest

from utils.binance_api import (
    BinanceCallPolicy,
    BinanceRateLimiter,
    call_with_binance_retry,
    classify_binance_error,
)


def test_classify_binance_error():
    assert classify_binance_error(Exception("429 Too Many Requests")) == "rate_limit"
    assert classify_binance_error(Exception("Timestamp for this request is outside of the recvWindow. -1021")) == "time_sync"
    assert classify_binance_error(Exception("RequestTimeout: timed out")) == "timeout"
    assert classify_binance_error(Exception("AuthenticationError: bad api key")) == "fatal"


def test_call_with_binance_retry_retries_transient_rate_limit():
    calls = {"count": 0}

    async def scenario():
        async def op():
            calls["count"] += 1
            if calls["count"] < 3:
                raise Exception("429 Too Many Requests")
            return "ok"

        return await call_with_binance_retry(
            op=op,
            policy=BinanceCallPolicy(max_attempts=4, base_delay=0.01, max_delay=0.02),
            limiter=BinanceRateLimiter(max_concurrent=1),
        )

    result = asyncio.run(scenario())

    assert result == "ok"
    assert calls["count"] == 3


def test_call_with_binance_retry_does_not_retry_fatal_error():
    calls = {"count": 0}

    async def scenario():
        async def op():
            calls["count"] += 1
            raise Exception("AuthenticationError: invalid api key")

        return await call_with_binance_retry(
            op=op,
            policy=BinanceCallPolicy(max_attempts=4, base_delay=0.01, max_delay=0.02),
        )

    with pytest.raises(Exception):
        asyncio.run(scenario())

    assert calls["count"] == 1
