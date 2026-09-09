import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import fakeredis.aioredis
import pytest

from token_seckill.schemas.domain import ClaimOutcome
from token_seckill.services.activity_cache import ActivityCache
from token_seckill.services.seckill import SeckillService
from token_seckill.services.stock_stream import format_sse_event


@pytest.fixture
def redis_client() -> fakeredis.aioredis.FakeRedis:
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


async def seed_activity(
    redis_client: fakeredis.aioredis.FakeRedis,
    activity_id: int,
    stock: int,
) -> None:
    now = datetime.now(UTC)
    await redis_client.set(f"token:{activity_id}:stock", stock)
    await redis_client.hset(
        f"token:{activity_id}:meta",
        mapping={
            "status": "online",
            "starts_at_ms": int((now - timedelta(minutes=1)).timestamp() * 1000),
            "ends_at_ms": int((now + timedelta(minutes=5)).timestamp() * 1000),
            "sku_id": 10,
            "token_amount": 100_000,
        },
    )


@pytest.mark.asyncio
async def test_claim_publishes_stock_notification(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await seed_activity(redis_client, activity_id=1, stock=3)
    service = SeckillService(
        redis_client, cast(ActivityCache, cast(Any, None)), "stream:token:grants"
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(service.stock_channel(1))
    await pubsub.get_message(timeout=1.0)  # consume SUBSCRIBE ack so delivery is active

    result = await service.claim(user_id=9, activity_id=1)
    assert result.outcome is ClaimOutcome.ACCEPTED

    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
    assert message is not None
    assert message["type"] == "message"
    assert int(message["data"]) == 2
    await pubsub.aclose()


@pytest.mark.asyncio
async def test_each_claim_pushes_decremented_stock(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await seed_activity(redis_client, activity_id=2, stock=2)
    service = SeckillService(
        redis_client, cast(ActivityCache, cast(Any, None)), "stream:token:grants"
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(service.stock_channel(2))
    await pubsub.get_message(timeout=1.0)  # consume SUBSCRIBE ack

    await service.claim(user_id=1, activity_id=2)
    await service.claim(user_id=2, activity_id=2)

    values: list[int] = []
    for _ in range(2):
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
        if message is not None and message["type"] == "message":
            values.append(int(message["data"]))
        else:
            await asyncio.sleep(0.01)
    assert sorted(values) == [0, 1]
    await pubsub.aclose()


def test_format_sse_event() -> None:
    frame = format_sse_event("sync", {"remaining": 3, "activity_id": 5})
    assert frame.startswith("event: sync")
    assert '"remaining":3' in frame
    assert frame.endswith("\n\n")
