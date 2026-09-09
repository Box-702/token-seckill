from datetime import UTC, datetime, timedelta
from typing import Any, cast

import fakeredis.aioredis
import pytest

from token_seckill.core.config import Settings
from token_seckill.schemas.domain import ClaimOutcome
from token_seckill.services.activity_cache import ActivityCache
from token_seckill.services.order_timeout import OrderTimeoutCanceller
from token_seckill.services.seckill import SeckillService


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


def make_service(redis_client: fakeredis.aioredis.FakeRedis) -> SeckillService:
    return SeckillService(
        redis_client,
        cast(ActivityCache, cast(Any, None)),
        "stream:token:grants",
        order_fulfillment_timeout_seconds=1,
    )


@pytest.mark.asyncio
async def test_claim_schedules_delayed_cancel(redis_client: fakeredis.aioredis.FakeRedis) -> None:
    await seed_activity(redis_client, activity_id=1, stock=3)
    service = make_service(redis_client)

    result = await service.claim(user_id=5, activity_id=1)

    assert result.outcome is ClaimOutcome.ACCEPTED
    assert result.order_id is not None
    assert await redis_client.zscore(service.delay_key(), result.order_id) is not None
    assert await redis_client.zcard(service.delay_key()) == 1


@pytest.mark.asyncio
async def test_timeout_cancel_releases_stock_and_claim(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await seed_activity(redis_client, activity_id=2, stock=3)
    service = make_service(redis_client)
    result = await service.claim(user_id=7, activity_id=2)
    order_id = result.order_id
    assert order_id is not None

    # Force the reservation past its deadline so the scanner picks it up.
    past_ms = int(datetime.now(UTC).timestamp() * 1000) - 5_000
    await redis_client.zadd(service.delay_key(), {order_id: past_ms})

    canceller = OrderTimeoutCanceller(redis_client, service, Settings())
    cancelled = await canceller.scan_once()

    assert cancelled == 1
    assert int(await redis_client.get("token:2:stock")) == 3
    assert await redis_client.sismember("token:2:claimed", 7) == 0
    assert await redis_client.zcard(service.delay_key()) == 0
    status = await service.raw_order_status(order_id)
    assert status["status"] == "cancelled"
    assert "deadline" in status["failure_reason"]


@pytest.mark.asyncio
async def test_timeout_cancel_skips_granted_order(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await seed_activity(redis_client, activity_id=3, stock=3)
    service = make_service(redis_client)
    result = await service.claim(user_id=9, activity_id=3)
    order_id = result.order_id
    assert order_id is not None

    # The grant worker completed the order before the deadline.
    await redis_client.hset(service.order_key(order_id), "status", "success")
    past_ms = int(datetime.now(UTC).timestamp() * 1000) - 5_000
    await redis_client.zadd(service.delay_key(), {order_id: past_ms})

    canceller = OrderTimeoutCanceller(redis_client, service, Settings())
    cancelled = await canceller.scan_once()

    assert cancelled == 0
    assert int(await redis_client.get("token:3:stock")) == 2
    assert await redis_client.zcard(service.delay_key()) == 0
    status = await service.raw_order_status(order_id)
    assert status["status"] == "success"
