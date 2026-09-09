import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import fakeredis.aioredis
import pytest

from token_seckill.schemas.domain import ClaimOutcome
from token_seckill.services.activity_cache import ActivityCache
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


@pytest.mark.asyncio
async def test_claim_is_one_user_one_order(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await seed_activity(redis_client, activity_id=1, stock=2)
    service = SeckillService(
        redis_client,
        cast(ActivityCache, cast(Any, None)),
        "stream:token:grants",
    )

    first = await service.claim(user_id=7, activity_id=1)
    second = await service.claim(user_id=7, activity_id=1)

    assert first.outcome is ClaimOutcome.ACCEPTED
    assert first.order_id is not None
    assert second.outcome is ClaimOutcome.DUPLICATE
    assert int(await redis_client.get("token:1:stock")) == 1
    assert await redis_client.xlen("stream:token:grants") == 1


@pytest.mark.asyncio
async def test_stock_never_goes_negative(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await seed_activity(redis_client, activity_id=2, stock=1)
    service = SeckillService(
        redis_client,
        cast(ActivityCache, cast(Any, None)),
        "stream:token:grants",
    )

    outcomes = [
        (await service.claim(user_id=user_id, activity_id=2)).outcome for user_id in (1, 2, 3)
    ]

    assert outcomes.count(ClaimOutcome.ACCEPTED) == 1
    assert outcomes.count(ClaimOutcome.SOLD_OUT) == 2
    assert int(await redis_client.get("token:2:stock")) == 0
    assert await redis_client.xlen("stream:token:grants") == 1


@pytest.mark.asyncio
async def test_inactive_activity_does_not_consume_stock(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await redis_client.set("token:3:stock", 1)
    await redis_client.hset(
        "token:3:meta",
        mapping={
            "status": "draft",
            "starts_at_ms": 0,
            "ends_at_ms": 9_999_999_999_999,
            "sku_id": 10,
            "token_amount": 100_000,
        },
    )
    service = SeckillService(
        redis_client,
        cast(ActivityCache, cast(Any, None)),
        "stream:token:grants",
    )

    result = await service.claim(user_id=1, activity_id=3)

    assert result.outcome is ClaimOutcome.NOT_ACTIVE
    assert int(await redis_client.get("token:3:stock")) == 1
    assert await redis_client.xlen("stream:token:grants") == 0


@pytest.mark.asyncio
async def test_concurrent_claims_accept_exactly_available_stock(
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    await seed_activity(redis_client, activity_id=4, stock=10)
    service = SeckillService(
        redis_client,
        cast(ActivityCache, cast(Any, None)),
        "stream:token:grants",
    )

    results = await asyncio.gather(
        *(service.claim(user_id=user_id, activity_id=4) for user_id in range(1, 101))
    )

    assert sum(result.outcome is ClaimOutcome.ACCEPTED for result in results) == 10
    assert sum(result.outcome is ClaimOutcome.SOLD_OUT for result in results) == 90
    assert int(await redis_client.get("token:4:stock")) == 0
    assert await redis_client.xlen("stream:token:grants") == 10
