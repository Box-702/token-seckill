import fakeredis.aioredis
import pytest
from fastapi import HTTPException

from token_seckill.core.config import Settings
from token_seckill.services.rate_limit import RateLimitService


@pytest.fixture
def redis_client() -> fakeredis.aioredis.FakeRedis:
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


@pytest.mark.asyncio
async def test_user_tier_blocks_after_budget(redis_client: fakeredis.aioredis.FakeRedis) -> None:
    settings = Settings(
        rate_limit_enabled=True,
        rate_limit_user_limit=2,
        rate_limit_user_window=60,
        rate_limit_ip_limit=100,
        rate_limit_ip_window=60,
        rate_limit_activity_limit=100,
        rate_limit_activity_window=60,
    )
    service = RateLimitService(redis_client, settings)

    await service.check_claim(user_id=1, ip="10.0.0.1", activity_id=10)
    await service.check_claim(user_id=1, ip="10.0.0.1", activity_id=10)
    with pytest.raises(HTTPException) as exc:
        await service.check_claim(user_id=1, ip="10.0.0.1", activity_id=10)

    assert exc.value.status_code == 429
    assert exc.value.headers is not None
    assert "Retry-After" in exc.value.headers


@pytest.mark.asyncio
async def test_ip_tier_is_per_scope(redis_client: fakeredis.aioredis.FakeRedis) -> None:
    settings = Settings(
        rate_limit_user_limit=100,
        rate_limit_user_window=60,
        rate_limit_ip_limit=1,
        rate_limit_ip_window=60,
        rate_limit_activity_limit=100,
        rate_limit_activity_window=60,
    )
    service = RateLimitService(redis_client, settings)

    await service.check_claim(user_id=1, ip="10.0.0.1", activity_id=10)
    # a different user from the same IP is still blocked by the IP tier
    with pytest.raises(HTTPException) as exc:
        await service.check_claim(user_id=2, ip="10.0.0.1", activity_id=10)
    assert exc.value.status_code == 429

    # a different IP is allowed
    await service.check_claim(user_id=2, ip="10.0.0.2", activity_id=10)


@pytest.mark.asyncio
async def test_disabled_rate_limit_never_blocks(redis_client: fakeredis.aioredis.FakeRedis) -> None:
    settings = Settings(rate_limit_enabled=False)
    service = RateLimitService(redis_client, settings)

    for user_id in range(1, 100):
        await service.check_claim(user_id=user_id, ip="10.0.0.1", activity_id=10)


@pytest.mark.asyncio
async def test_challenge_is_single_use(redis_client: fakeredis.aioredis.FakeRedis) -> None:
    service = RateLimitService(redis_client, Settings(challenge_ttl_seconds=120))
    issued = await service.issue_challenge()
    stored = await redis_client.get(f"challenge:answer:{issued['challenge_id']}")
    assert stored is not None
    left, right = (int(part) for part in issued["question"].split(" = ")[0].split(" + "))
    assert int(stored.decode()) == left + right

    answer = stored.decode()
    assert await service.verify_challenge(f"{issued['challenge_id']}:{answer}") is True
    # consumed after a single verification
    assert await service.verify_challenge(f"{issued['challenge_id']}:{answer}") is False


@pytest.mark.asyncio
async def test_challenge_rejects_wrong_answer(redis_client: fakeredis.aioredis.FakeRedis) -> None:
    service = RateLimitService(redis_client, Settings(challenge_ttl_seconds=120))
    issued = await service.issue_challenge()
    assert await service.verify_challenge(f"{issued['challenge_id']}:999999") is False
