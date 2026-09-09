from typing import Any, cast

import fakeredis.aioredis
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from token_seckill.agents.quota_agent import QuotaAgentService
from token_seckill.core.config import Settings
from token_seckill.schemas.domain import ClaimOutcome, ClaimResponse
from token_seckill.services.seckill import SeckillService


class StubSeckillService:
    def __init__(self) -> None:
        self.calls = 0

    async def claim(self, user_id: int, activity_id: int) -> ClaimResponse:
        self.calls += 1
        return ClaimResponse(
            outcome=ClaimOutcome.ACCEPTED,
            order_id=999,
            message="accepted",
        )


@pytest.mark.asyncio
async def test_agent_approval_is_required_and_idempotent() -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    seckill = StubSeckillService()
    service = QuotaAgentService(
        Settings(llm_api_key="test-key", llm_model="test-model"),
        redis,
        session_factory,
        cast(SeckillService, cast(Any, seckill)),
    )
    await redis.hset(
        "agent:approval:action-1",
        mapping={"user_id": 1, "activity_id": 7, "status": "pending"},
    )

    first = await service.approve(user_id=1, action_id="action-1")
    second = await service.approve(user_id=1, action_id="action-1")

    assert first.status == second.status == "accepted"
    assert first.order_id == second.order_id == 999
    assert seckill.calls == 1
    assert service._build_graph(user_id=1) is not None
    await redis.aclose()
    await engine.dispose()
