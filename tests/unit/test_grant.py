from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from token_seckill.db.base import Base
from token_seckill.db.models import (
    GrantOrder,
    SeckillActivity,
    TokenLedger,
    TokenSku,
    User,
)
from token_seckill.services.grant import GrantMessage, GrantService


@pytest.mark.asyncio
async def test_grant_is_idempotent_and_updates_ledger_once() -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        session.add(User(id=1, email="demo@example.com"))
        session.add(
            TokenSku(
                id=10,
                name="10 万 Token 体验包",
                token_amount=100_000,
                price_cents=0,
                validity_days=30,
            )
        )
        session.add(
            SeckillActivity(
                id=20,
                sku_id=10,
                starts_at=datetime.now(UTC) - timedelta(minutes=1),
                ends_at=datetime.now(UTC) + timedelta(minutes=10),
                initial_stock=1,
                db_stock=1,
            )
        )
        await session.commit()

    service = GrantService(session_factory)
    message = GrantMessage(
        order_id=123456,
        user_id=1,
        activity_id=20,
        sku_id=10,
        token_amount=100_000,
    )

    first = await service.process(message)
    second = await service.process(message)

    assert first.id == second.id == 123456
    async with session_factory() as session:
        user = await session.get(User, 1)
        activity = await session.get(SeckillActivity, 20)
        orders = (await session.scalars(select(GrantOrder))).all()
        ledger = (await session.scalars(select(TokenLedger))).all()
        assert user is not None and user.token_balance == 100_000
        assert activity is not None and activity.db_stock == 0
        assert len(orders) == 1
        assert len(ledger) == 1

    await engine.dispose()
