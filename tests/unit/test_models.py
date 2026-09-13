"""模型层约束回归测试：锁定枚举词汇表、默认值与 CHECK 约束"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from token_seckill.db.base import Base
from token_seckill.db.models import (
    ActivityStatus,
    GrantOrder,
    OrderStatus,
    SeckillActivity,
    TokenSku,
    User,
)


async def _create_engine() -> AsyncEngine:
    """建一个全新的内存库并创建全部表"""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=StaticPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return engine


async def _seed(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """写入一条活动 + 一条订单，状态使用枚举对象"""
    async with session_factory() as session:
        session.add(User(id=1, email="demo@example.com"))
        session.add(
            TokenSku(id=10, name="10 万 Token 体验包", token_amount=100_000, price_cents=0)
        )
        session.add(
            SeckillActivity(
                id=20,
                sku_id=10,
                starts_at=datetime.now(UTC) - timedelta(minutes=1),
                ends_at=datetime.now(UTC) + timedelta(minutes=10),
                initial_stock=5,
                db_stock=5,
                status=ActivityStatus.ONLINE,
            )
        )
        session.add(
            GrantOrder(
                id=99,
                user_id=1,
                activity_id=20,
                sku_id=10,
                token_amount=100_000,
                status=OrderStatus.PROCESSING,
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_enum_status_is_persisted_as_lowercase_value() -> None:
    """枚举落库的是小写值（online），而不是大写成员名（ONLINE）"""
    engine = await _create_engine()
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    await _seed(session_factory)

    async with engine.begin() as connection:
        activity_status = (
            await connection.execute(
                text("SELECT status FROM seckill_activities WHERE id = 20")
            )
        ).scalar()
        order_status = (
            await connection.execute(text("SELECT status FROM grant_orders WHERE id = 99"))
        ).scalar()

    assert activity_status == "online"
    assert order_status == "processing"

    # ORM 读回后仍是枚举对象，且与 Redis/API 使用同一套小写值
    async with session_factory() as session:
        activity = await session.get(SeckillActivity, 20)
        order = await session.get(GrantOrder, 99)
        assert activity is not None and activity.status is ActivityStatus.ONLINE
        assert order is not None and order.status is OrderStatus.PROCESSING

    await engine.dispose()


@pytest.mark.asyncio
async def test_status_check_constraint_rejects_unknown_value() -> None:
    """CHECK 约束在数据库层拒绝非法状态，不再依赖应用层自觉

    SQLite 默认二进制排序规则本身就大小写敏感；MySQL 默认 utf8mb4_unicode_ci 会放行
    'ONLINE'，因此模型对 MySQL 指定了 utf8mb4_bin（见 _status_enum）。
    """
    engine = await _create_engine()
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    await _seed(session_factory)

    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE grant_orders SET status = 'bogus' WHERE id = 99")
            )

    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE seckill_activities SET status = 'ONLINE' WHERE id = 20")
            )

    await engine.dispose()


@pytest.mark.asyncio
async def test_server_defaults_apply_to_raw_insert() -> None:
    """默认值已落到数据库 DEFAULT，绕过 ORM 的裸 INSERT 也能取到默认值"""
    engine = await _create_engine()
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as connection:
        # 只给必填列，token_balance / price_cents / validity_days / active /
        # allowed_models / status 走默认值
        await connection.execute(
            text("INSERT INTO users (email) VALUES ('raw@example.com')")
        )
        await connection.execute(
            text("INSERT INTO token_skus (name, token_amount) VALUES ('裸插入套餐', 1000)")
        )
        balance = (
            await connection.execute(
                text("SELECT token_balance FROM users WHERE email = 'raw@example.com'")
            )
        ).scalar()
        sku = (
            await connection.execute(
                text(
                    "SELECT price_cents, validity_days, active FROM token_skus "
                    "WHERE name = '裸插入套餐'"
                )
            )
        ).one()

    assert balance == 0
    assert tuple(sku) == (0, 30, 1)

    # JSON 默认值必须是合法 JSON，ORM 读回应反序列化成空列表
    async with session_factory() as session:
        raw_sku = (
            await session.scalars(select(TokenSku).where(TokenSku.name == "裸插入套餐"))
        ).one()
        assert raw_sku.allowed_models == []

    await engine.dispose()
