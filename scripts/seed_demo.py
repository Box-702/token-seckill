"""种子数据脚本：创建演示用户、SKU 和秒杀活动，并发布到 Redis"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis
from sqlalchemy import select

from token_seckill.core.config import get_settings
from token_seckill.db.models import SeckillActivity, TokenSku, User
from token_seckill.db.session import create_engine_and_session, init_database
from token_seckill.services.activity_cache import ActivityCache
from token_seckill.services.seckill import SeckillService


async def main() -> None:
    settings = get_settings()
    engine, session_factory = create_engine_and_session(settings.database_url)
    await init_database(engine)
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    cache = ActivityCache(
        redis,
        local_max_entries=settings.local_cache_max_entries,
        local_ttl_seconds=settings.local_cache_ttl_seconds,
        shared_ttl_seconds=settings.shared_cache_ttl_seconds,
        negative_ttl_seconds=settings.negative_cache_ttl_seconds,
    )
    service = SeckillService(
        redis,
        cache,
        settings.grant_stream,
        order_fulfillment_timeout_seconds=settings.order_fulfillment_timeout_seconds,
        stock_channel_prefix=settings.stock_channel_prefix,
    )

    async with session_factory() as session:
        # 创建演示用户
        user = (
            await session.execute(select(User).where(User.email == "demo@example.com"))
        ).scalar_one_or_none()
        if user is None:
            user = User(email="demo@example.com")
            session.add(user)
        # 创建 Token 套餐 SKU
        sku = (
            await session.execute(
                select(TokenSku).where(TokenSku.name == "新用户 10 万 Token 体验包")
            )
        ).scalar_one_or_none()
        if sku is None:
            sku = TokenSku(
                name="新用户 10 万 Token 体验包",
                token_amount=100_000,
                price_cents=0,
                validity_days=30,
                allowed_models=["demo-chat", "demo-reasoner"],
            )
            session.add(sku)
        await session.flush()
        # 创建秒杀活动（100 库存，1小时后结束）
        activity = (
            (await session.execute(select(SeckillActivity).where(SeckillActivity.sku_id == sku.id)))
            .scalars()
            .first()
        )
        if activity is None:
            activity = SeckillActivity(
                sku_id=sku.id,
                starts_at=datetime.now(UTC) - timedelta(minutes=1),
                ends_at=datetime.now(UTC) + timedelta(hours=1),
                initial_stock=100,
                db_stock=100,
            )
            session.add(activity)
            await session.flush()
        await session.commit()
        # 发布活动到 Redis
        await service.publish(session, activity.id)
        print(
            {
                "user_id": user.id,
                "sku_id": sku.id,
                "activity_id": activity.id,
            },
            flush=True,
        )

    await asyncio.wait_for(redis.aclose(), timeout=5)
    await asyncio.wait_for(engine.dispose(), timeout=5)


if __name__ == "__main__":
    asyncio.run(main())
