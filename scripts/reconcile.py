"""数据一致性对账脚本：比对 Redis 库存、MySQL 库存、成功订单数和流水条数"""
from __future__ import annotations

import argparse
import asyncio
import json

from redis.asyncio import Redis
from sqlalchemy import func, select

from token_seckill.core.config import get_settings
from token_seckill.db.models import GrantOrder, OrderStatus, SeckillActivity, TokenLedger
from token_seckill.db.session import create_engine_and_session
from token_seckill.services.seckill import SeckillService


async def reconcile(activity_id: int) -> bool:
    """对账核心逻辑：检查 Redis/MySQL/订单/流水四者是否一致"""
    settings = get_settings()
    engine, session_factory = create_engine_and_session(settings.database_url)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    async with session_factory() as session:
        activity = await session.get(SeckillActivity, activity_id)
        if activity is None:
            raise ValueError(f"activity {activity_id} not found")
        # 统计成功订单数
        successful_orders = int(
            await session.scalar(
                select(func.count(GrantOrder.id)).where(
                    GrantOrder.activity_id == activity_id,
                    GrantOrder.status == OrderStatus.SUCCESS,
                )
            )
            or 0
        )
        # 统计流水条数
        ledger_entries = int(
            await session.scalar(
                select(func.count(TokenLedger.id))
                .join(GrantOrder, GrantOrder.id == TokenLedger.order_id)
                .where(GrantOrder.activity_id == activity_id)
            )
            or 0
        )
        # 读取 Redis 库存
        stock_key, _, _ = SeckillService.activity_keys(activity_id)
        redis_stock_raw = await redis.get(stock_key)
        redis_stock = int(redis_stock_raw) if redis_stock_raw is not None else None
        expected_stock = activity.initial_stock - successful_orders
        # 四方一致性校验
        consistent = (
            redis_stock == expected_stock
            and activity.db_stock == expected_stock
            and ledger_entries == successful_orders
        )
        print(
            json.dumps(
                {
                    "activity_id": activity_id,
                    "initial_stock": activity.initial_stock,
                    "redis_stock": redis_stock,
                    "database_stock": activity.db_stock,
                    "successful_orders": successful_orders,
                    "ledger_entries": ledger_entries,
                    "consistent": consistent,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    await asyncio.wait_for(redis.aclose(), timeout=5)
    await asyncio.wait_for(engine.dispose(), timeout=5)
    return consistent


def main() -> None:
    """命令行入口：传入活动 ID 执行对账"""
    parser = argparse.ArgumentParser(description="Reconcile activity stock, orders and ledger")
    parser.add_argument("activity_id", type=int)
    args = parser.parse_args()
    if not asyncio.run(reconcile(args.activity_id)):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
