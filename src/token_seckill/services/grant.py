from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from token_seckill.db.models import (
    GrantOrder,
    OrderStatus,
    SeckillActivity,
    TokenLedger,
    TokenSku,
    User,
)


@dataclass(frozen=True, slots=True)
class GrantMessage:
    """Redis Stream 消息结构：秒杀成功后写入 Stream 的订单信息"""
    order_id: int
    user_id: int
    activity_id: int
    sku_id: int
    token_amount: int

    @classmethod
    def from_stream(cls, fields: dict[bytes | str, bytes | str]) -> GrantMessage:
        """从 Redis Stream 字段解析消息（处理 bytes/str 键值）"""
        decoded = {
            (key.decode() if isinstance(key, bytes) else key): (
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in fields.items()
        }
        return cls(**{key: int(value) for key, value in decoded.items()})


class PermanentGrantError(RuntimeError):
    """永久性发放错误（如用户不存在、SKU 下架、库存耗尽），触发补偿回滚"""
    pass


class GrantService:
    """异步发放服务：消费 Redis Stream 消息，完成数据库侧的订单落库和 Token 发放

    核心流程（单个事务内完成）：
    1. 幂等检查：按 order_id 和 user+activity 唯一约束去重
    2. 锁定用户行（SELECT FOR UPDATE）
    3. 条件扣减数据库库存（db_stock > 0）
    4. 创建订单 + 写入流水账本 + 更新用户余额
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def process(self, message: GrantMessage) -> GrantOrder:
        """处理一条发放消息（整个过程在一个数据库事务内完成）"""
        async with self.session_factory() as session, session.begin():
            # 幂等检查 1：订单已存在则直接返回
            existing = await session.get(GrantOrder, message.order_id)
            if existing is not None:
                return existing

            # 幂等检查 2：同一用户同一活动已有订单则返回（user+activity 唯一约束）
            prior = (
                await session.execute(
                    select(GrantOrder).where(
                        GrantOrder.user_id == message.user_id,
                        GrantOrder.activity_id == message.activity_id,
                    )
                )
            ).scalar_one_or_none()
            if prior is not None:
                return prior

            # 锁定用户行，防止并发发放
            user = await session.get(User, message.user_id, with_for_update=True)
            if user is None:
                raise PermanentGrantError("user not found")
            sku = await session.get(TokenSku, message.sku_id)
            if sku is None or not sku.active:
                raise PermanentGrantError("token SKU is unavailable")

            # 条件扣减数据库库存（乐观锁：db_stock > 0 才扣减）
            stock_update = cast(
                CursorResult[Any],
                await session.execute(
                    update(SeckillActivity)
                    .where(
                        SeckillActivity.id == message.activity_id,
                        SeckillActivity.sku_id == message.sku_id,
                        SeckillActivity.db_stock > 0,
                    )
                    .values(db_stock=SeckillActivity.db_stock - 1)
                ),
            )
            if stock_update.rowcount != 1:
                raise PermanentGrantError("database stock exhausted")

            # 更新用户余额 + 创建订单 + 写入流水账本
            user.token_balance += sku.token_amount
            order = GrantOrder(
                id=message.order_id,
                user_id=message.user_id,
                activity_id=message.activity_id,
                sku_id=message.sku_id,
                token_amount=sku.token_amount,
                status=OrderStatus.SUCCESS,
            )
            session.add(order)
            session.add(
                TokenLedger(
                    user_id=message.user_id,
                    order_id=message.order_id,
                    delta_tokens=sku.token_amount,
                    balance_after=user.token_balance,
                    expires_at=datetime.now(UTC) + timedelta(days=sku.validity_days),
                )
            )
            await session.flush()
            return order
