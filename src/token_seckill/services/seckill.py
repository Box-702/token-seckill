from __future__ import annotations

from datetime import UTC, datetime
from importlib.resources import files
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from token_seckill.db.models import ActivityStatus, SeckillActivity, TokenSku
from token_seckill.schemas.domain import ClaimOutcome, ClaimResponse
from token_seckill.services.activity_cache import ActivityCache

# 加载 Redis Lua 脚本（打包为 Python 包资源）
RESOURCE_ROOT = files("token_seckill.resources")
SECKILL_LUA = RESOURCE_ROOT.joinpath("seckill.lua").read_text(encoding="utf-8")
COMPENSATE_LUA = RESOURCE_ROOT.joinpath("compensate.lua").read_text(encoding="utf-8")
CANCEL_LUA = RESOURCE_ROOT.joinpath("cancel.lua").read_text(encoding="utf-8")
# 雪花 ID 纪元：2024-01-01 00:00:00 UTC
ID_EPOCH = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp())


class SeckillService:
    """秒杀核心服务：基于 Redis Lua 脚本实现原子化的库存扣减

    核心秒杀流程（单次 Redis EVAL 原子执行）：
    1. 校验活动状态和时间窗口
    2. 检查库存 > 0
    3. 检查用户未领取过（SISMEMBER）
    4. 扣减库存（DECR）+ 记录已领取用户（SADD）
    5. 写入 Redis Stream（异步落库的消息队列）
    6. 写入订单 Hash（状态快照）
    7. 加入延时取消 ZSET（超时自动释放）
    8. PUBLISH 库存变更通知（SSE 实时推送）
    """

    def __init__(
        self,
        redis: Redis,
        activity_cache: ActivityCache,
        stream: str,
        *,
        order_fulfillment_timeout_seconds: int = 900,
        stock_channel_prefix: str = "token:stock",
    ) -> None:
        self.redis = redis
        self.activity_cache = activity_cache
        self.stream = stream
        self.order_fulfillment_timeout_seconds = order_fulfillment_timeout_seconds
        self.stock_channel_prefix = stock_channel_prefix

    async def publish(self, session: AsyncSession, activity_id: int) -> ActivityStatus:
        """发布活动到 Redis：同步库存、元数据、清空已领取集合

        根据当前时间自动判断活动状态（READY/ONLINE/ENDED），
        并将库存和元数据写入 Redis，同时使缓存失效。
        """
        statement = (
            select(SeckillActivity, TokenSku)
            .join(TokenSku, TokenSku.id == SeckillActivity.sku_id)
            .where(SeckillActivity.id == activity_id)
        )
        row = (await session.execute(statement)).one_or_none()
        if row is None:
            raise ValueError("activity not found")
        activity, sku = row
        now = datetime.now(UTC)
        starts_at = self._as_utc(activity.starts_at)
        ends_at = self._as_utc(activity.ends_at)
        # 根据当前时间自动设置活动状态
        if ends_at <= now:
            activity.status = ActivityStatus.ENDED
        elif starts_at <= now:
            activity.status = ActivityStatus.ONLINE
        else:
            activity.status = ActivityStatus.READY
        activity.version += 1
        await session.commit()

        # 同步到 Redis：库存 key + 已领取集合 + 元数据 Hash
        stock_key, claimed_key, meta_key = self.activity_keys(activity_id)
        await self.redis.set(stock_key, activity.db_stock)
        await self.redis.delete(claimed_key)
        await self.redis.hset(
            meta_key,
            mapping={
                "status": activity.status.value,
                "starts_at_ms": int(starts_at.timestamp() * 1000),
                "ends_at_ms": int(ends_at.timestamp() * 1000),
                "sku_id": sku.id,
                "token_amount": sku.token_amount,
            },
        )
        await self.activity_cache.invalidate(activity_id)
        return ActivityStatus(activity.status)

    async def claim(self, user_id: int, activity_id: int) -> ClaimResponse:
        """执行秒杀：通过 Lua 脚本原子完成库存校验和扣减

        返回值：
        - ACCEPTED: 秒杀成功，订单已写入 Stream 等待异步落库
        - SOLD_OUT: 库存不足
        - DUPLICATE: 用户已领取过
        - NOT_ACTIVE: 活动未开始/已结束/未发布
        """
        order_id = await self._next_order_id()
        stock_key, claimed_key, meta_key = self.activity_keys(activity_id)
        order_key = self.order_key(order_id)
        delay_key = self.delay_key()
        channel = self.stock_channel(activity_id)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        expire_at_ms = now_ms + self.order_fulfillment_timeout_seconds * 1000
        # 执行 Lua 脚本（原子操作：校验 + 扣减 + 写 Stream + 写订单 + 加延时队列 + 推送）
        result = int(
            await self.redis.eval(
                SECKILL_LUA,
                6,
                stock_key,
                claimed_key,
                meta_key,
                self.stream,
                order_key,
                delay_key,
                user_id,
                order_id,
                now_ms,
                activity_id,
                expire_at_ms,
                channel,
            )
        )
        if result == 0:
            return ClaimResponse(
                outcome=ClaimOutcome.ACCEPTED,
                order_id=order_id,
                message="资格校验通过，Token 正在异步发放",
            )
        if result == 1:
            return ClaimResponse(
                outcome=ClaimOutcome.SOLD_OUT,
                message="库存不足",
            )
        if result == 2:
            return ClaimResponse(
                outcome=ClaimOutcome.DUPLICATE,
                message="同一活动每位用户只能领取一次",
            )
        return ClaimResponse(
            outcome=ClaimOutcome.NOT_ACTIVE,
            message="活动尚未开始、已经结束或未发布",
        )

    async def cancel_timeout(
        self,
        *,
        activity_id: int,
        user_id: int,
        order_id: int,
        reason: str,
    ) -> bool:
        """延时取消：释放超时未履约的订单库存"""
        stock_key, claimed_key, _ = self.activity_keys(activity_id)
        released = await self.redis.eval(
            CANCEL_LUA,
            4,
            self.order_key(order_id),
            stock_key,
            claimed_key,
            self.delay_key(),
            user_id,
            order_id,
            reason,
        )
        return bool(released)

    async def compensate(
        self,
        *,
        activity_id: int,
        user_id: int,
        order_id: int,
        reason: str,
    ) -> bool:
        """补偿：永久性发放失败时回滚 Redis 侧库存"""
        stock_key, claimed_key, _ = self.activity_keys(activity_id)
        changed = await self.redis.eval(
            COMPENSATE_LUA,
            3,
            stock_key,
            claimed_key,
            self.order_key(order_id),
            user_id,
            reason,
        )
        return bool(changed)

    async def _next_order_id(self) -> int:
        """生成雪花风格的订单 ID：(秒级时间戳 << 32) | 当日自增序列"""
        now = datetime.now(UTC)
        seconds = int(now.timestamp()) - ID_EPOCH
        sequence = await self.redis.incr(f"id:order:{now:%Y%m%d}")
        await self.redis.expire(f"id:order:{now:%Y%m%d}", 172_800)
        return (seconds << 32) | int(sequence)

    @staticmethod
    def activity_keys(activity_id: int) -> tuple[str, str, str]:
        """返回活动相关的三个 Redis key：(库存, 已领取集合, 元数据)"""
        return (
            f"token:{activity_id}:stock",
            f"token:{activity_id}:claimed",
            f"token:{activity_id}:meta",
        )

    @staticmethod
    def delay_key() -> str:
        """延时取消队列的 Redis ZSET key"""
        return "token:order:delay"

    def stock_channel(self, activity_id: int) -> str:
        """库存变更推送的 Redis Pub/Sub 频道名"""
        return f"{self.stock_channel_prefix}:{activity_id}"

    @staticmethod
    def order_key(order_id: int) -> str:
        """订单状态 Hash 的 Redis key"""
        return f"token:order:{order_id}"

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        """将 datetime 转换为 UTC 时区（无时区信息的视为 UTC）"""
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    async def raw_order_status(self, order_id: int) -> dict[str, Any]:
        """从 Redis 读取订单状态 Hash（原始字典）"""
        payload = await self.redis.hgetall(self.order_key(order_id))
        return {
            (key.decode() if isinstance(key, bytes) else str(key)): (
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in payload.items()
        }
