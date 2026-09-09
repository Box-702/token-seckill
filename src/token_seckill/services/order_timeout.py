from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis

from token_seckill.core.config import Settings
from token_seckill.core.metrics import AUTO_CANCELLED_ORDERS
from token_seckill.services.seckill import SeckillService

logger = logging.getLogger(__name__)


def _as_int(value: Any) -> int:
    """将 Redis ZSET 成员（bytes/str）规范化为订单 ID"""
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        return int(value)
    return int(str(value))


class OrderTimeoutCanceller:
    """方向 A：延时取消队列扫描器

    秒杀成功时订单被写入 Redis ZSET（score = 履约截止时间戳）。
    本扫描器定期检查过期条目，在与发放 Worker 相同的分布式锁保护下
    重新检查订单状态，仅取消仍处于 processing 状态的订单。
    已发放/已失败的订单直接从队列中移除。
    """

    def __init__(
        self,
        redis: Redis,
        seckill_service: SeckillService,
        settings: Settings,
    ) -> None:
        self.redis = redis
        self.seckill_service = seckill_service
        self.settings = settings

    async def run_forever(self) -> None:
        """持续运行的扫描循环"""
        while True:
            try:
                await self.scan_once()
            except Exception:  # noqa: BLE001 - 保持循环存活，忽略瞬态错误
                logger.exception("delay-cancel scan failed")
            await asyncio.sleep(self.settings.delay_cancel_scan_interval_seconds)

    async def scan_once(self) -> int:
        """扫描一次延时取消队列，处理所有过期订单"""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        expired = await self.redis.zrangebyscore(
            self.seckill_service.delay_key(),
            min=0,
            max=now_ms,
            start=0,
            num=self.settings.delay_cancel_batch_size,
        )
        cancelled = 0
        for raw_order_id in expired:
            order_id = _as_int(raw_order_id)
            if await self.cancel_one(order_id):
                cancelled += 1
        return cancelled

    async def cancel_one(self, order_id: int) -> bool:
        """取消单个订单：获取分布式锁 -> 重新检查状态 -> 仅取消 processing 状态的订单"""
        fields = await self.seckill_service.raw_order_status(order_id)
        if not fields:
            # 订单 Hash 不存在，直接从队列移除
            await self.redis.zrem(self.seckill_service.delay_key(), order_id)
            return False
        user_id = int(fields["user_id"])
        activity_id = int(fields["activity_id"])
        # 获取与发放 Worker 相同的用户级分布式锁，保证串行化
        lock = self.redis.lock(f"lock:grant:user:{user_id}", timeout=30, blocking_timeout=2)
        if not await lock.acquire():
            logger.warning("delay-cancel lock busy for user", extra={"user_id": user_id})
            return False
        try:
            current = await self.seckill_service.raw_order_status(order_id)
            # 与发放 Worker 串行化：已发放/已失败/已取消的订单不能释放库存
            if current.get("status") not in ("processing",):
                await self.redis.zrem(self.seckill_service.delay_key(), order_id)
                return False
            # 仅 processing 状态的订单执行取消
            released = await self.seckill_service.cancel_timeout(
                activity_id=activity_id,
                user_id=user_id,
                order_id=order_id,
                reason="unfulfilled before deadline",
            )
            if released:
                AUTO_CANCELLED_ORDERS.inc()
            return released
        finally:
            await lock.release()
