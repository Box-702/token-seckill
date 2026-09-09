from __future__ import annotations

import asyncio
import logging
from typing import cast

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from token_seckill.core.config import Settings, get_settings
from token_seckill.db.session import create_engine_and_session, init_database
from token_seckill.services.activity_cache import ActivityCache
from token_seckill.services.grant import (
    GrantMessage,
    GrantService,
    PermanentGrantError,
)
from token_seckill.services.order_timeout import OrderTimeoutCanceller
from token_seckill.services.seckill import SeckillService

logger = logging.getLogger(__name__)

# Redis Stream 类型别名
type StreamFields = dict[bytes | str, bytes | str]
type StreamEntry = tuple[bytes | str, StreamFields]
type StreamReadResult = list[tuple[bytes | str, list[StreamEntry]]]


class GrantConsumer:
    """Redis Stream 消费者：消费秒杀成功的订单消息，完成异步 Token 发放

    核心流程：
    1. XREADGROUP 读取 Stream 新消息
    2. 获取用户级分布式锁（与延时取消器共享）
    3. 重新检查订单状态（延时取消器可能已释放）
    4. 调用 GrantService.process() 完成 DB 落库
    5. 更新 Redis 订单状态 + 从延时队列移除 + ACK 消息
    6. 永久性失败触发补偿回滚，瞬态失败保留消息待重试
    """

    def __init__(
        self,
        settings: Settings,
        redis: Redis,
        grant_service: GrantService,
        seckill_service: SeckillService,
    ) -> None:
        self.settings = settings
        self.redis = redis
        self.grant_service = grant_service
        self.seckill_service = seckill_service

    async def ensure_group(self) -> None:
        """确保消费组存在（首次启动时创建）"""
        try:
            await self.redis.xgroup_create(
                self.settings.grant_stream,
                self.settings.grant_group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run_forever(self) -> None:
        """持续消费循环：读取新消息 + 恢复超时消息"""
        await self.ensure_group()
        while True:
            # 先认领超时未确认的消息（防止 Worker 崩溃导致消息丢失）
            await self.recover_stale()
            # 读取新消息
            messages = cast(
                StreamReadResult,
                await self.redis.xreadgroup(
                    self.settings.grant_group,
                    self.settings.grant_consumer,
                    {self.settings.grant_stream: ">"},
                    count=self.settings.grant_batch_size,
                    block=2_000,
                ),
            )
            for _, entries in messages:
                for message_id, fields in entries:
                    await self.process_entry(message_id, fields)

    async def recover_stale(self) -> None:
        """认领超时未确认的消息（XAUTOCLAIM）：处理 Worker 崩溃的情况"""
        claimed = await self.redis.xautoclaim(
            self.settings.grant_stream,
            self.settings.grant_group,
            self.settings.grant_consumer,
            min_idle_time=self.settings.grant_pending_idle_ms,
            start_id="0-0",
            count=self.settings.grant_batch_size,
        )
        entries = cast(list[StreamEntry], claimed[1] if len(claimed) >= 2 else [])
        for message_id, fields in entries:
            await self.process_entry(message_id, fields)

    async def process_entry(
        self,
        message_id: bytes | str,
        fields: dict[bytes | str, bytes | str],
    ) -> None:
        """处理单条 Stream 消息：加锁 -> 状态检查 -> 发放/补偿 -> ACK"""
        message = GrantMessage.from_stream(fields)
        # 获取用户级分布式锁，保证同一用户的订单串行处理
        lock = self.redis.lock(
            f"lock:grant:user:{message.user_id}",
            timeout=30,
            blocking_timeout=2,
        )
        if not await lock.acquire():
            logger.warning("grant lock busy", extra={"user_id": message.user_id})
            return
        try:
            # 延时取消器可能在等待锁期间已释放此订单，需重新检查
            current = await self.seckill_service.raw_order_status(message.order_id)
            if current.get("status") != "processing":
                # 已被取消/失败，清理延时队列并 ACK
                await self.redis.zrem(self.seckill_service.delay_key(), message.order_id)
                await self.redis.xack(
                    self.settings.grant_stream,
                    self.settings.grant_group,
                    message_id,
                )
                return
            # 执行数据库侧发放（创建订单 + 写流水 + 更新余额）
            order = await self.grant_service.process(message)
            # 更新 Redis 订单状态为成功
            await self.redis.hset(
                self.seckill_service.order_key(message.order_id),
                mapping={
                    "status": "success",
                    "persisted_order_id": order.id,
                },
            )
            # 从延时取消队列移除 + ACK 消息
            await self.redis.zrem(self.seckill_service.delay_key(), message.order_id)
            await self.redis.xack(
                self.settings.grant_stream,
                self.settings.grant_group,
                message_id,
            )
        except PermanentGrantError as exc:
            # 永久性失败：补偿回滚 Redis 库存 + ACK（不再重试）
            await self.seckill_service.compensate(
                activity_id=message.activity_id,
                user_id=message.user_id,
                order_id=message.order_id,
                reason=str(exc),
            )
            await self.redis.xack(
                self.settings.grant_stream,
                self.settings.grant_group,
                message_id,
            )
            logger.error("permanent grant failure: %s", exc)
        except Exception:
            # 瞬态失败：不 ACK，消息保留在 pending 列表等待重试
            logger.exception(
                "transient grant failure; message remains pending",
                extra={"order_id": message.order_id},
            )
        finally:
            await lock.release()


async def worker_main() -> None:
    """Worker 入口：初始化基础设施，并发运行发放消费者和延时取消扫描器"""
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()

    # 初始化数据库和 Redis
    engine, session_factory = create_engine_and_session(settings.database_url)
    await init_database(engine)
    redis = Redis.from_url(settings.redis_url, decode_responses=False)

    # 初始化缓存和秒杀服务
    cache = ActivityCache(
        redis,
        local_max_entries=settings.local_cache_max_entries,
        local_ttl_seconds=settings.local_cache_ttl_seconds,
        shared_ttl_seconds=settings.shared_cache_ttl_seconds,
        negative_ttl_seconds=settings.negative_cache_ttl_seconds,
    )
    seckill_service = SeckillService(
        redis,
        cache,
        settings.grant_stream,
        order_fulfillment_timeout_seconds=settings.order_fulfillment_timeout_seconds,
        stock_channel_prefix=settings.stock_channel_prefix,
    )

    # 创建发放消费者
    consumer = GrantConsumer(
        settings,
        redis,
        GrantService(session_factory),
        seckill_service,
    )

    # 并发运行：发放消费者 + 延时取消扫描器（可选）
    tasks: list[asyncio.Task[None]] = [asyncio.create_task(consumer.run_forever())]
    if settings.delay_cancel_enabled:
        canceller = OrderTimeoutCanceller(redis, seckill_service, settings)
        tasks.append(asyncio.create_task(canceller.run_forever()))
    try:
        await asyncio.gather(*tasks)
    finally:
        # 优雅关闭：取消所有任务并等待完成
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await redis.aclose()
        await engine.dispose()


def run() -> None:
    """控制台入口点"""
    asyncio.run(worker_main())


if __name__ == "__main__":
    run()
