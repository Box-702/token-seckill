from __future__ import annotations

import asyncio
import random
from typing import Any, cast

import aiomcache
import orjson
from cachetools import TTLCache
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from token_seckill.core.request_context import get_request_cache
from token_seckill.db.models import SeckillActivity, TokenSku
from token_seckill.schemas.domain import ActivityView


class ActivityCache:
    """多级活动缓存：请求级 -> 进程级 -> Memcached -> Redis -> 数据库

    读取链路（逐级穿透）：
    1. ContextVar 请求缓存（同一请求内复用，零开销）
    2. TTLCache 进程级缓存（跨请求共享，毫秒级）
    3. Memcached 共享缓存（跨进程共享，可选）
    4. Redis 缓存（持久化共享层）
    5. 数据库（最终数据源，回填所有缓存层）

    写入/失效时会同步清除所有层。
    """

    def __init__(
        self,
        redis: Redis,
        *,
        local_max_entries: int,
        local_ttl_seconds: int,
        shared_ttl_seconds: int,
        negative_ttl_seconds: int,
        memcached: aiomcache.Client | None = None,
    ) -> None:
        self.redis = redis
        self.memcached = memcached
        # 进程级 TTL 缓存
        self.local: TTLCache[str, bytes] = TTLCache(
            maxsize=local_max_entries,
            ttl=local_ttl_seconds,
        )
        self.local_lock = asyncio.Lock()
        self.shared_ttl_seconds = shared_ttl_seconds
        self.negative_ttl_seconds = negative_ttl_seconds

    @staticmethod
    def key(activity_id: int) -> str:
        """缓存 key 格式"""
        return f"cache:token:activity:{activity_id}"

    async def get(self, session: AsyncSession, activity_id: int) -> ActivityView | None:
        """按读取链路逐级查找活动数据，未命中则从数据库加载并回填缓存"""
        key = self.key(activity_id)

        # 第1层：请求级缓存（ContextVar）
        scope = get_request_cache()
        if key in scope:
            return cast(ActivityView | None, scope[key])

        # 第2层：进程级 TTL 缓存
        async with self.local_lock:
            local_value = self.local.get(key)
        if local_value is not None:
            result = self._decode(local_value)
            scope[key] = result
            return result

        # 第3层：Memcached 共享缓存（可选）
        if self.memcached is not None:
            memcached_value = await self.memcached.get(key.encode())
            if memcached_value is not None:
                result = self._decode(memcached_value)
                await self._fill_local(key, memcached_value)
                scope[key] = result
                return result

        # 第4层：Redis 缓存
        redis_value = await self.redis.get(key)
        if redis_value is not None:
            redis_bytes = redis_value.encode() if isinstance(redis_value, str) else redis_value
            result = self._decode(redis_bytes)
            # 回填上层缓存
            if self.memcached is not None:
                await self.memcached.set(key.encode(), redis_bytes, exptime=self.shared_ttl_seconds)
            await self._fill_local(key, redis_bytes)
            scope[key] = result
            return result

        # 第5层：数据库（最终数据源）
        result = await self._load_from_database(session, activity_id)
        encoded = self._encode(result)
        # 负缓存（防止缓存穿透）使用较短 TTL
        ttl = (
            self.negative_ttl_seconds
            if result is None
            else self.shared_ttl_seconds + random.randint(0, 30)  # 加随机抖动防止缓存雪崩
        )
        # 回填所有缓存层
        await self.redis.set(key, encoded, ex=ttl)
        if self.memcached is not None:
            await self.memcached.set(key.encode(), encoded, exptime=ttl)
        await self._fill_local(key, encoded)
        scope[key] = result
        return result

    async def invalidate(self, activity_id: int) -> None:
        """清除所有缓存层的活动数据"""
        key = self.key(activity_id)
        get_request_cache().pop(key, None)
        async with self.local_lock:
            self.local.pop(key, None)
        if self.memcached is not None:
            await self.memcached.delete(key.encode())
        await self.redis.delete(key)

    async def _fill_local(self, key: str, value: bytes) -> None:
        """写入进程级缓存"""
        async with self.local_lock:
            self.local[key] = value

    @staticmethod
    async def _load_from_database(
        session: AsyncSession,
        activity_id: int,
    ) -> ActivityView | None:
        """从数据库加载活动详情（JOIN SKU 表）"""
        statement = (
            select(SeckillActivity, TokenSku)
            .join(TokenSku, TokenSku.id == SeckillActivity.sku_id)
            .where(SeckillActivity.id == activity_id)
        )
        row = (await session.execute(statement)).one_or_none()
        if row is None:
            return None
        activity, sku = row
        return ActivityView(
            id=activity.id,
            sku_id=sku.id,
            sku_name=sku.name,
            token_amount=sku.token_amount,
            price_cents=sku.price_cents,
            allowed_models=sku.allowed_models,
            starts_at=activity.starts_at,
            ends_at=activity.ends_at,
            initial_stock=activity.initial_stock,
            status=activity.status,
            version=activity.version,
        )

    @staticmethod
    def _encode(value: ActivityView | None) -> bytes:
        """序列化活动数据（含负缓存标记）"""
        if value is None:
            return orjson.dumps({"missing": True})
        return orjson.dumps({"missing": False, "value": value.model_dump(mode="json")})

    @staticmethod
    def _decode(value: bytes | str) -> ActivityView | None:
        """反序列化活动数据"""
        payload: dict[str, Any] = orjson.loads(value)
        if payload.get("missing"):
            return None
        return ActivityView.model_validate(payload["value"])
