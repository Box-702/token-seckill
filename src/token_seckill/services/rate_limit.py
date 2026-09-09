from __future__ import annotations

import logging
import secrets
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, status
from redis.asyncio import Redis

from token_seckill.core.config import Settings
from token_seckill.core.metrics import RATE_LIMIT_REJECTIONS

logger = logging.getLogger(__name__)

# 验证码答案在 Redis 中的 key 前缀
_CHALLENGE_PREFIX = "challenge:answer:"


class RateLimitService:
    """方向 B：多维滑动窗口限流 + 行为验证

    秒杀接口受三层独立的滑动窗口保护（用户级、IP 级、活动级）。
    每个窗口是一个 Redis ZSET，成员为请求标记，分数为毫秒时间戳；
    裁剪窗口只需 ZREMRANGEBYSCORE + ZCARD，开销极低且无乐观锁竞争。
    可选的一次性数学验证码提供反机器人"行为验证"层。
    """

    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings

    def _window_key(self, namespace: str, scope: str) -> str:
        """生成限流窗口的 Redis key"""
        return f"rl:{namespace}:{scope}"

    async def sliding_window(self, key: str, limit: int, window: int) -> tuple[bool, int]:
        """滑动窗口限流检查，返回 (是否允许, 重试等待秒数)

        window 单位为秒；内部时间戳使用毫秒以保证精度。
        """
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        window_ms = window * 1000
        # 裁剪窗口外的过期记录
        await self.redis.zremrangebyscore(key, min=0, max=now_ms - window_ms)
        count = await self.redis.zcard(key)
        if count < limit:
            # 未超限，记录本次请求
            member = f"{now_ms}:{secrets.token_hex(8)}"
            await self.redis.zadd(key, {member: now_ms})
            await self.redis.expire(key, window * 2)
            return True, 0

        # 已超限，计算最早过期记录的重试等待时间
        oldest = await self.redis.zrange(key, start=0, end=0, withscores=True)
        if oldest:
            oldest_ms = int(oldest[0][1])
            retry_after = max(1, int((oldest_ms + window_ms - now_ms) / 1000))
        else:
            retry_after = 1
        return False, retry_after

    async def check_claim(self, user_id: int, ip: str, activity_id: int) -> None:
        """检查秒杀请求是否超出三层限流预算，超限则抛出 429"""
        if not self.settings.rate_limit_enabled:
            return
        # 三层限流配置：(key, 限制次数, 窗口秒数, 层名称)
        tiers: list[tuple[str, int, int, str]] = [
            (
                self._window_key("user", str(user_id)),
                self.settings.rate_limit_user_limit,
                self.settings.rate_limit_user_window,
                "user",
            ),
            (
                self._window_key("ip", ip),
                self.settings.rate_limit_ip_limit,
                self.settings.rate_limit_ip_window,
                "ip",
            ),
            (
                self._window_key("activity", str(activity_id)),
                self.settings.rate_limit_activity_limit,
                self.settings.rate_limit_activity_window,
                "activity",
            ),
        ]
        for key, limit, window, tier in tiers:
            allowed, retry_after = await self.sliding_window(key, limit, window)
            if not allowed:
                RATE_LIMIT_REJECTIONS.labels(tier).inc()
                headers = {"Retry-After": str(retry_after)}
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="请求过于频繁，请稍后再试",
                    headers=headers,
                )

    async def issue_challenge(self) -> dict[str, str]:
        """生成一次性数学验证码，返回题目供客户端展示"""
        left = secrets.randbelow(89) + 10
        right = secrets.randbelow(89) + 10
        answer = str(left + right)
        challenge_id = uuid4().hex
        await self.redis.set(
            f"{_CHALLENGE_PREFIX}{challenge_id}",
            answer,
            ex=self.settings.challenge_ttl_seconds,
        )
        return {"challenge_id": challenge_id, "question": f"{left} + {right} = ?"}

    async def verify_challenge(self, value: str) -> bool:
        """校验 X-Challenge 请求头（格式：id:answer），一次性消费"""
        challenge_id, separator, answer = value.partition(":")
        if not separator or not challenge_id or not answer:
            return False
        key = f"{_CHALLENGE_PREFIX}{challenge_id}"
        expected_raw: str | bytes | None = await self.redis.get(key)
        if expected_raw is None:
            return False
        expected = expected_raw.decode() if isinstance(expected_raw, bytes) else expected_raw
        # 无论答案正确与否都删除验证码（一次性消费）
        await self.redis.delete(key)
        if not secrets.compare_digest(expected, answer):
            logger.warning("challenge answer mismatch", extra={"challenge_id": challenge_id})
        return secrets.compare_digest(expected, answer)

    async def peek_budget(self, namespace: str, scope: str) -> dict[str, Any]:
        """诊断接口：查看指定限流窗口的当前使用情况"""
        key = self._window_key(namespace, scope)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        # 使用60秒作为诊断窗口的默认裁剪范围
        await self.redis.zremrangebyscore(key, min=0, max=now_ms - 60_000)
        count = await self.redis.zcard(key)
        return {"key": key, "current": count, "scope": scope, "namespace": namespace}
