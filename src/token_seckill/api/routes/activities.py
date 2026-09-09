from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from sqlalchemy.ext.asyncio import AsyncSession

from token_seckill.api.dependencies import get_current_user_id, get_session
from token_seckill.core.metrics import CLAIM_OUTCOMES
from token_seckill.db.models import User
from token_seckill.schemas.domain import ActivityView, ClaimResponse
from token_seckill.services.activity_cache import ActivityCache
from token_seckill.services.rate_limit import RateLimitService
from token_seckill.services.seckill import SeckillService
from token_seckill.services.stock_stream import format_sse_event

router = APIRouter(prefix="/api/v1/activities", tags=["activities"])


async def _stock_sync_payload(
    request: Request,
    seckill_service: SeckillService,
    session: AsyncSession,
    activity_id: int,
) -> dict[str, object]:
    """构建 SSE 同步帧数据：活动信息 + 当前库存"""
    activity_cache = cast(ActivityCache, request.app.state.activity_cache)
    activity: ActivityView | None = await activity_cache.get(session, activity_id)
    raw_stock = await request.app.state.redis.get(seckill_service.activity_keys(activity_id)[0])
    fallback_stock = activity.initial_stock if activity else None
    remaining = int(raw_stock) if raw_stock is not None else fallback_stock
    return {
        "activity_id": activity_id,
        "status": activity.status.value if activity else "unknown",
        "initial_stock": activity.initial_stock if activity else None,
        "remaining": remaining,
        "starts_at": activity.starts_at.isoformat() if activity else None,
        "ends_at": activity.ends_at.isoformat() if activity else None,
    }


@router.get("/{activity_id}", response_model=ActivityView)
async def get_activity(
    activity_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> ActivityView:
    """查询活动详情（含 Redis 实时库存提示）"""
    activity_cache = cast(ActivityCache, request.app.state.activity_cache)
    seckill_service = cast(SeckillService, request.app.state.seckill_service)
    activity = await activity_cache.get(session, activity_id)
    if activity is None:
        raise HTTPException(status_code=404, detail="activity not found")
    stock_key, _, _ = seckill_service.activity_keys(activity_id)
    raw_stock = await request.app.state.redis.get(stock_key)
    hint = int(raw_stock) if raw_stock is not None else None
    return activity.model_copy(update={"remaining_stock_hint": hint})


@router.get("/{activity_id}/stock/stream")
async def stream_activity_stock(
    activity_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> StreamingResponse:
    """方向 C：SSE 实时库存推送

    连接时先发送 sync 帧（倒计时 + 当前库存），之后每次秒杀扣减库存时
    通过 Redis Pub/Sub 推送 stock 帧。Lua 脚本在扣减后会 PUBLISH 到
    每个活动独立的 Redis 频道，本端点订阅该频道并转发给客户端。
    """
    seckill_service = cast(SeckillService, request.app.state.seckill_service)
    settings = request.app.state.settings
    channel = seckill_service.stock_channel(activity_id)
    redis_url = settings.redis_url
    # 在流式传输前预加载同步帧数据（依赖注入的 session 在 SSE 生命周期内不保证存活）
    sync_payload = await _stock_sync_payload(request, seckill_service, session, activity_id)

    async def event_source() -> Any:
        # SSE 需要独立的 Redis 连接用于长时间订阅
        client: Redis = Redis.from_url(redis_url, decode_responses=False)
        pubsub: PubSub = client.pubsub()
        await pubsub.subscribe(channel)
        # 消费 SUBSCRIBE 确认消息，确保订阅激活后再推送首帧
        await pubsub.get_message(timeout=1.0)
        yield format_sse_event("sync", sync_payload)
        try:
            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message is None or message["type"] != "message":
                    continue
                data = message["data"]
                if isinstance(data, bytes):
                    remaining = int(data)
                else:
                    remaining = int(str(data))
                yield format_sse_event("stock", {"remaining": remaining})
        finally:
            # 清理订阅连接
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()  # type: ignore[no-untyped-call]
            await client.aclose()

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{activity_id}/claim", response_model=ClaimResponse)
async def claim_activity(
    activity_id: int,
    request: Request,
    user_id: int = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
    x_challenge: str | None = Header(default=None, alias="X-Challenge"),
) -> ClaimResponse:
    """秒杀接口：限流校验 -> 验证码校验 -> 用户存在性校验 -> 执行秒杀"""
    # 第一层：多维限流检查
    rate_limit = cast(RateLimitService, request.app.state.rate_limit_service)
    ip = request.client.host if request.client is not None else "unknown"
    await rate_limit.check_claim(user_id, ip, activity_id)
    # 第二层：验证码校验（可选）
    if request.app.state.settings.challenge_enabled:
        if x_challenge is None or not await rate_limit.verify_challenge(x_challenge):
            raise HTTPException(status_code=403, detail="需要先通过验证码/答题校验")
    # 第三层：用户存在性校验
    if await session.get(User, user_id) is None:
        raise HTTPException(status_code=401, detail="unknown user")
    # 执行秒杀（Redis Lua 原子操作）
    seckill_service = cast(SeckillService, request.app.state.seckill_service)
    result = await seckill_service.claim(user_id, activity_id)
    CLAIM_OUTCOMES.labels(result.outcome.value).inc()
    return result
