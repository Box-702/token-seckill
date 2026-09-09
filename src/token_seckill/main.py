from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aiomcache
import uvicorn
from fastapi import FastAPI
from redis.asyncio import Redis

from token_seckill.agents.quota_agent import QuotaAgentService
from token_seckill.api.routes import activities, admin, agent, health, orders, rate
from token_seckill.core.config import get_settings
from token_seckill.core.metrics import MetricsMiddleware
from token_seckill.core.request_context import RequestCacheMiddleware
from token_seckill.db.session import create_engine_and_session, init_database
from token_seckill.services.activity_cache import ActivityCache
from token_seckill.services.rate_limit import RateLimitService
from token_seckill.services.seckill import SeckillService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """应用生命周期管理：启动时初始化所有基础设施，关闭时释放资源

    初始化顺序：数据库 -> Redis -> Memcached -> 多级缓存 -> 秒杀服务 -> 限流服务 -> Agent 服务
    """
    settings = get_settings()

    # 初始化数据库引擎并自动建表
    engine, session_factory = create_engine_and_session(
        settings.database_url,
        echo=settings.app_env == "local-sql-debug",
    )
    await init_database(engine)

    # 初始化 Redis 连接
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    await redis.ping()

    # 可选：初始化 Memcached 作为共享缓存层
    memcached = (
        aiomcache.Client(settings.memcached_host, settings.memcached_port)
        if settings.memcached_enabled
        else None
    )

    # 初始化多级活动缓存：请求级 -> 进程级 -> Memcached -> Redis -> DB
    activity_cache = ActivityCache(
        redis,
        local_max_entries=settings.local_cache_max_entries,
        local_ttl_seconds=settings.local_cache_ttl_seconds,
        shared_ttl_seconds=settings.shared_cache_ttl_seconds,
        negative_ttl_seconds=settings.negative_cache_ttl_seconds,
        memcached=memcached,
    )

    # 初始化秒杀核心服务（Redis Lua 原子操作）
    seckill_service = SeckillService(
        redis,
        activity_cache,
        settings.grant_stream,
        order_fulfillment_timeout_seconds=settings.order_fulfillment_timeout_seconds,
        stock_channel_prefix=settings.stock_channel_prefix,
    )

    # 将所有服务实例挂载到 app.state 供路由依赖注入使用
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.redis = redis
    app.state.memcached = memcached
    app.state.activity_cache = activity_cache
    app.state.seckill_service = seckill_service
    app.state.rate_limit_service = RateLimitService(redis, settings)
    app.state.agent_service = QuotaAgentService(
        settings,
        redis,
        session_factory,
        seckill_service,
    )
    try:
        yield
    finally:
        # 关闭时释放所有连接资源
        if memcached is not None:
            await memcached.close()
        await redis.aclose()
        await engine.dispose()


# 创建 FastAPI 应用实例
app = FastAPI(
    title="TokenSeckill · 限量 Token 套餐高并发秒杀平台",
    version="0.1.0",
    description="面向 AI Agent 的限量 Token 套餐高并发秒杀平台：Redis Lua 原子发放、异步落库、延时取消、多维限流与 SSE 实时库存推送",
    lifespan=lifespan,
)
# 注册中间件（Starlette 按注册的逆序执行：Metrics -> RequestCache）
app.add_middleware(RequestCacheMiddleware)
app.add_middleware(MetricsMiddleware)
# 注册路由
app.include_router(health.router)
app.include_router(admin.router)
app.include_router(activities.router)
app.include_router(orders.router)
app.include_router(agent.router)
app.include_router(rate.router)


def run() -> None:
    """控制台入口点：启动 uvicorn 服务器"""
    uvicorn.run(
        "token_seckill.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )


if __name__ == "__main__":
    run()
