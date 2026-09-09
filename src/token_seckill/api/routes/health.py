from fastapi import APIRouter, Request
from sqlalchemy import text

from token_seckill.core.metrics import metrics_response

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(request: Request) -> dict[str, str]:
    """健康检查：验证 Redis 和 MySQL 连接"""
    await request.app.state.redis.ping()
    async with request.app.state.session_factory() as session:
        await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@router.get("/metrics", include_in_schema=False)
async def metrics() -> object:
    """Prometheus 指标端点"""
    return metrics_response()
