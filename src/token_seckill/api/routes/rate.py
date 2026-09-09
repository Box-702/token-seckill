from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, Request

from token_seckill.services.rate_limit import RateLimitService

router = APIRouter(prefix="/api/v1/rate", tags=["rate"])


@router.post("/challenge")
async def issue_challenge(request: Request) -> dict[str, str]:
    """方向 B：生成一次性数学验证码

    当 CHALLENGE_ENABLED=true 时，客户端必须先调用此接口获取验证码，
    然后在秒杀请求中通过 X-Challenge: <id>:<answer> 请求头提交答案。
    """
    rate_limit = cast(RateLimitService, request.app.state.rate_limit_service)
    return await rate_limit.issue_challenge()


@router.get("/budget/{namespace}/{scope}")
async def rate_budget(request: Request, namespace: str, scope: str) -> dict[str, Any]:
    """诊断接口：查看指定限流窗口的当前使用情况（非业务 API）"""
    rate_limit = cast(RateLimitService, request.app.state.rate_limit_service)
    return await rate_limit.peek_budget(namespace, scope)
