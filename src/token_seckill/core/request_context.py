from contextvars import ContextVar, Token
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

# 请求级缓存上下文变量，每个请求独立，请求结束后自动清除
request_cache: ContextVar[dict[str, Any] | None] = ContextVar("request_cache", default=None)


def get_request_cache() -> dict[str, Any]:
    """获取当前请求的缓存字典，不存在则创建"""
    cache = request_cache.get()
    if cache is None:
        cache = {}
        request_cache.set(cache)
    return cache


class RequestCacheMiddleware(BaseHTTPMiddleware):
    """请求级缓存中间件：为每个 HTTP 请求创建独立的缓存作用域

    利用 ContextVar 实现请求级别的数据隔离，确保同一请求内的多次
    数据库/缓存查询可以复用结果，请求结束后自动清理。
    """

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        token: Token[dict[str, Any] | None] = request_cache.set({})
        try:
            return await call_next(request)
        finally:
            request_cache.reset(token)
