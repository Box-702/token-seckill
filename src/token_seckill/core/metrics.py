from time import perf_counter

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

# HTTP 请求总数计数器（按方法/路由/状态码分组）
HTTP_REQUESTS = Counter(
    "agent_token_http_requests_total",
    "HTTP requests handled by the API",
    ("method", "route", "status"),
)
# HTTP 请求延迟直方图（按方法/路由分组）
HTTP_LATENCY = Histogram(
    "agent_token_http_request_duration_seconds",
    "HTTP request latency",
    ("method", "route"),
)
# 秒杀结果计数器（按结果类型分组：accepted/sold_out/duplicate/not_active）
CLAIM_OUTCOMES = Counter(
    "agent_token_claim_outcomes_total",
    "Token claim outcomes",
    ("outcome",),
)
# 限流拒绝计数器（按限流层级分组：user/ip/activity）
RATE_LIMIT_REJECTIONS = Counter(
    "agent_token_rate_limit_rejections_total",
    "Claim requests rejected by a rate-limit tier",
    ("tier",),
)
# 延时取消自动释放订单计数器
AUTO_CANCELLED_ORDERS = Counter(
    "agent_token_order_auto_cancelled_total",
    "Orders auto-released by the delayed-cancellation queue",
)


class MetricsMiddleware(BaseHTTPMiddleware):
    """Prometheus 指标采集中间件：记录每个 HTTP 请求的数量和延迟"""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        started = perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        HTTP_REQUESTS.labels(request.method, route_path, response.status_code).inc()
        HTTP_LATENCY.labels(request.method, route_path).observe(perf_counter() - started)
        return response


def metrics_response() -> Response:
    """生成 Prometheus 格式的指标响应"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
