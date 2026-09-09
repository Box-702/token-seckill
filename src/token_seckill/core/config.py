from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全局配置类，从 .env 文件和环境变量加载"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---- 基础配置 ----
    app_name: str = "TokenSeckill"
    app_env: str = "development"
    database_url: str = "sqlite+aiosqlite:///./agent_token.db"
    redis_url: str = "redis://localhost:6379/0"
    memcached_enabled: bool = False
    memcached_host: str = "localhost"
    memcached_port: int = 11211
    admin_key: SecretStr = SecretStr("dev-admin-key")

    # ---- 多级缓存配置 ----
    request_cache_max_entries: int = 128      # 请求级缓存最大条目
    local_cache_max_entries: int = 2_000      # 进程级 TTL 缓存最大条目
    local_cache_ttl_seconds: int = 15         # 进程级缓存 TTL（秒）
    shared_cache_ttl_seconds: int = 300       # Redis/Memcached 共享缓存 TTL（秒）
    negative_cache_ttl_seconds: int = 30      # 缓存穿透保护 TTL（秒）

    # ---- Redis Stream 消费组配置 ----
    grant_stream: str = "stream:token:grants"
    grant_group: str = "g-token-grant"
    grant_consumer: str = "grant-worker-1"
    grant_pending_idle_ms: int = 30_000       # 消息空闲多久后触发自动认领
    grant_batch_size: int = 20

    # ---- 方向 A：延时取消队列（秒杀 -> 超时自动释放）----
    order_fulfillment_timeout_seconds: int = 900   # 订单履约超时（秒）
    delay_cancel_batch_size: int = 50              # 每次扫描批量
    delay_cancel_scan_interval_seconds: int = 5    # 扫描间隔（秒）
    delay_cancel_enabled: bool = True

    # ---- 方向 B：多维限流 + 行为验证 ----
    rate_limit_enabled: bool = True
    rate_limit_user_limit: int = 10           # 用户级每窗口最大请求数
    rate_limit_user_window: int = 60          # 用户级窗口大小（秒）
    rate_limit_ip_limit: int = 30             # IP 级每窗口最大请求数
    rate_limit_ip_window: int = 60            # IP 级窗口大小（秒）
    rate_limit_activity_limit: int = 100      # 活动级每窗口最大请求数
    rate_limit_activity_window: int = 60      # 活动级窗口大小（秒）
    challenge_enabled: bool = False           # 是否启用验证码
    challenge_ttl_seconds: int = 120          # 验证码有效期（秒）

    # ---- 方向 C：SSE 实时库存推送 ----
    stock_channel_prefix: str = "token:stock"
    stock_sse_heartbeat_seconds: int = 15     # SSE 心跳间隔（秒）

    # ---- LLM / Agent 配置 ----
    llm_api_key: SecretStr | None = None
    llm_model: str | None = None
    llm_base_url: str | None = None
    agent_history_limit: int = 20             # Agent 对话历史保留条数


@lru_cache
def get_settings() -> Settings:
    """获取全局配置单例"""
    return Settings()
