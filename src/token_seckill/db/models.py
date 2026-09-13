from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from token_seckill.db.base import Base

# 主键类型：MySQL 使用 BigInteger，SQLite 降级为 Integer
PK_INT = BigInteger().with_variant(Integer, "sqlite")


def _enum_values(enum_cls: type[StrEnum]) -> list[str]:
    """让 SQLAlchemy 持久化枚举成员的小写值（draft），而不是大写成员名（DRAFT）

    Redis Lua、Redis 订单 Hash 与 API 响应使用的都是小写值，统一后
    同一个状态在数据库和 Redis 中词汇表一致，避免手写 SQL 查不到数据。
    """
    return [member.value for member in enum_cls]


class ActivityStatus(StrEnum):
    """秒杀活动状态"""
    DRAFT = "draft"       # 草稿
    READY = "ready"       # 已发布未开始
    ONLINE = "online"     # 进行中
    ENDED = "ended"       # 已结束
    PAUSED = "paused"     # 已暂停


class OrderStatus(StrEnum):
    """订单状态"""
    PROCESSING = "processing"   # 处理中（Redis 已扣减，等待异步落库）
    SUCCESS = "success"         # 成功（已落库并发放 Token）
    FAILED = "failed"           # 失败（永久性错误，已补偿库存）
    CANCELLED = "cancelled"     # 已取消（超时未履约，已释放库存）


class User(Base):
    """用户表"""
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("token_balance >= 0", name="token_balance_nonnegative"),
    )

    id: Mapped[int] = mapped_column(PK_INT, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    token_balance: Mapped[int] = mapped_column(BigInteger, default=0, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    orders: Mapped[list[GrantOrder]] = relationship(back_populates="user")
    ledger_entries: Mapped[list[TokenLedger]] = relationship(back_populates="user")


class TokenSku(Base):
    """Token 套餐 SKU 表"""
    __tablename__ = "token_skus"
    __table_args__ = (
        CheckConstraint("token_amount > 0", name="token_amount_positive"),
        CheckConstraint("price_cents >= 0", name="price_nonnegative"),
    )

    id: Mapped[int] = mapped_column(PK_INT, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200))
    token_amount: Mapped[int] = mapped_column(BigInteger)
    price_cents: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    validity_days: Mapped[int] = mapped_column(Integer, default=30, server_default=text("30"))
    allowed_models: Mapped[list[str]] = mapped_column(
        JSON, default=list, server_default=text("(JSON_ARRAY())")
    )
    active: Mapped[bool] = mapped_column(default=True, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    activities: Mapped[list[SeckillActivity]] = relationship(back_populates="sku")


class SeckillActivity(Base):
    """秒杀活动表"""
    __tablename__ = "seckill_activities"
    __table_args__ = (
        CheckConstraint("initial_stock > 0", name="initial_stock_positive"),
        CheckConstraint("db_stock >= 0", name="db_stock_nonnegative"),
        CheckConstraint("per_user_limit = 1", name="one_order_per_user"),
    )

    id: Mapped[int] = mapped_column(PK_INT, primary_key=True, autoincrement=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("token_skus.id"), index=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ends_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    initial_stock: Mapped[int] = mapped_column(Integer)
    db_stock: Mapped[int] = mapped_column(Integer)           # 数据库侧库存（异步落库时扣减）
    per_user_limit: Mapped[int] = mapped_column(Integer, default=1, server_default=text("1"))
    status: Mapped[ActivityStatus] = mapped_column(
        Enum(
            ActivityStatus,
            native_enum=False,
            length=16,
            create_constraint=True,
            name="activity_status",
            values_callable=_enum_values,
        ),
        default=ActivityStatus.DRAFT,
        server_default=text("'draft'"),
    )
    # 修订号：每次 publish 自增，用于缓存失效与客户端判断配置是否变更（非并发控制手段）
    version: Mapped[int] = mapped_column(Integer, default=1, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    sku: Mapped[TokenSku] = relationship(back_populates="activities")
    orders: Mapped[list[GrantOrder]] = relationship(back_populates="activity")


class GrantOrder(Base):
    """发放订单表：用户 + 活动唯一约束保证每人每活动只能下一单"""
    __tablename__ = "grant_orders"
    __table_args__ = (
        UniqueConstraint("user_id", "activity_id", name="user_activity"),
        Index("ix_grant_orders_status_created", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)  # 雪花 ID
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    activity_id: Mapped[int] = mapped_column(ForeignKey("seckill_activities.id"), index=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("token_skus.id"))
    token_amount: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[OrderStatus] = mapped_column(
        Enum(
            OrderStatus,
            native_enum=False,
            length=16,
            create_constraint=True,
            name="order_status",
            values_callable=_enum_values,
        ),
        default=OrderStatus.PROCESSING,
        server_default=text("'processing'"),
    )
    failure_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    user: Mapped[User] = relationship(back_populates="orders")
    activity: Mapped[SeckillActivity] = relationship(back_populates="orders")
    ledger_entry: Mapped[TokenLedger | None] = relationship(back_populates="order")


class TokenLedger(Base):
    """Token 流水账本表：记录每次 Token 变动的明细"""
    __tablename__ = "token_ledger"

    id: Mapped[int] = mapped_column(PK_INT, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("grant_orders.id"), unique=True, index=True)
    delta_tokens: Mapped[int] = mapped_column(BigInteger)       # 本次变动 Token 数量
    balance_after: Mapped[int] = mapped_column(BigInteger)      # 变动后余额
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # Token 过期时间
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped[User] = relationship(back_populates="ledger_entries")
    order: Mapped[GrantOrder] = relationship(back_populates="ledger_entry")


class AgentRun(Base):
    """Agent 调用记录表"""
    __tablename__ = "agent_runs"

    id: Mapped[int] = mapped_column(PK_INT, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    input_text: Mapped[str] = mapped_column(Text)
    output_text: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, server_default=text("(JSON_OBJECT())")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
