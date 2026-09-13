"""一次性数据迁移：统一 status 枚举词汇表，并补齐新约束与列默认值

背景：models.py 早期使用 SQLAlchemy Enum 的默认行为，落库的是枚举“成员名”的大写形式
（PROCESSING / ONLINE）；现已改为落库小写“值”（processing / online），与 Redis Lua、
Redis 订单 Hash 和 API 响应保持一致。

init_database() 使用 create_all，只会创建缺失的表，不会修改已存在的表结构，因此已有
数据库必须手动执行本脚本一次。脚本幂等，可安全重复执行。

用法：
    python scripts/migrate_enum_lowercase.py
"""

from __future__ import annotations

import asyncio
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from token_seckill.core.config import get_settings
from token_seckill.db.models import ActivityStatus, OrderStatus
from token_seckill.db.session import create_engine_and_session


def _enum_literals(enum_cls: type[StrEnum]) -> str:
    """生成 CHECK 约束用的枚举字面量：'draft', 'ready', ..."""
    return ", ".join(f"'{member.value}'" for member in enum_cls)


# (表名, 列名)：需要把历史大写值规范化为小写
STATUS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("seckill_activities", "status"),
    ("grant_orders", "status"),
)

# (表名, 约束名, 约束表达式)：与 models.py 中 create_constraint=True 生成的约束一致
CHECK_CONSTRAINTS: tuple[tuple[str, str, str], ...] = (
    (
        "seckill_activities",
        "ck_seckill_activities_activity_status",
        f"status IN ({_enum_literals(ActivityStatus)})",
    ),
    (
        "grant_orders",
        "ck_grant_orders_order_status",
        f"status IN ({_enum_literals(OrderStatus)})",
    ),
    ("users", "ck_users_token_balance_nonnegative", "token_balance >= 0"),
)

# (表名, 列名, 默认值字面量)：与 models.py 中的 server_default 一致
COLUMN_DEFAULTS: tuple[tuple[str, str, str], ...] = (
    ("users", "token_balance", "0"),
    ("token_skus", "price_cents", "0"),
    ("token_skus", "validity_days", "30"),
    ("token_skus", "active", "1"),
    ("token_skus", "allowed_models", "(JSON_ARRAY())"),
    ("seckill_activities", "per_user_limit", "1"),
    ("seckill_activities", "status", "'draft'"),
    ("seckill_activities", "version", "1"),
    ("grant_orders", "status", "'processing'"),
    ("agent_runs", "metadata_json", "(JSON_OBJECT())"),
)


async def _normalize_status_values(engine: AsyncEngine) -> None:
    """把 status 列的历史大写成员名改为小写值（幂等）"""
    async with engine.begin() as connection:
        for table, column in STATUS_COLUMNS:
            result = await connection.execute(
                text(
                    f"UPDATE {table} SET {column} = LOWER({column}) "
                    f"WHERE {column} <> LOWER({column})"
                )
            )
            print(f"  {table}.{column}: 规范化 {result.rowcount} 行")


async def _check_constraint_exists(connection: AsyncConnection, table: str, name: str) -> bool:
    """查询 information_schema 判断 CHECK 约束是否已存在"""
    result = await connection.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE CONSTRAINT_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :table AND CONSTRAINT_NAME = :name"
        ),
        {"table": table, "name": name},
    )
    return bool(result.scalar())


async def _ensure_check_constraints(engine: AsyncEngine) -> None:
    """补齐 CHECK 约束；必须在规范化之后执行，否则历史大写值会导致校验失败"""
    for table, name, expression in CHECK_CONSTRAINTS:
        async with engine.begin() as connection:
            if await _check_constraint_exists(connection, table, name):
                print(f"  {name}: 已存在，跳过")
                continue
            await connection.execute(
                text(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression})")
            )
            print(f"  {name}: 已创建")


async def _column_default(connection: AsyncConnection, table: str, column: str) -> object:
    """读取 information_schema 中该列的当前默认值"""
    result = await connection.execute(
        text(
            "SELECT COLUMN_DEFAULT FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :table AND COLUMN_NAME = :column"
        ),
        {"table": table, "column": column},
    )
    return result.scalar()


def _normalize_default(value: str) -> str:
    """规范化默认值以便比较：MySQL 对表达式默认值返回 json_array() 这类形式"""
    return value.strip().strip("'").strip("()").replace(" ", "").lower()


async def _ensure_column_defaults(engine: AsyncEngine) -> None:
    """把原本只存在于 Python 侧的默认值落到数据库 DEFAULT（幂等）"""
    for table, column, default in COLUMN_DEFAULTS:
        async with engine.begin() as connection:
            current = await _column_default(connection, table, column)
            if current is not None and _normalize_default(str(current)) == _normalize_default(
                default
            ):
                print(f"  {table}.{column}: 默认值已为 {default}，跳过")
                continue
            await connection.execute(
                text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT {default}")
            )
            print(f"  {table}.{column}: 默认值已设为 {default}")


async def migrate() -> None:
    settings = get_settings()
    engine, _ = create_engine_and_session(settings.database_url)
    try:
        if engine.dialect.name != "mysql":
            print(
                f"当前方言为 {engine.dialect.name}，表结构由 create_all 全新创建，无需执行本迁移"
            )
            return
        print("1/3 规范化 status 枚举值")
        await _normalize_status_values(engine)
        print("2/3 补齐 CHECK 约束")
        await _ensure_check_constraints(engine)
        print("3/3 补齐列默认值")
        await _ensure_column_defaults(engine)
        print("迁移完成")
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
