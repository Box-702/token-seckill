"""一次性数据迁移：统一 status 枚举词汇表，并补齐新约束、排序规则与列默认值

背景：models.py 早期使用 SQLAlchemy Enum 的默认行为，落库的是枚举“成员名”的大写形式
（PROCESSING / ONLINE）；现已改为落库小写“值”（processing / online），与 Redis Lua、
Redis 订单 Hash 和 API 响应保持一致。

init_database() 使用 create_all，只会创建缺失的表，不会修改已存在的表结构，因此已有
数据库必须手动执行本脚本一次。脚本幂等，可安全重复执行。

执行顺序有依赖，不要调整：
    1. 规范化 status 历史大写值
    2. 调整 status 列排序规则（见下）
    3. 补齐 CHECK 约束（必须在 1、2 之后，否则旧数据或大小写不敏感会让校验失败）
    4. 补齐列默认值

关于排序规则：MySQL 默认的 utf8mb4_unicode_ci 大小写不敏感，`status IN (...)` 这个
CHECK 会把 'ONLINE' 当成 'online' 放行；但 ORM 只认小写值，读到 'ONLINE' 会抛
LookupError。因此状态列统一改为二进制排序规则 utf8mb4_bin，让 CHECK 严格匹配大小写。

用法：
    python scripts/migrate_enum_lowercase.py
"""

from __future__ import annotations

import asyncio
from enum import StrEnum
from typing import NamedTuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from token_seckill.core.config import get_settings
from token_seckill.db.models import ActivityStatus, OrderStatus
from token_seckill.db.session import create_engine_and_session

# MySQL 上状态列使用的二进制排序规则（大小写敏感）
CASE_SENSITIVE_COLLATION = "utf8mb4_bin"


def _enum_literals(enum_cls: type[StrEnum]) -> str:
    """生成 CHECK 约束用的枚举字面量：'draft', 'ready', ..."""
    return ", ".join(f"'{member.value}'" for member in enum_cls)


class StatusColumn(NamedTuple):
    """一个需要迁移的状态列"""

    table: str
    column: str
    enum_cls: type[StrEnum]
    check_name: str
    default: str


STATUS_COLUMNS: tuple[StatusColumn, ...] = (
    StatusColumn(
        "seckill_activities",
        "status",
        ActivityStatus,
        "ck_seckill_activities_activity_status",
        "draft",
    ),
    StatusColumn(
        "grant_orders",
        "status",
        OrderStatus,
        "ck_grant_orders_order_status",
        "processing",
    ),
)

# (表名, 约束名, 约束表达式)：与 models.py 中 create_constraint=True 生成的约束一致
CHECK_CONSTRAINTS: tuple[tuple[str, str, str], ...] = (
    *((s.table, s.check_name, f"status IN ({_enum_literals(s.enum_cls)})") for s in STATUS_COLUMNS),
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
    ("seckill_activities", "version", "1"),
    ("agent_runs", "metadata_json", "(JSON_OBJECT())"),
    *((s.table, s.column, f"'{s.default}'") for s in STATUS_COLUMNS),
)


async def _table_exists(connection: AsyncConnection, table: str) -> bool:
    result = await connection.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
        ),
        {"table": table},
    )
    return bool(result.scalar())


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


async def _column_meta(
    connection: AsyncConnection, table: str, column: str
) -> tuple[object, object]:
    """读取该列的默认值与排序规则"""
    result = await connection.execute(
        text(
            "SELECT COLUMN_DEFAULT, COLLATION_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :table AND COLUMN_NAME = :column"
        ),
        {"table": table, "column": column},
    )
    row = result.one()
    return row[0], row[1]


async def _normalize_status_values(engine: AsyncEngine) -> None:
    """把 status 列的历史大写成员名改为小写值（幂等）"""
    async with engine.begin() as connection:
        for status_column in STATUS_COLUMNS:
            result = await connection.execute(
                text(
                    f"UPDATE {status_column.table} SET {status_column.column} = "
                    f"LOWER({status_column.column}) "
                    f"WHERE {status_column.column} <> LOWER({status_column.column})"
                )
            )
            print(f"  {status_column.table}.{status_column.column}: 规范化 {result.rowcount} 行")


async def _ensure_status_collation(engine: AsyncEngine) -> None:
    """把 status 列改成大小写敏感的二进制排序规则（幂等）

    若排序规则需要变更，会先删掉既有 CHECK 约束再改列，最后交由
    _ensure_check_constraints 重新创建，确保约束按新排序规则生效。
    """
    for status_column in STATUS_COLUMNS:
        async with engine.begin() as connection:
            _, collation = await _column_meta(connection, status_column.table, status_column.column)
            if collation == CASE_SENSITIVE_COLLATION:
                print(f"  {status_column.table}.{status_column.column}: 排序规则已正确，跳过")
                continue
            if await _check_constraint_exists(
                connection, status_column.table, status_column.check_name
            ):
                await connection.execute(
                    text(
                        f"ALTER TABLE {status_column.table} "
                        f"DROP CHECK {status_column.check_name}"
                    )
                )
            await connection.execute(
                text(
                    f"ALTER TABLE {status_column.table} MODIFY {status_column.column} "
                    f"VARCHAR(16) CHARACTER SET utf8mb4 COLLATE {CASE_SENSITIVE_COLLATION} "
                    f"NOT NULL DEFAULT '{status_column.default}'"
                )
            )
            print(
                f"  {status_column.table}.{status_column.column}: "
                f"排序规则已改为 {CASE_SENSITIVE_COLLATION}"
            )


async def _ensure_check_constraints(engine: AsyncEngine) -> None:
    """补齐 CHECK 约束；必须在规范化与排序规则调整之后执行"""
    for table, name, expression in CHECK_CONSTRAINTS:
        async with engine.begin() as connection:
            if await _check_constraint_exists(connection, table, name):
                print(f"  {name}: 已存在，跳过")
                continue
            await connection.execute(
                text(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression})")
            )
            print(f"  {name}: 已创建")


def _normalize_default(value: str) -> str:
    """规范化默认值以便比较：MySQL 对表达式默认值返回 json_array() 这类形式"""
    return value.strip().strip("'").strip("()").replace(" ", "").lower()


async def _ensure_column_defaults(engine: AsyncEngine) -> None:
    """把原本只存在于 Python 侧的默认值落到数据库 DEFAULT（幂等）"""
    for table, column, default in COLUMN_DEFAULTS:
        async with engine.begin() as connection:
            current, _ = await _column_meta(connection, table, column)
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
        async with engine.begin() as connection:
            if not await _table_exists(connection, "grant_orders"):
                print("未找到业务表，请先启动一次应用让 create_all 建表，再执行本迁移")
                return
        print("1/4 规范化 status 枚举值")
        await _normalize_status_values(engine)
        print("2/4 调整 status 列排序规则为大小写敏感")
        await _ensure_status_collation(engine)
        print("3/4 补齐 CHECK 约束")
        await _ensure_check_constraints(engine)
        print("4/4 补齐列默认值")
        await _ensure_column_defaults(engine)
        print("迁移完成")
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
