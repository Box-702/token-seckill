from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

# 约束命名规范：确保数据库迁移时约束名称一致且可预测
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """SQLAlchemy 声明式基类"""
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
