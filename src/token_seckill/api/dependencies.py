from collections.abc import AsyncIterator

from fastapi import Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """FastAPI 依赖：提供数据库会话"""
    async with request.app.state.session_factory() as session:
        yield session


async def get_current_user_id(
    x_user_id: int = Header(..., alias="X-User-Id", gt=0),
) -> int:
    """FastAPI 依赖：从请求头提取用户 ID"""
    return x_user_id


async def require_admin(
    request: Request,
    x_admin_key: str = Header(..., alias="X-Admin-Key"),
) -> None:
    """FastAPI 依赖：校验管理员密钥"""
    expected = request.app.state.settings.admin_key.get_secret_value()
    if x_admin_key != expected:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="invalid admin key")
