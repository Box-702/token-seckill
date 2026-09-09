from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from token_seckill.api.dependencies import get_session, require_admin
from token_seckill.db.models import SeckillActivity, TokenSku, User
from token_seckill.schemas.domain import (
    CreateActivityRequest,
    CreateSkuRequest,
    CreateUserRequest,
    SkuView,
    UserView,
)

router = APIRouter(prefix="/api/v1", tags=["admin"])


@router.post("/users", response_model=UserView, status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: CreateUserRequest,
    session: AsyncSession = Depends(get_session),
) -> User:
    """创建用户"""
    user = User(email=payload.email)
    session.add(user)
    try:
        await session.commit()
    except Exception as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="email already exists") from exc
    await session.refresh(user)
    return user


@router.post(
    "/admin/skus",
    response_model=SkuView,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin)],
)
async def create_sku(
    payload: CreateSkuRequest,
    session: AsyncSession = Depends(get_session),
) -> TokenSku:
    """创建 Token 套餐 SKU（需管理员权限）"""
    sku = TokenSku(**payload.model_dump())
    session.add(sku)
    await session.commit()
    await session.refresh(sku)
    return sku


@router.post(
    "/admin/activities",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin)],
)
async def create_activity(
    payload: CreateActivityRequest,
    session: AsyncSession = Depends(get_session),
) -> dict[str, int | str]:
    """创建秒杀活动（需管理员权限）"""
    if await session.get(TokenSku, payload.sku_id) is None:
        raise HTTPException(status_code=404, detail="SKU not found")
    activity = SeckillActivity(
        sku_id=payload.sku_id,
        starts_at=payload.starts_at,
        ends_at=payload.ends_at,
        initial_stock=payload.stock,
        db_stock=payload.stock,
    )
    session.add(activity)
    await session.commit()
    await session.refresh(activity)
    return {"id": activity.id, "status": activity.status.value}


@router.post(
    "/admin/activities/{activity_id}/publish",
    dependencies=[Depends(require_admin)],
)
async def publish_activity(
    activity_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    """发布活动到 Redis（需管理员权限）：同步库存/元数据到 Redis 缓存"""
    try:
        activity_status = await request.app.state.seckill_service.publish(session, activity_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"status": activity_status.value}
