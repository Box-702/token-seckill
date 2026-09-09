from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from token_seckill.api.dependencies import get_current_user_id, get_session
from token_seckill.db.models import GrantOrder, OrderStatus
from token_seckill.schemas.domain import OrderView

router = APIRouter(prefix="/api/v1/orders", tags=["orders"])


@router.get("/{order_id}", response_model=OrderView)
async def get_order(
    order_id: int,
    request: Request,
    user_id: int = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> OrderView:
    """查询订单状态：优先查 DB，未命中则回退到 Redis（订单可能还在处理中）"""
    # 先查数据库（已落库的订单）
    order = await session.get(GrantOrder, order_id)
    if order is not None:
        if order.user_id != user_id:
            raise HTTPException(status_code=404, detail="order not found")
        return OrderView.model_validate(order, from_attributes=True)

    # DB 未命中，回退查 Redis（订单可能还在异步处理中）
    pending = await request.app.state.seckill_service.raw_order_status(order_id)
    if not pending or int(pending.get("user_id", 0)) != user_id:
        raise HTTPException(status_code=404, detail="order not found")
    return OrderView(
        id=order_id,
        user_id=user_id,
        activity_id=int(pending["activity_id"]),
        sku_id=int(pending["sku_id"]),
        token_amount=int(pending["token_amount"]),
        status=OrderStatus(pending["status"]),
        failure_reason=pending.get("failure_reason"),
    )
