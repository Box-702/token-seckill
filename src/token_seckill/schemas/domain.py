from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from token_seckill.db.models import ActivityStatus, OrderStatus


# ---- 请求模型 ----

class CreateUserRequest(BaseModel):
    """创建用户请求"""
    email: str = Field(min_length=3, max_length=255)


class CreateSkuRequest(BaseModel):
    """创建 Token 套餐 SKU 请求"""
    name: str = Field(min_length=2, max_length=200)
    token_amount: int = Field(gt=0)
    price_cents: int = Field(default=0, ge=0)
    validity_days: int = Field(default=30, gt=0, le=3650)
    allowed_models: list[str] = Field(default_factory=list)


class CreateActivityRequest(BaseModel):
    """创建秒杀活动请求"""
    sku_id: int
    starts_at: datetime
    ends_at: datetime
    stock: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_window(self) -> "CreateActivityRequest":
        """校验活动时间窗口"""
        if self.starts_at >= self.ends_at:
            raise ValueError("starts_at must be earlier than ends_at")
        return self


class AgentRequest(BaseModel):
    """Agent 对话请求"""
    message: str = Field(min_length=1, max_length=4000)
    thread_id: str | None = Field(default=None, max_length=64)


# ---- 响应/视图模型 ----

class UserView(BaseModel):
    """用户视图"""
    model_config = ConfigDict(from_attributes=True)

    id: int
    email: str
    token_balance: int


class SkuView(BaseModel):
    """SKU 视图"""
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    token_amount: int
    price_cents: int
    validity_days: int
    allowed_models: list[str]
    active: bool


class ActivityView(BaseModel):
    """活动视图"""
    id: int
    sku_id: int
    sku_name: str
    token_amount: int
    price_cents: int
    allowed_models: list[str]
    starts_at: datetime
    ends_at: datetime
    initial_stock: int
    remaining_stock_hint: int | None = None  # Redis 实时库存提示（仅供参考）
    status: ActivityStatus
    version: int


class ClaimOutcome(StrEnum):
    """秒杀结果枚举"""
    ACCEPTED = "accepted"       # 秒杀成功
    SOLD_OUT = "sold_out"       # 库存不足
    DUPLICATE = "duplicate"     # 重复领取
    NOT_ACTIVE = "not_active"   # 活动未开始/已结束/未发布


class ClaimResponse(BaseModel):
    """秒杀响应"""
    outcome: ClaimOutcome
    order_id: int | None = None
    message: str


class OrderView(BaseModel):
    """订单视图"""
    id: int
    user_id: int
    activity_id: int
    sku_id: int
    token_amount: int
    status: OrderStatus
    failure_reason: str | None = None


class AgentResponse(BaseModel):
    """Agent 对话响应"""
    thread_id: str
    answer: str


class ApprovalView(BaseModel):
    """Agent 审批动作视图"""
    action_id: str
    status: str
    activity_id: int
    order_id: int | None = None
