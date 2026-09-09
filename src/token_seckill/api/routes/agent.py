from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from token_seckill.agents.quota_agent import (
    AgentUnavailableError,
    QuotaAgentService,
)
from token_seckill.api.dependencies import get_current_user_id, get_session
from token_seckill.db.models import User
from token_seckill.schemas.domain import AgentRequest, AgentResponse, ApprovalView

router = APIRouter(prefix="/api/v1/agent", tags=["agent"])


@router.post("/invoke", response_model=AgentResponse)
async def invoke_agent(
    payload: AgentRequest,
    request: Request,
    user_id: int = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> AgentResponse:
    """调用 AI Agent 进行对话"""
    if await session.get(User, user_id) is None:
        raise HTTPException(status_code=401, detail="unknown user")
    agent_service = cast(QuotaAgentService, request.app.state.agent_service)
    try:
        return await agent_service.invoke(
            user_id=user_id,
            message=payload.message,
            thread_id=payload.thread_id,
        )
    except AgentUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/actions/{action_id}/approve", response_model=ApprovalView)
async def approve_action(
    action_id: str,
    request: Request,
    user_id: int = Depends(get_current_user_id),
) -> ApprovalView:
    """审批 Agent 创建的领取动作，触发真正的秒杀"""
    agent_service = cast(QuotaAgentService, request.app.state.agent_service)
    try:
        return await agent_service.approve(
            user_id=user_id,
            action_id=action_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
