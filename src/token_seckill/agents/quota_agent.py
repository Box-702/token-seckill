from __future__ import annotations

import json
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any
from uuid import uuid4

import orjson
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import BaseTool, tool
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from token_seckill.core.config import Settings
from token_seckill.db.models import (
    ActivityStatus,
    AgentRun,
    GrantOrder,
    SeckillActivity,
    TokenSku,
    User,
)
from token_seckill.schemas.domain import AgentResponse, ApprovalView
from token_seckill.services.seckill import SeckillService

# 系统提示词：约束 Agent 只能通过工具操作，不能绕过审批直接抢购
SYSTEM_PROMPT = """你是大模型 Token 额度运营助手。你只能依据工具返回的数据回答。
你可以查询用户余额、活动、订单状态，也可以创建一个待用户确认的领取动作。
你绝不能绕过审批直接领取 Token 包；prepare_token_claim 只生成审批动作，必须告诉用户 action_id，
并明确说明需要调用审批接口后才会真正抢购。回答简洁、准确，不编造库存、余额或订单结果。"""


class AgentUnavailableError(RuntimeError):
    """LLM 未配置时抛出此异常"""
    pass


class QuotaAgentService:
    """AI Agent 服务：基于 LangGraph 构建对话式 Token 额度管理助手

    核心流程：
    1. 用户发送消息 -> 加载历史上下文 -> 调用 LLM
    2. LLM 可通过工具查询余额/活动/订单，或创建待审批的领取动作
    3. 领取动作需要用户通过 /approve 接口确认后才会真正执行秒杀
    """

    def __init__(
        self,
        settings: Settings,
        redis: Redis,
        session_factory: async_sessionmaker[AsyncSession],
        seckill_service: SeckillService,
    ) -> None:
        self.settings = settings
        self.redis = redis
        self.session_factory = session_factory
        self.seckill_service = seckill_service
        # 按 user_id 缓存编译后的 LangGraph 图，避免每次 invoke 重建
        self._graph_cache: dict[int, Any] = {}

    async def invoke(
        self,
        *,
        user_id: int,
        message: str,
        thread_id: str | None,
    ) -> AgentResponse:
        """处理一次 Agent 对话调用"""
        if self.settings.llm_api_key is None or not self.settings.llm_model:
            raise AgentUnavailableError("请配置 LLM_API_KEY 和 LLM_MODEL 后再调用 Agent")
        thread_id = thread_id or uuid4().hex
        # 加载该会话的历史消息
        history = await self._load_history(user_id, thread_id)
        # 获取或构建该用户的 LangGraph（带缓存）
        graph = self._get_or_build_graph(user_id)
        result = await graph.ainvoke({"messages": [*history, HumanMessage(content=message)]})
        final_message = result["messages"][-1]
        answer = self._message_text(final_message)
        # 持久化对话历史到 Redis
        await self._append_history(user_id, thread_id, "human", message)
        await self._append_history(user_id, thread_id, "ai", answer)
        # 记录 Agent 调用日志到数据库
        async with self.session_factory() as session:
            session.add(
                AgentRun(
                    thread_id=thread_id,
                    user_id=user_id,
                    input_text=message,
                    output_text=answer,
                    metadata_json={"model": self.settings.llm_model},
                )
            )
            await session.commit()
        return AgentResponse(thread_id=thread_id, answer=answer)

    async def approve(self, *, user_id: int, action_id: str) -> ApprovalView:
        """审批 Agent 创建的领取动作，执行真正的秒杀"""
        key = f"agent:approval:{action_id}"
        lock = self.redis.lock(f"lock:{key}", timeout=10, blocking_timeout=2)
        if not await lock.acquire():
            raise RuntimeError("approval is being processed")
        try:
            payload = self._decode_hash(await self.redis.hgetall(key))
            if not payload or int(payload.get("user_id", 0)) != user_id:
                raise ValueError("approval action not found")
            activity_id = int(payload["activity_id"])
            # 如果已经处理过，直接返回当前状态（幂等）
            if payload.get("status") != "pending":
                order_id = int(payload["order_id"]) if payload.get("order_id") else None
                return ApprovalView(
                    action_id=action_id,
                    status=payload["status"],
                    activity_id=activity_id,
                    order_id=order_id,
                )
            # 执行秒杀
            claim = await self.seckill_service.claim(user_id, activity_id)
            status_value = claim.outcome.value
            await self.redis.hset(key, "status", status_value)
            if claim.order_id is not None:
                await self.redis.hset(key, "order_id", claim.order_id)
            return ApprovalView(
                action_id=action_id,
                status=status_value,
                activity_id=activity_id,
                order_id=claim.order_id,
            )
        finally:
            await lock.release()

    def _get_or_build_graph(self, user_id: int) -> Any:
        """获取缓存的 LangGraph 图，未命中则构建并缓存"""
        if user_id not in self._graph_cache:
            self._graph_cache[user_id] = self._build_graph(user_id)
        return self._graph_cache[user_id]

    def _build_graph(self, user_id: int) -> Any:
        """构建 LangGraph 状态图：assistant <-> tools 循环"""
        tools = self._make_tools(user_id)
        api_key = self.settings.llm_api_key
        if api_key is None or not self.settings.llm_model:
            raise AgentUnavailableError("LLM is not configured")
        kwargs: dict[str, Any] = {
            "model": self.settings.llm_model,
            "api_key": api_key.get_secret_value(),
            "temperature": 0,
        }
        if self.settings.llm_base_url:
            kwargs["base_url"] = self.settings.llm_base_url
        model = ChatOpenAI(**kwargs).bind_tools(tools)

        async def call_model(state: MessagesState) -> dict[str, list[BaseMessage]]:
            response = await model.ainvoke(
                [SystemMessage(content=SYSTEM_PROMPT), *state["messages"]]
            )
            return {"messages": [response]}

        # 构建状态图：START -> assistant -> (tools | END) -> assistant
        builder = StateGraph(MessagesState)
        builder.add_node("assistant", call_model)
        builder.add_node("tools", ToolNode(tools))
        builder.add_edge(START, "assistant")
        builder.add_conditional_edges("assistant", tools_condition, {"tools": "tools", END: END})
        builder.add_edge("tools", "assistant")
        return builder.compile()

    def _make_tools(self, user_id: int) -> list[BaseTool]:
        """为指定用户创建 Agent 工具集"""
        service = self

        @tool
        async def get_my_token_balance() -> str:
            """查询当前用户可用的 Token 余额。"""
            async with service.session_factory() as session:
                user = await session.get(User, user_id)
                if user is None:
                    return json.dumps({"error": "user not found"}, ensure_ascii=False)
                return json.dumps(
                    {"user_id": user.id, "token_balance": user.token_balance},
                    ensure_ascii=False,
                )

        @tool
        async def list_active_token_packages() -> str:
            """列出尚未结束的 Token 包活动、活动时间和价格。"""
            async with service.session_factory() as session:
                rows = (
                    await session.execute(
                        select(SeckillActivity, TokenSku)
                        .join(TokenSku, TokenSku.id == SeckillActivity.sku_id)
                        .where(
                            SeckillActivity.status.in_(
                                [ActivityStatus.READY, ActivityStatus.ONLINE]
                            )
                        )
                        .order_by(SeckillActivity.starts_at)
                    )
                ).all()
                data = [
                    {
                        "activity_id": activity.id,
                        "name": sku.name,
                        "token_amount": sku.token_amount,
                        "price_cents": sku.price_cents,
                        "starts_at": activity.starts_at.isoformat(),
                        "ends_at": activity.ends_at.isoformat(),
                        "status": activity.status.value,
                    }
                    for activity, sku in rows
                ]
                return json.dumps(data, ensure_ascii=False)

        @tool
        async def get_token_order_status(order_id: int) -> str:
            """按订单 ID 查询异步 Token 发放状态。"""
            async with service.session_factory() as session:
                order = await session.get(GrantOrder, order_id)
                if order is not None and order.user_id == user_id:
                    return json.dumps(
                        {
                            "order_id": order.id,
                            "status": order.status.value,
                            "token_amount": order.token_amount,
                            "failure_reason": order.failure_reason,
                        },
                        ensure_ascii=False,
                    )
            # DB 未找到时回退到 Redis 查询（订单可能还在处理中）
            pending = await service.seckill_service.raw_order_status(order_id)
            if pending and int(pending.get("user_id", 0)) == user_id:
                return json.dumps(pending, ensure_ascii=False)
            return json.dumps({"error": "order not found"}, ensure_ascii=False)

        @tool
        async def prepare_token_claim(activity_id: int) -> str:
            """创建待人工确认的 Token 包领取动作；此工具不会直接执行抢购。"""
            action_id = uuid4().hex
            key = f"agent:approval:{action_id}"
            await service.redis.hset(
                key,
                mapping={
                    "user_id": user_id,
                    "activity_id": activity_id,
                    "status": "pending",
                    "created_at": datetime.now(UTC).isoformat(),
                },
            )
            await service.redis.expire(key, 600)
            return json.dumps(
                {
                    "action_id": action_id,
                    "status": "pending",
                    "message": "需要用户确认后才会真正抢购",
                },
                ensure_ascii=False,
            )

        return [
            get_my_token_balance,
            list_active_token_packages,
            get_token_order_status,
            prepare_token_claim,
        ]

    async def _load_history(self, user_id: int, thread_id: str) -> list[BaseMessage]:
        """从 Redis 加载会话历史消息"""
        values = await self.redis.lrange(self._history_key(user_id, thread_id), 0, -1)
        messages: list[BaseMessage] = []
        for value in values:
            payload = orjson.loads(value)
            if payload["role"] == "human":
                messages.append(HumanMessage(content=payload["content"]))
            else:
                messages.append(AIMessage(content=payload["content"]))
        return messages

    async def _append_history(
        self,
        user_id: int,
        thread_id: str,
        role: str,
        content: str,
    ) -> None:
        """追加消息到 Redis 会话历史，并裁剪到限制长度"""
        key = self._history_key(user_id, thread_id)
        await self.redis.rpush(key, orjson.dumps({"role": role, "content": content}))
        await self.redis.ltrim(key, -self.settings.agent_history_limit, -1)
        await self.redis.expire(key, 604_800)  # 7 天过期

    @staticmethod
    def _history_key(user_id: int, thread_id: str) -> str:
        return f"agent:history:{user_id}:{thread_id}"

    @staticmethod
    def _message_text(message: BaseMessage) -> str:
        """提取消息文本内容"""
        if isinstance(message.content, str):
            return message.content
        return json.dumps(message.content, ensure_ascii=False)

    @staticmethod
    def _decode_hash(payload: dict[bytes | str, bytes | str]) -> dict[str, str]:
        """将 Redis hash 的 bytes 键值对解码为 str"""
        return {
            (key.decode() if isinstance(key, bytes) else key): (
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in payload.items()
        }
