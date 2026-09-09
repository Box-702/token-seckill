from __future__ import annotations

import orjson


def format_sse_event(event: str, payload: dict[str, object]) -> str:
    """格式化一个 SSE 帧（event 名称 + JSON 数据）"""
    return f"event: {event}\ndata: {orjson.dumps(payload).decode()}\n\n"
