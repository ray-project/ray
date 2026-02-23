"""
Client helpers using the official `a2a-sdk` REST transport.

This is used by agents that orchestrate other agents (agent-to-agent calls).
"""

from __future__ import annotations

from typing import Any, Iterable, Optional
from uuid import uuid4

import httpx
from a2a.client.helpers import create_text_message_object
from a2a.client.transports.rest import RestTransport
from a2a.types import Message, MessageSendConfiguration, MessageSendParams, Role, Task, TextPart


def _extract_text_parts(parts: Iterable[Any]) -> str:
    texts: list[str] = []
    for p in parts or []:
        # `Part` is a pydantic RootModel; `.root` holds TextPart/FilePart/DataPart.
        root = getattr(p, "root", p)
        if isinstance(root, TextPart):
            t = getattr(root, "text", "")
            if isinstance(t, str) and t.strip():
                texts.append(t.strip())
    return "\n".join(texts).strip()


def _extract_text_from_task(task: Task) -> str:
    status = getattr(task, "status", None)
    msg = getattr(status, "message", None) if status is not None else None
    if isinstance(msg, Message):
        txt = _extract_text_parts(getattr(msg, "parts", []))
        if txt:
            return txt

    history = getattr(task, "history", None) or []
    for m in reversed(history):
        if isinstance(m, Message) and getattr(m, "role", None) == Role.agent:
            txt = _extract_text_parts(getattr(m, "parts", []))
            if txt:
                return txt
    return ""


def _extract_text_from_result(result: Task | Message) -> str:
    if isinstance(result, Message):
        return _extract_text_parts(getattr(result, "parts", []))
    if isinstance(result, Task):
        return _extract_text_from_task(result)
    return ""


async def a2a_execute_text(
    base_url: str,
    input_text: str,
    *,
    timeout_s: float = 240.0,
    headers: Optional[dict[str, str]] = None,
) -> str:
    """
    Send a single text message to an A2A agent over the official REST API.

    Args:
        base_url: Base URL of the downstream agent (no trailing slash), e.g.
            "http://127.0.0.1:8000/a2a-research"
        input_text: User request to send
        timeout_s: Overall read timeout
        headers: Optional extra HTTP headers (auth, etc.)
    """
    base_url = (base_url or "").rstrip("/")
    if not base_url:
        raise ValueError("Missing downstream A2A base URL.")
    if not isinstance(input_text, str) or not input_text.strip():
        raise ValueError("Missing input text.")

    timeout = httpx.Timeout(connect=10.0, read=timeout_s, write=30.0, pool=30.0)
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        transport = RestTransport(client, url=base_url)
        message = create_text_message_object(role=Role.user, content=input_text)
        # Ensure deterministic IDs for easier tracing/debugging.
        message.message_id = str(uuid4())
        params = MessageSendParams(
            message=message,
            configuration=MessageSendConfiguration(blocking=True),
            metadata=None,
        )
        result = await transport.send_message(params)

    text = _extract_text_from_result(result)
    if not text:
        raise RuntimeError("Downstream A2A agent returned an empty result.")
    return text

