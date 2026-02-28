"""
Helper functions for tests.
"""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urljoin

import httpx
from a2a.client.helpers import create_text_message_object
from a2a.client.middleware import ClientCallContext
from a2a.client.transports.rest import RestTransport
from a2a.types import MessageSendConfiguration, MessageSendParams, Role
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# Whether to print request/response details
SHOW_IO = os.getenv("TEST_SHOW_IO", "1").strip().lower() in {"1", "true", "yes"}

# Max characters to print for IO
MAX_IO_CHARS = int(os.getenv("TEST_MAX_IO_CHARS", "2000"))

# Anyscale API token for authenticated requests
ANYSCALE_API_TOKEN = os.getenv("ANYSCALE_API_TOKEN", "").strip()


def get_auth_headers() -> dict[str, str]:
    """Get authorization headers if ANYSCALE_API_TOKEN is set."""
    if ANYSCALE_API_TOKEN:
        return {"Authorization": f"Bearer {ANYSCALE_API_TOKEN}"}
    return {}


def create_http_client() -> httpx.AsyncClient:
    """Create an httpx AsyncClient with auth headers if available."""
    headers = get_auth_headers()
    return httpx.AsyncClient(headers=headers)


def make_url(base: str, path: str) -> str:
    """Join base URL with path."""
    return urljoin(base.rstrip("/") + "/", path.lstrip("/"))


def truncate(text: str, max_chars: int = MAX_IO_CHARS) -> str:
    """Truncate text to max_chars."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 20] + f"... ({len(text)} chars total)"


def format_json(obj: Any, max_chars: int = MAX_IO_CHARS) -> str:
    """Format object as JSON string, truncated."""
    if obj is None:
        return ""
    try:
        text = json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        text = str(obj)
    return truncate(text, max_chars)


def print_io(url: str = None, request: Any = None, response: Any = None) -> None:
    """Print request/response details if SHOW_IO is enabled."""
    if not SHOW_IO:
        return
    print("  IO:")
    if url:
        print(f"    URL: {url}")
    if request is not None:
        print(f"    Request: {format_json(request)}")
    if response is not None:
        print(f"    Response: {format_json(response)}")


def print_result(passed: bool, name: str, detail: str = "", duration_ms: float = 0) -> bool:
    """Print test result and return pass/fail status."""
    status = "PASS" if passed else "FAIL"
    symbol = "✓" if passed else "✗"
    time_str = f" ({duration_ms:.0f}ms)" if duration_ms > 0 else ""
    
    print(f"  [{status}] {symbol} {name}{time_str}")
    if detail:
        print(f"         {detail[:150]}")
    
    return passed


async def read_sse_stream(
    client: httpx.AsyncClient,
    url: str,
    payload: dict,
    timeout_s: float,
) -> tuple[bool, str, list[str]]:
    """
    Read SSE stream from agent /chat endpoints.
    
    Returns:
        (success, last_data_or_error, list_of_data_frames)
    """
    data_count = 0
    last_data = ""
    error_event = False
    error_detail = ""
    frames: list[str] = []

    timeout = httpx.Timeout(connect=10.0, read=timeout_s, write=30.0, pool=30.0)
    
    async with client.stream("POST", url, json=payload, timeout=timeout) as response:
        response.raise_for_status()

        async for line in response.aiter_lines():
            if not line:
                continue
                
            # Check for error event
            if line.startswith("event:"):
                event_type = line.split(":", 1)[1].strip()
                if event_type == "error":
                    error_event = True
                continue
            
            # Parse data frames
            if line.startswith("data:"):
                data = line.split(":", 1)[1].strip()
                if data:
                    data_count += 1
                    last_data = data
                    if len(frames) < 3:
                        frames.append(data)
                    if error_event:
                        error_detail = data
                        break

            # Stop after a few frames (smoke test)
            if data_count >= 3:
                break

    if error_event:
        return False, error_detail or "SSE error event received", frames
    if data_count == 0:
        return False, "No SSE data frames received", frames
    return True, last_data, frames


async def mcp_list_tools(mcp_url: str, timeout_s: float) -> list[str]:
    """
    Connect to MCP server and list available tools.
    
    Returns:
        List of tool names
    """
    headers = get_auth_headers() or None
    async with streamablehttp_client(
        mcp_url.rstrip("/"),
        headers=headers,
        timeout=timeout_s,
        sse_read_timeout=timeout_s * 2,
    ) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools_result = await session.list_tools()
            
            names = []
            for tool in getattr(tools_result, "tools", []) or []:
                name = getattr(tool, "name", None)
                if isinstance(name, str) and name:
                    names.append(name)
            return names


def _model_dump_any(obj: Any) -> Any:
    """Best-effort conversion of pydantic/SDK objects to plain Python structures."""
    if obj is None:
        return None
    dump = getattr(obj, "model_dump", None)
    if callable(dump):
        try:
            return dump()
        except Exception:
            pass
    to_dict = getattr(obj, "dict", None)
    if callable(to_dict):
        try:
            return to_dict()
        except Exception:
            pass
    return obj


def _extract_text_from_mcp_result(result_dump: Any) -> str:
    """
    Extract human-readable text from a MCP CallToolResult-like dump.
    Works with common MCP SDK shapes: {"content": [{"type":"text","text":"..."}], ...}
    """
    if not isinstance(result_dump, dict):
        return ""
    content = result_dump.get("content")
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if isinstance(item, dict):
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts).strip()


async def mcp_call_tool(mcp_url: str, tool_name: str, arguments: dict, timeout_s: float) -> tuple[dict, str]:
    """
    Connect to MCP server and call a tool.

    Returns:
        (result_dump, extracted_text)
    """
    headers = get_auth_headers() or None
    async with streamablehttp_client(
        mcp_url.rstrip("/"),
        headers=headers,
        timeout=timeout_s,
        sse_read_timeout=timeout_s * 2,
    ) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)
            result_dump = _model_dump_any(result)
            if not isinstance(result_dump, dict):
                result_dump = {"result": result_dump}
            return result_dump, _extract_text_from_mcp_result(result_dump)


async def a2a_execute(
    client: httpx.AsyncClient,
    base_url: str,
    input_text: str,
    timeout_s: float,
) -> tuple[dict, dict]:
    """
    Execute an A2A call via the official REST API (`a2a-sdk`).
    
    Returns:
        (request_payload, response_data)
    """
    base_url = (base_url or "").rstrip("/")
    if not base_url:
        raise ValueError("Missing A2A base URL")

    timeout = httpx.Timeout(connect=10.0, read=timeout_s, write=30.0, pool=30.0)
    ctx = ClientCallContext(state={"http_kwargs": {"timeout": timeout}})

    message = create_text_message_object(role=Role.user, content=input_text)
    params = MessageSendParams(
        message=message,
        configuration=MessageSendConfiguration(blocking=True),
        metadata=None,
    )

    transport = RestTransport(client, url=base_url)
    result = await transport.send_message(params, context=ctx)

    request_payload = _model_dump_any(params)
    response_data = _model_dump_any(result)
    if not isinstance(request_payload, dict):
        request_payload = {"request": request_payload}
    if not isinstance(response_data, dict):
        response_data = {"response": response_data}
    return request_payload, response_data
