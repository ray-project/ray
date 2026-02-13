"""
Tests for MCP (Model Context Protocol) servers.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

# Allow running this file directly via: `python tests/test_mcp.py`
# (ensures repo root is on sys.path so `tests.*` imports work)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.helpers import make_url, mcp_call_tool, mcp_list_tools, print_io, print_result

# Base URL for the Ray Serve HTTP proxy
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")

# Timeout settings (seconds)
TIMEOUT = float(os.getenv("TEST_TIMEOUT_SECONDS", "60"))


async def test_web_search_mcp() -> bool:
    """Test MCP web search server - list tools and execute a real tool call."""
    print("\n  Test: Web Search MCP")
    
    mcp_url = make_url(BASE_URL, "/mcp-web-search/mcp")
    start = time.perf_counter()
    
    try:
        tools = await mcp_list_tools(mcp_url, TIMEOUT)

        # 1) Web search tool call
        search_tool = "brave_search" if "brave_search" in tools else ""
        search_args = {"query": "Ray Serve HTTP proxy", "num_results": 3}
        search_dump, search_text = await mcp_call_tool(mcp_url, search_tool, search_args, TIMEOUT)

        search_ok = False
        search_not_configured = False
        try:
            parsed = json.loads(search_text) if isinstance(search_text, str) else None
            search_ok = isinstance(parsed, list) and len(parsed) > 0
        except Exception:
            # Check if search is simply not configured (not an error)
            search_not_configured = "not configured" in (search_text or "").lower()

        # 2) Web fetch tool call (use a stable, robots-friendly URL)
        fetch_tool = "fetch_url" if "fetch_url" in tools else ""
        fetch_args = {
            "url": "https://example.com",
            "max_length": 2000,
            "start_index": 0,
            "raw": False,
            "ignore_robots_txt": False,
        }
        fetch_dump, fetch_text = await mcp_call_tool(mcp_url, fetch_tool, fetch_args, TIMEOUT)
        fetch_ok = isinstance(fetch_text, str) and ("Contents of https://example.com" in fetch_text)

        duration_ms = (time.perf_counter() - start) * 1000

        # Pass if fetch works; search is optional (requires BRAVE_API_KEY)
        is_valid = fetch_ok
        search_status = "ok" if search_ok else ("not configured" if search_not_configured else "fail")
        detail = f"fetch={fetch_tool}:{'ok' if fetch_ok else 'fail'} search={search_tool}:{search_status}"

        print_io(
            url=mcp_url,
            request={"search": {"tool": search_tool, "arguments": search_args}, "fetch": {"tool": fetch_tool, "arguments": fetch_args}},
            response={"search": search_dump, "fetch": fetch_dump},
        )
        return print_result(is_valid, "MCP web-search tool calls", detail, duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, "MCP web-search tool calls", f"{type(e).__name__}: {e}", duration_ms)


async def test_weather_mcp() -> bool:
    """Test MCP weather server - list tools and execute a real tool call."""
    print("\n  Test: Weather MCP")
    
    mcp_url = make_url(BASE_URL, "/mcp-weather/mcp")
    start = time.perf_counter()
    
    try:
        tools = await mcp_list_tools(mcp_url, TIMEOUT)

        # Prefer forecast (lat/lon) as a deterministic "real" tool call.
        tool_to_call = "get_forecast" if "get_forecast" in tools else (tools[0] if tools else "")
        # NYC coordinates (stable and commonly supported by weather tools)
        arguments = {"latitude": 40.7128, "longitude": -74.0060}

        result_dump, extracted_text = await mcp_call_tool(mcp_url, tool_to_call, arguments, TIMEOUT)
        duration_ms = (time.perf_counter() - start) * 1000

        is_valid = bool(extracted_text) or bool(result_dump)
        detail = f"Called {tool_to_call} (args={list(arguments.keys())})"

        print_io(url=mcp_url, request={"tool": tool_to_call, "arguments": arguments}, response=result_dump)
        return print_result(is_valid, "MCP weather tool call", detail, duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, "MCP weather tool call", f"{type(e).__name__}: {e}", duration_ms)


async def run_all() -> list[bool]:
    """Run all MCP tests."""
    print("\n" + "=" * 60)
    print("MCP SERVER TESTS")
    print("=" * 60)
    
    results = []
    results.append(await test_web_search_mcp())
    results.append(await test_weather_mcp())
    
    return results


def _print_summary(results: list[bool]) -> int:
    passed = sum(1 for r in results if r)
    failed = sum(1 for r in results if not r)
    total = len(results)
    print("\n" + "=" * 60)
    print("MCP TEST SUMMARY")
    print("=" * 60)
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Total:  {total}")
    return 1 if failed else 0


async def _run_as_script() -> int:
    results = await run_all()
    return _print_summary(results)


if __name__ == "__main__":
    asyncio.run(_run_as_script())
