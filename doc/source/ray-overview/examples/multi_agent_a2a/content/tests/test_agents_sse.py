"""
Tests for SSE (Server-Sent Events) agent chat endpoints.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import httpx

# Allow running this file directly via: `python tests/test_agents_sse.py`
# (ensures repo root is on sys.path so `tests.*` imports work)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.helpers import create_http_client, make_url, print_io, print_result, read_sse_stream

# Base URL for the Ray Serve HTTP proxy
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")

# Timeout settings (seconds)
TIMEOUT = float(os.getenv("TEST_TIMEOUT_SECONDS", "60"))


async def test_weather_agent(client: httpx.AsyncClient) -> bool:
    """Test /weather-agent/chat - Weather agent SSE streaming."""
    print("\n  Test: Weather Agent SSE")
    
    url = make_url(BASE_URL, "/weather-agent/chat")
    payload = {"user_request": "What is the current weather forecast for New York City?"}
    
    start = time.perf_counter()
    
    try:
        success, detail, frames = await read_sse_stream(client, url, payload, TIMEOUT * 2)
        duration_ms = (time.perf_counter() - start) * 1000
        
        result_detail = f"Received {len(frames)} SSE frames" if success else detail[:200]
        
        print_io(url=url, request=payload, response={"frames": frames})
        return print_result(success, "/weather-agent/chat", result_detail, duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, "/weather-agent/chat", f"{type(e).__name__}: {e}", duration_ms)


async def test_research_agent(client: httpx.AsyncClient) -> bool:
    """Test /research-agent/chat - Research agent SSE streaming."""
    print("\n  Test: Research Agent SSE")
    
    url = make_url(BASE_URL, "/research-agent/chat")
    payload = {"user_request": "What are the top 3 benefits of solar energy? Keep it brief."}
    
    start = time.perf_counter()
    
    try:
        success, detail, frames = await read_sse_stream(client, url, payload, TIMEOUT * 2)
        duration_ms = (time.perf_counter() - start) * 1000
        
        result_detail = f"Received {len(frames)} SSE frames" if success else detail[:200]
        
        print_io(url=url, request=payload, response={"frames": frames})
        return print_result(success, "/research-agent/chat", result_detail, duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, "/research-agent/chat", f"{type(e).__name__}: {e}", duration_ms)


async def test_travel_agent(client: httpx.AsyncClient) -> bool:
    """Test /travel-agent/chat - Travel agent SSE streaming (calls A2A agents)."""
    print("\n  Test: Travel Agent SSE")
    
    url = make_url(BASE_URL, "/travel-agent/chat")
    payload = {
        "user_request": (
            "Create a 1-day San Francisco itinerary for 2 adults. "
            "Defaults: month = January; budget = mid-range; interests = Golden Gate, Fisherman's Wharf, food. "
            "Include weather-aware suggestions. Do not ask questions; use defaults."
        )
    }
    
    start = time.perf_counter()
    
    try:
        success, detail, frames = await read_sse_stream(client, url, payload, TIMEOUT * 2)
        duration_ms = (time.perf_counter() - start) * 1000
        
        result_detail = f"Received {len(frames)} SSE frames" if success else detail[:200]
        
        print_io(url=url, request=payload, response={"frames": frames})
        return print_result(success, "/travel-agent/chat", result_detail, duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, "/travel-agent/chat", f"{type(e).__name__}: {e}", duration_ms)


async def run_all(client: httpx.AsyncClient) -> list[bool]:
    """Run all SSE agent tests."""
    print("\n" + "=" * 60)
    print("SSE AGENT TESTS")
    print("=" * 60)
    
    results = []
    results.append(await test_weather_agent(client))
    results.append(await test_research_agent(client))
    results.append(await test_travel_agent(client))
    
    return results


def _print_summary(results: list[bool]) -> int:
    passed = sum(1 for r in results if r)
    failed = sum(1 for r in results if not r)
    total = len(results)
    print("\n" + "=" * 60)
    print("SSE TEST SUMMARY")
    print("=" * 60)
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Total:  {total}")
    return 1 if failed else 0


async def _run_as_script() -> int:
    async with create_http_client() as client:
        results = await run_all(client)
    return _print_summary(results)


if __name__ == "__main__":
    asyncio.run(_run_as_script())
