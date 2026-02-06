"""
Tests for A2A (Agent-to-Agent) protocol endpoints.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx

# Allow running this file directly via: `python tests/test_a2a.py`
# (ensures repo root is on sys.path so `tests.*` imports work)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.helpers import a2a_execute, create_http_client, make_url, print_io, print_result

# Base URL for the Ray Serve HTTP proxy
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")

# Timeout settings (seconds)
TIMEOUT = float(os.getenv("TEST_TIMEOUT_SECONDS", "60"))


async def _test_a2a_health(client: httpx.AsyncClient, base_url: str, name: str) -> bool:
    """Test A2A agent health endpoint."""
    url = make_url(base_url, "/health")
    start = time.perf_counter()
    
    try:
        response = await client.get(url, timeout=httpx.Timeout(TIMEOUT))
        response.raise_for_status()
        data = response.json()
        
        duration_ms = (time.perf_counter() - start) * 1000
        is_ready = isinstance(data, dict) and data.get("ready") is True
        
        print_io(url=url, response=data)
        return print_result(is_ready, f"{name} /health", "ready=true" if is_ready else "ready=false", duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, f"{name} /health", f"{type(e).__name__}: {e}", duration_ms)


async def _test_a2a_agent_card(client: httpx.AsyncClient, base_url: str, name: str) -> bool:
    """Test A2A agent discovery endpoint."""
    url = make_url(base_url, "/.well-known/agent-card.json")
    start = time.perf_counter()
    
    try:
        response = await client.get(url, timeout=httpx.Timeout(TIMEOUT))
        response.raise_for_status()
        data = response.json()
        
        duration_ms = (time.perf_counter() - start) * 1000
        is_valid = isinstance(data, dict) and bool(data.get("name"))
        agent_name = data.get("name", "N/A") if isinstance(data, dict) else "N/A"
        
        print_io(url=url, response=data)
        return print_result(is_valid, f"{name} agent-card", f"Agent: {agent_name}", duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, f"{name} agent-card", f"{type(e).__name__}: {e}", duration_ms)


async def _test_a2a_execute(
    client: httpx.AsyncClient, 
    base_url: str, 
    name: str, 
    prompt: str,
    timeout_multiplier: float = 2.0,
) -> bool:
    """Test A2A agent execute endpoint."""
    start = time.perf_counter()
    
    try:
        request, data = await a2a_execute(client, base_url, prompt, TIMEOUT * timeout_multiplier)
        duration_ms = (time.perf_counter() - start) * 1000
        
        kind = data.get("kind")
        if kind == "task":
            status = (data.get("status") or {}).get("state")
            is_valid = status == "completed"
            detail = f"task.state={status}" if is_valid else json.dumps(data)[:150]
        elif kind == "message":
            # Minimal sanity check: must contain at least one text part.
            parts = data.get("parts") if isinstance(data, dict) else None
            is_valid = isinstance(parts, list) and len(parts) > 0
            detail = "message" if is_valid else json.dumps(data)[:150]
        else:
            is_valid = False
            detail = json.dumps(data)[:150]
        
        print_io(url=make_url(base_url, "/v1/message:send"), request=request, response=data)
        return print_result(is_valid, f"{name} /v1/message:send", detail, duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, f"{name} /v1/message:send", f"{type(e).__name__}: {e}", duration_ms)


async def test_weather_agent(client: httpx.AsyncClient) -> list[bool]:
    """Test A2A Weather Agent."""
    print("\n  --- A2A Weather Agent ---")
    
    base_url = make_url(BASE_URL, "/a2a-weather")
    results = []
    
    results.append(await _test_a2a_health(client, base_url, "weather"))
    results.append(await _test_a2a_agent_card(client, base_url, "weather"))
    results.append(await _test_a2a_execute(
        client, base_url, "weather",
        "What's the weather like in Palo Alto today?",
        timeout_multiplier=2.0,
    ))
    
    return results


async def test_research_agent(client: httpx.AsyncClient) -> list[bool]:
    """Test A2A Research Agent."""
    print("\n  --- A2A Research Agent ---")
    
    base_url = make_url(BASE_URL, "/a2a-research")
    results = []
    
    results.append(await _test_a2a_health(client, base_url, "research"))
    results.append(await _test_a2a_agent_card(client, base_url, "research"))
    results.append(await _test_a2a_execute(
        client, base_url, "research",
        "What is Anyscale Jobs? Keep it concise.",
        timeout_multiplier=3.0,
    ))
    
    return results


async def test_travel_agent(client: httpx.AsyncClient) -> list[bool]:
    """Test A2A Travel Agent."""
    print("\n  --- A2A Travel Agent ---")
    
    base_url = make_url(BASE_URL, "/a2a-travel")
    results = []
    
    results.append(await _test_a2a_health(client, base_url, "travel"))
    results.append(await _test_a2a_agent_card(client, base_url, "travel"))
    results.append(await _test_a2a_execute(
        client, base_url, "travel",
        (
            "Create a 2-day Seattle itinerary + packing list for 2 adults in the next week. "
            "Provide: timed itinerary, backup rainy plan, 3 restaurant picks/day, and a packing list tailored to June."
        ),
        timeout_multiplier=6.0,  # Travel agent calls downstream A2A agents
    ))
    
    return results


async def run_all(client: httpx.AsyncClient) -> list[bool]:
    """Run all A2A tests."""
    print("\n" + "=" * 60)
    print("A2A AGENT TESTS")
    print("=" * 60)
    
    results = []
    results.extend(await test_weather_agent(client))
    results.extend(await test_research_agent(client))
    results.extend(await test_travel_agent(client))
    
    return results


def _print_summary(results: list[bool]) -> int:
    passed = sum(1 for r in results if r)
    failed = sum(1 for r in results if not r)
    total = len(results)
    print("\n" + "=" * 60)
    print("A2A TEST SUMMARY")
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
