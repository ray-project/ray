"""
Tests for the LLM service (OpenAI-compatible API).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
import asyncio

import httpx

# Allow running this file directly via: `python tests/test_llm.py`
# (ensures repo root is on sys.path so `tests.*` imports work)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.helpers import create_http_client, make_url, print_io, print_result

# Base URL for the Ray Serve HTTP proxy
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")

# Default model ID for LLM tests
MODEL_ID = os.getenv("LLM_MODEL_ID", "Qwen/Qwen3-4B-Instruct-2507-FP8").strip()

# Timeout settings (seconds)
TIMEOUT = float(os.getenv("TEST_TIMEOUT_SECONDS", "60"))


async def test_list_models(client: httpx.AsyncClient) -> bool:
    """Test GET /llm/v1/models - list available models."""
    print("\n  Test: List available models")
    
    url = make_url(BASE_URL, "/llm/v1/models")
    start = time.perf_counter()
    
    try:
        response = await client.get(url, timeout=httpx.Timeout(TIMEOUT))
        response.raise_for_status()
        data = response.json()
        
        duration_ms = (time.perf_counter() - start) * 1000
        
        is_valid = isinstance(data, dict) and isinstance(data.get("data"), list)
        model_count = len(data.get("data", [])) if is_valid else 0
        
        print_io(url=url, response=data)
        return print_result(is_valid, "/llm/v1/models", f"Found {model_count} model(s)", duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, "/llm/v1/models", f"{type(e).__name__}: {e}", duration_ms)


async def test_chat_completion(client: httpx.AsyncClient) -> bool:
    """Test POST /llm/v1/chat/completions - chat inference."""
    print("\n  Test: Chat completion")
    
    url = make_url(BASE_URL, "/llm/v1/chat/completions")
    payload = {
        "model": MODEL_ID,
        "messages": [{"role": "user", "content": "Tell me a joke about AI"}],
        "max_tokens": 1000,
        "temperature": 0.0,
    }
    
    start = time.perf_counter()
    
    try:
        timeout = httpx.Timeout(connect=10.0, read=TIMEOUT * 3, write=30.0, pool=30.0)
        response = await client.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        
        duration_ms = (time.perf_counter() - start) * 1000
        
        is_valid = (
            isinstance(data, dict) 
            and isinstance(data.get("choices"), list) 
            and len(data["choices"]) > 0
        )
        
        print_io(url=url, request=payload, response=data)
        return print_result(is_valid, "/llm/v1/chat/completions", f"Model: {MODEL_ID}", duration_ms)
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return print_result(False, "/llm/v1/chat/completions", f"{type(e).__name__}: {e}", duration_ms)


async def run_all(client: httpx.AsyncClient) -> list[bool]:
    """Run all LLM tests."""
    print("\n" + "=" * 60)
    print("LLM SERVICE TESTS")
    print("=" * 60)
    
    results = []
    results.append(await test_list_models(client))
    results.append(await test_chat_completion(client))
    
    return results


def _print_summary(results: list[bool]) -> int:
    passed = sum(1 for r in results if r)
    failed = sum(1 for r in results if not r)
    total = len(results)
    print("\n" + "=" * 60)
    print("LLM TEST SUMMARY")
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
