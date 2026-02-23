"""
Run all tests for the multi-agent stack.

Usage:
    python -m tests.run_all
    
    # Or directly:
    python tests/run_all.py

Environment variables:
    BASE_URL           - Base URL for Ray Serve (default: http://127.0.0.1:8000)
    LLM_MODEL_ID       - Model ID for LLM tests (default: Qwen/Qwen3-4B-Instruct-2507-FP8)
    TEST_TIMEOUT_SECONDS - Base timeout in seconds (default: 60)
    TEST_SHOW_IO       - Show request/response details (default: 1)
    TEST_MAX_IO_CHARS  - Max chars to print (default: 2000)
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import httpx

# Allow running this file directly via: `python tests/run_all.py`
# (ensures repo root is on sys.path so `tests.*` imports work)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests import test_a2a, test_agents_sse, test_llm, test_mcp
from tests.helpers import create_http_client

# Base URL for the Ray Serve HTTP proxy
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")

# Whether to print request/response details
SHOW_IO = os.getenv("TEST_SHOW_IO", "1").strip().lower() in {"1", "true", "yes"}

# Max characters to print for IO
MAX_IO_CHARS = int(os.getenv("TEST_MAX_IO_CHARS", "2000"))


def print_banner():
    """Print test run banner."""
    print()
    print("=" * 60)
    print("MULTI-AGENT STACK SMOKE TEST")
    print("=" * 60)
    print(f"  Base URL:  {BASE_URL}")
    print(f"  Show IO:   {SHOW_IO} (max_chars={MAX_IO_CHARS})")
    print()


def print_summary(results: list[bool], total_time: float):
    """Print test summary."""
    passed = sum(1 for r in results if r)
    failed = sum(1 for r in results if not r)
    total = len(results)
    
    print()
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print()
    
    if failed > 0:
        print(f"  FAILED")
        print()
        print(f"  Passed: {passed}")
        print(f"  Failed: {failed}")
        print(f"  Total:  {total}")
        print(f"  Time:   {total_time:.2f}s")
        print()
        return 1
    else:
        print(f"  ALL TESTS PASSED")
        print()
        print(f"  Passed: {passed}")
        print(f"  Total:  {total}")
        print(f"  Time:   {total_time:.2f}s")
        print()
        return 0


async def run_tests() -> int:
    """Run all tests and return exit code."""
    all_results: list[bool] = []
    start_time = time.perf_counter()
    
    print_banner()
    
    async with create_http_client() as client:
        # LLM tests
        all_results.extend(await test_llm.run_all(client))
        
        # MCP tests
        all_results.extend(await test_mcp.run_all())
        
        # SSE agent tests
        all_results.extend(await test_agents_sse.run_all(client))
        
        # A2A agent tests
        all_results.extend(await test_a2a.run_all(client))
    
    total_time = time.perf_counter() - start_time
    return print_summary(all_results, total_time)


def main() -> int:
    """Main entry point."""
    try:
        return asyncio.run(run_tests())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
