"""Tests for benchmark smoke mode."""

import types

import aiohttp
import pytest

from ray.llm._internal.serve.benchmark.multiturn_bench import (
    TurnResult,
    run_smoke,
    send_chat_completion,
)


@pytest.mark.asyncio
async def test_send_chat_completion(mock_server: str) -> None:
    """send_chat_completion should parse SSE stream and return correct TurnResult."""
    messages = [{"role": "user", "content": "hello"}]
    async with aiohttp.ClientSession() as session:
        result = await send_chat_completion(
            session=session,
            base_url=mock_server,
            model="test-model",
            messages=messages,
            first_chunk_threshold=2,
        )

    assert isinstance(result, TurnResult)
    assert result.generated_text == "this is a test"
    assert result.input_tokens == 10
    assert result.output_tokens == 4
    assert result.ttft_ms > 0
    assert result.latency_ms > 0
    assert result.fc_ms > 0
    assert isinstance(result.tpot_ms, float)


def test_run_smoke(mock_server: str) -> None:
    """run_smoke should return 0 and print JSON metrics."""
    args = types.SimpleNamespace(
        base_url=mock_server,
        model="test-model",
        first_chunk_threshold=2,
    )
    exit_code = run_smoke(args)
    assert exit_code == 0


def test_run_smoke_connection_error(free_port: int) -> None:
    """run_smoke should return 1 when the server is unreachable."""
    args = types.SimpleNamespace(
        base_url=f"http://127.0.0.1:{free_port}",
        model="test-model",
        first_chunk_threshold=16,
    )
    exit_code = run_smoke(args)
    assert exit_code == 1
