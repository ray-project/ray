"""Single-turn execution primitive for the benchmark.

This module provides the pure core of turn execution: send an HTTP request,
build a TurnMetric, and inject the response. It has NO side effects — callers
are responsible for inflight tracking, metric recording, and queue management.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

from ray.llm._internal.serve.benchmark.http_client import send_chat_completion
from ray.llm._internal.serve.benchmark.models import TurnMetric, TurnResult
from ray.llm._internal.serve.benchmark.text_gen import Conversation


@dataclass
class TurnOutcome:
    """Result of executing a single benchmark turn."""

    metric: TurnMetric
    result: TurnResult


async def execute_single_turn(
    http_session: aiohttp.ClientSession,
    conv: Conversation,
    turn_idx: int,
    base_url: str,
    model: str,
    max_tokens: int,
    bench_start_ns: int,
    first_chunk_threshold: int = 16,
    api_key: Optional[str] = None,
) -> TurnOutcome:
    """Execute a single benchmark turn: HTTP call, build metric, inject response.

    This is the pure core shared by all three benchmark engines (concurrency,
    rate-based, interactive). The caller handles inflight tracking, warmup
    filtering, measurement windows, and queue re-enqueue.
    """
    messages = conv.get_turn_messages(turn_idx)
    req_start_ns = time.perf_counter_ns()

    result = await send_chat_completion(
        session=http_session,
        base_url=base_url,
        model=model,
        messages=messages,
        session_id=conv.session_id,
        max_tokens=max_tokens,
        first_chunk_threshold=first_chunk_threshold,
        api_key=api_key,
    )

    metric = TurnMetric(
        session_id=conv.session_id,
        turn=turn_idx,
        ttft_ms=result.ttft_ms,
        fc_ms=result.fc_ms,
        itl_ms=result.itl_ms,
        latency_ms=result.latency_ms,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        start_time_ms=(req_start_ns - bench_start_ns) / 1e6,
        itl_ms_list=result.itl_ms_list,
    )

    conv.inject_assistant_response(turn_idx, result.generated_text)

    return TurnOutcome(metric=metric, result=result)
