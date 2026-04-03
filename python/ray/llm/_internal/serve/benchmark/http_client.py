"""HTTP client for OpenAI-compatible chat completion endpoints."""

from __future__ import annotations

import json
import time
from typing import Optional

import aiohttp

from ray.llm._internal.serve.benchmark.models import TurnResult


async def send_chat_completion(
    session: aiohttp.ClientSession,
    base_url: str,
    model: str,
    messages: list[dict[str, str]],
    session_id: str = "",
    max_tokens: int = 256,
    first_chunk_threshold: int = 16,
    timeout_sec: int = 300,
    api_key: Optional[str] = None,
) -> TurnResult:
    """Send a streaming chat completion request and collect metrics."""
    url = f"{base_url}/v1/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
        "temperature": 0.0,
    }

    headers: dict[str, str] = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if session_id:
        headers["X-Session-Id"] = session_id

    timeout = aiohttp.ClientTimeout(total=timeout_sec)

    start_ns = time.perf_counter_ns()
    ttft_ns: Optional[int] = None
    fc_ns: Optional[int] = None
    content_chunk_count = 0
    chunk_times: list[int] = []
    generated_text = ""
    input_tokens = 0
    output_tokens = 0
    prev_ts = start_ns

    async with session.post(
        url, json=payload, headers=headers, timeout=timeout
    ) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {body[:500]}")

        async for raw_line in resp.content:
            line = raw_line.strip()
            if not line:
                continue
            text = line.decode("utf-8", errors="replace")
            if not text.startswith("data: "):
                continue
            data_str = text[6:]
            if data_str == "[DONE]":
                continue

            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            usage = data.get("usage")
            if usage:
                input_tokens = usage.get("prompt_tokens", input_tokens)
                output_tokens = usage.get("completion_tokens", output_tokens)

            choices = data.get("choices", [])
            if not choices:
                continue

            delta = choices[0].get("delta", {})
            content = delta.get("content") or delta.get("reasoning")
            if content:
                now_ns = time.perf_counter_ns()
                content_chunk_count += 1
                if ttft_ns is None:
                    ttft_ns = now_ns - start_ns
                else:
                    chunk_times.append(now_ns - prev_ts)
                if fc_ns is None and content_chunk_count >= first_chunk_threshold:
                    fc_ns = now_ns - start_ns
                prev_ts = now_ns
                generated_text += content

    end_ns = time.perf_counter_ns()
    latency_ns = end_ns - start_ns

    if ttft_ns is None:
        ttft_ns = latency_ns
    if fc_ns is None:
        fc_ns = latency_ns

    itl_ms_list = [t / 1e6 for t in chunk_times]
    itl_ms = sum(itl_ms_list) / len(itl_ms_list) if itl_ms_list else 0.0

    return TurnResult(
        ttft_ms=ttft_ns / 1e6,
        fc_ms=fc_ns / 1e6,
        itl_ms=itl_ms,
        latency_ms=latency_ns / 1e6,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        generated_text=generated_text,
        itl_ms_list=itl_ms_list,
    )
