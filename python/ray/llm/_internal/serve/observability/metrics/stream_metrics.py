"""TTFT, TPOT, and generation time metrics for LLM streaming responses."""

import time
from typing import AsyncGenerator, TypeVar

from ray.llm._internal.serve.observability.metrics.utils import (
    SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
)
from ray.util import metrics

T = TypeVar("T")

_TPOT_HISTOGRAM_BUCKETS_MS = [
    1, 5, 10, 25, 50, 100, 150, 250, 500, 1000, 2500, 5000,
]

llm_ttft_ms = metrics.Histogram(
    "ray_llm_ttft_ms",
    description=(
        "Time to first token in milliseconds, "
        "measured at the LLMServer boundary."
    ),
    boundaries=SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
    tag_keys=("api_type",),
)

llm_tpot_ms = metrics.Histogram(
    "ray_llm_tpot_ms",
    description=(
        "Time per output token in milliseconds, "
        "measured at the LLMServer boundary."
    ),
    boundaries=_TPOT_HISTOGRAM_BUCKETS_MS,
    tag_keys=("api_type",),
)

llm_generation_time_ms = metrics.Histogram(
    "ray_llm_generation_time_ms",
    description=(
        "Total generation time in milliseconds from first token to last, "
        "measured at the LLMServer boundary."
    ),
    boundaries=SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
    tag_keys=("api_type",),
)


async def instrumented_stream(
    stream: AsyncGenerator[T, None],
    api_type: str,
) -> AsyncGenerator[T, None]:
    """Wraps an engine stream to record TTFT and TPOT metrics."""
    tags = {"api_type": api_type}
    start = time.perf_counter()
    first_token_time = 0.0
    is_first = True
    prev = start
    try:
        async for item in stream:
            now = time.perf_counter()
            if is_first:
                llm_ttft_ms.observe((now - start) * 1e3, tags)
                first_token_time = now
                is_first = False
            else:
                llm_tpot_ms.observe((now - prev) * 1e3, tags)
            prev = now
            yield item
    finally:
        if not is_first:
            llm_generation_time_ms.observe(
                (prev - first_token_time) * 1e3, tags
            )
