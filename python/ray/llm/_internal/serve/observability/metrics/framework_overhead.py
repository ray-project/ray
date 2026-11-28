"""Framework-overhead metrics for Ray Serve LLM.

Splits replica-boundary streaming latency into framework and engine segments
so operators can answer "is Ray Serve slow or is the engine slow" from metrics
alone (ray-project/ray#63585):

- ``rayllm_ttft_ms`` / ``rayllm_inter_token_latency_ms``: caller-observed
  TTFT / TPOT at the ``LLMServer`` boundary. These deliberately exclude
  router/proxy queueing.
- ``rayllm_framework_ingress_overhead_ms``: request receipt -> first pull on
  the engine stream (request-id injection, LoRA resolution, scheduling).
- ``rayllm_framework_first_token_egress_overhead_ms`` /
  ``rayllm_framework_inter_token_egress_overhead_ms``: engine-observation ->
  caller-emission delay per token emission, including the deliberate output
  batching interval (``stream_batching_interval_ms`` /
  ``RAYLLM_MODEL_RESPONSE_BATCH_TIMEOUT_MS``).

Conservation: ``rayllm_ttft_ms`` = ingress + engine-segment + first-token
egress, where the engine segment includes Ray's engine adapter layer (it is
"everything below the framework boundary", not a pure engine measurement).

Only streaming requests on the default OpenAI ingress path
(``LLMServer._run_request``) are instrumented, so all five histograms share
the same sample population and the conservation equation holds per
population. ``RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING`` mode serves the
engine's native ASGI app from the replica and has no Ray-side yield point, so
these metrics are not emitted there.

Engine adapters pass OpenAI-compatible SSE strings through from the engine.
Two sentinel shapes are handled specially: the terminal ``data: [DONE]``
chunk is delivered to the caller but never counted as a token emission, and a
first chunk carrying an SSE error payload is treated like an
``ErrorResponse`` (ingress overhead only).
"""

import json
import time
from collections import deque
from typing import Any, AsyncGenerator, Deque, Optional, Protocol

from ray.llm._internal.serve.core.configs.openai_api_models import ErrorResponse
from ray.llm._internal.serve.observability.metrics.utils import (
    SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
)
from ray.util import metrics

_SSE_DATA_PREFIX = "data: "
_SSE_DONE_SENTINEL_PREFIX = "data: [DONE]"


def _is_done_sentinel(chunk: Any) -> bool:
    """Whether the chunk is the terminal ``data: [DONE]`` SSE sentinel."""
    return isinstance(chunk, str) and chunk.startswith(_SSE_DONE_SENTINEL_PREFIX)


def _is_error_chunk(chunk: Any) -> bool:
    """Whether the chunk carries an error instead of a token.

    Errors surface either as ``ErrorResponse`` objects (Ray engine adapters)
    or as SSE strings with an ``error`` JSON payload (engines whose streaming
    protocol serializes mid-stream errors, e.g. vLLM). In an SSE chunk the
    payload is parsed so that a quoted ``error`` string *value* (generated
    content, a tool name, a finish reason) is not mistaken for the top-level
    error key. Runs once per stream, on the first chunk only.
    """
    if isinstance(chunk, ErrorResponse):
        return True
    if not isinstance(chunk, str) or not chunk.startswith(_SSE_DATA_PREFIX):
        return False
    payload = chunk[len(_SSE_DATA_PREFIX) :].strip()
    if not payload.startswith("{"):
        return False
    try:
        return "error" in json.loads(payload)
    except ValueError:
        # Not parseable JSON: conservative — treat it as a token.
        return False


class FrameworkOverheadRecorder(Protocol):
    """Sink for framework-overhead observations (milliseconds)."""

    def observe_ttft_ms(self, value: float) -> None:
        ...

    def observe_inter_token_latency_ms(self, value: float) -> None:
        ...

    def observe_framework_ingress_overhead_ms(self, value: float) -> None:
        ...

    def observe_framework_first_token_egress_overhead_ms(self, value: float) -> None:
        ...

    def observe_framework_inter_token_egress_overhead_ms(self, value: float) -> None:
        ...


class RayFrameworkOverheadRecorder:
    """Prometheus-backed recorder for framework-overhead observations."""

    def __init__(self, model_id: str):
        tag_keys = ("model_id",)
        self._tags = {"model_id": model_id}
        self._ttft = metrics.Histogram(
            "rayllm_ttft_ms",
            description=(
                "Time from LLMServer receiving a streaming request to the "
                "first token chunk yielded to its caller (replica-boundary "
                "TTFT; excludes router/proxy queueing)."
            ),
            boundaries=SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
            tag_keys=tag_keys,
        )
        self._inter_token = metrics.Histogram(
            "rayllm_inter_token_latency_ms",
            description=(
                "Caller-observed latency between consecutive token emissions "
                "at the LLMServer boundary (replica-boundary TPOT)."
            ),
            boundaries=SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
            tag_keys=tag_keys,
        )
        self._ingress_overhead = metrics.Histogram(
            "rayllm_framework_ingress_overhead_ms",
            description=(
                "Framework overhead from request receipt to the first pull on "
                "the engine stream (request-id injection, LoRA resolution, "
                "scheduling)."
            ),
            boundaries=SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
            tag_keys=tag_keys,
        )
        self._first_token_egress_overhead = metrics.Histogram(
            "rayllm_framework_first_token_egress_overhead_ms",
            description=(
                "Framework overhead between the engine yielding the first "
                "token chunk and that chunk reaching the caller (includes "
                "output batching delay)."
            ),
            boundaries=SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
            tag_keys=tag_keys,
        )
        self._inter_token_egress_overhead = metrics.Histogram(
            "rayllm_framework_inter_token_egress_overhead_ms",
            description=(
                "Framework overhead per token emission after the first "
                "(engine-observation to caller-emission; includes output "
                "batching delay)."
            ),
            boundaries=SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
            tag_keys=tag_keys,
        )

    def observe_ttft_ms(self, value: float) -> None:
        self._ttft.observe(value, tags=self._tags)

    def observe_inter_token_latency_ms(self, value: float) -> None:
        self._inter_token.observe(value, tags=self._tags)

    def observe_framework_ingress_overhead_ms(self, value: float) -> None:
        self._ingress_overhead.observe(value, tags=self._tags)

    def observe_framework_first_token_egress_overhead_ms(self, value: float) -> None:
        self._first_token_egress_overhead.observe(value, tags=self._tags)

    def observe_framework_inter_token_egress_overhead_ms(self, value: float) -> None:
        self._inter_token_egress_overhead.observe(value, tags=self._tags)


class StreamingFrameworkOverheadTracker:
    """Per-request tracker that instruments the two stream boundaries.

    ``instrument_engine_stream`` wraps the raw engine stream (below output
    batching); ``instrument_serve_stream`` wraps the final stream returned to
    the caller (above output batching). Streams whose first item is an
    ``ErrorResponse`` or an SSE error payload only record ingress overhead —
    an error is not a first token. A mid-stream error terminates the stream
    and is counted as one final emission in the inter-token metrics. The
    terminal ``data: [DONE]`` SSE sentinel is delivered to the caller but
    never counted as a token emission."""

    def __init__(
        self,
        recorder: FrameworkOverheadRecorder,
        t_receipt: Optional[float] = None,
    ):
        self._recorder = recorder
        self._t_receipt = t_receipt if t_receipt is not None else time.perf_counter()
        self._t_first_pull: Optional[float] = None
        self._t_engine_first_chunk: Optional[float] = None
        self._engine_chunk_times: Deque[float] = deque()
        self._is_error_stream = False
        self._serve_first_emitted = False
        self._t_serve_prev_emission: Optional[float] = None

    def instrument_engine_stream(
        self, stream: AsyncGenerator[Any, None]
    ) -> AsyncGenerator[Any, None]:
        """Wrap the raw engine stream, recording ingress and per-chunk times."""

        async def _gen() -> AsyncGenerator[Any, None]:
            # The generator body first runs on the first pull, so this records
            # receipt -> first-pull overhead lazily, before iterating.
            self._t_first_pull = time.perf_counter()
            self._recorder.observe_framework_ingress_overhead_ms(
                (self._t_first_pull - self._t_receipt) * 1000
            )
            try:
                async for item in stream:
                    if _is_done_sentinel(item):
                        # Terminal sentinel: delivered to the caller, but not
                        # a token and not timed.
                        yield item
                        continue
                    now = time.perf_counter()
                    self._engine_chunk_times.append(now)
                    if self._t_engine_first_chunk is None:
                        if _is_error_chunk(item):
                            self._is_error_stream = True
                        else:
                            self._t_engine_first_chunk = now
                    yield item
            finally:
                # Forward closure so caller-side cancellation reaches the
                # engine stream deterministically instead of relying on
                # async-generator GC finalization.
                await stream.aclose()

        return _gen()

    def instrument_serve_stream(
        self, stream: AsyncGenerator[Any, None]
    ) -> AsyncGenerator[Any, None]:
        """Wrap the caller-facing stream, recording TTFT/TPOT and egress overhead."""

        async def _gen() -> AsyncGenerator[Any, None]:
            try:
                async for emission in stream:
                    now = time.perf_counter()
                    # Chunks that are the terminal ``data: [DONE]`` sentinel
                    # are not tokens: exclude them from the emission count and
                    # skip metrics for pure-sentinel emissions.
                    if isinstance(emission, list):
                        num_chunks = sum(
                            1 for chunk in emission if not _is_done_sentinel(chunk)
                        )
                    elif _is_done_sentinel(emission):
                        num_chunks = 0
                    else:
                        num_chunks = 1
                    if (
                        num_chunks
                        and not self._is_error_stream
                        and self._t_engine_first_chunk is not None
                    ):
                        # Pop the engine-side timestamps of the chunks that make
                        # up this emission (a batch when batching is enabled).
                        t_engine_first_of_emission: Optional[float] = None
                        for _ in range(min(num_chunks, len(self._engine_chunk_times))):
                            chunk_time = self._engine_chunk_times.popleft()
                            if t_engine_first_of_emission is None:
                                t_engine_first_of_emission = chunk_time
                        if not self._serve_first_emitted:
                            self._serve_first_emitted = True
                            self._recorder.observe_ttft_ms(
                                (now - self._t_receipt) * 1000
                            )
                            self._recorder.observe_framework_first_token_egress_overhead_ms(
                                (now - self._t_engine_first_chunk) * 1000
                            )
                        else:
                            self._recorder.observe_inter_token_latency_ms(
                                (now - self._t_serve_prev_emission) * 1000
                            )
                            if t_engine_first_of_emission is not None:
                                self._recorder.observe_framework_inter_token_egress_overhead_ms(
                                    (now - t_engine_first_of_emission) * 1000
                                )
                        self._t_serve_prev_emission = now
                    yield emission
            finally:
                # Forward closure so caller-side cancellation reaches the
                # batcher/engine chain deterministically instead of relying on
                # async-generator GC finalization.
                await stream.aclose()

        return _gen()
