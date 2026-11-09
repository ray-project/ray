"""Unit tests for framework-overhead metrics (ray-project/ray#63585)."""

import asyncio
import sys
import time
from typing import Any, AsyncGenerator, List

import pytest

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ErrorInfo,
    ErrorResponse,
)
from ray.llm._internal.serve.observability.metrics.framework_overhead import (
    StreamingFrameworkOverheadTracker,
)
from ray.llm._internal.serve.utils.batcher import Batcher


class FakeRecorder:
    """Captures framework-overhead observations without a metrics backend."""

    def __init__(self):
        self.ttft_ms: List[float] = []
        self.inter_token_latency_ms: List[float] = []
        self.framework_ingress_overhead_ms: List[float] = []
        self.framework_first_token_egress_overhead_ms: List[float] = []
        self.framework_inter_token_egress_overhead_ms: List[float] = []

    def observe_ttft_ms(self, value: float) -> None:
        self.ttft_ms.append(value)

    def observe_inter_token_latency_ms(self, value: float) -> None:
        self.inter_token_latency_ms.append(value)

    def observe_framework_ingress_overhead_ms(self, value: float) -> None:
        self.framework_ingress_overhead_ms.append(value)

    def observe_framework_first_token_egress_overhead_ms(self, value: float) -> None:
        self.framework_first_token_egress_overhead_ms.append(value)

    def observe_framework_inter_token_egress_overhead_ms(self, value: float) -> None:
        self.framework_inter_token_egress_overhead_ms.append(value)

    def all(self) -> List[List[float]]:
        return [
            self.ttft_ms,
            self.inter_token_latency_ms,
            self.framework_ingress_overhead_ms,
            self.framework_first_token_egress_overhead_ms,
            self.framework_inter_token_egress_overhead_ms,
        ]


async def _fake_engine_stream(
    chunks: List[Any], inter_chunk_delay_s: float = 0.01, startup_delay_s: float = 0.02
) -> AsyncGenerator[Any, None]:
    await asyncio.sleep(startup_delay_s)
    for chunk in chunks:
        yield chunk
        await asyncio.sleep(inter_chunk_delay_s)


async def _consume(stream: AsyncGenerator[Any, None]) -> List[Any]:
    return [item async for item in stream]


@pytest.mark.asyncio
async def test_unbatched_streaming_happy_path():
    recorder = FakeRecorder()
    t_receipt = time.perf_counter()
    tracker = StreamingFrameworkOverheadTracker(recorder, t_receipt=t_receipt)

    stream = tracker.instrument_engine_stream(
        _fake_engine_stream(["a", "b", "c"], inter_chunk_delay_s=0.02)
    )
    stream = tracker.instrument_serve_stream(stream)
    assert await _consume(stream) == ["a", "b", "c"]

    # One observation each for receipt->first-pull, TTFT, and first-token egress.
    assert len(recorder.framework_ingress_overhead_ms) == 1
    assert len(recorder.ttft_ms) == 1
    assert len(recorder.framework_first_token_egress_overhead_ms) == 1
    # Two inter-token observations for a 3-chunk stream.
    assert len(recorder.inter_token_latency_ms) == 2
    assert len(recorder.framework_inter_token_egress_overhead_ms) == 2

    ingress = recorder.framework_ingress_overhead_ms[0]
    ttft = recorder.ttft_ms[0]
    first_egress = recorder.framework_first_token_egress_overhead_ms[0]
    # Ingress is measured before the (lazy) engine stream is first pulled, so it
    # must be small and must not include the engine's startup delay.
    assert 0 <= ingress < 20
    # TTFT covers the fake engine's 20ms startup delay.
    assert ttft >= 20
    # Conservation: ttft ~= ingress + engine-segment + first-token egress.
    assert 0 <= first_egress <= ttft
    assert ttft >= ingress + first_egress
    # Caller-observed inter-token latency tracks the engine's 20ms cadence.
    for delta in recorder.inter_token_latency_ms:
        assert delta >= 15
    # Unbatched egress overhead per emission is small (no batching interval).
    for value in recorder.framework_inter_token_egress_overhead_ms:
        assert 0 <= value < 20


@pytest.mark.asyncio
async def test_batched_streaming_surfaces_batch_interval():
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    # The Batcher wakes early once the engine stream completes, so the batch
    # interval only delays emissions while the stream outlives a batch window.
    # A 60ms chunk cadence against a 50ms window keeps the stream alive across
    # three windows, so each emission carries a real batching delay.
    engine_stream = tracker.instrument_engine_stream(
        _fake_engine_stream(
            ["a", "b", "c"], inter_chunk_delay_s=0.06, startup_delay_s=0.005
        )
    )
    batched = Batcher(engine_stream, interval_ms=50).stream()
    stream = tracker.instrument_serve_stream(batched)
    batches = await _consume(stream)

    # All three chunks are delivered; emissions are lists of chunks.
    assert [chunk for batch in batches for chunk in batch] == ["a", "b", "c"]
    assert len(recorder.ttft_ms) == 1
    # The batcher's interval delays the first emission: the first-token egress
    # overhead must reflect a substantial fraction of the 50ms interval.
    assert recorder.framework_first_token_egress_overhead_ms[0] >= 25
    # Steady-state emissions also carry the batching delay (nominally 45/35/25ms
    # for the three emissions; tolerate generous scheduling jitter).
    assert all(
        value >= 10 for value in recorder.framework_inter_token_egress_overhead_ms
    )


@pytest.mark.asyncio
async def test_error_first_chunk_records_no_token_metrics():
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    error = ErrorResponse(
        error=ErrorInfo(message="boom", type="invalid_request", code=400)
    )
    stream = tracker.instrument_engine_stream(
        _fake_engine_stream([error], inter_chunk_delay_s=0)
    )
    stream = tracker.instrument_serve_stream(stream)
    assert await _consume(stream) == [error]

    # Ingress is real framework work and is still recorded; token metrics are not.
    assert len(recorder.framework_ingress_overhead_ms) == 1
    assert recorder.ttft_ms == []
    assert recorder.inter_token_latency_ms == []
    assert recorder.framework_first_token_egress_overhead_ms == []
    assert recorder.framework_inter_token_egress_overhead_ms == []


@pytest.mark.asyncio
async def test_error_after_first_chunk_still_records_ttft():
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    error = ErrorResponse(
        error=ErrorInfo(message="mid-stream", type="internal", code=500)
    )
    stream = tracker.instrument_engine_stream(_fake_engine_stream(["a", error]))
    stream = tracker.instrument_serve_stream(stream)
    assert await _consume(stream) == ["a", error]

    # The stream already produced a real first token.
    assert len(recorder.ttft_ms) == 1
    assert len(recorder.framework_first_token_egress_overhead_ms) == 1


@pytest.mark.asyncio
async def test_done_sentinel_is_not_counted_as_token():
    """The terminal ``data: [DONE]`` chunk must pass through uncounted."""
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    chunks = ["data: a\n\n", "data: b\n\n", "data: [DONE]\n\n"]
    stream = tracker.instrument_engine_stream(
        _fake_engine_stream(chunks, inter_chunk_delay_s=0.01)
    )
    stream = tracker.instrument_serve_stream(stream)
    assert await _consume(stream) == chunks

    # TTFT on "a"; one inter-token sample for "b"; [DONE] adds nothing.
    assert len(recorder.ttft_ms) == 1
    assert len(recorder.inter_token_latency_ms) == 1
    assert len(recorder.framework_inter_token_egress_overhead_ms) == 1


@pytest.mark.asyncio
async def test_done_sentinel_mixed_into_final_batch():
    """A batch bundling tokens with [DONE] counts the tokens only."""
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    chunks = ["data: a\n\n", "data: b\n\n", "data: [DONE]\n\n"]
    engine_stream = tracker.instrument_engine_stream(
        _fake_engine_stream(chunks, inter_chunk_delay_s=0.01)
    )
    # interval_ms=None: batch everything into a single emission.
    batched = Batcher(engine_stream, interval_ms=None).stream()
    stream = tracker.instrument_serve_stream(batched)
    batches = await _consume(stream)

    assert batches == [chunks]
    # One emission, counted once as the first token (2 real chunks, sentinel
    # excluded from the engine-timestamp pop count).
    assert len(recorder.ttft_ms) == 1
    assert recorder.inter_token_latency_ms == []


@pytest.mark.asyncio
async def test_sse_error_first_chunk_records_no_token_metrics():
    """An SSE error payload as the first chunk is not a first token."""
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    error_sse = 'data: {"error": {"message": "boom", "type": "engine_dead"}}\n\n'
    chunks = [error_sse, "data: [DONE]\n\n"]
    stream = tracker.instrument_engine_stream(
        _fake_engine_stream(chunks, inter_chunk_delay_s=0)
    )
    stream = tracker.instrument_serve_stream(stream)
    assert await _consume(stream) == chunks

    assert len(recorder.framework_ingress_overhead_ms) == 1
    assert recorder.ttft_ms == []
    assert recorder.inter_token_latency_ms == []


@pytest.mark.asyncio
async def test_error_as_json_value_is_not_misclassified():
    """A quoted "error" string *value* (content, tool name, finish reason)
    must not be mistaken for the top-level error key of an error payload."""
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    first = 'data: {"choices": [{"delta": {"content": "error"}}]}\n\n'
    chunks = [first, "data: b\n\n"]
    stream = tracker.instrument_engine_stream(
        _fake_engine_stream(chunks, inter_chunk_delay_s=0.01)
    )
    stream = tracker.instrument_serve_stream(stream)
    assert await _consume(stream) == chunks

    assert len(recorder.ttft_ms) == 1
    assert len(recorder.inter_token_latency_ms) == 1


@pytest.mark.asyncio
async def test_empty_stream_records_nothing_but_ingress():
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    stream = tracker.instrument_engine_stream(_fake_engine_stream([]))
    stream = tracker.instrument_serve_stream(stream)
    assert await _consume(stream) == []

    assert len(recorder.framework_ingress_overhead_ms) == 1
    assert recorder.ttft_ms == []


@pytest.mark.asyncio
async def test_unpulled_stream_records_nothing():
    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)

    # Never consumed: the engine stream is lazy, so nothing may be recorded.
    tracker.instrument_engine_stream(_fake_engine_stream(["a"]))
    assert all(not values for values in recorder.all())


@pytest.mark.asyncio
async def test_aclose_forwards_to_engine_stream_unbatched():
    """Client disconnect must close the engine stream, not orphan it."""
    closed = False

    async def engine() -> AsyncGenerator[str, None]:
        nonlocal closed
        try:
            yield "a"
            yield "b"
        finally:
            closed = True

    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)
    stream = tracker.instrument_engine_stream(engine())
    stream = tracker.instrument_serve_stream(stream)

    assert await stream.__anext__() == "a"
    await stream.aclose()
    assert closed


@pytest.mark.asyncio
async def test_aclose_forwards_through_batcher_to_engine():
    """Closure must cross the Batcher: its finally cancels read_task, and the
    cancellation then reaches the engine stream."""
    closed = False

    async def engine() -> AsyncGenerator[str, None]:
        nonlocal closed
        try:
            yield "a"
            await asyncio.sleep(3600)
            yield "b"
        finally:
            closed = True

    recorder = FakeRecorder()
    tracker = StreamingFrameworkOverheadTracker(recorder)
    engine_stream = tracker.instrument_engine_stream(engine())
    batched = Batcher(engine_stream, interval_ms=50).stream()
    stream = tracker.instrument_serve_stream(batched)

    assert await stream.__anext__() == ["a"]
    await stream.aclose()
    # Batcher.stream's finally cancels read_task without awaiting it, so the
    # engine cleanup lands on a later loop tick.
    for _ in range(100):
        if closed:
            break
        await asyncio.sleep(0.01)
    assert closed


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
