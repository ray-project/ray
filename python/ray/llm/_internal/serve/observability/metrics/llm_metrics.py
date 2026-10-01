"""Metrics shared by LLM orchestration and HTTP streaming."""

import base64
import json
import logging
import queue
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict

import ray
from ray import serve
from ray.util import metrics

# Match vLLM's TTFT, ITL and request-duration buckets, in seconds.
TTFT_BUCKETS = [
    0.001,
    0.005,
    0.01,
    0.02,
    0.04,
    0.06,
    0.08,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    20.0,
    40.0,
    80.0,
    160.0,
    640.0,
    2560.0,
]
ITL_BUCKETS = [
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.15,
    0.2,
    0.3,
    0.4,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    20.0,
    40.0,
    80.0,
]
REQUEST_DURATION_BUCKETS = [
    0.3,
    0.5,
    0.8,
    1.0,
    1.5,
    2.0,
    2.5,
    5.0,
    10.0,
    15.0,
    20.0,
    30.0,
    40.0,
    50.0,
    60.0,
    120.0,
    240.0,
    480.0,
    960.0,
    1920.0,
    7680.0,
]

LLM_METRIC_TAG_KEYS = ("model_name", "ReplicaId", "engine_worker_id")
LLM_METRICS_HEADER = b"x-ray-llm-metric-tags"
LLM_METRICS_MARKER = b"ray_llm_stream|"
_MAX_ACTIVE_METRIC_STREAMS = 4096
_METRIC_STREAM_IDLE_TIMEOUT_S = 300
_MAX_PENDING_METRIC_CHUNKS = 256
_MAX_METRIC_CHUNK_BYTES = 256 * 1024
logger = logging.getLogger(__name__)


def get_llm_metric_tags(model_name: str) -> Dict[str, str]:
    """Match vLLM's identity on the native engine application's replica."""
    context = serve.context._get_internal_replica_context()
    return {
        "model_name": model_name,
        "ReplicaId": context.replica_id.unique_id if context else "",
        "engine_worker_id": (
            ray.get_runtime_context().get_worker_id() if ray.is_initialized() else ""
        ),
    }


class LLMMetrics:
    """Collector-owned instruments with a common, bounded set of engine labels.

    Add orchestration counters/histograms here using LLM_METRIC_TAG_KEYS so
    future metrics (for example, reused tokenization) share the same identity.
    Request IDs belong in logs/traces, rather than Prometheus labels.
    """

    def __init__(self):
        self.time_to_first_token = metrics.Histogram(
            "serve_llm_time_to_first_token_seconds",
            description="Time from HAProxy request entry to the first output token at HAProxy.",
            boundaries=TTFT_BUCKETS,
            tag_keys=LLM_METRIC_TAG_KEYS,
        )
        self.inter_token_latency = metrics.Histogram(
            "serve_llm_inter_token_latency_seconds",
            description="Time between output token chunks at HAProxy per choice.",
            boundaries=ITL_BUCKETS,
            tag_keys=LLM_METRIC_TAG_KEYS,
        )
        self.router_overhead = metrics.Histogram(
            "serve_llm_router_overhead_seconds",
            description="HAProxy round trip to the ingress router for a replica decision.",
            boundaries=TTFT_BUCKETS,
            tag_keys=LLM_METRIC_TAG_KEYS,
        )
        self.request_duration = metrics.Histogram(
            "serve_llm_request_duration_seconds",
            description="Time from HAProxy request entry to response completion at HAProxy.",
            boundaries=REQUEST_DURATION_BUCKETS,
            tag_keys=LLM_METRIC_TAG_KEYS,
        )
        self.collection_errors = metrics.Counter(
            "serve_llm_stream_metrics_collection_errors",
            description="Metric collection failures due to overload, missing data or invalid streams.",
        )


@dataclass
class _PendingStream:
    tracker: "_StreamMetrics"
    expected: tuple = (0, 0)
    touched_at: float = 0.0


class HAProxyLLMStreamMetrics:
    """Decode HAProxy observations and emit metrics on one background thread.

    HAProxy supplies both timestamps from its monotonic clock. Queue entries,
    active streams and incomplete SSE buffers are bounded. Sequence gaps stop
    collection for the affected stream instead of inventing delivery intervals.
    """

    def __init__(self):
        self.metrics = LLMMetrics()
        self.queue = queue.Queue(maxsize=_MAX_PENDING_METRIC_CHUNKS)
        self.streams = OrderedDict()
        self._closed = threading.Event()
        self._drop_lock = threading.Lock()
        self._dropped_packets = 0
        self._thread = threading.Thread(
            target=self._run, name="llm-stream-metrics", daemon=True
        )
        self._thread.start()

    def submit(self, data: bytes):
        if self._closed.is_set():
            return
        try:
            self.queue.put_nowait(data)
        except queue.Full:
            # A gap in the stream sequence will invalidate its subsequent data.
            with self._drop_lock:
                self._dropped_packets += 1

    def _disable(self, pending):
        if not pending.tracker.disabled:
            pending.tracker.disabled = True
            pending.tracker.buffer = b""
            self.metrics.collection_errors.inc()

    def _expire(self):
        cutoff = time.monotonic() - _METRIC_STREAM_IDLE_TIMEOUT_S
        while self.streams:
            _, pending = next(iter(self.streams.items()))
            if pending.touched_at > cutoff:
                break
            self.streams.popitem(last=False)
            self._disable(pending)

    def _record(self, data: bytes):
        if not data.startswith(LLM_METRICS_MARKER):
            raise ValueError("Invalid HAProxy LLM metric message")
        message = data[len(LLM_METRICS_MARKER) :].rstrip(b"\n")
        if message.startswith(b"drop|"):
            self.metrics.collection_errors.inc(int(message.split(b"|")[1]))
            return
        # ID | frame | part | parts | start_us | now_us | tags64 | size | hex
        # Completion packets have size=-1 and carry router_us instead of hex.
        fields = message.split(b"|", 8)
        stream_id, frame, part, parts, started, now, tags64, size, encoded = fields
        frame, part, parts, size = map(int, (frame, part, parts, size))
        if frame < 0 or part < 0 or parts < 1 or part >= parts or size < -1:
            raise ValueError("Invalid HAProxy stream sequence")
        pending = self.streams.get(stream_id)
        if pending is None:
            tags = json.loads(base64.b64decode(tags64, validate=True))
            if set(tags) != set(LLM_METRIC_TAG_KEYS) or not all(
                isinstance(value, str) for value in tags.values()
            ):
                raise ValueError("Invalid engine metric identity")
            if size == -1:
                # Empty streams or lost payloads still have valid request-level
                # timing: completion does not depend on decoding every token.
                self._record_request_metrics(tags, started, now, encoded)
                return
            pending = _PendingStream(
                _StreamMetrics(self.metrics, tags, started_at=int(started) / 1_000_000)
            )
            if len(self.streams) >= _MAX_ACTIVE_METRIC_STREAMS:
                _, evicted = self.streams.popitem(last=False)
                self._disable(evicted)
            self.streams[stream_id] = pending
        pending.touched_at = time.monotonic()
        self.streams.move_to_end(stream_id)
        if pending.expected != (frame, part):
            self._disable(pending)
        if size == -1:
            del self.streams[stream_id]
            self._record_request_metrics(pending.tracker.tags, started, now, encoded)
            return
        pending.expected = (frame + 1, 0) if part == parts - 1 else (frame, part + 1)
        if pending.tracker.disabled:
            return
        try:
            body = bytes.fromhex(encoded.decode("ascii"))
            if len(body) != size or size > _MAX_METRIC_CHUNK_BYTES:
                raise ValueError("Truncated/oversized HAProxy stream fragment")
            pending.tracker.observe(body, int(now) / 1_000_000)
        except Exception:
            self._disable(pending)
            logger.debug("Failed to record LLM stream metrics", exc_info=True)

    def _record_request_metrics(self, tags, started, completed, router_us):
        duration_us = int(completed) - int(started)
        router_us = int(router_us)
        if duration_us < 0 or router_us < -1 or router_us > duration_us:
            raise ValueError("Invalid HAProxy request timing")
        self.metrics.request_duration.observe(duration_us / 1_000_000, tags=tags)
        if router_us >= 0:
            self.metrics.router_overhead.observe(router_us / 1_000_000, tags=tags)

    def _run(self):
        while True:
            try:
                data = self.queue.get(timeout=0.1)
            except queue.Empty:
                data = None
            try:
                if data is not None:
                    self._record(data)
                with self._drop_lock:
                    dropped, self._dropped_packets = self._dropped_packets, 0
                if dropped:
                    self.metrics.collection_errors.inc(dropped)
                self._expire()
            except Exception:
                logger.debug("Invalid LLM stream metrics datagram", exc_info=True)
            finally:
                if data is not None:
                    self.queue.task_done()
            if data is None and self._closed.is_set():
                self.streams.clear()
                return

    def close(self):
        """Drain on proxy shutdown, never on request completion."""
        self._closed.set()
        self._thread.join()


class _StreamMetrics:
    """Request-local SSE decoding and per-choice timing."""

    def __init__(self, metrics: LLMMetrics, tags: Dict[str, str], *, started_at: float):
        self.metrics = metrics
        self.tags = tags
        self.started_at = started_at
        self.last_token_at: Dict[int, float] = {}
        self.buffer = b""
        self.disabled = False

    def observe(self, body: bytes, now: float) -> None:
        self.buffer = (self.buffer + body).replace(b"\r\n", b"\n")
        events = self.buffer.split(b"\n\n")
        self.buffer = events.pop()
        if len(self.buffer) > _MAX_METRIC_CHUNK_BYTES:
            raise ValueError("SSE metric fragment exceeds buffer limit")
        for event in events:
            data = b"\n".join(
                line[5:].lstrip()
                for line in event.splitlines()
                if line.startswith(b"data:")
            )
            if not data or data == b"[DONE]":
                continue
            try:
                payload = json.loads(data)
            except (ValueError, UnicodeDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            choices = payload.get("choices")
            if not isinstance(choices, list):
                continue
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                delta = choice.get("delta") or {}
                if not isinstance(delta, dict):
                    continue
                # Role-only, usage, terminal and error chunks are not tokens.
                if not (
                    choice.get("text")
                    or choice.get("token_ids")
                    or delta.get("content")
                    or delta.get("reasoning_content")
                    or delta.get("reasoning")
                    or delta.get("tool_calls")
                    or delta.get("function_call")
                ):
                    continue
                index = choice.get("index", 0)
                previous = self.last_token_at.get(index)
                if previous is None:
                    self.metrics.time_to_first_token.observe(
                        now - self.started_at, tags=self.tags
                    )
                else:
                    self.metrics.inter_token_latency.observe(
                        now - previous, tags=self.tags
                    )
                self.last_token_at[index] = now


class LLMStreamMetricsMetadataMiddleware:
    """Identify native LLM streams for HAProxy's response observer.

    No response body interception, timing, decoding or metric emission happens
    here. HAProxy consumes and removes this internal response header.
    """

    def __init__(self, app, *, model_name: str):
        self.app = app
        self._header = base64.b64encode(
            json.dumps(get_llm_metric_tags(model_name)).encode()
        )

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] not in (
            "/v1/chat/completions",
            "/v1/completions",
        ):
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = dict(message.get("headers", []))
                if message["status"] == 200 and headers.get(
                    b"content-type", b""
                ).startswith(b"text/event-stream"):
                    message = dict(message)
                    message["headers"] = list(message.get("headers", [])) + [
                        (LLM_METRICS_HEADER, self._header)
                    ]
            await send(message)

        await self.app(scope, receive, send_wrapper)
