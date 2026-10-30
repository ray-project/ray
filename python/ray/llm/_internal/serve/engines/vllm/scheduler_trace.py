"""Opt-in request and batch tracing for the vLLM V1 scheduler.

Set ``RAY_SERVE_VLLM_SCHEDULER_TRACE_DIR`` in the LLM deployment's runtime
environment to enable tracing. The optional ``*_FILE_PREFIX``,
``*_MAX_QUEUE_SIZE``, ``*_MAX_FILE_BYTES``, and ``*_FLUSH_INTERVAL_S``
environment variables tune output naming and buffering. The scheduler hot path
only performs a nonblocking queue write; JSON encoding and file I/O run on a
background thread.
"""

import json
import os
import queue
import re
import socket
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Tuple

from vllm.v1.core.sched.async_scheduler import AsyncScheduler
from vllm.v1.core.sched.scheduler import Scheduler

from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

SCHEDULER_TRACE_DIR_ENV = "RAY_SERVE_VLLM_SCHEDULER_TRACE_DIR"
SCHEDULER_TRACE_FILE_PREFIX_ENV = "RAY_SERVE_VLLM_SCHEDULER_TRACE_FILE_PREFIX"
SCHEDULER_TRACE_MAX_QUEUE_SIZE_ENV = "RAY_SERVE_VLLM_SCHEDULER_TRACE_MAX_QUEUE_SIZE"
SCHEDULER_TRACE_MAX_FILE_BYTES_ENV = "RAY_SERVE_VLLM_SCHEDULER_TRACE_MAX_FILE_BYTES"
SCHEDULER_TRACE_FLUSH_INTERVAL_S_ENV = "RAY_SERVE_VLLM_SCHEDULER_TRACE_FLUSH_INTERVAL_S"

_DEFAULT_MAX_QUEUE_SIZE = 10_000
_DEFAULT_MAX_FILE_BYTES = 256 * 1024 * 1024
_DEFAULT_FLUSH_INTERVAL_S = 1.0
_TRACE_SCHEMA_VERSION = 1
_STOP = object()

_SYNC_SCHEDULER_PATH = (
    "ray.llm._internal.serve.engines.vllm.scheduler_trace.TracingScheduler"
)
_ASYNC_SCHEDULER_PATH = (
    "ray.llm._internal.serve.engines.vllm.scheduler_trace.TracingAsyncScheduler"
)


def scheduler_trace_enabled() -> bool:
    return bool(os.environ.get(SCHEDULER_TRACE_DIR_ENV, "").strip())


def configure_scheduler_trace(vllm_config: Any) -> bool:
    """Install the tracing scheduler selected for the configured execution mode."""
    if not scheduler_trace_enabled():
        return False

    scheduler_config = vllm_config.scheduler_config
    current_cls = scheduler_config.scheduler_cls
    tracing_classes = {
        _SYNC_SCHEDULER_PATH,
        _ASYNC_SCHEDULER_PATH,
        TracingScheduler,
        TracingAsyncScheduler,
    }
    if current_cls is not None and current_cls not in tracing_classes:
        raise ValueError(
            f"{SCHEDULER_TRACE_DIR_ENV} cannot be combined with a custom "
            f"vLLM scheduler_cls ({current_cls!r})."
        )

    scheduler_config.scheduler_cls = (
        _ASYNC_SCHEDULER_PATH
        if scheduler_config.async_scheduling
        else _SYNC_SCHEDULER_PATH
    )
    return True


def _positive_int_from_env(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _positive_float_from_env(name: str, default: float) -> float:
    value = float(os.environ.get(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _safe_filename_component(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-.")
    return value or "engine"


class _JsonlTraceWriter:
    """A bounded, nonblocking producer with one background JSONL writer."""

    def __init__(
        self,
        trace_dir: str,
        *,
        file_prefix: Optional[str] = None,
        max_queue_size: int = _DEFAULT_MAX_QUEUE_SIZE,
        max_file_bytes: int = _DEFAULT_MAX_FILE_BYTES,
        flush_interval_s: float = _DEFAULT_FLUSH_INTERVAL_S,
    ) -> None:
        self._trace_dir = Path(trace_dir)
        self._trace_dir.mkdir(parents=True, exist_ok=True)
        default_prefix = f"{socket.gethostname()}_pid-{os.getpid()}"
        if file_prefix:
            default_prefix = f"{file_prefix}_{default_prefix}"
        self._file_prefix = _safe_filename_component(default_prefix)
        self._max_file_bytes = max_file_bytes
        self._flush_interval_s = flush_interval_s
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=max_queue_size)
        self._dropped_events = 0
        self._drop_lock = threading.Lock()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name="vllm-scheduler-trace-writer",
            daemon=True,
        )
        self._thread.start()

    def emit(self, event: Dict[str, Any]) -> None:
        if self._closed:
            return

        with self._drop_lock:
            dropped_events = self._dropped_events
            self._dropped_events = 0

        if dropped_events:
            drop_event = {
                "schema_version": _TRACE_SCHEMA_VERSION,
                "event": "trace_events_dropped",
                "monotonic_s": time.monotonic(),
                "wall_time_s": time.time(),
                "count": dropped_events,
            }
            try:
                self._queue.put_nowait(drop_event)
            except queue.Full:
                with self._drop_lock:
                    self._dropped_events += dropped_events

        try:
            self._queue.put_nowait(event)
        except queue.Full:
            with self._drop_lock:
                self._dropped_events += 1

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self._drop_lock:
            dropped_events = self._dropped_events
            self._dropped_events = 0
        if dropped_events:
            try:
                self._queue.put(
                    {
                        "schema_version": _TRACE_SCHEMA_VERSION,
                        "event": "trace_events_dropped",
                        "monotonic_s": time.monotonic(),
                        "wall_time_s": time.time(),
                        "count": dropped_events,
                    },
                    timeout=1,
                )
            except queue.Full:
                pass
        try:
            self._queue.put(_STOP, timeout=1)
        except queue.Full:
            # The writer is still draining. Avoid blocking engine shutdown.
            return
        self._thread.join(timeout=5)

    def _path(self, index: int) -> Path:
        return self._trace_dir / f"{self._file_prefix}.{index:04d}.jsonl"

    def _run(self) -> None:
        file_index = 0
        output = self._path(file_index).open("a", encoding="utf-8")
        last_flush = time.monotonic()
        try:
            while True:
                timeout = max(
                    0.01,
                    self._flush_interval_s - (time.monotonic() - last_flush),
                )
                try:
                    event = self._queue.get(timeout=timeout)
                except queue.Empty:
                    output.flush()
                    last_flush = time.monotonic()
                    if self._closed:
                        break
                    continue

                try:
                    if event is _STOP:
                        break
                    line = json.dumps(event, separators=(",", ":"), sort_keys=True)
                    encoded_size = len(line.encode("utf-8")) + 1
                    if output.tell() > 0 and (
                        output.tell() + encoded_size > self._max_file_bytes
                    ):
                        output.flush()
                        output.close()
                        file_index += 1
                        output = self._path(file_index).open("a", encoding="utf-8")
                    output.write(line)
                    output.write("\n")
                finally:
                    self._queue.task_done()

                if time.monotonic() - last_flush >= self._flush_interval_s:
                    output.flush()
                    last_flush = time.monotonic()
        except Exception:
            logger.exception("vLLM scheduler trace writer failed")
        finally:
            output.flush()
            output.close()


class SchedulerTraceMixin:
    """Add diagnostic JSONL tracing without changing scheduler decisions."""

    scheduler_config: Any
    requests: Dict[str, Any]
    max_num_scheduled_tokens: int
    max_num_running_reqs: int

    if TYPE_CHECKING:

        def get_request_counts(self) -> Tuple[int, int]:
            ...

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        trace_dir = os.environ.get(SCHEDULER_TRACE_DIR_ENV, "").strip()
        self._trace_writer: Optional[_JsonlTraceWriter] = None
        if trace_dir:
            self._trace_writer = _JsonlTraceWriter(
                trace_dir,
                file_prefix=os.environ.get(SCHEDULER_TRACE_FILE_PREFIX_ENV),
                max_queue_size=_positive_int_from_env(
                    SCHEDULER_TRACE_MAX_QUEUE_SIZE_ENV,
                    _DEFAULT_MAX_QUEUE_SIZE,
                ),
                max_file_bytes=_positive_int_from_env(
                    SCHEDULER_TRACE_MAX_FILE_BYTES_ENV,
                    _DEFAULT_MAX_FILE_BYTES,
                ),
                flush_interval_s=_positive_float_from_env(
                    SCHEDULER_TRACE_FLUSH_INTERVAL_S_ENV,
                    _DEFAULT_FLUSH_INTERVAL_S,
                ),
            )

        self._trace_batch_index = 0
        self._trace_pending_batches: Dict[int, Dict[str, Any]] = {}
        self._trace_queued_at: Dict[str, float] = {}
        self._trace_queued_after_batch: Dict[str, int] = {}
        self._trace_external_request_ids: Dict[str, str] = {}
        self._trace_first_scheduled_at: Dict[str, float] = {}
        self._trace_first_scheduled_batch: Dict[str, int] = {}
        self._trace_first_token_seen: set[str] = set()
        self._trace_finished: set[str] = set()
        self._emit(
            "trace_started",
            scheduler_class=type(self).__name__,
            async_scheduling=bool(self.scheduler_config.async_scheduling),
            max_num_batched_tokens=self.scheduler_config.max_num_batched_tokens,
            max_num_seqs=self.scheduler_config.max_num_seqs,
        )

    def _emit(self, event_type: str, **fields: Any) -> None:
        if self._trace_writer is None:
            return
        self._trace_writer.emit(
            {
                "schema_version": _TRACE_SCHEMA_VERSION,
                "event": event_type,
                "monotonic_s": time.monotonic(),
                "wall_time_s": time.time(),
                **fields,
            }
        )

    @staticmethod
    def _external_request_id(request: Any) -> str:
        headers = getattr(request, "trace_headers", None) or {}
        if isinstance(headers, Mapping):
            for key, value in headers.items():
                if key.replace("_", "-").lower() == "x-request-id":
                    return str(value)
        # Ray canonicalizes X-Request-Id before invoking vLLM, so the engine
        # request ID is normally already the externally visible request ID.
        return str(request.request_id)

    def add_request(self, request: Any) -> None:
        existed = request.request_id in self.requests
        queued_at = time.monotonic()
        super().add_request(request)  # type: ignore[misc]
        if existed:
            self._emit(
                "request_stream_updated",
                request_id=request.request_id,
                external_request_id=self._external_request_id(request),
                prompt_tokens=request.num_prompt_tokens,
            )
            return

        self._trace_queued_at[request.request_id] = queued_at
        self._trace_queued_after_batch[request.request_id] = self._trace_batch_index
        external_request_id = self._external_request_id(request)
        self._trace_external_request_ids[request.request_id] = external_request_id
        running_requests, waiting_requests = self.get_request_counts()
        self._emit(
            "request_queued",
            request_id=request.request_id,
            external_request_id=external_request_id,
            arrival_time_s=request.arrival_time,
            prompt_tokens=request.num_prompt_tokens,
            max_tokens=request.max_tokens,
            priority=request.priority,
            queued_after_batch=self._trace_batch_index,
            running_requests=running_requests,
            waiting_requests=waiting_requests,
        )

    def _request_entries(self, scheduler_output: Any) -> list[Dict[str, Any]]:
        new_req_ids = {req.req_id for req in scheduler_output.scheduled_new_reqs}
        computed_before = {
            req.req_id: req.num_computed_tokens
            for req in scheduler_output.scheduled_new_reqs
        }
        cached_reqs = scheduler_output.scheduled_cached_reqs
        computed_before.update(
            dict(zip(cached_reqs.req_ids, cached_reqs.num_computed_tokens))
        )

        entries = []
        for (
            request_id,
            scheduled_tokens,
        ) in scheduler_output.num_scheduled_tokens.items():
            request = self.requests.get(request_id)
            if request is None:
                continue
            is_prefill = request_id in new_req_ids or cached_reqs.is_context_phase(
                request_id
            )
            before = computed_before.get(
                request_id,
                max(0, request.num_computed_tokens - scheduled_tokens),
            )
            in_flight_after = request.num_in_flight_tokens
            in_flight_before = max(0, in_flight_after - scheduled_tokens)
            completed_or_cached_before = max(0, before - in_flight_before)
            prompt_tokens = request.num_prompt_tokens
            cached_tokens = 0
            prefill_stats = getattr(request, "prefill_stats", None)
            if prefill_stats is not None:
                cached_tokens = prefill_stats.num_cached_tokens

            entry = {
                "request_id": request_id,
                "external_request_id": self._external_request_id(request),
                "phase": "prefill" if is_prefill else "decode",
                "prompt_tokens": prompt_tokens,
                "cached_prefix_tokens": cached_tokens,
                "computed_before": before,
                "completed_or_cached_before": completed_or_cached_before,
                "locally_computed_before": max(
                    0, completed_or_cached_before - cached_tokens
                ),
                "in_flight_before": in_flight_before,
                "scheduled_tokens": scheduled_tokens,
                "remaining_prefill_after": max(
                    0, prompt_tokens - before - scheduled_tokens
                ),
                "context_tokens_after": before + scheduled_tokens,
                "num_preemptions": request.num_preemptions,
            }
            entries.append(entry)
        return entries

    def schedule(self, *args, **kwargs):
        waiting_before = self.get_request_counts()[1]
        running_before = self.get_request_counts()[0]
        progress_before = {
            request_id: {
                "computed_tokens": request.num_computed_tokens,
                "in_flight_tokens": request.num_in_flight_tokens,
            }
            for request_id, request in self.requests.items()
        }
        schedule_started = time.monotonic()
        scheduler_output = super().schedule(*args, **kwargs)
        schedule_finished = time.monotonic()

        if scheduler_output.total_num_scheduled_tokens <= 0:
            self._emit(
                "scheduler_idle_step",
                scheduler_step=getattr(self, "current_step", None),
                scheduler_time_ms=(schedule_finished - schedule_started) * 1000,
                running_requests=self.get_request_counts()[0],
                waiting_requests=self.get_request_counts()[1],
            )
            return scheduler_output

        self._trace_batch_index += 1
        batch_index = self._trace_batch_index
        request_entries = self._request_entries(scheduler_output)
        first_scheduled_ids = []
        for entry in request_entries:
            request_id = entry["request_id"]
            if request_id not in self._trace_first_scheduled_at:
                self._trace_first_scheduled_at[request_id] = schedule_finished
                self._trace_first_scheduled_batch[request_id] = batch_index
                first_scheduled_ids.append(request_id)

        prefill_entries = [e for e in request_entries if e["phase"] == "prefill"]
        decode_entries = [e for e in request_entries if e["phase"] == "decode"]
        running_after, waiting_after = self.get_request_counts()
        remaining_token_budget = max(
            0,
            self.max_num_scheduled_tokens - scheduler_output.total_num_scheduled_tokens,
        )
        if waiting_after <= 0:
            waiting_constraint = None
        elif remaining_token_budget == 0:
            waiting_constraint = "token_budget_exhausted"
        elif running_after >= self.max_num_running_reqs:
            waiting_constraint = "max_num_seqs_reached"
        else:
            # Exact reasons such as LoRA capacity, encoder budget, or remote KV
            # waits occur inside vLLM's scheduling scan and are not exposed by
            # SchedulerOutput. The surrounding state remains available here.
            waiting_constraint = "other_or_request_specific"
        dispatched_at = time.monotonic()
        self._trace_pending_batches[id(scheduler_output)] = {
            "batch_index": batch_index,
            "dispatched_at": dispatched_at,
            "request_ids": [entry["request_id"] for entry in request_entries],
        }
        self._emit(
            "batch_scheduled",
            batch_index=batch_index,
            scheduler_step=getattr(self, "current_step", None),
            scheduler_time_ms=(schedule_finished - schedule_started) * 1000,
            running_requests_before=running_before,
            waiting_requests_before=waiting_before,
            running_requests_after=running_after,
            waiting_requests_after=waiting_after,
            waiting_constraint=waiting_constraint,
            kv_cache_usage=self.get_kv_cache_usage(),
            max_num_scheduled_tokens=self.max_num_scheduled_tokens,
            total_scheduled_tokens=scheduler_output.total_num_scheduled_tokens,
            remaining_token_budget=remaining_token_budget,
            prefill_requests=len(prefill_entries),
            prefill_tokens=sum(e["scheduled_tokens"] for e in prefill_entries),
            decode_requests=len(decode_entries),
            decode_tokens=sum(e["scheduled_tokens"] for e in decode_entries),
            decode_context_tokens=sum(
                e["context_tokens_after"] for e in decode_entries
            ),
            first_scheduled_request_ids=first_scheduled_ids,
            preempted_request_ids=sorted(scheduler_output.preempted_req_ids or ()),
            requests=request_entries,
        )
        entries_by_id = {entry["request_id"]: entry for entry in request_entries}
        for request_id in first_scheduled_ids:
            queued_at = self._trace_queued_at.get(request_id)
            entry = entries_by_id[request_id]
            self._emit(
                "request_admitted",
                request_id=request_id,
                external_request_id=entry["external_request_id"],
                batch_index=batch_index,
                queue_wait_ms=(
                    (schedule_finished - queued_at) * 1000
                    if queued_at is not None
                    else None
                ),
                batches_waited=(
                    batch_index - self._trace_batch_index_at_queue(request_id) - 1
                ),
                prompt_tokens=entry["prompt_tokens"],
                cached_prefix_tokens=entry["cached_prefix_tokens"],
                scheduled_tokens=entry["scheduled_tokens"],
            )
        for request_id in sorted(scheduler_output.preempted_req_ids or ()):
            request = self.requests.get(request_id)
            self._emit(
                "request_preempted",
                request_id=request_id,
                external_request_id=self._trace_external_request_ids.get(
                    request_id, request_id
                ),
                batch_index=batch_index,
                computed_tokens_before=progress_before.get(request_id, {}).get(
                    "computed_tokens"
                ),
                in_flight_tokens_before=progress_before.get(request_id, {}).get(
                    "in_flight_tokens"
                ),
                num_preemptions=(request.num_preemptions if request else None),
            )
        for request_id in sorted(
            scheduler_output.scheduled_cached_reqs.resumed_req_ids
        ):
            self._emit(
                "request_resumed",
                request_id=request_id,
                external_request_id=self._trace_external_request_ids.get(
                    request_id, request_id
                ),
                batch_index=batch_index,
            )
        return scheduler_output

    @staticmethod
    def _iter_engine_outputs(outputs: Any) -> Iterable[Any]:
        for engine_outputs in outputs.values():
            yield from engine_outputs.outputs

    def update_from_output(self, scheduler_output: Any, model_runner_output: Any):
        completed_at = time.monotonic()
        outputs = super().update_from_output(  # type: ignore[misc]
            scheduler_output, model_runner_output
        )
        pending = self._trace_pending_batches.pop(id(scheduler_output), None)
        if pending is None:
            return outputs

        first_token_ids = []
        finished_ids = []
        for output in self._iter_engine_outputs(outputs):
            request_id = output.request_id
            if output.new_token_ids and request_id not in self._trace_first_token_seen:
                self._trace_first_token_seen.add(request_id)
                first_token_ids.append(request_id)
                queued_at = self._trace_queued_at.get(request_id)
                first_scheduled_at = self._trace_first_scheduled_at.get(request_id)
                self._emit(
                    "request_first_token",
                    request_id=request_id,
                    external_request_id=self._trace_external_request_ids.get(
                        request_id, request_id
                    ),
                    batch_index=pending["batch_index"],
                    engine_ttft_ms=(
                        (completed_at - queued_at) * 1000
                        if queued_at is not None
                        else None
                    ),
                    queue_wait_ms=(
                        (first_scheduled_at - queued_at) * 1000
                        if queued_at is not None and first_scheduled_at is not None
                        else None
                    ),
                    prefill_span_ms=(
                        (completed_at - first_scheduled_at) * 1000
                        if first_scheduled_at is not None
                        else None
                    ),
                    batches_waited=(
                        self._trace_first_scheduled_batch[request_id]
                        - self._trace_batch_index_at_queue(request_id)
                        - 1
                        if request_id in self._trace_first_scheduled_batch
                        else None
                    ),
                )

            if (
                output.finish_reason is not None
                and request_id not in self._trace_finished
            ):
                self._trace_finished.add(request_id)
                finished_ids.append(request_id)
                self._emit(
                    "request_finished",
                    request_id=request_id,
                    external_request_id=self._trace_external_request_ids.get(
                        request_id, request_id
                    ),
                    batch_index=pending["batch_index"],
                    finish_reason=str(output.finish_reason),
                )
                self._clear_request_trace_state(request_id)

        self._emit(
            "batch_completed",
            batch_index=pending["batch_index"],
            dispatch_to_output_ms=(completed_at - pending["dispatched_at"]) * 1000,
            request_ids=pending["request_ids"],
            first_token_request_ids=first_token_ids,
            finished_request_ids=finished_ids,
        )
        return outputs

    def _trace_batch_index_at_queue(self, request_id: str) -> int:
        # The exact queued batch index is retained separately from time so the
        # number of intervening model executions does not depend on idle steps.
        return self._trace_queued_after_batch.get(request_id, 0)

    def _clear_request_trace_state(self, request_id: str) -> None:
        self._trace_queued_at.pop(request_id, None)
        self._trace_queued_after_batch.pop(request_id, None)
        self._trace_external_request_ids.pop(request_id, None)
        self._trace_first_scheduled_at.pop(request_id, None)
        self._trace_first_scheduled_batch.pop(request_id, None)
        self._trace_first_token_seen.discard(request_id)

    def finish_requests(self, request_ids: Any, finished_status: Any):
        finished = super().finish_requests(  # type: ignore[misc]
            request_ids, finished_status
        )
        for request in finished:
            request_id = request.request_id
            if request_id in self._trace_finished:
                continue
            self._trace_finished.add(request_id)
            self._emit(
                "request_finished",
                request_id=request_id,
                external_request_id=self._trace_external_request_ids.get(
                    request_id, request_id
                ),
                finish_reason=str(finished_status),
                externally_finished=True,
            )
            self._clear_request_trace_state(request_id)
        return finished

    def shutdown(self) -> None:
        try:
            super().shutdown()  # type: ignore[misc]
        finally:
            if self._trace_writer is not None:
                self._trace_writer.close()


class TracingScheduler(SchedulerTraceMixin, Scheduler):
    pass


class TracingAsyncScheduler(SchedulerTraceMixin, AsyncScheduler):
    pass


__all__ = [
    "SCHEDULER_TRACE_DIR_ENV",
    "TracingAsyncScheduler",
    "TracingScheduler",
    "configure_scheduler_trace",
    "scheduler_trace_enabled",
]
