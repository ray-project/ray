import json
import time
from types import SimpleNamespace

import pytest

from ray.llm._internal.serve.engines.vllm.scheduler_trace import (
    _ASYNC_SCHEDULER_PATH,
    _SYNC_SCHEDULER_PATH,
    SCHEDULER_TRACE_DIR_ENV,
    SCHEDULER_TRACE_FILE_PREFIX_ENV,
    SchedulerTraceMixin,
    _JsonlTraceWriter,
    configure_scheduler_trace,
)


def _read_events(trace_dir):
    events = []
    for path in sorted(trace_dir.glob("*.jsonl")):
        events.extend(json.loads(line) for line in path.read_text().splitlines())
    return events


@pytest.mark.parametrize(
    ("async_scheduling", "expected_cls"),
    [(False, _SYNC_SCHEDULER_PATH), (True, _ASYNC_SCHEDULER_PATH)],
)
def test_configure_scheduler_trace(
    monkeypatch, tmp_path, async_scheduling, expected_cls
):
    monkeypatch.setenv(SCHEDULER_TRACE_DIR_ENV, str(tmp_path))
    scheduler_config = SimpleNamespace(
        async_scheduling=async_scheduling,
        scheduler_cls=None,
    )
    vllm_config = SimpleNamespace(scheduler_config=scheduler_config)

    assert configure_scheduler_trace(vllm_config)
    assert scheduler_config.scheduler_cls == expected_cls


def test_configure_scheduler_trace_rejects_another_custom_scheduler(
    monkeypatch, tmp_path
):
    monkeypatch.setenv(SCHEDULER_TRACE_DIR_ENV, str(tmp_path))
    vllm_config = SimpleNamespace(
        scheduler_config=SimpleNamespace(
            async_scheduling=False,
            scheduler_cls="custom.Scheduler",
        )
    )

    with pytest.raises(ValueError, match="custom vLLM scheduler_cls"):
        configure_scheduler_trace(vllm_config)


def test_jsonl_trace_writer_rotates(tmp_path):
    writer = _JsonlTraceWriter(
        str(tmp_path),
        file_prefix="trace",
        max_queue_size=10,
        max_file_bytes=80,
        flush_interval_s=0.01,
    )
    writer.emit({"event": "one", "value": "x" * 40})
    writer.emit({"event": "two", "value": "y" * 40})
    writer.close()

    assert [event["event"] for event in _read_events(tmp_path)] == ["one", "two"]
    assert len(list(tmp_path.glob("*.jsonl"))) == 2


class _FakeScheduler:
    def __init__(self):
        self.scheduler_config = SimpleNamespace(
            async_scheduling=False,
            max_num_batched_tokens=16,
            max_num_seqs=4,
        )
        self.max_num_scheduled_tokens = 16
        self.max_num_running_reqs = 4
        self.current_step = 0
        self.requests = {}
        self._waiting = 0

    def add_request(self, request):
        self.requests[request.request_id] = request
        self._waiting += 1

    def get_request_counts(self):
        return (len(self.requests) - self._waiting, self._waiting)

    def get_kv_cache_usage(self):
        return 0.5

    def schedule(self):
        self.current_step += 1
        request = self.requests["request-1"]
        scheduled_tokens = 8
        output = SimpleNamespace(
            scheduled_new_reqs=[
                SimpleNamespace(
                    req_id=request.request_id,
                    num_computed_tokens=request.num_computed_tokens,
                )
            ],
            scheduled_cached_reqs=SimpleNamespace(
                req_ids=[],
                num_computed_tokens=[],
                resumed_req_ids=set(),
                is_context_phase=lambda _: False,
            ),
            num_scheduled_tokens={request.request_id: scheduled_tokens},
            total_num_scheduled_tokens=scheduled_tokens,
            preempted_req_ids=set(),
        )
        request.num_computed_tokens += scheduled_tokens
        request.num_in_flight_tokens += scheduled_tokens
        self._waiting = 0
        return output

    def update_from_output(self, scheduler_output, model_runner_output):
        request = self.requests["request-1"]
        request.num_in_flight_tokens -= scheduler_output.total_num_scheduled_tokens
        return {
            0: SimpleNamespace(
                outputs=[
                    SimpleNamespace(
                        request_id=request.request_id,
                        new_token_ids=[1],
                        finish_reason="stop",
                    )
                ]
            )
        }

    def finish_requests(self, request_ids, finished_status):
        return []

    def shutdown(self):
        pass


class _FakeTracingScheduler(SchedulerTraceMixin, _FakeScheduler):
    pass


def test_scheduler_trace_records_request_batch_and_first_token(monkeypatch, tmp_path):
    monkeypatch.setenv(SCHEDULER_TRACE_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(SCHEDULER_TRACE_FILE_PREFIX_ENV, "engine")
    scheduler = _FakeTracingScheduler()
    request = SimpleNamespace(
        request_id="request-1",
        trace_headers={"x-request-id": "client-1"},
        arrival_time=time.time(),
        num_prompt_tokens=13,
        max_tokens=5,
        priority=0,
        num_computed_tokens=4,
        num_in_flight_tokens=0,
        num_preemptions=0,
        prefill_stats=SimpleNamespace(num_cached_tokens=2),
    )

    scheduler.add_request(request)
    scheduler_output = scheduler.schedule()
    scheduler.update_from_output(scheduler_output, SimpleNamespace())
    scheduler.shutdown()

    events = _read_events(tmp_path)
    event_by_name = {event["event"]: event for event in events}
    assert event_by_name["request_queued"]["external_request_id"] == "client-1"

    batch = event_by_name["batch_scheduled"]
    assert batch["prefill_requests"] == 1
    assert batch["prefill_tokens"] == 8
    assert batch["decode_requests"] == 0
    assert batch["requests"] == [
        {
            "request_id": "request-1",
            "external_request_id": "client-1",
            "phase": "prefill",
            "prompt_tokens": 13,
            "cached_prefix_tokens": 2,
            "computed_before": 4,
            "completed_or_cached_before": 4,
            "locally_computed_before": 2,
            "in_flight_before": 0,
            "scheduled_tokens": 8,
            "remaining_prefill_after": 1,
            "context_tokens_after": 12,
            "num_preemptions": 0,
        }
    ]

    assert event_by_name["request_first_token"]["batches_waited"] == 0
    assert event_by_name["request_admitted"]["cached_prefix_tokens"] == 2
    assert event_by_name["batch_completed"]["first_token_request_ids"] == ["request-1"]
    assert event_by_name["request_finished"]["external_request_id"] == "client-1"
