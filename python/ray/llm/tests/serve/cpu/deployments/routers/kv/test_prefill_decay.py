"""Deterministic tests of the router's estimated remaining-prefill metric."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ray.llm._internal.serve.routing_policies.kv_aware import kv_token_tracker
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    REQUEST_TRACKING_TTL_S,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    KVTokenTracker,
)
from ray.serve.experimental.llm_token_autoscaling_policy import (
    LLMTokenAutoscalingPolicy,
)

PREFILL = "outstanding_effective_prefill_tokens"


class _LocalTracker(KVTokenTracker):
    def _create_selection_service(self):
        self._svc = SimpleNamespace(
            prefill_complete=AsyncMock(),
            free_reservation=AsyncMock(),
            create_reservation=AsyncMock(),
            delete_worker=AsyncMock(),
        )

    def _start_replica_tracking(self):
        pass


@pytest.fixture
def clock(monkeypatch):
    clock = SimpleNamespace(now=1000.0)
    # Replace this module's clock without changing asyncio's own monotonic clock.
    monkeypatch.setattr(
        kv_token_tracker, "time", SimpleNamespace(monotonic=lambda: clock.now)
    )
    return clock


@pytest.fixture
def make_tracker():
    trackers = []

    def make(rate=10000):
        tracker = _LocalTracker(prefill_decay_tokens_per_second=rate)
        tracker._register_block_size(16, "test-worker")
        trackers.append(tracker)
        return tracker

    yield make
    for tracker in trackers:
        if tracker._prefill_decay_timer is not None:
            tracker._prefill_decay_timer.cancel()
        for task in tracker._pending_tasks:
            task.cancel()


def add(tracker, request_id, worker_id=1, tokens=100000):
    tracker._track_request_state(request_id, worker_id, tokens, None, tokens)


def prefill(tracker):
    return tracker._get_autoscaling_snapshot()[PREFILL]


@pytest.mark.parametrize("rate", [-1, float("inf"), float("nan"), True, None, "15000"])
def test_invalid_rate(rate):
    with pytest.raises(ValueError, match="prefill_decay_tokens_per_second"):
        _LocalTracker(prefill_decay_tokens_per_second=rate)


def test_disabled_by_default(clock, make_tracker):
    tracker = make_tracker(rate=0)
    add(tracker, "a")
    clock.now += 100
    assert prefill(tracker) == 100000
    assert tracker._prefill_decay is None
    assert tracker._prefill_decay_timer is None


def test_oldest_first_shares_capacity(clock, make_tracker):
    tracker = make_tracker()
    add(tracker, "a")
    clock.now += 1
    add(tracker, "b")
    clock.now += 4
    assert prefill(tracker) == 150000
    assert tracker._prefill_decay._pending[1] == {"a": 50000, "b": 100000}
    clock.now += 7
    assert prefill(tracker) == 80000
    assert tracker._prefill_decay._pending[1] == {"b": 80000}
    # Neither lifecycle state nor the original reservation amount is decayed.
    assert not tracker._requests["a"].prefill_completed
    assert tracker._requests["a"].effective_prefill_tokens == 100000
    assert tracker._outstanding_effective_prefill_tokens == 200000
    tracker._svc.prefill_complete.assert_not_called()


def test_each_worker_has_independent_capacity(clock, make_tracker):
    tracker = make_tracker()
    add(tracker, "a", worker_id=1)
    add(tracker, "b", worker_id=2)
    clock.now += 5
    assert prefill(tracker) == 100000


def test_idle_capacity_is_not_banked_and_cached_requests_do_not_block(
    clock, make_tracker
):
    tracker = make_tracker()
    add(tracker, "a", tokens=10000)
    clock.now += 10
    assert prefill(tracker) == 0
    assert tracker._prefill_decay._last_updated == {}
    clock.now += 10
    add(tracker, "cached", tokens=0)
    add(tracker, "b")
    assert prefill(tracker) == 100000
    clock.now += 3
    assert prefill(tracker) == 70000


@pytest.mark.asyncio
async def test_first_token_and_abort_clear_only_residual(clock, make_tracker):
    tracker = make_tracker()
    add(tracker, "a")
    add(tracker, "b")
    clock.now += 3
    await tracker.on_prefill_complete("a")
    assert prefill(tracker) == 100000
    assert tracker._active_decode_requests == 1
    tracker._svc.prefill_complete.assert_awaited_once_with("a")
    clock.now += 2
    await tracker.on_prefill_complete("a")
    assert prefill(tracker) == 80000
    await tracker.on_request_completed("b")
    await tracker.on_request_completed("b")
    assert prefill(tracker) == 0
    assert tracker._outstanding_effective_prefill_tokens == 0
    await tracker.on_request_completed("a")
    assert tracker._active_decode_requests == 0
    assert tracker._prefill_decay._pending == {}


@pytest.mark.asyncio
async def test_younger_completion_does_not_consume_extra_capacity(clock, make_tracker):
    tracker = make_tracker()
    add(tracker, "a")
    add(tracker, "b")
    clock.now += 2
    await tracker.on_prefill_complete("b")
    assert prefill(tracker) == 80000
    clock.now += 2
    assert prefill(tracker) == 60000


@pytest.mark.asyncio
async def test_completion_after_estimated_zero_is_idempotent(clock, make_tracker):
    tracker = make_tracker(rate=15000)
    add(tracker, "a", tokens=10000)
    clock.now += 1
    assert prefill(tracker) == 0
    await tracker.on_prefill_complete("a")
    await tracker.on_prefill_complete("a")
    await tracker.on_request_completed("a")
    await tracker.on_request_completed("a")
    assert tracker._get_autoscaling_snapshot() == {
        PREFILL: 0,
        "active_decode_requests": 0,
    }


def test_request_replacement_removes_old_contribution(clock, make_tracker):
    tracker = make_tracker()
    add(tracker, "a", worker_id=1)
    clock.now += 2
    add(tracker, "a", worker_id=2)
    assert prefill(tracker) == 100000
    assert 1 not in tracker._prefill_decay._pending
    clock.now += 1
    assert prefill(tracker) == 90000


@pytest.mark.asyncio
async def test_worker_removal_and_ttl_cleanup(clock, make_tracker):
    tracker = make_tracker()
    add(tracker, "a", worker_id=1)
    add(tracker, "b", worker_id=2)
    clock.now += 2
    tracker.remove_worker(1)
    await asyncio.gather(*list(tracker._pending_tasks))
    assert prefill(tracker) == 80000
    assert 1 not in tracker._prefill_decay._pending
    tracker._requests["b"].created_at = clock.now - REQUEST_TRACKING_TTL_S - 1
    assert (await tracker.get_autoscaling_snapshot())[PREFILL] == 0
    tracker._svc.free_reservation.assert_awaited_once_with("b")
    assert tracker._requests == {}
    assert tracker._prefill_decay._pending == {}
    assert tracker._prefill_decay._last_updated == {}


@pytest.mark.asyncio
async def test_timer_publishes_decay_without_lifecycle_events(clock, make_tracker):
    tracker = make_tracker(rate=15000)
    published = []
    tracker.set_autoscaling_metrics_callback(published.append)
    add(tracker, "a")
    tracker._publish_autoscaling_snapshot()
    timer = tracker._prefill_decay_timer
    assert timer is not None
    tracker._publish_autoscaling_snapshot()
    assert tracker._prefill_decay_timer is timer
    for second in range(1, 8):
        clock.now += 1
        # Invoke the scheduled callback deterministically, without real sleeps.
        tracker._prefill_decay_timer.cancel()
        tracker._publish_prefill_decay_snapshot()
        assert published[-1][PREFILL] == max(0, 100000 - 15000 * second)
    assert tracker._prefill_decay_timer is None
    assert "a" in tracker._requests
    assert tracker._active_decode_requests == 0


@pytest.mark.asyncio
async def test_lifecycle_batch_publishes_each_change_and_cancels_timer(
    clock, make_tracker
):
    tracker = make_tracker()
    published = []
    tracker.set_autoscaling_metrics_callback(published.append)
    add(tracker, "a")
    tracker._publish_autoscaling_snapshot()
    timer = tracker._prefill_decay_timer
    before = len(published)
    clock.now += 1
    await tracker.on_lifecycle_events(
        [
            ("on_prefill_complete", ("a",)),
            ("on_request_completed", ("a",)),
        ]
    )
    assert len(published) == before + 2
    assert published[-1] == {PREFILL: 0, "active_decode_requests": 0}
    assert timer.cancelled()
    assert tracker._prefill_decay_timer is None


@pytest.mark.asyncio
async def test_policy_averages_decaying_snapshots_across_peer_routers(
    clock, make_tracker
):
    tracker = make_tracker(rate=15000)
    add(tracker, "a")
    samples = []
    tracker.set_autoscaling_metrics_callback(
        lambda snapshot: samples.append(
            SimpleNamespace(timestamp=clock.now, value=snapshot[PREFILL])
        )
    )
    tracker._publish_autoscaling_snapshot()
    for _ in range(4):
        clock.now += 1
        tracker._prefill_decay_timer.cancel()
        tracker._publish_prefill_decay_snapshot()
    policy = LLMTokenAutoscalingPolicy(
        target_prefill_tokens_per_replica=20000, upscale_lookback_s=4
    )
    context = SimpleNamespace(
        raw_router_metrics={PREFILL: {"router-a": samples, "router-b": samples}},
        current_time=clock.now,
        config=SimpleNamespace(look_back_period_s=60),
        target_num_replicas=1,
        total_queued_requests=0,
    )
    required, state = policy(context)
    # The time-weighted mean is (100K + 85K + 70K + 55K) / 4 = 77.5K.
    assert required == 4
    assert state["required_replicas"] == 4


@pytest.mark.asyncio
async def test_peer_reservations_decay_and_late_reservation_does_not_resurrect(
    clock, make_tracker
):
    routing, peer = make_tracker(rate=15000), make_tracker(rate=15000)
    add(routing, "a")
    booking = {
        "selection_id": "a",
        "worker_id": 1,
        "isl_tokens": 100000,
        "effective_prefill_tokens": 100000,
        "expected_output_tokens": None,
    }
    await peer._apply_reservation(booking)
    clock.now += 2
    assert prefill(routing) == prefill(peer) == 70000
    for tracker in (routing, peer):
        await tracker.on_request_completed("a")
    await peer._apply_reservation(booking)
    assert prefill(routing) == prefill(peer) == 0
    assert peer._prefill_decay._pending == {}
