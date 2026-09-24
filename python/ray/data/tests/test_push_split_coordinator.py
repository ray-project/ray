"""Tests for the push-based streaming_split coordinator."""

import threading
import time
from typing import Any

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.iterator.push_based_split_iterator import (
    PushSplitReceiverMixin,
)
from ray.data._internal.iterator.push_split_coordinator import (
    PushSplitCoordinator,
    _create_split_dataset,
    _SplitFlow,
)

# ---------------------------------------------------------------------------
# _SplitFlow unit tests (no Ray involved).
# ---------------------------------------------------------------------------


def test_flow_window_math():
    flow = _SplitFlow()
    # No window declared yet: nothing may be sent.
    assert flow.rows_to_send() == 0

    flow.report(target_rows=100, consumed_rows=0, consumed_bytes=0)
    assert flow.rows_to_send() == 100

    flow.record_push(num_rows=60, size_bytes=600)
    assert flow.rows_to_send() == 40

    # Whole blocks overshoot the window; sending resumes once consumed.
    flow.record_push(num_rows=60, size_bytes=600)
    assert flow.rows_to_send() == -20
    flow.report(target_rows=100, consumed_rows=60, consumed_bytes=600)
    assert flow.rows_to_send() == 40
    assert flow.in_flight_bytes() == 600


def test_flow_finish_clears_contribution():
    flow = _SplitFlow()
    flow.report(target_rows=100, consumed_rows=0, consumed_bytes=0)
    flow.record_push(num_rows=50, size_bytes=500)

    flow.finish()
    assert flow.in_flight_bytes() == 0
    assert flow.rows_to_send() == 0


def test_flow_wait_for_room_wakes_on_report():
    flow = _SplitFlow()
    assert not flow.wait_for_room(timeout=0.01)

    def report_later():
        time.sleep(0.2)
        flow.report(target_rows=10, consumed_rows=0, consumed_bytes=0)

    thread = threading.Thread(target=report_later)
    thread.start()
    start = time.monotonic()
    assert flow.wait_for_room(timeout=30)
    # Woken by the report, not by the timeout.
    assert time.monotonic() - start < 10
    thread.join()


# ---------------------------------------------------------------------------
# Coordinator actor tests.
# ---------------------------------------------------------------------------


@ray.remote(num_cpus=0)
class _Receiver(PushSplitReceiverMixin):
    # No iterator creates receive state for these keys, so any deliveries
    # are dropped.
    pass


def _get(ref: Any) -> Any:
    # Actor method results are untyped; keep type checkers from assuming the
    # list overload of ray.get.
    return ray.get(ref)


def _make_coordinator(num_rows: int = 100, n: int = 2):
    split_dataset = _create_split_dataset(ray.data.range(num_rows), n, equal=True)
    # pyrefly: ignore[missing-attribute]  # @ray.remote hides ActorClass.options
    coordinator = PushSplitCoordinator.options(max_concurrency=n + 2).remote(
        split_dataset, n
    )
    ray.get(
        [
            # pyrefly: ignore[missing-attribute]  # @ray.remote hides .remote
            coordinator.register.remote(i, _Receiver.remote(), key=f"test:{i}")
            for i in range(n)
        ]
    )
    return coordinator


def test_barrier_starts_one_epoch_for_all_splits(ray_start_regular_shared):
    coordinator = _make_coordinator()

    assert ray.get([coordinator.start_epoch.remote(i) for i in range(2)]) == [0, 0]
    assert ray.get([coordinator.start_epoch.remote(i) for i in range(2)]) == [1, 1]


def test_barrier_waits_for_every_split(ray_start_regular_shared):
    coordinator = _make_coordinator()

    first = coordinator.start_epoch.remote(0)
    ready, _ = ray.wait([first], timeout=1)
    assert not ready, "epoch started before all splits arrived"

    second = coordinator.start_epoch.remote(1)
    assert ray.get([first, second]) == [0, 0]


@pytest.mark.parametrize("split_idx", [-1, 2])
def test_start_epoch_rejects_out_of_range_split(ray_start_regular_shared, split_idx):
    coordinator = _make_coordinator()
    with pytest.raises(ValueError, match="split_idx must be between"):
        _get(coordinator.start_epoch.remote(split_idx))


def test_barrier_releases_when_teardown_fails(ray_start_regular_shared):
    coordinator = _make_coordinator()
    assert ray.get([coordinator.start_epoch.remote(i) for i in range(2)]) == [0, 0]

    def _failing_teardown(self):
        raise RuntimeError("teardown failed")

    # Make the previous epoch's teardown fail for the next barrier.
    ray.get(
        coordinator.__ray_call__.remote(
            lambda self: setattr(
                self, "_teardown_epoch", _failing_teardown.__get__(self)
            )
        )
    )
    # Every split still gets the next epoch instead of hanging.
    refs = [coordinator.start_epoch.remote(i) for i in range(2)]
    assert ray.get(refs, timeout=60) == [1, 1]


def test_register_rejects_out_of_range_split(ray_start_regular_shared):
    coordinator = _make_coordinator()
    receiver = _Receiver.remote()  # pyrefly: ignore[missing-attribute]
    with pytest.raises(ValueError, match="split_idx must be between"):
        _get(coordinator.register.remote(2, receiver, key="test:2"))


def test_request_rows_only_updates_current_epoch(ray_start_regular_shared):
    coordinator = _make_coordinator()
    ray.get([coordinator.start_epoch.remote(i) for i in range(2)])

    _get(coordinator.request_rows.remote(0, 0, 400, 50, 500))
    # Stale (and future) epochs are ignored.
    _get(coordinator.request_rows.remote(1, -1, 999, 999, 999))
    _get(coordinator.request_rows.remote(1, 5, 999, 999, 999))

    state = _get(coordinator.debug_state.remote())
    assert state["target_rows"] == {0: 400, 1: 0}
    assert state["rows_consumed"] == {0: 50, 1: 0}
    assert state["bytes_consumed"] == {0: 500, 1: 0}

    # A new epoch starts with fresh flow state.
    ray.get([coordinator.start_epoch.remote(i) for i in range(2)])
    state = _get(coordinator.debug_state.remote())
    assert state["target_rows"] == {0: 0, 1: 0}
    assert state["rows_consumed"] == {0: 0, 1: 0}


def test_executor_shuts_down_after_all_splits_finish(ray_start_regular_shared):
    coordinator = _make_coordinator()
    ray.get([coordinator.start_epoch.remote(i) for i in range(2)])

    _get(coordinator.notify_split_finished.remote(0, 0))
    # A stale-epoch notification doesn't count.
    _get(coordinator.notify_split_finished.remote(-1, 1))
    assert not _get(coordinator._is_executor_shutdown.remote())

    _get(coordinator.notify_split_finished.remote(0, 1))
    wait_for_condition(lambda: _get(coordinator._is_executor_shutdown.remote()))


def test_dataset_metadata(ray_start_regular_shared):
    coordinator = _make_coordinator()

    assert _get(coordinator.get_dataset_schema.remote()).names == ["id"]
    assert _get(coordinator.get_dataset_tag.remote(1))["split_index"] == "1"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
