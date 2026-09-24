"""Tests for the push-based streaming_split (push_based_split_iterator.py)."""

import threading
import time
from typing import Optional

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.iterator.push_based_split_iterator import (
    PushSplitReceiverMixin,
    _BlockDelivery,
    _BlockPush,
    _EndOfEpoch,
    _ExecutorError,
    _PushReceiver,
)

# ---------------------------------------------------------------------------
# _PushReceiver unit tests (no Ray involved).
# ---------------------------------------------------------------------------


def _drain(q):
    items = []
    while not q.empty():
        items.append(q.get_nowait())
    return items


def test_receiver_reorders_out_of_order_deliveries():
    receiver = _PushReceiver()
    receiver.begin_epoch(0)

    receiver.deliver(0, 1, _BlockPush(size_bytes=10, num_rows=5), "b1")
    # seq 0 hasn't arrived: nothing released yet.
    assert receiver.queue.qsize() == 0

    receiver.deliver(0, 0, _BlockPush(size_bytes=10, num_rows=5), "b0")
    items = _drain(receiver.queue)
    assert [item.block for item in items] == ["b0", "b1"]
    assert all(isinstance(item, _BlockDelivery) for item in items)


def test_receiver_eof_is_sequenced():
    receiver = _PushReceiver()
    receiver.begin_epoch(0)

    # EOF (seq 2) arrives before both blocks: it must not overtake them.
    receiver.deliver(0, 2, _EndOfEpoch(0))
    receiver.deliver(0, 1, _BlockPush(size_bytes=1, num_rows=1), "b1")
    assert receiver.queue.qsize() == 0
    receiver.deliver(0, 0, _BlockPush(size_bytes=1, num_rows=1), "b0")

    items = _drain(receiver.queue)
    assert [type(item) for item in items] == [
        _BlockDelivery,
        _BlockDelivery,
        _EndOfEpoch,
    ]


def test_receiver_drops_stale_or_early_deliveries():
    receiver = _PushReceiver()
    # Before begin_epoch, every delivery is dropped.
    receiver.deliver(0, 0, _BlockPush(size_bytes=1, num_rows=1), "early")
    assert receiver.queue.qsize() == 0

    receiver.begin_epoch(1)
    # Stale epoch dropped, current epoch delivered.
    receiver.deliver(0, 0, _BlockPush(size_bytes=1, num_rows=1), "stale")
    receiver.deliver(1, 0, _BlockPush(size_bytes=1, num_rows=1), "current")
    items = _drain(receiver.queue)
    assert [item.block for item in items] == ["current"]


def test_receiver_error_bypasses_sequencing():
    receiver = _PushReceiver()
    receiver.begin_epoch(0)

    # A sequencing gap is open (seq 0 missing), but errors fail fast.
    receiver.deliver(0, 1, _BlockPush(size_bytes=1, num_rows=1), "b1")
    error = _ExecutorError(RuntimeError("boom"))
    receiver.deliver_error(0, error)
    assert receiver.queue.get_nowait() is error


def test_receiver_reset_swaps_queue():
    receiver = _PushReceiver()
    receiver.begin_epoch(0)
    receiver.deliver(0, 0, _BlockPush(size_bytes=1, num_rows=1), "old")
    old_queue = receiver.queue

    receiver.reset()
    receiver.begin_epoch(1)
    receiver.deliver(1, 0, _BlockPush(size_bytes=1, num_rows=1), "new")

    # A generator stuck on the old epoch's queue sees nothing new; the new
    # epoch's queue holds only the new delivery.
    assert old_queue.qsize() == 1
    assert receiver.queue.get_nowait().block == "new"


# ---------------------------------------------------------------------------
# End-to-end tests: a minimal consumer actor shaped like a Ray Train worker
# (single-threaded actor, consume loop on a background thread).
# ---------------------------------------------------------------------------


@ray.remote(num_cpus=0)
class _Consumer(PushSplitReceiverMixin):
    def __init__(self, data_iterator):
        self._it = data_iterator
        self._thread: Optional[threading.Thread] = None
        self._result = None

    def start_epoch(
        self,
        batch_size: int = 100,
        delay_s: float = 0.0,
        max_rows: Optional[int] = None,
        prefetch_batches: int = 4,
    ) -> None:
        assert self._thread is None or not self._thread.is_alive()
        self._result = None

        def _run():
            try:
                rows = 0
                for batch in self._it.iter_batches(
                    batch_size=batch_size, prefetch_batches=prefetch_batches
                ):
                    rows += len(batch["id"])
                    if delay_s:
                        time.sleep(delay_s)
                    if max_rows is not None and rows >= max_rows:
                        break
                self._result = {"rows": rows}
            except BaseException as e:  # noqa: BLE001 - surfaced via epoch_result
                self._result = e

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def epoch_result(self):
        """None while the epoch is still running; raises if it failed."""
        if self._thread is not None and self._thread.is_alive():
            return None
        result = self._result
        if isinstance(result, BaseException):
            raise result
        return result


def _run_epochs(consumers, kwargs_list=None, timeout_s: float = 120.0):
    kwargs_list = kwargs_list or [{}] * len(consumers)
    ray.get([c.start_epoch.remote(**kw) for c, kw in zip(consumers, kwargs_list)])
    results = []
    for consumer in consumers:
        deadline = time.monotonic() + timeout_s
        while True:
            result = ray.get(consumer.epoch_result.remote())
            if result is not None:
                results.append(result)
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"epoch still running after {timeout_s}s")
            time.sleep(0.05)
    return results


def test_push_split_equal_across_epochs(ray_start_regular_shared):
    ds = ray.data.range(1000, override_num_blocks=20)
    iterators = ds.streaming_split_push_based(2, equal=True)
    consumers = [_Consumer.remote(it) for it in iterators]

    for _ in range(2):
        results = _run_epochs(consumers)
        assert [r["rows"] for r in results] == [500, 500]


def test_push_split_early_exit_then_full_epoch(ray_start_regular_shared):
    ds = ray.data.range(1000, override_num_blocks=20)
    iterators = ds.streaming_split_push_based(2, equal=True)
    consumers = [_Consumer.remote(it) for it in iterators]

    results = _run_epochs(consumers, [{"max_rows": 200}, {}])
    assert results[0]["rows"] == 200
    assert results[1]["rows"] == 500

    # The next epoch recovers and serves full shares.
    results = _run_epochs(consumers)
    assert [r["rows"] for r in results] == [500, 500]


def test_push_split_error_propagation(ray_start_regular_shared):
    def _boom(row):
        raise ValueError("boom")

    ds = ray.data.range(100).map(_boom)
    iterators = ds.streaming_split_push_based(2)
    consumers = [_Consumer.remote(it) for it in iterators]

    ray.get([c.start_epoch.remote(batch_size=10) for c in consumers])
    for consumer in consumers:

        def _errored(consumer=consumer):
            try:
                return ray.get(consumer.epoch_result.remote()) is not None
            except Exception:
                return True

        wait_for_condition(_errored, timeout=120)
        with pytest.raises(Exception, match="boom"):
            ray.get(consumer.epoch_result.remote())


def test_push_split_flow_control_bounds_buffering(ray_start_regular_shared):
    # 50-row blocks; window = 4 batches x 100 rows = 400 rows. A slow
    # consumer must never have more than window + one block outstanding.
    num_rows, num_blocks = 2000, 40
    block_rows = num_rows // num_blocks
    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)
    iterators = ds.streaming_split_push_based(2, equal=True)
    consumers = [_Consumer.remote(it) for it in iterators]

    ray.get([c.start_epoch.remote(delay_s=0.2) for c in consumers])
    coordinator = iterators[0]._coord_actor

    target_rows = 4 * 100
    sampled = 0
    deadline = time.monotonic() + 120
    while sampled < 5 and time.monotonic() < deadline:
        state = ray.get(coordinator.debug_state.remote())
        for split in state["rows_pushed"]:
            outstanding = (
                state["rows_pushed"][split] - state["rows_consumed_reported"][split]
            )
            assert outstanding <= target_rows + block_rows, state
        if any(pushed > 0 for pushed in state["rows_pushed"].values()):
            sampled += 1
        time.sleep(0.5)
    assert sampled == 5, "producer never started pushing"
    _run_epochs(consumers)


def test_push_split_requires_actor_host(ray_start_regular_shared):
    ds = ray.data.range(100)
    iterators = ds.streaming_split_push_based(1)
    with pytest.raises(RuntimeError, match="PushSplitReceiverMixin"):
        next(iter(iterators[0].iter_batches(batch_size=10)))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
