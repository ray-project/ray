"""Tests for the push-based streaming_split (push_split_iterator.py)."""

import threading
import time
from types import SimpleNamespace
from typing import Any, List, Optional

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.iterator.push_split_coordinator import (
    _BlockPush,
    _EndOfEpoch,
    _ExecutorError,
)
from ray.data._internal.iterator.push_split_iterator import (
    PushSplitReceiverMixin,
    _BlockDelivery,
    _PushReceiver,
    streaming_split_push_based,
)

# ---------------------------------------------------------------------------
# _PushReceiver unit tests (no Ray involved).
# ---------------------------------------------------------------------------


def _drain(q) -> List[Any]:
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
    item = receiver.queue.get_nowait()
    assert isinstance(item, _BlockDelivery) and item.block == "new"


def test_receiver_tracks_queued_bytes():
    receiver = _PushReceiver()
    receiver.begin_epoch(0)
    # Out of order: seq 1 waits in the reorder buffer and isn't queued yet.
    receiver.deliver(0, 1, _BlockPush(size_bytes=30, num_rows=3), block="b1")
    assert receiver.queued_bytes == 0
    receiver.deliver(0, 0, _BlockPush(size_bytes=10, num_rows=1), block="b0")
    assert receiver.queued_bytes == 40

    epoch_queue = receiver.queue
    assert receiver.on_block_popped(epoch_queue, 10) == 30

    # After a reset, a stale generator's pops don't change the new count.
    receiver.reset()
    assert receiver.queued_bytes == 0
    assert receiver.on_block_popped(epoch_queue, 30) == 0


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
                ids = []
                for batch in self._it.iter_batches(
                    batch_size=batch_size, prefetch_batches=prefetch_batches
                ):
                    rows += len(batch["id"])
                    ids.extend(batch["id"].tolist())
                    if delay_s:
                        time.sleep(delay_s)
                    if max_rows is not None and rows >= max_rows:
                        break
                self._result = {"rows": rows, "ids": ids}
            except BaseException as e:  # noqa: BLE001 - surfaced via epoch_result
                self._result = e

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def start_materialize(self) -> None:
        assert self._thread is None or not self._thread.is_alive()
        self._result = None

        def _run():
            try:
                materialized = self._it.materialize()
                ids = [row["id"] for row in materialized.take_all()]
                self._result = {"rows": len(ids), "ids": ids}
            except BaseException as e:  # noqa: BLE001 - surfaced via epoch_result
                self._result = e

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def prefetched_bytes(self) -> int:
        return self._it._iter_stats.iter_prefetched_bytes

    def stats(self) -> str:
        return self._it.stats()

    def epoch_result(self):
        """None while the epoch is still running; raises if it failed."""
        if self._thread is not None and self._thread.is_alive():
            return None
        result = self._result
        if isinstance(result, BaseException):
            raise result
        return result

    def checkpointer_calls(self):
        checkpointer = self._it._checkpointer
        return checkpointer.events, checkpointer.recorded_ids

    def num_receivers(self) -> int:
        # Imported here so this reads the worker process's registry; a
        # module-level name would be pickled by value with this class.
        from ray.data._internal.iterator import push_split_iterator

        return len(push_split_iterator._RECEIVER_REGISTRY)


def _make_consumers(iterators) -> List[Any]:
    # pyrefly: ignore[missing-attribute]  # @ray.remote hides ActorClass.remote
    return [_Consumer.remote(it) for it in iterators]


def _run_epochs(consumers, kwargs_list=None, timeout_s: float = 120.0):
    kwargs_list = kwargs_list or [{}] * len(consumers)
    ray.get([c.start_epoch.remote(**kw) for c, kw in zip(consumers, kwargs_list)])
    return _wait_for_results(consumers, timeout_s)


def _wait_for_results(consumers, timeout_s: float = 120.0):
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
    iterators = streaming_split_push_based(ds, 2, equal=True)
    consumers = _make_consumers(iterators)

    for _ in range(2):
        results = _run_epochs(consumers)
        assert [r["rows"] for r in results] == [500, 500]


def test_push_split_early_exit_then_full_epoch(ray_start_regular_shared):
    ds = ray.data.range(1000, override_num_blocks=20)
    iterators = streaming_split_push_based(ds, 2, equal=True)
    consumers = _make_consumers(iterators)

    results = _run_epochs(consumers, [{"max_rows": 200}, {}])
    assert results[0]["rows"] == 200
    assert results[1]["rows"] == 500

    # Receive state is dropped when iteration ends, even after an early exit.
    assert ray.get([c.num_receivers.remote() for c in consumers]) == [0, 0]

    # The next epoch recovers and serves full shares.
    results = _run_epochs(consumers)
    assert [r["rows"] for r in results] == [500, 500]
    assert ray.get([c.num_receivers.remote() for c in consumers]) == [0, 0]


def test_push_split_error_propagation(ray_start_regular_shared):
    def _boom(row):
        raise ValueError("boom")

    ds = ray.data.range(100).map(_boom)
    iterators = streaming_split_push_based(ds, 2)
    consumers = _make_consumers(iterators)

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
    iterators = streaming_split_push_based(ds, 2, equal=True)
    consumers = _make_consumers(iterators)

    ray.get([c.start_epoch.remote(delay_s=0.2) for c in consumers])
    coordinator = iterators[0]._coord_actor

    target_rows = 4 * 100
    max_held_blocks = (target_rows + block_rows) // block_rows + 1
    sampled = 0
    held_refs_seen = False
    deadline = time.monotonic() + 120
    while sampled < 5 and time.monotonic() < deadline:
        state: Any = ray.get(coordinator.debug_state.remote())
        for split in state["rows_pushed"]:
            outstanding = state["rows_pushed"][split] - state["rows_consumed"][split]
            assert outstanding <= target_rows + block_rows, state
            # Refs to sent-but-unconsumed blocks are held (so Ray Data counts
            # them), but never more than the window's worth.
            assert state["unconsumed_refs"][split] <= max_held_blocks, state
            held_refs_seen |= state["unconsumed_refs"][split] > 0
        if any(pushed > 0 for pushed in state["rows_pushed"].values()):
            sampled += 1
        time.sleep(0.5)
    assert sampled == 5, "producer never started pushing"
    assert held_refs_seen, "no refs held for in-flight blocks"
    _run_epochs(consumers)


def test_push_split_materialize(ray_start_regular_shared):
    ds = ray.data.range(1000, override_num_blocks=20)
    iterators = streaming_split_push_based(ds, 2, equal=True)
    consumers = _make_consumers(iterators)

    ray.get([c.start_materialize.remote() for c in consumers])
    results = _wait_for_results(consumers)
    assert [r["rows"] for r in results] == [500, 500]
    assert sorted(results[0]["ids"] + results[1]["ids"]) == list(range(1000))
    # materialize() ends the epoch itself: no receive state is left behind.
    assert ray.get([c.num_receivers.remote() for c in consumers]) == [0, 0]

    # A split can materialize one epoch and iterate batches in the next.
    results = _run_epochs(consumers)
    assert [r["rows"] for r in results] == [500, 500]


def test_push_split_materialize_mixed_with_iter_batches(ray_start_regular_shared):
    # One split materializes while the other iterates batches in the same
    # epoch, as a train worker and a validation step may do.
    ds = ray.data.range(1000, override_num_blocks=20)
    iterators = streaming_split_push_based(ds, 2, equal=True)
    consumers = _make_consumers(iterators)

    ray.get(
        [consumers[0].start_materialize.remote(), consumers[1].start_epoch.remote()]
    )
    results = _wait_for_results(consumers)
    assert [r["rows"] for r in results] == [500, 500]
    assert sorted(results[0]["ids"] + results[1]["ids"]) == list(range(1000))


def test_push_split_materialize_error_propagation(ray_start_regular_shared):
    def _boom(row):
        raise ValueError("boom")

    ds = ray.data.range(100).map(_boom)
    iterators = streaming_split_push_based(ds, 1)
    consumers = _make_consumers(iterators)

    ray.get(consumers[0].start_materialize.remote())

    def _errored():
        try:
            return ray.get(consumers[0].epoch_result.remote()) is not None
        except Exception:
            return True

    wait_for_condition(_errored, timeout=120)
    with pytest.raises(Exception, match="boom"):
        ray.get(consumers[0].epoch_result.remote())
    assert ray.get(consumers[0].num_receivers.remote()) == 0


def test_push_split_stats_match_pull_split(ray_start_regular_shared):
    """The push split reports the iterator stats that the pull split
    reports: prefetched bytes and the coordinator overhead time."""
    num_rows, num_blocks = 2000, 40
    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)
    iterators = streaming_split_push_based(ds, 2, equal=True)
    consumers = _make_consumers(iterators)

    # A slow consumer lets the prefetch window fill the local queue.
    ray.get([c.start_epoch.remote(delay_s=0.2) for c in consumers])
    wait_for_condition(
        lambda: all(ray.get([c.prefetched_bytes.remote() for c in consumers])),
        timeout=120,
    )
    _wait_for_results(consumers)

    # Nothing is prefetched once the epoch ends.
    assert ray.get([c.prefetched_bytes.remote() for c in consumers]) == [0, 0]
    for stats in ray.get([c.stats.remote() for c in consumers]):
        assert "Streaming split coordinator overhead time" in stats


def test_push_split_checkpointing_records_yielded_row_ids(ray_start_regular_shared):
    # Define the class in the test so cloudpickle sends it by value. The
    # consumer actor can't import this test module.
    class _RecordingCheckpointer:
        """Stands in for ``RowIDBasedDataIteratorCheckpointer``. It records the
        calls that ``CheckpointingBatchIterator`` makes, in the consumer actor."""

        def __init__(self):
            self._id_column = "id"
            self._checkpoint_config = SimpleNamespace(generate_id_column=False)
            self.events: List[str] = []
            self.recorded_ids: List[int] = []

        def start_epoch(self) -> None:
            self.events.append("start_epoch")

        def end_epoch(self) -> None:
            self.events.append("end_epoch")

        def record_yielded_batch(self, batch) -> None:
            self.recorded_ids.extend(batch.metadata.row_ids.column("id").to_pylist())

    ds = ray.data.range(1000, override_num_blocks=20)
    iterators = streaming_split_push_based(ds, 2, equal=True)
    for it in iterators:
        it._enable_checkpointing(
            _RecordingCheckpointer()  # pyrefly: ignore[bad-argument-type]
        )
    consumers = _make_consumers(iterators)

    results = _run_epochs(consumers)

    all_ids = []
    for consumer, result in zip(consumers, results):
        events, recorded_ids = ray.get(consumer.checkpointer_calls.remote())
        assert events == ["start_epoch", "end_epoch"]
        # Each split records exactly the row IDs it yielded, in order.
        assert recorded_ids == result["ids"]
        all_ids.extend(recorded_ids)
    assert sorted(all_ids) == list(range(1000))


def test_push_split_requires_actor_host(ray_start_regular_shared):
    ds = ray.data.range(100)
    iterators = streaming_split_push_based(ds, 1)
    with pytest.raises(RuntimeError, match="PushSplitReceiverMixin"):
        next(iter(iterators[0].iter_batches(batch_size=10)))


@ray.remote(num_cpus=0)
class _TaskThreadConsumer(PushSplitReceiverMixin):
    def iterate(self, data_iterator):
        # Runs on the default actor's task thread, which also has to run
        # the delivery tasks.
        next(iter(data_iterator.iter_batches(batch_size=10)))


def test_push_split_rejects_actor_task_thread(ray_start_regular_shared):
    ds = ray.data.range(100)
    iterators = streaming_split_push_based(ds, 1)
    consumer = _TaskThreadConsumer.remote()  # pyrefly: ignore[missing-attribute]
    with pytest.raises(Exception, match="background thread"):
        ray.get(consumer.iterate.remote(iterators[0]), timeout=60)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
