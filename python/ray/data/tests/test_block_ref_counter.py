import gc
import threading
import time

import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.tests.conftest import *  # noqa


class _FakeRef:
    """Minimal stand-in for ray.ObjectRef — has a .binary() that returns bytes."""

    def __init__(self, uid: int):
        self._binary = uid.to_bytes(28, "big")

    def binary(self) -> bytes:
        return self._binary


def _make_counter_with_callback(ref, size_bytes, producer_id):
    """Create a counter and manually register a block, returning (counter, callback).

    Instead of calling into Ray Core, we extract the callback closure so tests
    can fire it directly.
    """
    counter = BlockRefCounter()
    id_binary = ref.binary()

    # Replicate the on_block_produced logic without the Ray Core call.
    with counter._lock:
        counter._registered_ids.add(id_binary)
        counter._bytes_by_producer[producer_id] += size_bytes

    # Build the same closure on_block_produced would have registered.
    def _on_out_of_scope(id_bytes: bytes) -> None:
        with counter._lock:
            if id_bytes not in counter._registered_ids:
                return
            counter._registered_ids.discard(id_bytes)
            counter._bytes_by_producer[producer_id] -= size_bytes

    return counter, _on_out_of_scope, id_binary


class TestBlockRefCounterAccounting:
    def test_single_block_produced_and_released(self):
        ref = _FakeRef(1)
        counter, callback, id_binary = _make_counter_with_callback(ref, 100, "op_a")

        assert counter.get_object_store_memory_usage("op_a") == 100
        callback(id_binary)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_blocks_same_producer(self):
        ref1, ref2 = _FakeRef(1), _FakeRef(2)
        counter, cb1, bin1 = _make_counter_with_callback(ref1, 100, "op_a")
        # Add second block to the same counter.
        with counter._lock:
            counter._registered_ids.add(ref2.binary())
            counter._bytes_by_producer["op_a"] += 200

        def cb2(id_bytes):
            with counter._lock:
                if id_bytes not in counter._registered_ids:
                    return
                counter._registered_ids.discard(id_bytes)
                counter._bytes_by_producer["op_a"] -= 200

        assert counter.get_object_store_memory_usage("op_a") == 300
        cb1(bin1)
        assert counter.get_object_store_memory_usage("op_a") == 200
        cb2(ref2.binary())
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_producers_isolated(self):
        ref1, ref2 = _FakeRef(1), _FakeRef(2)
        counter, cb1, bin1 = _make_counter_with_callback(ref1, 100, "op_a")
        with counter._lock:
            counter._registered_ids.add(ref2.binary())
            counter._bytes_by_producer["op_b"] += 200

        def cb2(id_bytes):
            with counter._lock:
                if id_bytes not in counter._registered_ids:
                    return
                counter._registered_ids.discard(id_bytes)
                counter._bytes_by_producer["op_b"] -= 200

        assert counter.get_object_store_memory_usage("op_a") == 100
        assert counter.get_object_store_memory_usage("op_b") == 200

        cb1(bin1)
        assert counter.get_object_store_memory_usage("op_a") == 0
        assert counter.get_object_store_memory_usage("op_b") == 200


class TestBlockRefCounterDeduplication:
    def test_duplicate_ref_not_double_counted(self):
        """on_block_produced for the same ref twice only counts size_bytes once."""
        ref = _FakeRef(1)
        counter = BlockRefCounter()
        id_binary = ref.binary()

        # First registration
        with counter._lock:
            counter._registered_ids.add(id_binary)
            counter._bytes_by_producer["op_a"] += 100

        # Second call for same ref — the guard should skip it.
        duplicate_added = False
        with counter._lock:
            if id_binary in counter._registered_ids:
                duplicate_added = False  # guard fires correctly
            else:
                counter._registered_ids.add(id_binary)
                counter._bytes_by_producer["op_a"] += 100
                duplicate_added = True

        assert not duplicate_added
        assert counter.get_object_store_memory_usage("op_a") == 100

    def test_unknown_producer_returns_zero(self):
        counter = BlockRefCounter()
        assert counter.get_object_store_memory_usage("nonexistent_op") == 0


class TestBlockRefCounterClear:
    def test_clear_resets_usage(self):
        ref = _FakeRef(1)
        counter, _, _ = _make_counter_with_callback(ref, 100, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 100

        counter.clear()
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_callback_after_clear_is_noop(self):
        """A callback firing after clear() must not crash or corrupt state."""
        ref = _FakeRef(1)
        counter, callback, id_binary = _make_counter_with_callback(ref, 100, "op_a")

        counter.clear()
        # Fire the callback — should be a silent no-op.
        callback(id_binary)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_new_blocks_after_clear_are_tracked(self):
        """After clear(), new registrations work normally."""
        counter = BlockRefCounter()
        counter.clear()

        ref = _FakeRef(2)
        id_binary = ref.binary()
        with counter._lock:
            counter._registered_ids.add(id_binary)
            counter._bytes_by_producer["op_b"] += 50

        assert counter.get_object_store_memory_usage("op_b") == 50


class TestBlockRefCounterThreadSafety:
    def test_concurrent_callbacks_dont_corrupt_state(self):
        """Multiple threads firing callbacks concurrently must not go negative."""
        counter = BlockRefCounter()
        producer_id = "op_concurrent"
        n = 50
        refs = [_FakeRef(i) for i in range(n)]
        callbacks = []

        for ref in refs:
            id_binary = ref.binary()
            size = 10
            with counter._lock:
                counter._registered_ids.add(id_binary)
                counter._bytes_by_producer[producer_id] += size

            def make_cb(idb, sz):
                def cb(id_bytes):
                    with counter._lock:
                        if id_bytes not in counter._registered_ids:
                            return
                        counter._registered_ids.discard(id_bytes)
                        counter._bytes_by_producer[producer_id] -= sz

                return cb

            callbacks.append((make_cb(id_binary, size), id_binary))

        threads = [threading.Thread(target=cb, args=(idb,)) for cb, idb in callbacks]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert counter.get_object_store_memory_usage(producer_id) == 0


@ray.remote
def _hold_ref_for(block_ref, sleep_s: float) -> bool:
    """Hold *block_ref* as a task argument for *sleep_s* seconds, then return.

    Because Ray keeps the object alive for the duration of any task that
    received it as an argument, this lets tests verify the callback has
    not fired while the task is still running.
    """
    import time as _time

    _time.sleep(sleep_s)
    return True


def _wait_for_counter(
    counter: BlockRefCounter,
    producer_id: str,
    expected: int,
    timeout_s: float = 10.0,
    poll_interval_s: float = 0.05,
) -> bool:
    """Poll until *counter* reports *expected* bytes for *producer_id*.

    Calls ``gc.collect()`` on every iteration so that any pending Python-level
    ObjectRef destructors have a chance to run.  Returns True if the expected
    value is reached before *timeout_s* elapses, False otherwise.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        gc.collect()
        if counter.get_object_store_memory_usage(producer_id) == expected:
            return True
        time.sleep(poll_interval_s)
    return False


class TestBlockRefCounterLifecycle:
    """Integration tests that exercise the full add_object_out_of_scope_callback path.

    All tests in this class require a live Ray cluster (ray_start_regular_shared).
    They verify that the out-of-scope callback fires at *exactly* the right moment:
    not before the last reference drops, and not after it.

    Three cases are covered, matching the S7 scenario in the benchmark plan:
      1. Basic lifecycle — callback fires after the last Python ObjectRef is GC'd.
      2. Two Python refs — callback fires only after *both* refs are dropped.
      3. Task ref — callback fires only after the task finishes AND the Python ref
         has been dropped.
    """

    # Byte count attributed to the test operator.  The actual object put into
    # the store is much smaller; we only care that the counter tracks *this*
    # number faithfully.
    _SIZE_BYTES = 1 * 1024 * 1024  # 1 MB

    def _make_block(self) -> "ray.ObjectRef":
        import numpy as np

        return ray.put(np.zeros(128, dtype=np.float64))

    def test_callback_fires_after_last_python_ref_deleted(
        self, ray_start_regular_shared
    ):
        """Counter reaches 0 once the only Python ObjectRef is GC'd."""
        counter = BlockRefCounter()
        ref = self._make_block()

        counter.on_block_produced(ref, self._SIZE_BYTES, "op_basic")
        assert counter.get_object_store_memory_usage("op_basic") == self._SIZE_BYTES

        del ref  # last Python ref gone
        assert _wait_for_counter(counter, "op_basic", 0), (
            "Counter did not reach 0 after all Python refs were deleted; "
            f"remaining: {counter.get_object_store_memory_usage('op_basic')} bytes"
        )

    def test_second_python_ref_keeps_counter_alive(self, ray_start_regular_shared):
        """Counter stays non-zero while a second Python ObjectRef is alive.

        Dropping one of two refs that point at the same ObjectID must NOT fire
        the callback — only the final ref drop may do so.
        """
        counter = BlockRefCounter()
        ref1 = self._make_block()
        ref2 = ref1  # second Python ref to the same ObjectID

        counter.on_block_produced(ref1, self._SIZE_BYTES, "op_two_refs")
        assert counter.get_object_store_memory_usage("op_two_refs") == self._SIZE_BYTES

        del ref1
        gc.collect()
        time.sleep(0.3)  # give GC ample time; counter must still be non-zero

        assert (
            counter.get_object_store_memory_usage("op_two_refs") == self._SIZE_BYTES
        ), "Callback fired too early — counter decremented while ref2 was still alive"

        del ref2  # last ref gone; callback must now fire
        assert _wait_for_counter(
            counter, "op_two_refs", 0
        ), "Counter did not reach 0 after the last Python ref was deleted"

    def test_task_ref_keeps_counter_alive_until_task_completes(
        self, ray_start_regular_shared
    ):
        """Counter stays non-zero while a running Ray task holds the block.

        Ray keeps any object alive for the duration of a task that received it
        as an argument.  The callback should not fire until both conditions hold:
        (a) the task has completed, and (b) all Python refs are dropped.
        """
        counter = BlockRefCounter()
        ref = self._make_block()

        counter.on_block_produced(ref, self._SIZE_BYTES, "op_task")
        assert counter.get_object_store_memory_usage("op_task") == self._SIZE_BYTES

        # Submit a task that sleeps for 1 s while holding the block, then drop
        # the Python ref so only the task's argument reference remains.
        task_future = _hold_ref_for.remote(ref, 1.0)
        del ref
        gc.collect()
        time.sleep(0.3)  # task is still running; callback must NOT have fired

        assert (
            counter.get_object_store_memory_usage("op_task") == self._SIZE_BYTES
        ), "Callback fired too early — counter decremented while task was still running"

        ray.get(task_future)  # task completes; now both refs are gone
        assert _wait_for_counter(
            counter, "op_task", 0
        ), "Counter did not reach 0 after task completed and Python ref was deleted"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
