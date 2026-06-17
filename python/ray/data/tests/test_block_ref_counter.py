import gc
import threading
import time
import unittest.mock as mock

import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.tests.conftest import *  # noqa


class _FakeRef:
    """Minimal stand-in for ray.ObjectRef. Has a .binary() that returns bytes."""

    def __init__(self, uid: int):
        self._binary = uid.to_bytes(28, "big")

    def binary(self) -> bytes:
        return self._binary


def _register_block(counter, ref, size_bytes, producer_id):
    """Call on_block_produced on an existing counter with a mocked core worker.

    Returns the captured _on_out_of_scope callback so tests can fire it directly.
    """
    captured_callback = None

    class _MockCoreWorker:
        def add_object_out_of_scope_callback(self, block_ref, cb):
            nonlocal captured_callback
            captured_callback = cb
            return True

    with mock.patch(
        "ray._private.worker.global_worker",
        mock.Mock(core_worker=_MockCoreWorker()),
    ):
        counter.on_block_produced(ref, size_bytes, producer_id)

    return captured_callback, ref.binary()


class TestBlockRefCounterAccounting:
    def test_single_block_produced_and_released(self):
        counter = BlockRefCounter()
        ref = _FakeRef(1)
        callback, id_binary = _register_block(counter, ref, 100, "op_a")

        assert counter.get_object_store_memory_usage("op_a") == 100
        callback(id_binary)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_blocks_same_producer(self):
        counter = BlockRefCounter()
        ref1, ref2 = _FakeRef(1), _FakeRef(2)
        cb1, bin1 = _register_block(counter, ref1, 100, "op_a")
        cb2, bin2 = _register_block(counter, ref2, 200, "op_a")

        assert counter.get_object_store_memory_usage("op_a") == 300
        cb1(bin1)
        assert counter.get_object_store_memory_usage("op_a") == 200
        cb2(bin2)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_producers_isolated(self):
        counter = BlockRefCounter()
        ref1, ref2 = _FakeRef(1), _FakeRef(2)
        cb1, bin1 = _register_block(counter, ref1, 100, "op_a")
        _register_block(counter, ref2, 200, "op_b")

        assert counter.get_object_store_memory_usage("op_a") == 100
        assert counter.get_object_store_memory_usage("op_b") == 200

        cb1(bin1)
        assert counter.get_object_store_memory_usage("op_a") == 0
        assert counter.get_object_store_memory_usage("op_b") == 200


class TestBlockRefCounterClear:
    def test_clear_resets_usage(self):
        counter = BlockRefCounter()
        _register_block(counter, _FakeRef(1), 100, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 100

        counter.clear()
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_callback_after_clear_is_noop(self):
        """A callback firing after clear() must not crash or corrupt state."""
        counter = BlockRefCounter()
        ref = _FakeRef(1)
        callback, id_binary = _register_block(counter, ref, 100, "op_a")

        counter.clear()
        callback(id_binary)  # must be a silent no-op
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_new_blocks_after_clear_are_tracked(self):
        """After clear(), new registrations work normally."""
        counter = BlockRefCounter()
        _register_block(counter, _FakeRef(1), 50, "op_b")
        counter.clear()
        assert counter.get_object_store_memory_usage("op_b") == 0

        _register_block(counter, _FakeRef(2), 50, "op_b")
        assert counter.get_object_store_memory_usage("op_b") == 50

    def test_clear_races_with_object_already_freed(self):
        """clear() between byte-increment and the registered=False undo must not go negative.

        If add_object_out_of_scope_callback returns False (object already gone),
        on_block_produced calls _on_object_freed to undo the increment. If clear()
        fires in that window, the undo must be a no-op (id_binary is no longer in
        _registered_ids), not a double-decrement.
        """
        counter = BlockRefCounter()
        ref = _FakeRef(1)

        class _ClearOnRegisterCoreWorker:
            def add_object_out_of_scope_callback(self, block_ref, cb):
                counter.clear()  # race: clear fires before finally runs
                return False  # object already out of scope

        with mock.patch(
            "ray._private.worker.global_worker",
            mock.Mock(core_worker=_ClearOnRegisterCoreWorker()),
        ):
            counter.on_block_produced(ref, 100, "op_a")

        assert counter.get_object_store_memory_usage("op_a") == 0


class TestBlockRefCounterThreadSafety:
    def test_concurrent_callbacks_dont_corrupt_state(self):
        """Multiple threads firing callbacks concurrently must not go negative."""
        counter = BlockRefCounter()
        producer_id = "op_concurrent"
        n = 50
        refs = [_FakeRef(i) for i in range(n)]
        callbacks = []

        for ref in refs:
            cb, id_binary = _register_block(counter, ref, 10, producer_id)
            callbacks.append((cb, id_binary))

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
    They verify that the out-of-scope callback fires at exactly the right moment:
    not before the last reference drops, and not after it.

    Three cases are covered:
      1. Basic lifecycle: callback fires after the last Python ObjectRef is GC'd.
      2. Two Python refs: callback fires only after both refs are dropped.
      3. Task ref: callback fires only after the holding task finishes and all
         Python refs are dropped. This matches the real operator lifecycle where
         a block stays live until the task that received it as an argument completes.
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
        the callback. Only the final ref drop may do so.
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
        ), "Callback fired too early: counter decremented while task was still running"

        ray.get(task_future)  # task completes; now both refs are gone
        assert _wait_for_counter(
            counter, "op_task", 0
        ), "Counter did not reach 0 after task completed and Python ref was deleted"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
