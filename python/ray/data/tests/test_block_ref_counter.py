import gc
import time

import numpy as np
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.util import MiB
from ray.tests.conftest import *  # noqa

# Grace period for asserting a callback has NOT fired. Must be shorter than
# the task sleep in test_task_ref_keeps_counter_alive (1.0s); 0.3s leaves
# wide margin even on slow CI while still surfacing early-fire bugs.
_EARLY_FIRE_GRACE_S = 0.3


@ray.remote
def _hold_ref_for(block_ref, sleep_s: float) -> bool:
    """Hold *block_ref* as a task argument for *sleep_s* seconds, then return.

    Ray keeps an object alive for the duration of any task that received it as
    an argument, so this lets tests assert the callback has not fired while the
    task is still running.
    """
    time.sleep(sleep_s)
    return True


@pytest.fixture(params=["inlined", "regular"])
def make_block(request):
    """Factory for a block ObjectRef, parametrized over the two storage paths.

    Ray Core inlines tiny objects in the in-process store and puts larger ones
    in the shared-memory object store; the out-of-scope callback must work for
    both. Returning a factory (rather than the ref itself) avoids pytest holding
    an extra reference that would keep the object alive past the test's own ``del``.
    """

    def _make() -> "ray.ObjectRef":
        if request.param == "inlined":
            return ray.put(0)
        return ray.put(np.zeros(1 * MiB, dtype=np.uint8))

    return _make


def _wait_for_counter(counter, producer_id, expected, timeout_s: float = 10.0):
    """Wait until *counter* reports *expected* bytes for *producer_id*.

    ``gc.collect()`` runs on each poll so any pending Python-level ObjectRef
    destructors get a chance to run; the polling/timeout loop is delegated to
    ``wait_for_condition`` (raises on timeout).
    """

    def _reached():
        gc.collect()
        return counter.get_object_store_memory_usage(producer_id) == expected

    wait_for_condition(_reached, timeout=timeout_s)


class TestBlockRefCounterLifecycle:
    def test_callback_fires_after_last_python_ref_deleted(
        self, ray_start_regular_shared, make_block
    ):
        """Counter reaches 0 once the only Python ObjectRef is GC'd."""
        counter = BlockRefCounter()
        ref = make_block()

        counter.on_block_produced(ref, 1 * MiB, "op_basic")
        assert counter.get_object_store_memory_usage("op_basic") == 1 * MiB

        del ref  # last Python ref gone
        _wait_for_counter(counter, "op_basic", 0)

    def test_second_python_ref_keeps_counter_alive(
        self, ray_start_regular_shared, make_block
    ):
        """Counter stays non-zero while a second Python ObjectRef is alive.

        Dropping one of two refs that point at the same ObjectID must NOT fire
        the callback. Only the final ref drop may do so.
        """
        counter = BlockRefCounter()
        ref1 = make_block()
        ref2 = ref1  # second Python ref to the same ObjectID

        counter.on_block_produced(ref1, 1 * MiB, "op_two_refs")
        assert counter.get_object_store_memory_usage("op_two_refs") == 1 * MiB

        del ref1
        gc.collect()
        time.sleep(_EARLY_FIRE_GRACE_S)  # counter must still be non-zero

        assert (
            counter.get_object_store_memory_usage("op_two_refs") == 1 * MiB
        ), "Callback fired too early — counter decremented while ref2 was still alive"

        del ref2  # last ref gone; callback must now fire
        _wait_for_counter(counter, "op_two_refs", 0)

    def test_task_ref_keeps_counter_alive_until_task_completes(
        self, ray_start_regular_shared
    ):
        """Counter stays non-zero while a running Ray task holds the block.

        Ray keeps any object alive for the duration of a task that received it
        as an argument. The callback should not fire until both conditions hold:
        (a) the task has completed, and (b) all Python refs are dropped.

        Uses a plasma (by-reference) object specifically: tiny objects are
        inlined into the task by value, so they would not get a task-argument
        reference and this lifetime-extension behavior would not apply.
        """
        counter = BlockRefCounter()
        ref = ray.put(np.zeros(1 * MiB, dtype=np.uint8))

        counter.on_block_produced(ref, 1 * MiB, "op_task")
        assert counter.get_object_store_memory_usage("op_task") == 1 * MiB

        # Submit a task that sleeps while holding the block, then drop the Python
        # ref so only the task's argument reference remains.
        task_future = _hold_ref_for.remote(ref, 1.0)
        del ref
        gc.collect()
        time.sleep(_EARLY_FIRE_GRACE_S)  # task still running; callback must NOT fire

        assert (
            counter.get_object_store_memory_usage("op_task") == 1 * MiB
        ), "Callback fired too early: counter decremented while task was still running"

        ray.get(task_future)  # task completes; now both refs are gone
        _wait_for_counter(counter, "op_task", 0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
