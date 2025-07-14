"""All tests in this file use a module-scoped fixture to reduce runtime.

If you need a customized Ray instance (e.g., to change system config or env vars),
put the test in `test_reference_counting_standalone.py`.
"""
# coding: utf-8
import copy
import logging
import os
import sys
import gc
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import (
    kill_actor_and_wait_for_failure,
    put_object,
)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def check_refcounts_empty():
    """Verify that all tests leave the ref counter empty."""
    yield
    check_refcounts({})


@pytest.fixture(scope="module")
def one_cpu_100MiB_shared():
    # It has lots of tests that don't require object spilling.
    config = {"task_retry_delay_ms": 0, "automatic_object_spilling_enabled": False}
    yield ray.init(
        num_cpus=1, object_store_memory=100 * 1024 * 1024, _system_config=config
    )
    ray.shutdown()


def _fill_object_store_and_get(obj, succeed=True, object_MiB=20, num_objects=5):
    for _ in range(num_objects):
        ray.put(np.zeros(object_MiB * 1024 * 1024, dtype=np.uint8))

    if type(obj) is bytes:
        obj = ray.ObjectRef(obj)

    if succeed:
        wait_for_condition(
            lambda: ray._private.worker.global_worker.core_worker.object_exists(obj)
        )
    else:
        wait_for_condition(
            lambda: not ray._private.worker.global_worker.core_worker.object_exists(
                obj
            ),
            timeout=30,
        )


def _check_refcounts(expected):
    actual = ray._private.worker.global_worker.core_worker.get_all_reference_counts()
    assert len(expected) == len(actual)
    for object_ref, (local, submitted) in expected.items():
        hex_id = object_ref.hex().encode("ascii")
        assert hex_id in actual
        assert local == actual[hex_id]["local"]
        assert submitted == actual[hex_id]["submitted"]


def check_refcounts(expected, timeout=10):
    start = time.time()
    while True:
        try:
            gc.collect()
            _check_refcounts(expected)
            break
        except AssertionError as e:
            if time.time() - start > timeout:
                raise e
            else:
                time.sleep(0.1)


def test_local_refcounts(one_cpu_100MiB_shared):
    obj_ref1 = ray.put(None)
    check_refcounts({obj_ref1: (1, 0)})
    obj_ref1_copy = copy.copy(obj_ref1)
    check_refcounts({obj_ref1: (2, 0)})
    del obj_ref1
    check_refcounts({obj_ref1_copy: (1, 0)})
    del obj_ref1_copy
    check_refcounts({})


def test_dependency_refcounts(one_cpu_100MiB_shared):
    @ray.remote
    def one_dep(dep, signal=None, fail=False):
        if signal is not None:
            ray.get(signal.wait.remote())
        if fail:
            raise Exception("failed on purpose")

    @ray.remote
    def one_dep_large(dep, signal=None):
        if signal is not None:
            ray.get(signal.wait.remote())
        # This will be spilled to plasma.
        return np.zeros(10 * 1024 * 1024, dtype=np.uint8)

    # Test that regular plasma dependency refcounts are decremented once the
    # task finishes.
    signal = SignalActor.remote()
    large_dep = ray.put(np.zeros(10 * 1024 * 1024, dtype=np.uint8))
    result = one_dep.remote(large_dep, signal=signal)
    check_refcounts({large_dep: (1, 1), result: (1, 0)})
    ray.get(signal.send.remote())
    # Reference count should be removed once the task finishes.
    check_refcounts({large_dep: (1, 0), result: (1, 0)})
    del large_dep, result
    check_refcounts({})

    # Test that inlined dependency refcounts are decremented once they are
    # inlined.
    signal = SignalActor.remote()
    dep = one_dep.remote(None, signal=signal)
    check_refcounts({dep: (1, 0)})
    result = one_dep.remote(dep)
    check_refcounts({dep: (1, 1), result: (1, 0)})
    ray.get(signal.send.remote())
    # Reference count should be removed as soon as the dependency is inlined.
    check_refcounts({dep: (1, 0), result: (1, 0)})
    del dep, result
    check_refcounts({})

    # Test that spilled plasma dependency refcounts are decremented once
    # the task finishes.
    signal1, signal2 = SignalActor.remote(), SignalActor.remote()
    dep = one_dep_large.remote(None, signal=signal1)
    check_refcounts({dep: (1, 0)})
    result = one_dep.remote(dep, signal=signal2)
    check_refcounts({dep: (1, 1), result: (1, 0)})
    ray.get(signal1.send.remote())
    ray.get(dep, timeout=10)
    # Reference count should remain because the dependency is in plasma.
    check_refcounts({dep: (1, 1), result: (1, 0)})
    ray.get(signal2.send.remote())
    # Reference count should be removed because the task finished.
    check_refcounts({dep: (1, 0), result: (1, 0)})
    del dep, result
    check_refcounts({})

    # Test that regular plasma dependency refcounts are decremented if a task
    # fails.
    signal = SignalActor.remote()
    large_dep = ray.put(np.zeros(10 * 1024 * 1024, dtype=np.uint8))
    result = one_dep.remote(large_dep, signal=signal, fail=True)
    check_refcounts({large_dep: (1, 1), result: (1, 0)})
    ray.get(signal.send.remote())
    # Reference count should be removed once the task finishes.
    check_refcounts({large_dep: (1, 0), result: (1, 0)})
    del large_dep, result
    check_refcounts({})

    # Test that spilled plasma dependency refcounts are decremented if a task
    # fails.
    signal1, signal2 = SignalActor.remote(), SignalActor.remote()
    dep = one_dep_large.remote(None, signal=signal1)
    check_refcounts({dep: (1, 0)})
    result = one_dep.remote(dep, signal=signal2, fail=True)
    check_refcounts({dep: (1, 1), result: (1, 0)})
    ray.get(signal1.send.remote())
    ray.get(dep, timeout=10)
    # Reference count should remain because the dependency is in plasma.
    check_refcounts({dep: (1, 1), result: (1, 0)})
    ray.get(signal2.send.remote())
    # Reference count should be removed because the task finished.
    check_refcounts({dep: (1, 0), result: (1, 0)})
    del dep, result
    check_refcounts({})


def test_basic_pinning(one_cpu_100MiB_shared):
    @ray.remote
    def f(array):
        return np.sum(array)

    @ray.remote
    class Actor(object):
        def __init__(self):
            # Hold a long-lived reference to a ray.put object's ID. The object
            # should not be garbage collected while the actor is alive because
            # the object is pinned by the raylet.
            self.large_object = ray.put(np.zeros(25 * 1024 * 1024, dtype=np.uint8))

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()

    # Fill up the object store with short-lived objects. These should be
    # evicted before the long-lived object whose reference is held by
    # the actor.
    for batch in range(10):
        intermediate_result = f.remote(np.zeros(10 * 1024 * 1024, dtype=np.uint8))
        ray.get(intermediate_result)

    # The ray.get below would fail with only LRU eviction, as the object
    # that was ray.put by the actor would have been evicted.
    ray.get(actor.get_large_object.remote())


def test_pending_task_dependency_pinning(one_cpu_100MiB_shared):
    @ray.remote
    def pending(input1, input2):
        return

    # The object that is ray.put here will go out of scope immediately, so if
    # pending task dependencies aren't considered, it will be evicted before
    # the ray.get below due to the subsequent ray.puts that fill up the object
    # store.
    np_array = np.zeros(20 * 1024 * 1024, dtype=np.uint8)
    signal = SignalActor.remote()
    obj_ref = pending.remote(np_array, signal.wait.remote())

    for _ in range(2):
        ray.put(np.zeros(20 * 1024 * 1024, dtype=np.uint8))

    ray.get(signal.send.remote())
    ray.get(obj_ref)


# Remote function takes serialized reference and doesn't hold onto it after
# finishing. Referenced object shouldn't be evicted while the task is pending
# and should be evicted after it returns.
@pytest.mark.parametrize("use_ray_put", [False, True])
@pytest.mark.parametrize("failure", [False, True])
def test_basic_serialized_reference(one_cpu_100MiB_shared, use_ray_put, failure):
    @ray.remote(max_retries=0)
    def pending(ref, dep):
        ray.get(ref[0])
        if failure:
            os._exit(0)

    array_oid = put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)
    signal = SignalActor.remote()
    obj_ref = pending.remote([array_oid], signal.wait.remote())

    # Remove the local reference.
    array_oid_bytes = array_oid.binary()
    del array_oid

    # Check that the remote reference pins the object.
    _fill_object_store_and_get(array_oid_bytes)

    # Fulfill the dependency, causing the task to finish.
    ray.get(signal.send.remote())
    try:
        ray.get(obj_ref)
        assert not failure
    except ray.exceptions.WorkerCrashedError:
        assert failure

    # Reference should be gone, check that array gets evicted.
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Call a recursive chain of tasks that pass a serialized reference to the end
# of the chain. The reference should still exist while the final task in the
# chain is running and should be removed once it finishes.
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_recursive_serialized_reference(one_cpu_100MiB_shared, use_ray_put, failure):
    @ray.remote(max_retries=1)
    def recursive(ref, signal, max_depth, depth=0):
        ray.get(ref[0])
        if depth == max_depth:
            ray.get(signal.wait.remote())
            if failure:
                os._exit(0)
            return
        else:
            return recursive.remote(ref, signal, max_depth, depth + 1)

    signal = SignalActor.remote()

    max_depth = 5
    array_oid = put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)
    head_oid = recursive.remote([array_oid], signal, max_depth)

    # Remove the local reference.
    array_oid_bytes = array_oid.binary()
    del array_oid

    tail_oid = head_oid
    for _ in range(max_depth):
        tail_oid = ray.get(tail_oid)

    # Check that the remote reference pins the object.
    _fill_object_store_and_get(array_oid_bytes)

    # Fulfill the dependency, causing the tail task to finish.
    ray.get(signal.send.remote())
    try:
        assert ray.get(tail_oid) is None
        assert not failure
    except ray.exceptions.OwnerDiedError:
        # There is only 1 core, so the same worker will execute all `recursive`
        # tasks. Therefore, if we kill the worker during the last task, its
        # owner (the worker that executed the second-to-last task) will also
        # have died.
        assert failure

    # Reference should be gone, check that array gets evicted.
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that a passed reference held by an actor after the method finishes
# is kept until the reference is removed from the actor. Also tests giving
# the actor a duplicate reference to the same object ref.
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_actor_holding_serialized_reference(
    one_cpu_100MiB_shared, use_ray_put, failure
):
    @ray.remote
    class GreedyActor(object):
        def __init__(self):
            pass

        def set_ref1(self, ref):
            self.ref1 = ref

        def add_ref2(self, new_ref):
            self.ref2 = new_ref

        def delete_ref1(self):
            self.ref1 = None

        def delete_ref2(self):
            self.ref2 = None

    # Test that the reference held by the actor isn't evicted.
    array_oid = put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)
    actor = GreedyActor.remote()
    actor.set_ref1.remote([array_oid])

    # Test that giving the same actor a duplicate reference works.
    ray.get(actor.add_ref2.remote([array_oid]))

    # Remove the local reference.
    array_oid_bytes = array_oid.binary()
    del array_oid

    # Test that the remote references still pin the object.
    _fill_object_store_and_get(array_oid_bytes)

    # Test that removing only the first reference doesn't unpin the object.
    ray.get(actor.delete_ref1.remote())
    _fill_object_store_and_get(array_oid_bytes)

    if failure:
        # Test that the actor exiting stops the reference from being pinned.
        # Kill the actor and wait for the actor to exit.
        kill_actor_and_wait_for_failure(actor)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.delete_ref1.remote())
    else:
        # Test that deleting the second reference stops it from being pinned.
        ray.get(actor.delete_ref2.remote())
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that a passed reference held by an actor after a task finishes
# is kept until the reference is removed from the worker. Also tests giving
# the worker a duplicate reference to the same object ref.
@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_worker_holding_serialized_reference(
    one_cpu_100MiB_shared, use_ray_put, failure
):
    @ray.remote(max_retries=1)
    def child(dep1, dep2):
        if failure:
            os._exit(0)
        return

    @ray.remote
    class Submitter:
        def __init__(self):
            pass

        def launch_pending_task(self, ref, signal):
            return child.remote(ref[0], signal.wait.remote())

    signal = SignalActor.remote()

    # Test that the reference held by the actor isn't evicted.
    array_oid = put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)
    s = Submitter.remote()
    child_return_id = ray.get(s.launch_pending_task.remote([array_oid], signal))

    # Remove the local reference.
    array_oid_bytes = array_oid.binary()
    del array_oid

    # Test that the reference prevents the object from being evicted.
    _fill_object_store_and_get(array_oid_bytes)

    ray.get(signal.send.remote())
    try:
        ray.get(child_return_id)
        assert not failure
    except ray.exceptions.WorkerCrashedError:
        assert failure
    del child_return_id

    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that an object containing object refs within it pins the inner IDs.
def test_basic_nested_ids(one_cpu_100MiB_shared):
    inner_oid = ray.put(np.zeros(20 * 1024 * 1024, dtype=np.uint8))
    outer_oid = ray.put([inner_oid])

    # Remove the local reference to the inner object.
    inner_oid_bytes = inner_oid.binary()
    del inner_oid

    # Check that the outer reference pins the inner object.
    _fill_object_store_and_get(inner_oid_bytes)

    # Remove the outer reference and check that the inner object gets evicted.
    del outer_oid
    _fill_object_store_and_get(inner_oid_bytes, succeed=False)


# Test that a reference borrowed by an actor constructor is freed if the actor is
# cancelled before being scheduled.
def test_actor_constructor_borrow_cancellation(one_cpu_100MiB_shared):
    # Schedule the actor with a non-existent resource so it's guaranteed to never be
    # scheduled.
    @ray.remote(resources={"nonexistent_resource": 1})
    class Actor:
        def __init__(self, obj_containing_ref):
            raise ValueError(
                "The actor constructor should not be reached; the actor creation task "
                "should be cancelled before the actor is scheduled."
            )

        def should_not_be_run(self):
            raise ValueError("This method should never be reached.")

    # Test with implicit cancellation by letting the actor handle go out-of-scope.
    def test_implicit_cancel():
        ref = ray.put(1)
        print(Actor.remote({"foo": ref}))

    test_implicit_cancel()

    # Confirm that the ref object is not leaked.
    check_refcounts({})

    # Test with explicit cancellation via ray.kill().
    ref = ray.put(1)
    a = Actor.remote({"foo": ref})
    ray.kill(a)
    del ref

    # Confirm that the ref object is not leaked.
    check_refcounts({})

    # Check that actor death cause is propagated.
    with pytest.raises(
        ray.exceptions.RayActorError, match="it was killed by `ray.kill"
    ) as exc_info:
        ray.get(a.should_not_be_run.remote())
    print(exc_info._excinfo[1])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
