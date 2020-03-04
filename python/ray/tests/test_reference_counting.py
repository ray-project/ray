# coding: utf-8
import copy
import json
import logging
import gc
import time
import weakref

import numpy as np

import pytest

import ray
import ray.cluster_utils
from ray.test_utils import SignalActor, wait_for_condition
from ray.internal.internal_api import global_gc

logger = logging.getLogger(__name__)


@pytest.fixture
def one_worker_100MiB(request):
    config = json.dumps({
        "distributed_ref_counting_enabled": 1,
        "object_store_full_max_retries": 1,
    })
    yield ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _internal_config=config)
    ray.shutdown()


def _fill_object_store_and_get(oid, succeed=True, object_MiB=40,
                               num_objects=5):
    for _ in range(num_objects):
        ray.put(np.zeros(object_MiB * 1024 * 1024, dtype=np.uint8))

    if type(oid) is bytes:
        oid = ray.ObjectID(oid)

    if succeed:
        ray.get(oid)
    else:
        if oid.is_direct_call_type():
            with pytest.raises(ray.exceptions.RayTimeoutError):
                ray.get(oid, timeout=0.1)
        else:
            with pytest.raises(ray.exceptions.UnreconstructableError):
                ray.get(oid)


def _check_refcounts(expected):
    actual = ray.worker.global_worker.core_worker.get_all_reference_counts()
    assert len(expected) == len(actual)
    for object_id, (local, submitted) in expected.items():
        hex_id = object_id.hex().encode("ascii")
        assert hex_id in actual
        assert local == actual[hex_id]["local"]
        assert submitted == actual[hex_id]["submitted"]


def check_refcounts(expected, timeout=10):
    start = time.time()
    while True:
        try:
            _check_refcounts(expected)
            break
        except AssertionError as e:
            if time.time() - start > timeout:
                raise e
            else:
                time.sleep(0.1)


def test_global_gc(shutdown_only):
    cluster = ray.cluster_utils.Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=1, num_gpus=0)
    ray.init(address=cluster.address)

    class ObjectWithCyclicRef:
        def __init__(self):
            self.loop = self

    @ray.remote(num_cpus=1)
    class GarbageHolder:
        def __init__(self):
            gc.disable()
            x = ObjectWithCyclicRef()
            self.garbage = weakref.ref(x)

        def has_garbage(self):
            return self.garbage() is not None

    try:
        gc.disable()

        # Local driver.
        local_ref = weakref.ref(ObjectWithCyclicRef())

        # Remote workers.
        actors = [GarbageHolder.remote() for _ in range(2)]
        assert local_ref() is not None
        assert all(ray.get([a.has_garbage.remote() for a in actors]))

        # GC should be triggered for all workers, including the local driver.
        global_gc()

        def check_refs_gced():
            return (local_ref() is None and
                    not any(ray.get([a.has_garbage.remote() for a in actors])))

        wait_for_condition(check_refs_gced, timeout_ms=10000)
    finally:
        gc.enable()


def test_global_gc_when_full(shutdown_only):
    cluster = ray.cluster_utils.Cluster()
    for _ in range(2):
        cluster.add_node(
            num_cpus=1, num_gpus=0, object_store_memory=100 * 1024 * 1024)
    ray.init(address=cluster.address)

    class LargeObjectWithCyclicRef:
        def __init__(self):
            self.loop = self
            self.large_object = ray.put(
                np.zeros(40 * 1024 * 1024, dtype=np.uint8))

    @ray.remote(num_cpus=1)
    class GarbageHolder:
        def __init__(self):
            gc.disable()
            x = LargeObjectWithCyclicRef()
            self.garbage = weakref.ref(x)

        def has_garbage(self):
            return self.garbage() is not None

        def return_large_array(self):
            return np.zeros(80 * 1024 * 1024, dtype=np.uint8)

    try:
        gc.disable()

        # Local driver.
        local_ref = weakref.ref(LargeObjectWithCyclicRef())

        # Remote workers.
        actors = [GarbageHolder.remote() for _ in range(2)]
        assert local_ref() is not None
        assert all(ray.get([a.has_garbage.remote() for a in actors]))

        # GC should be triggered for all workers, including the local driver,
        # when the driver tries to ray.put a value that doesn't fit in the
        # object store. This should cause the captured ObjectIDs' numpy arrays
        # to be evicted.
        ray.put(np.zeros(80 * 1024 * 1024, dtype=np.uint8))

        def check_refs_gced():
            return (local_ref() is None and
                    not any(ray.get([a.has_garbage.remote() for a in actors])))

        wait_for_condition(check_refs_gced, timeout_ms=10000)

        # Local driver.
        local_ref = weakref.ref(LargeObjectWithCyclicRef())

        # Remote workers.
        actors = [GarbageHolder.remote() for _ in range(2)]

        def check_refs_gced():
            return (local_ref() is None and
                    not any(ray.get([a.has_garbage.remote() for a in actors])))

        wait_for_condition(check_refs_gced, timeout_ms=10000)

        # GC should be triggered for all workers, including the local driver,
        # when a remote task tries to put a return value that doesn't fit in
        # the object store. This should cause the captured ObjectIDs' numpy
        # arrays to be evicted.
        ray.get(actors[0].return_large_array.remote())
        assert local_ref() is None
        assert not any(ray.get([a.has_garbage.remote() for a in actors]))
    finally:
        gc.enable()


def test_local_refcounts(ray_start_regular):
    oid1 = ray.put(None)
    check_refcounts({oid1: (1, 0)})
    oid1_copy = copy.copy(oid1)
    check_refcounts({oid1: (2, 0)})
    del oid1
    check_refcounts({oid1_copy: (1, 0)})
    del oid1_copy
    check_refcounts({})


def test_dependency_refcounts(ray_start_regular):
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


def test_basic_pinning(one_worker_100MiB):
    @ray.remote
    def f(array):
        return np.sum(array)

    @ray.remote
    class Actor(object):
        def __init__(self):
            # Hold a long-lived reference to a ray.put object's ID. The object
            # should not be garbage collected while the actor is alive because
            # the object is pinned by the raylet.
            self.large_object = ray.put(
                np.zeros(25 * 1024 * 1024, dtype=np.uint8))

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()

    # Fill up the object store with short-lived objects. These should be
    # evicted before the long-lived object whose reference is held by
    # the actor.
    for batch in range(10):
        intermediate_result = f.remote(
            np.zeros(10 * 1024 * 1024, dtype=np.uint8))
        ray.get(intermediate_result)

    # The ray.get below would fail with only LRU eviction, as the object
    # that was ray.put by the actor would have been evicted.
    ray.get(actor.get_large_object.remote())


def test_pending_task_dependency_pinning(one_worker_100MiB):
    @ray.remote
    def pending(input1, input2):
        return

    # The object that is ray.put here will go out of scope immediately, so if
    # pending task dependencies aren't considered, it will be evicted before
    # the ray.get below due to the subsequent ray.puts that fill up the object
    # store.
    np_array = np.zeros(40 * 1024 * 1024, dtype=np.uint8)
    signal = SignalActor.remote()
    oid = pending.remote(np_array, signal.wait.remote())

    for _ in range(2):
        ray.put(np.zeros(40 * 1024 * 1024, dtype=np.uint8))

    ray.get(signal.send.remote())
    ray.get(oid)


def test_feature_flag(shutdown_only):
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _internal_config=json.dumps({
            "object_pinning_enabled": 0
        }))

    @ray.remote
    def f(array):
        return np.sum(array)

    @ray.remote
    class Actor(object):
        def __init__(self):
            self.large_object = ray.put(
                np.zeros(25 * 1024 * 1024, dtype=np.uint8))

        def wait_for_actor_to_start(self):
            pass

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()
    ray.get(actor.wait_for_actor_to_start.remote())

    # The ray.get below fails with only LRU eviction, as the object
    # that was ray.put by the actor should have been evicted.
    _fill_object_store_and_get(actor.get_large_object.remote(), succeed=False)


# Remote function takes serialized reference and doesn't hold onto it after
# finishing. Referenced object shouldn't be evicted while the task is pending
# and should be evicted after it returns.
def test_basic_serialized_reference(one_worker_100MiB):
    @ray.remote
    def pending(ref, dep):
        ray.get(ref[0])

    # TODO(edoakes): currently these tests don't work with ray.put() so we need
    # to return from a task like this instead. Once that is fixed, should have
    # tests run with both codepaths.
    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    array_oid = put.remote()
    signal = SignalActor.remote()
    oid = pending.remote([array_oid], signal.wait.remote())

    # Remove the local reference.
    array_oid_bytes = array_oid.binary()
    del array_oid

    # Check that the remote reference pins the object.
    _fill_object_store_and_get(array_oid_bytes)

    # Fulfill the dependency, causing the task to finish.
    ray.get(signal.send.remote())
    ray.get(oid)

    # Reference should be gone, check that array gets evicted.
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Call a recursive chain of tasks that pass a serialized reference to the end
# of the chain. The reference should still exist while the final task in the
# chain is running and should be removed once it finishes.
def test_recursive_serialized_reference(one_worker_100MiB):
    @ray.remote
    def recursive(ref, signal, max_depth, depth=0):
        ray.get(ref[0])
        if depth == max_depth:
            return ray.get(signal.wait.remote())
        else:
            return recursive.remote(ref, signal, max_depth, depth + 1)

    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    signal = SignalActor.remote()

    max_depth = 5
    array_oid = put.remote()
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
    assert ray.get(tail_oid) is None

    # Reference should be gone, check that array gets evicted.
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that a passed reference held by an actor after the method finishes
# is kept until the reference is removed from the actor. Also tests giving
# the actor a duplicate reference to the same object ID.
def test_actor_holding_serialized_reference(one_worker_100MiB):
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

    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    # Test that the reference held by the actor isn't evicted.
    array_oid = put.remote()
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

    # Test that deleting the second reference stops it from being pinned.
    ray.get(actor.delete_ref2.remote())
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that a passed reference held by an actor after a task finishes
# is kept until the reference is removed from the worker. Also tests giving
# the worker a duplicate reference to the same object ID.
def test_worker_holding_serialized_reference(one_worker_100MiB):
    @ray.remote
    def child(dep1, dep2):
        return

    @ray.remote
    def launch_pending_task(ref, signal):
        return child.remote(ref[0], signal.wait.remote())

    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    signal = SignalActor.remote()

    # Test that the reference held by the actor isn't evicted.
    array_oid = put.remote()
    child_return_id = ray.get(launch_pending_task.remote([array_oid], signal))

    # Remove the local reference.
    array_oid_bytes = array_oid.binary()
    del array_oid

    # Test that the reference prevents the object from being evicted.
    _fill_object_store_and_get(array_oid_bytes)

    ray.get(signal.send.remote())
    ray.get(child_return_id)
    del child_return_id

    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that an object containing object IDs within it pins the inner IDs.
def test_basic_nested_ids(one_worker_100MiB):
    inner_oid = ray.put(np.zeros(40 * 1024 * 1024, dtype=np.uint8))
    outer_oid = ray.put([inner_oid])

    # Remove the local reference to the inner object.
    inner_oid_bytes = inner_oid.binary()
    del inner_oid

    # Check that the outer reference pins the inner object.
    _fill_object_store_and_get(inner_oid_bytes)

    # Remove the outer reference and check that the inner object gets evicted.
    del outer_oid
    _fill_object_store_and_get(inner_oid_bytes, succeed=False)


# Test that an object containing object IDs within it pins the inner IDs
# recursively and for submitted tasks.
def test_recursively_nest_ids(one_worker_100MiB):
    @ray.remote
    def recursive(ref, signal, max_depth, depth=0):
        unwrapped = ray.get(ref[0])
        if depth == max_depth:
            return ray.get(signal.wait.remote())
        else:
            return recursive.remote(unwrapped, signal, max_depth, depth + 1)

    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    signal = SignalActor.remote()

    max_depth = 5
    array_oid = put.remote()
    nested_oid = array_oid
    for _ in range(max_depth):
        nested_oid = ray.put([nested_oid])
    head_oid = recursive.remote([nested_oid], signal, max_depth)

    # Remove the local reference.
    array_oid_bytes = array_oid.binary()
    del array_oid, nested_oid

    tail_oid = head_oid
    for _ in range(max_depth):
        tail_oid = ray.get(tail_oid)

    # Check that the remote reference pins the object.
    _fill_object_store_and_get(array_oid_bytes)

    # Fulfill the dependency, causing the tail task to finish.
    ray.get(signal.send.remote())
    ray.get(tail_oid)

    # Reference should be gone, check that array gets evicted.
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that serialized objectIDs returned from remote tasks are pinned until
# they go out of scope on the caller side.
def test_return_object_id(one_worker_100MiB):
    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    @ray.remote
    def return_an_id():
        return [put.remote()]

    outer_oid = return_an_id.remote()
    inner_oid_binary = ray.get(outer_oid)[0].binary()

    # Check that the inner ID is pinned by the outer ID.
    _fill_object_store_and_get(inner_oid_binary)

    # Check that taking a reference to the inner ID and removing the outer ID
    # doesn't unpin the object.
    inner_oid = ray.get(outer_oid)[0]
    del outer_oid
    _fill_object_store_and_get(inner_oid_binary)

    # Check that removing the inner ID unpins the object.
    del inner_oid
    _fill_object_store_and_get(inner_oid_binary, succeed=False)


# Test that serialized objectIDs returned from remote tasks are pinned if
# passed into another remote task by the caller.
def test_pass_returned_object_id(one_worker_100MiB):
    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    @ray.remote
    def return_an_id():
        return [put.remote()]

    @ray.remote
    def pending(ref, signal):
        ray.get(signal.wait.remote())
        ray.get(ref[0])

    signal = SignalActor.remote()
    outer_oid = return_an_id.remote()
    inner_oid_binary = ray.get(outer_oid)[0].binary()
    pending_oid = pending.remote([outer_oid], signal)

    # Remove the local reference to the returned ID.
    del outer_oid

    # Check that the inner ID is pinned by the remote task ID.
    _fill_object_store_and_get(inner_oid_binary)

    # Check that the task finishing unpins the object.
    ray.get(signal.send.remote())
    ray.get(pending_oid)
    _fill_object_store_and_get(inner_oid_binary, succeed=False)


# Call a recursive chain of tasks that pass a serialized reference that was
# returned by another task to the end of the chain. The reference should still
# exist while the final task in the chain is running and should be removed once
# it finishes.
def test_recursively_pass_returned_object_id(one_worker_100MiB):
    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    @ray.remote
    def return_an_id():
        return [put.remote()]

    @ray.remote
    def recursive(ref, signal, max_depth, depth=0):
        ray.get(ref[0])
        if depth == max_depth:
            return ray.get(signal.wait.remote())
        else:
            return recursive.remote(ref, signal, max_depth, depth + 1)

    max_depth = 5
    outer_oid = return_an_id.remote()
    inner_oid_bytes = ray.get(outer_oid)[0].binary()
    signal = SignalActor.remote()
    head_oid = recursive.remote([outer_oid], signal, max_depth)

    # Remove the local reference.
    del outer_oid

    tail_oid = head_oid
    for _ in range(max_depth):
        tail_oid = ray.get(tail_oid)

    # Check that the remote reference pins the object.
    _fill_object_store_and_get(inner_oid_bytes)

    # Fulfill the dependency, causing the tail task to finish.
    ray.get(signal.send.remote())
    ray.get(tail_oid)

    # Reference should be gone, check that returned ID gets evicted.
    _fill_object_store_and_get(inner_oid_bytes, succeed=False)


# Call a recursive chain of tasks. The final task in the chain returns an
# ObjectID returned by a task that it submitted. Every other task in the chain
# returns the same ObjectID by calling ray.get() on its submitted task and
# returning the result. The reference should still exist while the driver has a
# reference to the final task's ObjectID.
def test_recursively_return_borrowed_object_id(one_worker_100MiB):
    @ray.remote
    def put():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    @ray.remote
    def recursive(num_tasks_left):
        if num_tasks_left == 0:
            return put.remote()

        final_id = ray.get(recursive.remote(num_tasks_left - 1))
        ray.get(final_id)
        return final_id

    max_depth = 5
    head_oid = recursive.remote(max_depth)
    final_oid = ray.get(head_oid)
    final_oid_bytes = final_oid.binary()

    # Check that the driver's reference pins the object.
    _fill_object_store_and_get(final_oid_bytes)

    # Remove the local reference and try it again.
    final_oid = ray.get(head_oid)
    _fill_object_store_and_get(final_oid_bytes)

    # Remove all references.
    del head_oid
    del final_oid
    # Reference should be gone, check that returned ID gets evicted.
    _fill_object_store_and_get(final_oid_bytes, succeed=False)


def test_out_of_band_serialized_object_id(one_worker_100MiB):
    assert len(
        ray.worker.global_worker.core_worker.get_all_reference_counts()) == 0
    oid = ray.put("hello")
    _check_refcounts({oid: (1, 0)})
    oid_str = ray.cloudpickle.dumps(oid)
    _check_refcounts({oid: (2, 0)})
    del oid
    assert len(
        ray.worker.global_worker.core_worker.get_all_reference_counts()) == 1
    assert ray.get(ray.cloudpickle.loads(oid_str)) == "hello"


def test_captured_object_id(one_worker_100MiB):
    captured_id = ray.put(np.zeros(10 * 1024 * 1024, dtype=np.uint8))

    @ray.remote
    def f(signal):
        ray.get(signal.wait.remote())
        ray.get(captured_id)  # noqa: F821

    signal = SignalActor.remote()
    oid = f.remote(signal)

    # Delete local references.
    del f
    del captured_id

    # Test that the captured object ID is pinned despite having no local
    # references.
    ray.get(signal.send.remote())
    _fill_object_store_and_get(oid)

    captured_id = ray.put(np.zeros(10 * 1024 * 1024, dtype=np.uint8))

    @ray.remote
    class Actor:
        def get(self, signal):
            ray.get(signal.wait.remote())
            ray.get(captured_id)  # noqa: F821

    signal = SignalActor.remote()
    actor = Actor.remote()
    oid = actor.get.remote(signal)

    # Delete local references.
    del Actor
    del captured_id

    # Test that the captured object ID is pinned despite having no local
    # references.
    ray.get(signal.send.remote())
    _fill_object_store_and_get(oid)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
