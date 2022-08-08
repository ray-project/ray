# coding: utf-8
import copy
import logging
import os
import sys
import time

import numpy as np
import pytest

import ray
import ray._private.gcs_utils as gcs_utils
import ray.cluster_utils
from ray._private.test_utils import (
    SignalActor,
    convert_actor_state,
    kill_actor_and_wait_for_failure,
    put_object,
    wait_for_condition,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def one_worker_100MiB(request):
    # It has lots of tests that don't require object spilling.
    config = {"task_retry_delay_ms": 0, "automatic_object_spilling_enabled": False}
    yield ray.init(
        num_cpus=1, object_store_memory=100 * 1024 * 1024, _system_config=config
    )
    ray.shutdown()


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
            _check_refcounts(expected)
            break
        except AssertionError as e:
            if time.time() - start > timeout:
                raise e
            else:
                time.sleep(0.1)


def test_local_refcounts(ray_start_regular):
    obj_ref1 = ray.put(None)
    check_refcounts({obj_ref1: (1, 0)})
    obj_ref1_copy = copy.copy(obj_ref1)
    check_refcounts({obj_ref1: (2, 0)})
    del obj_ref1
    check_refcounts({obj_ref1_copy: (1, 0)})
    del obj_ref1_copy
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


def test_pending_task_dependency_pinning(one_worker_100MiB):
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


def test_feature_flag(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    def f(array):
        return np.sum(array)

    @ray.remote
    class Actor(object):
        def __init__(self):
            self.large_object = ray.put(np.zeros(25 * 1024 * 1024, dtype=np.uint8))

        def wait_for_actor_to_start(self):
            pass

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()
    ray.get(actor.wait_for_actor_to_start.remote())

    # The ray.get below fails with only LRU eviction, as the object
    # that was ray.put by the actor should have been evicted.
    ref = actor.get_large_object.remote()
    ray.get(ref)

    # Keep refs in scope so that they don't get GCed immediately.
    for _ in range(5):
        put_ref = ray.put(np.zeros(40 * 1024 * 1024, dtype=np.uint8))
    del put_ref

    wait_for_condition(
        lambda: not ray._private.worker.global_worker.core_worker.object_exists(ref)
    )


def test_out_of_band_serialized_object_ref(one_worker_100MiB):
    assert (
        len(ray._private.worker.global_worker.core_worker.get_all_reference_counts())
        == 0
    )
    obj_ref = ray.put("hello")
    _check_refcounts({obj_ref: (1, 0)})
    obj_ref_str = ray.cloudpickle.dumps(obj_ref)
    _check_refcounts({obj_ref: (2, 0)})
    del obj_ref
    assert (
        len(ray._private.worker.global_worker.core_worker.get_all_reference_counts())
        == 1
    )
    assert ray.get(ray.cloudpickle.loads(obj_ref_str)) == "hello"


# Test that a reference borrowed by an actor constructor is freed if the actor is
# cancelled before being scheduled.
def test_actor_constructor_borrow_cancellation(ray_start_regular):
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
        Actor.remote({"foo": ref})

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


def _all_actors_dead():
    return all(
        actor["State"] == convert_actor_state(gcs_utils.ActorTableData.DEAD)
        for actor in list(ray._private.state.actors().values())
    )


def test_kill_actor_immediately_after_creation(ray_start_regular):
    @ray.remote
    class A:
        pass

    a = A.remote()
    b = A.remote()

    ray.kill(a)
    ray.kill(b)
    wait_for_condition(_all_actors_dead, timeout=10)


def test_remove_actor_immediately_after_creation(ray_start_regular):
    @ray.remote
    class A:
        pass

    a = A.remote()
    b = A.remote()

    del a
    del b
    wait_for_condition(_all_actors_dead, timeout=10)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
