# coding: utf-8
import logging
import os
import sys

import numpy as np
import pytest

import ray
import ray.cluster_utils
from ray._private.test_utils import (
    SignalActor,
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
            lambda: not ray._private.worker.global_worker.core_worker.object_exists(obj)
        )


def test_captured_object_ref(one_worker_100MiB):
    captured_id = ray.put(np.zeros(10 * 1024 * 1024, dtype=np.uint8))

    @ray.remote
    def f(signal):
        ray.get(signal.wait.remote())
        ray.get(captured_id)  # noqa: F821

    signal = SignalActor.remote()
    obj_ref = f.remote(signal)

    # Delete local references.
    del f
    del captured_id

    # Test that the captured object ref is pinned despite having no local
    # references.
    ray.get(signal.send.remote())
    _fill_object_store_and_get(obj_ref)

    captured_id = ray.put(np.zeros(10 * 1024 * 1024, dtype=np.uint8))

    @ray.remote
    class Actor:
        def get(self, signal):
            ray.get(signal.wait.remote())
            ray.get(captured_id)  # noqa: F821

    signal = SignalActor.remote()
    actor = Actor.remote()
    obj_ref = actor.get.remote(signal)

    # Delete local references.
    del Actor
    del captured_id

    # Test that the captured object ref is pinned despite having no local
    # references.
    ray.get(signal.send.remote())
    _fill_object_store_and_get(obj_ref)


# Remote function takes serialized reference and doesn't hold onto it after
# finishing. Referenced object shouldn't be evicted while the task is pending
# and should be evicted after it returns.
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_basic_serialized_reference(one_worker_100MiB, use_ray_put, failure):
    @ray.remote(max_retries=1)
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
def test_recursive_serialized_reference(one_worker_100MiB, use_ray_put, failure):
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
def test_actor_holding_serialized_reference(one_worker_100MiB, use_ray_put, failure):
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
def test_worker_holding_serialized_reference(one_worker_100MiB, use_ray_put, failure):
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
def test_basic_nested_ids(one_worker_100MiB):
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


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
