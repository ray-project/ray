# coding: utf-8
import logging
import os
import copy
import platform
import random
import signal
import sys
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
from ray._private.internal_api import memory_summary
from ray._private.test_utils import (
    SignalActor,
    put_object,
    wait_for_condition,
    wait_for_num_actors,
)
import ray._private.gcs_utils as gcs_utils

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM

logger = logging.getLogger(__name__)


@pytest.fixture
def one_worker_100MiB(request):
    config = {
        "task_retry_delay_ms": 0,
        "object_timeout_milliseconds": 1000,
        "automatic_object_spilling_enabled": False,
    }
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


# Test that an object containing object refs within it pins the inner IDs
# recursively and for submitted tasks.
@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_recursively_nest_ids(one_worker_100MiB, use_ray_put, failure):
    @ray.remote(max_retries=1)
    def recursive(ref, signal, max_depth, depth=0):
        unwrapped = ray.get(ref[0])
        if depth == max_depth:
            ray.get(signal.wait.remote())
            if failure:
                os._exit(0)
            return
        else:
            return recursive.remote(unwrapped, signal, max_depth, depth + 1)

    signal = SignalActor.remote()

    max_depth = 5
    array_oid = put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)
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
    if not failure:
        ray.get(tail_oid)
    else:
        # There is only 1 core, so the same worker will execute all `recursive`
        # tasks. Therefore, if we kill the worker during the last task, its
        # owner (the worker that executed the second-to-last task) will also
        # have died.
        with pytest.raises(ray.exceptions.OwnerDiedError):
            ray.get(tail_oid)

    # Reference should be gone, check that array gets evicted.
    _fill_object_store_and_get(array_oid_bytes, succeed=False)


# Test that serialized ObjectRefs returned from remote tasks are pinned until
# they go out of scope on the caller side.
@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_return_object_ref(one_worker_100MiB, use_ray_put, failure):
    @ray.remote
    def return_an_id():
        return [put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)]

    @ray.remote(max_retries=1)
    def exit():
        os._exit(0)

    outer_oid = return_an_id.remote()
    inner_oid_binary = ray.get(outer_oid)[0].binary()

    # Check that taking a reference to the inner ID and removing the outer ID
    # doesn't unpin the object.
    inner_oid = ray.get(outer_oid)[0]  # noqa: F841
    del outer_oid
    _fill_object_store_and_get(inner_oid_binary)

    if failure:
        # Check that the owner dying unpins the object. This should execute on
        # the same worker because there is only one started and the other tasks
        # have finished.
        with pytest.raises(ray.exceptions.WorkerCrashedError):
            ray.get(exit.remote())
    else:
        # Check that removing the inner ID unpins the object.
        del inner_oid
    _fill_object_store_and_get(inner_oid_binary, succeed=False)


# Test that serialized ObjectRefs returned from remote tasks are pinned if
# passed into another remote task by the caller.
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_pass_returned_object_ref(one_worker_100MiB, use_ray_put, failure):
    @ray.remote
    def return_an_id():
        return [put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)]

    # TODO(edoakes): this fails with an ActorError with max_retries=1.
    @ray.remote(max_retries=0)
    def pending(ref, signal):
        ray.get(signal.wait.remote())
        ray.get(ref[0])
        if failure:
            os._exit(0)

    signal = SignalActor.remote()
    outer_oid = return_an_id.remote()
    inner_oid_binary = ray.get(outer_oid)[0].binary()
    pending_oid = pending.remote([outer_oid], signal)

    # Remove the local reference to the returned ID.
    del outer_oid

    # Check that the inner ID is pinned by the remote task ID and finishing
    # the task unpins the object.
    ray.get(signal.send.remote())
    try:
        # Should succeed because inner_oid is pinned if no failure.
        ray.get(pending_oid)
        assert not failure
    except ray.exceptions.WorkerCrashedError:
        assert failure

    def ref_not_exists():
        worker = ray._private.worker.global_worker
        inner_oid = ray.ObjectRef(inner_oid_binary)
        return not worker.core_worker.object_exists(inner_oid)

    wait_for_condition(ref_not_exists)


# Call a recursive chain of tasks that pass a serialized reference that was
# returned by another task to the end of the chain. The reference should still
# exist while the final task in the chain is running and should be removed once
# it finishes.
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_recursively_pass_returned_object_ref(one_worker_100MiB, use_ray_put, failure):
    @ray.remote
    def return_an_id():
        return put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put)

    @ray.remote(max_retries=1)
    def recursive(ref, signal, max_depth, depth=0):
        inner_id = ray.get(ref[0])
        if depth == max_depth:
            ray.get(signal.wait.remote())
            if failure:
                os._exit(0)
            return inner_id
        else:
            return inner_id, recursive.remote(ref, signal, max_depth, depth + 1)

    max_depth = 5
    outer_oid = return_an_id.remote()
    signal = SignalActor.remote()
    head_oid = recursive.remote([outer_oid], signal, max_depth)

    # Remove the local reference.
    inner_oid = None
    outer_oid = head_oid
    for i in range(max_depth):
        inner_oid, outer_oid = ray.get(outer_oid)

    # Check that the remote reference pins the object.
    _fill_object_store_and_get(outer_oid, succeed=False)

    # Fulfill the dependency, causing the tail task to finish.
    ray.get(signal.send.remote())

    try:
        # Check that the remote reference pins the object.
        ray.get(outer_oid)
        _fill_object_store_and_get(inner_oid)
        assert not failure
    except ray.exceptions.OwnerDiedError:
        # There is only 1 core, so the same worker will execute all `recursive`
        # tasks. Therefore, if we kill the worker during the last task, its
        # owner (the worker that executed the second-to-last task) will also
        # have died.
        assert failure

    inner_oid_bytes = inner_oid.binary()
    del inner_oid
    del head_oid
    del outer_oid

    # Reference should be gone, check that returned ID gets evicted.
    _fill_object_store_and_get(inner_oid_bytes, succeed=False)


# Call a recursive chain of tasks. The final task in the chain returns an
# ObjectRef returned by a task that it submitted. Every other task in the chain
# returns the same ObjectRef by calling ray.get() on its submitted task and
# returning the result. The reference should still exist while the driver has a
# reference to the final task's ObjectRef.
@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "use_ray_put,failure", [(False, False), (False, True), (True, False), (True, True)]
)
def test_recursively_return_borrowed_object_ref(
    one_worker_100MiB, use_ray_put, failure
):
    @ray.remote
    def recursive(num_tasks_left):
        if num_tasks_left == 0:
            return (
                put_object(np.zeros(20 * 1024 * 1024, dtype=np.uint8), use_ray_put),
                os.getpid(),
            )

        return ray.get(recursive.remote(num_tasks_left - 1))

    max_depth = 5
    head_oid = recursive.remote(max_depth)
    final_oid, owner_pid = ray.get(head_oid)
    final_oid_bytes = final_oid.binary()

    # Check that the driver's reference pins the object.
    _fill_object_store_and_get(final_oid_bytes)

    # Remove the local reference and try it again.
    _fill_object_store_and_get(final_oid_bytes)

    if failure:
        os.kill(owner_pid, SIGKILL)
    else:
        # Remove all references.
        del head_oid
        del final_oid

    # Reference should be gone, check that returned ID gets evicted.
    _fill_object_store_and_get(final_oid_bytes, succeed=False)

    if failure:
        with pytest.raises(ray.exceptions.OwnerDiedError):
            ray.get(final_oid)


@pytest.mark.parametrize("failure", [False, True])
def test_borrowed_id_failure(one_worker_100MiB, failure):
    @ray.remote
    class Parent:
        def __init__(self):
            pass

        def pass_ref(self, ref, borrower):
            self.ref = ref[0]
            ray.get(borrower.receive_ref.remote(ref))
            if failure:
                sys.exit(-1)

        def ping(self):
            return

    @ray.remote
    class Borrower:
        def __init__(self):
            self.ref = None

        def receive_ref(self, ref):
            self.ref = ref[0]

        def resolve_ref(self):
            assert self.ref is not None
            if failure:
                with pytest.raises(ray.exceptions.ReferenceCountingAssertionError):
                    ray.get(self.ref)
            else:
                ray.get(self.ref)

        def ping(self):
            return

    parent = Parent.remote()
    borrower = Borrower.remote()
    ray.get(borrower.ping.remote())

    obj = ray.put(np.zeros(20 * 1024 * 1024, dtype=np.uint8))
    if failure:
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(parent.pass_ref.remote([obj], borrower))
    else:
        ray.get(parent.pass_ref.remote([obj], borrower))
    obj_bytes = obj.binary()
    del obj

    _fill_object_store_and_get(obj_bytes, succeed=not failure)
    # The borrower should not hang when trying to get the object's value.
    ray.get(borrower.resolve_ref.remote())


@pytest.mark.skipif(platform.system() in ["Windows"], reason="Failing on Windows.")
def test_object_unpin(ray_start_cluster):
    nodes = []
    cluster = ray_start_cluster
    head_node = cluster.add_node(
        num_cpus=0,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "subscriber_timeout_ms": 100,
            "health_check_initial_delay_ms": 0,
            "health_check_period_ms": 1000,
            "health_check_failure_threshold": 5,
        },
    )
    ray.init(address=cluster.address)

    # Add worker nodes.
    for i in range(2):
        nodes.append(
            cluster.add_node(
                num_cpus=1,
                resources={f"node_{i}": 1},
                object_store_memory=100 * 1024 * 1024,
            )
        )
    cluster.wait_for_nodes()

    one_mb_array = np.ones(1 * 1024 * 1024, dtype=np.uint8)
    ten_mb_array = np.ones(10 * 1024 * 1024, dtype=np.uint8)

    @ray.remote
    class ObjectsHolder:
        def __init__(self):
            self.ten_mb_objs = []
            self.one_mb_objs = []

        def put_10_mb(self):
            self.ten_mb_objs.append(ray.put(ten_mb_array))

        def put_1_mb(self):
            self.one_mb_objs.append(ray.put(one_mb_array))

        def pop_10_mb(self):
            if len(self.ten_mb_objs) == 0:
                return False
            self.ten_mb_objs.pop()
            return True

        def pop_1_mb(self):
            if len(self.one_mb_objs) == 0:
                return False
            self.one_mb_objs.pop()
            return True

    # Head node contains 11MB of data.
    one_mb_arrays = []
    ten_mb_arrays = []

    one_mb_arrays.append(ray.put(one_mb_array))
    ten_mb_arrays.append(ray.put(ten_mb_array))

    def check_memory(mb):
        return f"Plasma memory usage {mb} MiB" in memory_summary(
            address=head_node.address, stats_only=True
        )

    def wait_until_node_dead(node):
        for n in ray.nodes():
            if n["ObjectStoreSocketName"] == node.address_info["object_store_address"]:
                return not n["Alive"]
        return False

    wait_for_condition(lambda: check_memory(11))

    # Pop one mb array and see if it works.
    one_mb_arrays.pop()
    wait_for_condition(lambda: check_memory(10))

    # Pop 10 MB.
    ten_mb_arrays.pop()
    wait_for_condition(lambda: check_memory(0))

    # Put 11 MB for each actor.
    # actor 1: 1MB + 10MB
    # actor 2: 1MB + 10MB
    actor_on_node_1 = ObjectsHolder.options(resources={"node_0": 1}).remote()
    actor_on_node_2 = ObjectsHolder.options(resources={"node_1": 1}).remote()
    ray.get(actor_on_node_1.put_1_mb.remote())
    ray.get(actor_on_node_1.put_10_mb.remote())
    ray.get(actor_on_node_2.put_1_mb.remote())
    ray.get(actor_on_node_2.put_10_mb.remote())
    wait_for_condition(lambda: check_memory(22))

    # actor 1: 10MB
    # actor 2: 1MB
    ray.get(actor_on_node_1.pop_1_mb.remote())
    ray.get(actor_on_node_2.pop_10_mb.remote())
    wait_for_condition(lambda: check_memory(11))

    # The second node is dead, and actor 2 is dead.
    cluster.remove_node(nodes[1], allow_graceful=False)
    wait_for_condition(lambda: wait_until_node_dead(nodes[1]))
    wait_for_condition(lambda: check_memory(10))

    # The first actor is dead, so object should be GC'ed.
    ray.kill(actor_on_node_1)
    wait_for_condition(lambda: check_memory(0))


@pytest.mark.skipif(platform.system() in ["Windows"], reason="Failing on Windows.")
def test_object_unpin_stress(ray_start_cluster):
    nodes = []
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=1, resources={"head": 1}, object_store_memory=1000 * 1024 * 1024
    )
    ray.init(address=cluster.address)

    # Add worker nodes.
    for i in range(2):
        nodes.append(
            cluster.add_node(
                num_cpus=1,
                resources={f"node_{i}": 1},
                object_store_memory=1000 * 1024 * 1024,
            )
        )
    cluster.wait_for_nodes()

    one_mb_array = np.ones(1 * 1024 * 1024, dtype=np.uint8)
    ten_mb_array = np.ones(10 * 1024 * 1024, dtype=np.uint8)

    @ray.remote
    class ObjectsHolder:
        def __init__(self):
            self.ten_mb_objs = []
            self.one_mb_objs = []

        def put_10_mb(self):
            self.ten_mb_objs.append(ray.put(ten_mb_array))

        def put_1_mb(self):
            self.one_mb_objs.append(ray.put(one_mb_array))

        def pop_10_mb(self):
            if len(self.ten_mb_objs) == 0:
                return False
            self.ten_mb_objs.pop()
            return True

        def pop_1_mb(self):
            if len(self.one_mb_objs) == 0:
                return False
            self.one_mb_objs.pop()
            return True

        def get_obj_size(self):
            return len(self.ten_mb_objs) * 10 + len(self.one_mb_objs)

    actor_on_node_1 = ObjectsHolder.options(resources={"node_0": 1}).remote()
    actor_on_node_2 = ObjectsHolder.options(resources={"node_1": 1}).remote()
    actor_on_head_node = ObjectsHolder.options(resources={"head": 1}).remote()

    ray.get(actor_on_node_1.get_obj_size.remote())
    ray.get(actor_on_node_2.get_obj_size.remote())
    ray.get(actor_on_head_node.get_obj_size.remote())

    def random_ops(actors):
        r = random.random()
        for actor in actors:
            if r <= 0.25:
                actor.put_10_mb.remote()
            elif r <= 0.5:
                actor.put_1_mb.remote()
            elif r <= 0.75:
                actor.pop_10_mb.remote()
            else:
                actor.pop_1_mb.remote()

    total_iter = 15
    for _ in range(total_iter):
        random_ops([actor_on_node_1, actor_on_node_2, actor_on_head_node])

    # Simulate node dead.
    cluster.remove_node(nodes[1])
    for _ in range(total_iter):
        random_ops([actor_on_node_1, actor_on_head_node])

    total_size = sum(
        [
            ray.get(actor_on_node_1.get_obj_size.remote()),
            ray.get(actor_on_head_node.get_obj_size.remote()),
        ]
    )

    wait_for_condition(
        lambda: (
            (f"Plasma memory usage {total_size} MiB") in memory_summary(stats_only=True)
        )
    )


@pytest.mark.parametrize("inline_args", [True, False])
def test_inlined_nested_refs(ray_start_cluster, inline_args):
    cluster = ray_start_cluster
    config = {}
    if not inline_args:
        config["max_direct_call_object_size"] = 0
    cluster.add_node(
        num_cpus=2, object_store_memory=100 * 1024 * 1024, _system_config=config
    )
    ray.init(address=cluster.address)

    @ray.remote
    class Actor:
        def __init__(self):
            return

        def nested(self):
            return ray.put("x")

    @ray.remote
    def nested_nested(a):
        return a.nested.remote()

    @ray.remote
    def foo(ref):
        time.sleep(1)
        return ray.get(ref)

    a = Actor.remote()
    nested_nested_ref = nested_nested.remote(a)
    # We get nested_ref's value directly from its owner.
    nested_ref = ray.get(nested_nested_ref)

    del nested_nested_ref
    x = foo.remote(nested_ref)
    del nested_ref
    ray.get(x)


# https://github.com/ray-project/ray/issues/17553
@pytest.mark.parametrize("inline_args", [True, False])
def test_return_nested_ids(shutdown_only, inline_args):
    config = dict()
    if inline_args:
        config["max_direct_call_object_size"] = 100 * 1024 * 1024
    else:
        config["max_direct_call_object_size"] = 0
    ray.init(object_store_memory=100 * 1024 * 1024, _system_config=config)

    class Nested:
        def __init__(self, blocks):
            self._blocks = blocks

    @ray.remote
    def echo(fn):
        return fn()

    @ray.remote
    def create_nested():
        refs = [ray.put(np.random.random(1024 * 1024)) for _ in range(10)]
        return Nested(refs)

    @ray.remote
    def test():
        ref = create_nested.remote()
        result1 = ray.get(ref)
        del ref
        result = echo.remote(lambda: result1)  # noqa
        del result1

        time.sleep(5)
        block = ray.get(result)._blocks[0]
        print(ray.get(block))

    ray.get(test.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_actor_constructor_borrowed_refs(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    class Borrower:
        def __init__(self, borrowed_refs):
            self.borrowed_refs = borrowed_refs

        def test(self):
            ray.get(self.borrowed_refs)

    # Actor is the only one with a ref.
    ref = ray.put(np.random.random(1024 * 1024))
    b = Borrower.remote([ref])
    del ref
    # Check that the actor's ref is usable.
    for _ in range(3):
        ray.get(b.test.remote())
        time.sleep(1)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_deep_nested_refs(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    def f(x):
        print(f"=> step {x}")
        if x > 200:
            return x
        return f.remote(x + 1)

    r = f.remote(1)
    i = 0
    while isinstance(r, ray.ObjectRef):
        print(i, r)
        i += 1
        r = ray.get(r)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_forward_nested_ref(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    def nested_ref():
        return ray.put(1)

    @ray.remote
    def nested_nested_ref():
        return nested_ref.remote()

    @ray.remote
    class Borrower:
        def __init__(self):
            return

        def pass_ref(self, middle_ref):
            self.inner_ref = ray.get(middle_ref)

        def check_ref(self):
            ray.get(self.inner_ref)

    @ray.remote
    def pass_nested_ref(borrower, outer_ref):
        ray.get(borrower.pass_ref.remote(outer_ref[0]))

    b = Borrower.remote()
    outer_ref = nested_nested_ref.remote()
    x = pass_nested_ref.remote(b, [outer_ref])
    del outer_ref
    ray.get(x)

    for _ in range(3):
        ray.get(b.check_ref.remote())
        time.sleep(1)


def test_out_of_band_actor_handle_deserialization(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    class Actor:
        def ping(self):
            return 1

    actor = Actor.remote()

    @ray.remote
    def func(config):
        # deep copy will pickle and unpickle the actor handle.
        config = copy.deepcopy(config)
        return ray.get(config["actor"].ping.remote())

    assert ray.get(func.remote({"actor": actor})) == 1


def test_out_of_band_actor_handle_bypass_reference_counting(shutdown_only):
    import pickle

    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    class Actor:
        def ping(self):
            return 1

    actor = Actor.remote()
    serialized = pickle.dumps({"actor": actor})
    del actor

    wait_for_num_actors(1, gcs_utils.ActorTableData.DEAD)

    config = pickle.loads(serialized)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(config["actor"].ping.remote())


def test_generators(one_worker_100MiB):
    @ray.remote(num_returns="dynamic")
    def remote_generator():
        for _ in range(3):
            yield np.zeros(10 * 1024 * 1024, dtype=np.uint8)

    gen = ray.get(remote_generator.remote())
    refs = list(gen)
    for r in refs:
        _fill_object_store_and_get(r)

    # Outer ID out of scope, we should still be able to get the dynamic
    # objects.
    del gen
    for r in refs:
        _fill_object_store_and_get(r)

    # Inner IDs out of scope.
    refs_oids = [r.binary() for r in refs]
    del r
    del refs

    for r_oid in refs_oids:
        _fill_object_store_and_get(r_oid, succeed=False)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
