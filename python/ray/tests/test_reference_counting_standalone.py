"""Reference counting tests that require their own custom fixture.

The other reference counting tests use a shared Ray instance across the test module
to reduce overheads & overall test runtime.
"""
# coding: utf-8
import logging
import platform
import random
import sys
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
from ray._common.test_utils import (
    SignalActor,
    wait_for_condition,
)
from ray._private.internal_api import memory_summary

logger = logging.getLogger(__name__)


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
            # Required for reducing the retry time of PubsubLongPolling and to trigger the failure callback for WORKER_OBJECT_EVICTION sooner
            "core_worker_rpc_server_reconnect_timeout_s": 0,
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


def _check_refcounts(expected):
    actual = ray._private.worker.global_worker.core_worker.get_all_reference_counts()
    assert len(expected) == len(actual)
    for object_ref, (local, submitted) in expected.items():
        hex_id = object_ref.hex().encode("ascii")
        assert hex_id in actual
        assert local == actual[hex_id]["local"]
        assert submitted == actual[hex_id]["submitted"]


def test_out_of_band_serialized_object_ref(ray_start_regular):
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


def test_captured_object_ref(ray_start_regular):
    captured_id = ray.put(np.zeros(1024, dtype=np.uint8))

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

    captured_id = ray.put(np.zeros(1024, dtype=np.uint8))

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
