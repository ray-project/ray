# coding: utf-8
import logging
import sys
import time

import numpy as np
import pytest

import ray.cluster_utils
from ray._private.test_utils import SignalActor, client_test_enabled

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


def test_task_arguments_inline_bytes_limit(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "max_direct_call_object_size": 100 * 1024,
            # if task_rpc_inlined_bytes_limit is greater than
            # max_grpc_message_size, this test fails.
            "task_rpc_inlined_bytes_limit": 18 * 1024,
            "max_grpc_message_size": 20 * 1024,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin_worker": 1})
    def foo(ref1, ref2, ref3):
        return ref1 == ref2 + ref3

    @ray.remote(resources={"pin_head": 1})
    def bar():
        # if the refs are inlined, the test fails.
        # refs = [ray.put(np.random.rand(1024) for _ in range(3))]
        # return ray.get(
        #     foo.remote(refs[0], refs[1], refs[2]))

        return ray.get(
            foo.remote(
                np.random.rand(1024),  # 8k
                np.random.rand(1024),  # 8k
                np.random.rand(1024),
            )
        )  # 8k

    ray.get(bar.remote())


# This case tests whether gcs-based actor scheduler works properly with
# a normal task co-existed.
def test_schedule_actor_and_normal_task(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(
        memory=1024 ** 3, _system_config={"gcs_actor_scheduling_enabled": True}
    )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=600 * 1024 ** 2, num_cpus=0.01)
    class Foo:
        def method(self):
            return 2

    @ray.remote(memory=600 * 1024 ** 2, num_cpus=0.01)
    def fun(singal1, signal_actor2):
        signal_actor2.send.remote()
        ray.get(singal1.wait.remote())
        return 1

    singal1 = SignalActor.remote()
    signal2 = SignalActor.remote()

    o1 = fun.remote(singal1, signal2)
    # Make sure the normal task is executing.
    ray.get(signal2.wait.remote())

    # The normal task is blocked now.
    # Try to create actor and make sure this actor is not created for the time
    # being.
    foo = Foo.remote()
    o2 = foo.method.remote()
    ready_list, remaining_list = ray.wait([o2], timeout=2)
    assert len(ready_list) == 0 and len(remaining_list) == 1

    # Send a signal to unblock the normal task execution.
    ray.get(singal1.send.remote())

    # Check the result of normal task.
    assert ray.get(o1) == 1

    # Make sure the actor is created.
    assert ray.get(o2) == 2


# This case tests whether gcs-based actor scheduler works properly
# in a large scale.
def test_schedule_many_actors_and_normal_tasks(ray_start_cluster):
    cluster = ray_start_cluster

    node_count = 10
    actor_count = 50
    each_actor_task_count = 50
    normal_task_count = 1000
    node_memory = 2 * 1024 ** 3

    for i in range(node_count):
        cluster.add_node(
            memory=node_memory,
            _system_config={"gcs_actor_scheduling_enabled": True} if i == 0 else {},
        )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=100 * 1024 ** 2, num_cpus=0.01)
    class Foo:
        def method(self):
            return 2

    @ray.remote(memory=100 * 1024 ** 2, num_cpus=0.01)
    def fun():
        return 1

    normal_task_object_list = [fun.remote() for _ in range(normal_task_count)]
    actor_list = [Foo.remote() for _ in range(actor_count)]
    actor_object_list = [
        actor.method.remote()
        for _ in range(each_actor_task_count)
        for actor in actor_list
    ]
    for object in ray.get(actor_object_list):
        assert object == 2

    for object in ray.get(normal_task_object_list):
        assert object == 1


# This case tests whether gcs actor scheduler distributes actors
# in a balanced way if using `SPREAD` policy.
@pytest.mark.parametrize("args", [[5, 20], [5, 3]])
def test_actor_distribution_balance(ray_start_cluster_enabled, args):
    cluster = ray_start_cluster_enabled

    node_count = args[0]
    actor_count = args[1]

    for i in range(node_count):
        cluster.add_node(
            memory=1024 ** 3,
            _system_config={"gcs_actor_scheduling_enabled": True} if i == 0 else {},
        )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=100 * 1024 ** 2, num_cpus=0.01, scheduling_strategy="SPREAD")
    class Foo:
        def method(self):
            return ray._private.worker.global_worker.node.unique_id

    actor_distribution = {}
    actor_list = [Foo.remote() for _ in range(actor_count)]
    for actor in actor_list:
        node_id = ray.get(actor.method.remote())
        if node_id not in actor_distribution.keys():
            actor_distribution[node_id] = []
        actor_distribution[node_id].append(actor)

    if node_count >= actor_count:
        assert len(actor_distribution) == actor_count
        for node_id, actors in actor_distribution.items():
            assert len(actors) == 1
    else:
        assert len(actor_distribution) == node_count
        for node_id, actors in actor_distribution.items():
            assert len(actors) <= int(actor_count / node_count)


# This case tests whether RequestWorkerLeaseReply carries normal task resources
# when the request is rejected (due to resource preemption by normal tasks).
def test_worker_lease_reply_with_resources(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(
        memory=2000 * 1024 ** 2,
        num_cpus=1,
        _system_config={
            "gcs_resource_report_poll_period_ms": 1000000,
            "gcs_actor_scheduling_enabled": True,
        },
    )
    node2 = cluster.add_node(memory=1000 * 1024 ** 2, num_cpus=1)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(memory=1500 * 1024 ** 2, num_cpus=0.01)
    def fun(signal):
        signal.send.remote()
        time.sleep(30)
        return 0

    signal = SignalActor.remote()
    fun.remote(signal)
    # Make sure that the `fun` is running.
    ray.get(signal.wait.remote())

    @ray.remote(memory=800 * 1024 ** 2, num_cpus=0.01)
    class Foo:
        def method(self):
            return ray._private.worker.global_worker.node.unique_id

    foo1 = Foo.remote()
    o1 = foo1.method.remote()
    ready_list, remaining_list = ray.wait([o1], timeout=10)
    # If RequestWorkerLeaseReply carries normal task resources,
    # GCS will then schedule foo1 to node2. Otherwise,
    # GCS would keep trying to schedule foo1 to
    # node1 and getting rejected.
    assert len(ready_list) == 1 and len(remaining_list) == 0
    assert ray.get(o1) == node2.unique_id


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
