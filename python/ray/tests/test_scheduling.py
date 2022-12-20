# coding: utf-8
import collections
import logging
import platform
import subprocess
import sys
import time
import unittest

import numpy as np
import pytest

import ray
import ray.cluster_utils
import ray.util.accelerators
from ray._private.internal_api import memory_summary
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray._private.test_utils import (
    Semaphore,
    SignalActor,
    object_memory_usage,
    get_metric_check_condition,
    wait_for_condition,
)

logger = logging.getLogger(__name__)


def attempt_to_load_balance(
    remote_function, args, total_tasks, num_nodes, minimum_count, num_attempts=100
):
    attempts = 0
    while attempts < num_attempts:
        locations = ray.get([remote_function.remote(*args) for _ in range(total_tasks)])
        counts = collections.Counter(locations)
        print(f"Counts are {counts}")
        if len(counts) == num_nodes and counts.most_common()[-1][1] >= minimum_count:
            break
        attempts += 1
    assert attempts < num_attempts


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on windows")
def test_load_balancing(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets
    # in a roughly equal manner.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 7
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.10)
        return ray._private.worker.global_worker.node.unique_id

    attempt_to_load_balance(f, [], 100, num_nodes, 10)
    attempt_to_load_balance(f, [], 1000, num_nodes, 100)


@pytest.mark.skipif(sys.platform == "win32", reason="Times out on Windows")
def test_hybrid_policy(ray_start_cluster):

    cluster = ray_start_cluster
    num_nodes = 2
    num_cpus = 10
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus, memory=num_cpus)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # `block_task` ensures that scheduled tasks do not return until all are
    # running.
    block_task = Semaphore.remote(0)
    # `block_driver` ensures that the driver does not allow tasks to continue
    # until all are running.
    block_driver = Semaphore.remote(0)

    # Add the memory resource because the cpu will be released in the ray.get
    @ray.remote(num_cpus=1, memory=1)
    def get_node():
        ray.get(block_driver.release.remote())
        ray.get(block_task.acquire.remote())
        return ray._private.worker.global_worker.current_node_id

    # Below the hybrid threshold we pack on the local node first.
    refs = [get_node.remote() for _ in range(5)]
    ray.get([block_driver.acquire.remote() for _ in refs])
    ray.get([block_task.release.remote() for _ in refs])
    nodes = ray.get(refs)
    assert len(set(nodes)) == 1

    # We pack the second node to the hybrid threshold.
    refs = [get_node.remote() for _ in range(10)]
    ray.get([block_driver.acquire.remote() for _ in refs])
    ray.get([block_task.release.remote() for _ in refs])
    nodes = ray.get(refs)
    counter = collections.Counter(nodes)
    for node_id in counter:
        print(f"{node_id}: {counter[node_id]}")
        assert counter[node_id] == 5

    # Once all nodes are past the hybrid threshold we round robin.
    # TODO (Alex): Ideally we could schedule less than 20 nodes here, but the
    # policy is imperfect if a resource report interrupts the process.
    refs = [get_node.remote() for _ in range(20)]
    ray.get([block_driver.acquire.remote() for _ in refs])
    ray.get([block_task.release.remote() for _ in refs])
    nodes = ray.get(refs)
    counter = collections.Counter(nodes)
    for node_id in counter:
        print(f"{node_id}: {counter[node_id]}")
        assert counter[node_id] == 10, counter


def test_legacy_spillback_distribution(ray_start_cluster):
    cluster = ray_start_cluster
    # Create a head node and wait until it is up.
    cluster.add_node(
        num_cpus=0,
        _system_config={
            "scheduler_spread_threshold": 0,
        },
    )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    num_nodes = 2
    # create 2 worker nodes.
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=8)
    cluster.wait_for_nodes()

    assert ray.cluster_resources()["CPU"] == 16

    @ray.remote
    def task():
        time.sleep(1)
        return ray._private.worker.global_worker.current_node_id

    # Make sure tasks are spilled back non-deterministically.
    locations = ray.get([task.remote() for _ in range(8)])
    counter = collections.Counter(locations)
    spread = max(counter.values()) - min(counter.values())
    # Ideally we'd want 4 tasks to go to each node, but we'll settle for
    # anything better than a 1-7 split since randomness is noisy.
    assert spread < 7
    assert len(counter) > 1

    @ray.remote(num_cpus=1)
    class Actor1:
        def __init__(self):
            pass

        def get_location(self):
            return ray._private.worker.global_worker.current_node_id

    actors = [Actor1.remote() for _ in range(10)]
    locations = ray.get([actor.get_location.remote() for actor in actors])
    counter = collections.Counter(locations)
    spread = max(counter.values()) - min(counter.values())
    assert spread < 7
    assert len(counter) > 1


def test_local_scheduling_first(ray_start_cluster):
    cluster = ray_start_cluster
    num_cpus = 8
    # Disable worker caching.
    cluster.add_node(
        num_cpus=num_cpus,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
        },
    )
    cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    def f():
        time.sleep(0.01)
        return ray._private.worker.global_worker.node.unique_id

    def local():
        return ray.get(f.remote()) == ray._private.worker.global_worker.node.unique_id

    # Wait for a worker to get started.
    wait_for_condition(local)

    # Check that we are scheduling locally while there are resources available.
    for i in range(20):
        assert local()


def test_load_balancing_with_dependencies(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        time.sleep(0.1)
        return ray._private.worker.global_worker.node.unique_id

    # This object will be local to one of the raylets. Make sure
    # this doesn't prevent tasks from being scheduled on other raylets.
    x = ray.put(np.zeros(1000000))

    attempt_to_load_balance(f, [x], 100, num_nodes, 20)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows. Multi node."
)
def test_spillback_waiting_task_on_oom(ray_start_cluster):
    # This test ensures that tasks are spilled if they are not schedulable due
    # to lack of object store memory.
    cluster = ray_start_cluster
    object_size = 1e8
    cluster.add_node(
        num_cpus=1,
        memory=1e9,
        object_store_memory=object_size * 2,
        _system_config={
            "automatic_object_spilling_enabled": False,
            "locality_aware_leasing_enabled": False,
        },
    )
    ray.init(address=cluster.address)
    cluster.add_node(
        num_cpus=1,
        resources={"custom": 1},
        memory=1e9,
        object_store_memory=object_size * 2,
    )

    @ray.remote(resources={"custom": 1})
    def create_remote_object():
        return np.zeros(int(object_size), dtype=np.uint8)

    local_obj = ray.put(np.zeros(int(object_size * 1.5), dtype=np.uint8))
    print(local_obj)

    @ray.remote
    def f(x):
        return

    dep = create_remote_object.remote()
    ray.wait([dep], fetch_local=False)
    # Wait for resource availabilities to propagate.
    time.sleep(1)
    # This task can't run on the local node. Make sure it gets spilled even
    # though we have the local CPUs to run it.
    ray.get(f.remote(dep), timeout=30)


def test_spread_scheduling_overrides_locality_aware_scheduling(ray_start_cluster):
    # This test ensures that explicit spread scheduling strategy has higher
    # priority than locality aware scheduling which means the lease request
    # will be sent to local raylet instead of locality favored raylet.
    cluster = ray_start_cluster
    local_node = cluster.add_node(
        num_cpus=8,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 0,
        },
    )
    ray.init(address=cluster.address)
    remote_node = cluster.add_node(num_cpus=8, resources={"pin": 1})
    cluster.wait_for_nodes()

    @ray.remote(resources={"pin": 1})
    def non_local():
        return ray._private.worker.global_worker.node.unique_id

    @ray.remote(scheduling_strategy="SPREAD")
    def f(x):
        return ray._private.worker.global_worker.node.unique_id

    # Test that task f() runs on the local node as well
    # even though remote node has the dependencies.
    obj1 = non_local.remote()
    obj2 = non_local.remote()
    assert {ray.get(f.remote(obj1)), ray.get(f.remote(obj2))} == {
        local_node.unique_id,
        remote_node.unique_id,
    }


def test_locality_aware_leasing(ray_start_cluster):
    # This test ensures that a task will run where its task dependencies are
    # located. We run an initial non_local() task that is pinned to a
    # non-local node via a custom resource constraint, and then we run an
    # unpinned task f() that depends on the output of non_local(), ensuring
    # that f() runs on the same node as non_local().
    cluster = ray_start_cluster

    # Disable worker caching so worker leases are not reused, and disable
    # inlining of return objects so return objects are always put into Plasma.
    cluster.add_node(
        num_cpus=1,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 0,
            "scheduler_spread_threshold": 0.1,
        },
    )
    ray.init(address=cluster.address)
    # Use a custom resource for pinning tasks to a node.
    non_local_node = cluster.add_node(num_cpus=2, resources={"pin": 2})
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, resources={"pin": 1})
    class Actor:
        def ping(self):
            pass

    actor = Actor.remote()
    ray.get(actor.ping.remote())

    @ray.remote(resources={"pin": 1})
    def non_local():
        return ray._private.worker.global_worker.node.unique_id

    @ray.remote
    def f(x):
        return ray._private.worker.global_worker.node.unique_id

    # Test that task f() runs on the same node as non_local()
    # even though local node is lower critical resource utilization.
    assert ray.get(f.remote(non_local.remote())) == non_local_node.unique_id


def test_locality_aware_leasing_cached_objects(ray_start_cluster):
    # This test ensures that a task will run where its task dependencies are
    # located, even when those objects aren't primary copies.
    cluster = ray_start_cluster

    # Disable worker caching so worker leases are not reused, and disable
    # inlining of return objects so return objects are always put into Plasma.
    cluster.add_node(
        num_cpus=1,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 0,
        },
    )
    # Use a custom resource for pinning tasks to a node.
    cluster.add_node(num_cpus=1, resources={"pin_worker1": 1})
    worker2 = cluster.add_node(num_cpus=1, resources={"pin_worker2": 1})
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        return ray._private.worker.global_worker.node.unique_id

    @ray.remote
    def g(x):
        return ray._private.worker.global_worker.node.unique_id

    @ray.remote
    def h(x, y):
        return ray._private.worker.global_worker.node.unique_id

    # f_obj1 pinned on worker1.
    f_obj1 = f.options(resources={"pin_worker1": 1}).remote()
    # f_obj2 pinned on worker2.
    f_obj2 = f.options(resources={"pin_worker2": 1}).remote()
    # f_obj1 cached copy pulled to worker 2 in order to execute g() task.
    ray.get(g.options(resources={"pin_worker2": 1}).remote(f_obj1))
    # Confirm that h is scheduled onto worker 2, since it should have the
    # primary copy of f_obj12 and a cached copy of f_obj1.
    assert ray.get(h.remote(f_obj1, f_obj2)) == worker2.unique_id


def test_locality_aware_leasing_borrowed_objects(ray_start_cluster):
    # This test ensures that a task will run where its task dependencies are
    # located, even when those objects are borrowed.
    cluster = ray_start_cluster

    # Disable worker caching so worker leases are not reused, and disable
    # inlining of return objects so return objects are always put into Plasma.
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 0,
        },
    )
    # Use a custom resource for pinning tasks to a node.
    worker_node = cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        return ray._private.worker.global_worker.node.unique_id

    @ray.remote
    def g(x):
        return ray.get(h.remote(x[0]))

    @ray.remote
    def h(x):
        return ray._private.worker.global_worker.node.unique_id

    # f will run on worker, f_obj will be pinned on worker.
    f_obj = f.options(resources={"pin_worker": 1}).remote()
    # Make sure owner has the location information for f_obj,
    # before we launch g so g worker can get the locality information
    # from the owner.
    ray.wait([f_obj], fetch_local=False)
    # g will run on head, f_obj will be borrowed by head, and we confirm that
    # h(f_obj) is scheduled onto worker, the node that has f_obj.
    assert (
        ray.get(g.options(resources={"pin_head": 1}).remote([f_obj]))
        == worker_node.unique_id
    )


@unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
def test_lease_request_leak(shutdown_only):
    ray.init(num_cpus=1, _system_config={"object_timeout_milliseconds": 200})

    @ray.remote
    def f(x):
        time.sleep(0.1)
        return

    # Submit pairs of tasks. Tasks in a pair can reuse the same worker leased
    # from the raylet.
    tasks = []
    for _ in range(10):
        obj_ref = ray.put(1)
        for _ in range(2):
            tasks.append(f.remote(obj_ref))
        del obj_ref
    ray.get(tasks)

    wait_for_condition(lambda: object_memory_usage() == 0)


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_many_args(ray_start_cluster):
    cluster = ray_start_cluster
    object_size = int(1e6)
    cluster.add_node(
        num_cpus=1,
        _system_config={
            # Lower this to prevent excessive delays in pull retries.
            "object_manager_pull_timeout_ms": 100,
            "debug_dump_period_milliseconds": 1000,
        },
        object_store_memory=int(1e8),
    )
    for _ in range(3):
        cluster.add_node(num_cpus=1, object_store_memory=int(1e8))
    ray.init(address=cluster.address)

    @ray.remote
    def f(i, *args):
        print(i)
        return

    @ray.remote
    def put():
        return np.zeros(object_size, dtype=np.uint8)

    xs = [put.remote() for _ in range(200)]
    ray.wait(xs, num_returns=len(xs), fetch_local=False)
    (
        num_tasks_submitted_before,
        num_leases_requested_before,
    ) = ray._private.worker.global_worker.core_worker.get_task_submission_stats()
    tasks = []
    for i in range(100):
        args = [np.random.choice(xs) for _ in range(10)]
        tasks.append(f.remote(i, *args))
    ray.get(tasks, timeout=30)

    (
        num_tasks_submitted,
        num_leases_requested,
    ) = ray._private.worker.global_worker.core_worker.get_task_submission_stats()
    num_tasks_submitted -= num_tasks_submitted_before
    num_leases_requested -= num_leases_requested_before
    print("submitted:", num_tasks_submitted, "leases requested:", num_leases_requested)
    assert num_tasks_submitted == 100
    assert num_leases_requested <= 10 * num_tasks_submitted


def test_pull_manager_at_capacity_reports(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=int(1e8))
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, object_store_memory=int(1e8))

    object_size = int(1e7)
    refs = []
    for _ in range(20):
        refs.append(ray.put(np.zeros(object_size, dtype=np.uint8)))

    def fetches_queued():
        return "fetches queued" in memory_summary(stats_only=True)

    assert not fetches_queued()

    @ray.remote
    def f(s, ref):
        ray.get(s.wait.remote())

    signal = SignalActor.remote()
    xs = [f.remote(signal, ref) for ref in refs]
    wait_for_condition(fetches_queued)

    signal.send.remote()
    ray.get(xs)
    wait_for_condition(lambda: not fetches_queued())


@pytest.mark.xfail(
    ray.cluster_utils.cluster_not_supported, reason="cluster not supported"
)
def build_cluster(num_cpu_nodes, num_gpu_nodes):
    cluster = ray.cluster_utils.Cluster()
    gpu_ids = [
        cluster.add_node(num_cpus=2, num_gpus=1).unique_id for _ in range(num_gpu_nodes)
    ]
    cpu_ids = [cluster.add_node(num_cpus=1).unique_id for _ in range(num_cpu_nodes)]
    cluster.wait_for_nodes()
    return cluster, cpu_ids, gpu_ids


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_gpu(monkeypatch):
    monkeypatch.setenv("RAY_scheduler_avoid_gpu_nodes", "1")
    n = 5

    cluster, cpu_node_ids, gpu_node_ids = build_cluster(n, n)
    try:
        ray.init(address=cluster.address)

        @ray.remote(num_cpus=1)
        class Actor1:
            def __init__(self):
                pass

            def get_location(self):
                return ray._private.worker.global_worker.node.unique_id

        @ray.remote(num_cpus=1)
        def task_cpu():
            time.sleep(10)
            return ray._private.worker.global_worker.node.unique_id

        @ray.remote(num_returns=2, num_gpus=0.5)
        def launcher():
            a = Actor1.remote()
            # Leave one cpu for the actor.
            task_results = [task_cpu.remote() for _ in range(n - 1)]
            actor_results = [a.get_location.remote() for _ in range(n)]
            return (
                ray.get(task_results + actor_results),
                ray._private.worker.global_worker.node.unique_id,
            )

        r = launcher.remote()

        ids, launcher_id = ray.get(r)

        assert (
            launcher_id in gpu_node_ids
        ), "expected launcher task to be scheduled on GPU nodes"

        for node_id in ids:
            assert (
                node_id in cpu_node_ids
            ), "expected non-GPU tasks/actors to be scheduled on non-GPU nodes."
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 0,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_head_node_without_cpu(ray_start_cluster):
    @ray.remote(num_cpus=1)
    def f():
        return 1

    f.remote()

    check_count = 0
    demand_1cpu = " {'CPU': 1.0}:"
    while True:
        status = subprocess.check_output(["ray", "status"]).decode()
        if demand_1cpu in status:
            break
        check_count += 1
        assert check_count < 5, f"Incorrect demand. Last status {status}"
        time.sleep(1)

    @ray.remote(num_cpus=2)
    def g():
        return 2

    g.remote()

    check_count = 0
    demand_2cpu = " {'CPU': 2.0}:"
    while True:
        status = subprocess.check_output(["ray", "status"]).decode()
        if demand_1cpu in status and demand_2cpu in status:
            break
        check_count += 1
        assert check_count < 5, f"Incorrect demand. Last status {status}"
        time.sleep(1)


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_gpu_scheduling_liveness(ray_start_cluster):
    """Check if the GPU scheduling is in progress when
    it is used with the placement group
    Issue: https://github.com/ray-project/ray/issues/19130
    """
    cluster = ray_start_cluster
    # Start a node without a gpu.
    cluster.add_node(num_cpus=6)
    ray.init(address=cluster.address)

    NUM_CPU_BUNDLES = 10

    @ray.remote(num_cpus=1)
    class Worker(object):
        def __init__(self, i):
            self.i = i

        def work(self):
            time.sleep(0.1)
            print("work ", self.i)

    @ray.remote(num_cpus=1, num_gpus=1)
    class Trainer(object):
        def __init__(self, i):
            self.i = i

        def train(self):
            time.sleep(0.2)
            print("train ", self.i)

    bundles = [{"CPU": 1, "GPU": 1}]
    bundles += [{"CPU": 1} for _ in range(NUM_CPU_BUNDLES)]

    pg = ray.util.placement_group(bundles, strategy="PACK")
    o = pg.ready()
    # Artificial delay to simulate the real world workload.
    time.sleep(3)
    print("Scaling up.")
    cluster.add_node(num_cpus=6, num_gpus=1)
    ray.get(o)

    workers = [
        Worker.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote(i)
        for i in range(NUM_CPU_BUNDLES)
    ]
    trainer = Trainer.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote(0)

    # If the gpu scheduling doesn't properly work, the below
    # code will hang.
    ray.get([workers[i].work.remote() for i in range(NUM_CPU_BUNDLES)], timeout=30)
    ray.get(trainer.train.remote(), timeout=30)


@pytest.mark.parametrize(
    "ray_start_regular",
    [
        {
            "_system_config": {
                "metrics_report_interval_ms": 1000,
            }
        }
    ],
    indirect=True,
)
def test_scheduling_class_depth(ray_start_regular):
    @ray.remote(num_cpus=1000)
    def infeasible():
        pass

    @ray.remote(num_cpus=0)
    def start_infeasible(n):
        if n == 1:
            ray.get(infeasible.remote())
        ray.get(start_infeasible.remote(n - 1))

    start_infeasible.remote(1)
    infeasible.remote()

    # We expect the 2 calls to `infeasible` to be separate scheduling classes
    # because one has depth=1, and the other has depth=2.
    metric_name = "ray_internal_num_infeasible_scheduling_classes"

    # timeout=60 necessary to pass on windows debug/asan builds.
    wait_for_condition(get_metric_check_condition({metric_name: 2}), timeout=60)
    start_infeasible.remote(2)
    wait_for_condition(get_metric_check_condition({metric_name: 3}), timeout=60)
    start_infeasible.remote(4)
    wait_for_condition(get_metric_check_condition({metric_name: 4}), timeout=60)


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
