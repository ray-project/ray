# coding: utf-8
import collections
import logging
import subprocess
import sys
import time
from typing import List

import numpy as np
import pytest

import ray
import ray.cluster_utils
import ray.util.accelerators
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.internal_api import memory_summary
from ray._private.test_utils import (
    MetricSamplePattern,
    get_metric_check_condition,
    object_memory_usage,
)
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
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
def test_hybrid_policy_threshold(ray_start_cluster):
    cluster = ray_start_cluster

    NUM_NODES = 2
    NUM_CPUS_PER_NODE = 4
    # The default hybrid policy packs nodes up to 50% capacity before spreading.
    PER_NODE_HYBRID_THRESHOLD = int(NUM_CPUS_PER_NODE / 2)
    for _ in range(NUM_NODES):
        cluster.add_node(
            num_cpus=NUM_CPUS_PER_NODE,
            memory=NUM_CPUS_PER_NODE,
        )

    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Use a SignalActor to ensure that the batches of tasks run in parallel.
    signal = SignalActor.remote()

    # Add the `memory` resource because the CPU will be released when the task is
    # blocked calling `ray.get()`.
    # NOTE(edoakes): this needs to be `memory`, not a custom resource.
    # See: https://github.com/ray-project/ray/pull/54271.
    @ray.remote(num_cpus=1, memory=1)
    def get_node_id() -> str:
        ray.get(signal.wait.remote())
        return ray.get_runtime_context().get_node_id()

    # Submit 1 * PER_NODE_HYBRID_THRESHOLD tasks.
    # They should all be packed on the local node.
    refs = [get_node_id.remote() for _ in range(PER_NODE_HYBRID_THRESHOLD)]
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == len(refs))
    ray.get(signal.send.remote())
    nodes = ray.get(refs, timeout=20)
    assert len(set(nodes)) == 1

    # Clear the signal between tests.
    ray.get(signal.send.remote(clear=True))

    # Submit 2 * PER_NODE_HYBRID_THRESHOLD tasks.
    # The first PER_NODE_HYBRID_THRESHOLD tasks should be packed on the local node, then
    # the second PER_NODE_HYBRID_THRESHOLD tasks should be packed on the remote node.
    refs = [get_node_id.remote() for _ in range(int(PER_NODE_HYBRID_THRESHOLD * 2))]
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == len(refs))
    ray.get(signal.send.remote())
    counter = collections.Counter(ray.get(refs, timeout=20))
    assert all(v == PER_NODE_HYBRID_THRESHOLD for v in counter.values()), counter


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


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on Windows (multi node).")
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
    """Test that a task runs where its dependencies are located for borrowed objects."""
    # This test ensures that a task will run where its task dependencies are
    # located, even when those objects are borrowed.
    cluster = ray_start_cluster
    head_node = cluster.add_node(
        _system_config={
            # Disable worker caching so worker leases are not reused.
            "worker_lease_timeout_milliseconds": 0,
            # Force all return objects to be put into the object store.
            "max_direct_call_object_size": 0,
        },
    )
    worker_node = cluster.add_node()
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0)
    def get_node_id(*args) -> str:
        return ray.get_runtime_context().get_node_id()

    @ray.remote(num_cpus=0)
    def borrower(o: List[ray.ObjectRef]) -> str:
        obj_ref = o[0]
        return ray.get(get_node_id.remote(obj_ref))

    # The result of worker_node_ref will be pinned on the worker node.
    worker_node_ref = get_node_id.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            worker_node.node_id, soft=False
        ),
    ).remote()

    # Run a borrower task on the head node. From within the borrower task, we launch
    # another task. The inner task should run on the worker node based on locality.
    assert (
        ray.get(
            borrower.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    head_node.node_id, soft=False
                ),
            ).remote([worker_node_ref])
        )
        == worker_node.node_id
    )


@pytest.mark.skipif(
    ray._private.client_mode_hook.is_client_mode_enabled, reason="Fails w/ Ray Client."
)
@pytest.mark.skipif(sys.platform == "win32", reason="Fails on Windows.")
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

    timeout = 60
    if sys.platform == "win32":
        # longer timeout is necessary to pass on windows debug/asan builds.
        timeout = 180

    wait_for_condition(
        get_metric_check_condition([MetricSamplePattern(name=metric_name, value=2)]),
        timeout=timeout,
    )
    start_infeasible.remote(2)
    wait_for_condition(
        get_metric_check_condition([MetricSamplePattern(name=metric_name, value=3)]),
        timeout=timeout,
    )


def test_no_resource_oversubscription_during_shutdown(shutdown_only):
    """
    Ensures that workers don't release their acquired resources
    until all running tasks have been drained.
    """
    # Initialize Ray with 1 CPU, so we can detect if it over-allocates.
    ray.init(num_cpus=1, log_to_driver=False)

    # Separate signal actors for each task to track their execution
    task1_started = SignalActor.remote()
    task1_can_finish = SignalActor.remote()
    task2_started = SignalActor.remote()
    task2_can_finish = SignalActor.remote()

    @ray.remote(num_cpus=1)
    def blocking_task(
        worker_id: str,
        started_signal: ray.actor.ActorHandle,
        can_finish_signal: ray.actor.ActorHandle,
    ) -> str:
        """A task that signals when it starts and waits for permission to finish."""
        print(f"  Worker {worker_id}: Starting execution")
        # Signal that this task has started executing
        ray.get(started_signal.send.remote())
        # Wait for permission to finish
        ray.get(can_finish_signal.wait.remote())
        print(f"  Worker {worker_id}: Completed")
        return f"Worker {worker_id} completed"

    # 1. Start task1 - should consume the only CPU
    task1 = blocking_task.remote("A", task1_started, task1_can_finish)

    # Wait for task1 to start executing
    ray.get(task1_started.wait.remote())
    print("Task1 is now executing")

    # 2. Start task2 - should be queued since CPU is occupied
    task2 = blocking_task.remote("B", task2_started, task2_can_finish)
    print("Task2 submitted (should be queued)")

    # 3. The key test: verify task2 does NOT start executing while task1 is running
    # If the bug exists, task2 will start immediately. If fixed, it should wait.

    # Check if task2 starts within 1 second (indicating the bug)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(task2_started.wait.remote(), timeout=0.5)

    # Now let task1 complete
    ray.get(task1_can_finish.send.remote())
    result1 = ray.get(task1)
    assert result1 == "Worker A completed"

    # After task1 completes, task2 should now be able to start
    ray.get(task2_started.wait.remote())

    # Let task2 complete
    ray.get(task2_can_finish.send.remote())
    result2 = ray.get(task2)
    assert result2 == "Worker B completed"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
