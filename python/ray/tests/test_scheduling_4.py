# coding: utf-8
import collections
import logging
import subprocess
import sys
import time

import numpy as np
import pytest

import ray
from ray.internal.internal_api import memory_summary
import ray.util.accelerators
import ray.cluster_utils
from ray._private.test_utils import fetch_prometheus

from ray._private.test_utils import (
    wait_for_condition,
    SignalActor,
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
    ) = ray.worker.global_worker.core_worker.get_task_submission_stats()
    tasks = []
    for i in range(100):
        args = [np.random.choice(xs) for _ in range(10)]
        tasks.append(f.remote(i, *args))
    ray.get(tasks, timeout=30)

    (
        num_tasks_submitted,
        num_leases_requested,
    ) = ray.worker.global_worker.core_worker.get_task_submission_stats()
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
                return ray.worker.global_worker.node.unique_id

        @ray.remote(num_cpus=1)
        def task_cpu():
            time.sleep(10)
            return ray.worker.global_worker.node.unique_id

        @ray.remote(num_returns=2, num_gpus=0.5)
        def launcher():
            a = Actor1.remote()
            # Leave one cpu for the actor.
            task_results = [task_cpu.remote() for _ in range(n - 1)]
            actor_results = [a.get_location.remote() for _ in range(n)]
            return (
                ray.get(task_results + actor_results),
                ray.worker.global_worker.node.unique_id,
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
        Worker.options(placement_group=pg).remote(i) for i in range(NUM_CPU_BUNDLES)
    ]
    trainer = Trainer.options(placement_group=pg).remote(0)

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

    node_info = ray.nodes()[0]
    metrics_export_port = node_info["MetricsExportPort"]
    addr = node_info["NodeManagerAddress"]
    prom_addr = f"{addr}:{metrics_export_port}"

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

    def make_condition(n):
        def condition():
            _, metric_names, metric_samples = fetch_prometheus([prom_addr])
            if metric_name in metric_names:
                for sample in metric_samples:
                    if sample.name == metric_name and sample.value == n:
                        return True
            return False

        return condition

    wait_for_condition(make_condition(2))
    start_infeasible.remote(2)
    wait_for_condition(make_condition(3))
    start_infeasible.remote(4)
    wait_for_condition(make_condition(4))


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
