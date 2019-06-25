from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pytest

import ray
from ray.tests.conftest import _ray_start_cluster

num_tasks_submitted = [10**n for n in range(0, 6)]
num_tasks_ids = ["{}_tasks".format(i) for i in num_tasks_submitted]


@ray.remote
def dummy_task(val):
    return val


def benchmark_task_submission(num_tasks):
    total_tasks = 100000
    for _ in range(total_tasks // num_tasks):
        ray.get([dummy_task.remote(i) for i in range(num_tasks)])


def warmup():
    x = np.zeros(10**6, dtype=np.uint8)
    for _ in range(5):
        for _ in range(5):
            ray.put(x)
        for _ in range(5):
            ray.get([dummy_task.remote(0) for _ in range(1000)])


@pytest.mark.benchmark
@pytest.mark.parametrize("num_tasks", num_tasks_submitted, ids=num_tasks_ids)
def test_task_submission(benchmark, num_tasks):
    num_cpus = 16
    ray.init(
        num_cpus=num_cpus, object_store_memory=10**7, ignore_reinit_error=True)
    # warm up the plasma store
    warmup()
    benchmark(benchmark_task_submission, num_tasks)
    ray.shutdown()


def benchmark_task_forward(f, num_tasks):
    ray.get([f.remote() for _ in range(num_tasks)])


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "num_tasks", [10**3, 10**4],
    ids=[str(num) + "_tasks" for num in [10**3, 10**4]])
def test_task_forward(benchmark, num_tasks):
    with _ray_start_cluster(num_cpus=16, object_store_memory=10**6) as cluster:
        cluster.add_node(resources={"my_resource": 100})
        ray.init(redis_address=cluster.redis_address)

        @ray.remote(resources={"my_resource": 0.001})
        def f():
            return 1

        # Warm up
        ray.get([f.remote() for _ in range(100)])
        benchmark(benchmark_task_forward, f, num_tasks)
