from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pytest

import ray

num_tasks_submitted = [10**n for n in range(0, 6)]
num_tasks_ids = ["{}_tasks".format(i) for i in num_tasks_submitted]


def init_ray(**kwargs):
    ray.init(**kwargs)
    # warm up the plasma store
    for _ in range(2):
        [ray.put(np.random.randint(0, 100, size=10**i)) for i in range(6, 0, -1)]


def teardown_ray():
    ray.shutdown()


@ray.remote
def dummy_task(val):
    return val


def benchmark_task_submission(num_tasks):
    ray.get([dummy_task.remote(i) for i in range(num_tasks)])


@pytest.mark.benchmark
@pytest.mark.parametrize("num_tasks", num_tasks_submitted, ids=num_tasks_ids)
def test_task_submission(benchmark, num_tasks):
    num_cpus = 16
    init_ray(num_cpus=num_cpus, object_store_memory=10**7)
    benchmark(benchmark_task_submission, num_tasks)
    teardown_ray()
