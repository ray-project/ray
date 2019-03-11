from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import pytest

# This evaluates tests for each number of CPUs between 2 and 32
num_cpus_test = list(range(2, 32))
num_cpus_ids = ["{}_cpus".format(i) for i in num_cpus_test]

num_tasks_submitted = [10**n for n in range(2, 10)]
num_tasks_ids = ["{}_tasks".format(i) for i in num_tasks_submitted]


def init_ray(**kwargs):
    ray.init(**kwargs)


def teardown_ray():
    ray.stop()


@ray.remote
def dummy_task(val):
    return val


def benchmark_task_submission(num_tasks):
    ray.get([dummy_task.remote(i) for i in range(num_tasks)])


@pytest.mark.parameterize("num_cpus", num_cpus_test, ids=num_cpus_ids)
@pytest.mark.parameterize("num_tasks", num_tasks_submitted, ids=num_tasks_ids)
def test_task_submission_benchmark(benchmark, num_cpus, num_tasks):
    init_ray(num_cpus=num_cpus)
    benchmark(benchmark_task_submission, num_tasks)
    teardown_ray()
