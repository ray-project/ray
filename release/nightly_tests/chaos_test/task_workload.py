import random
import string
import time

import numpy as np

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.progress_bar import ProgressBar


def run_task_workload(total_num_cpus, smoke):
    """Run task-based workload that doesn't require object reconstruction."""

    @ray.remote(num_cpus=1, max_retries=-1)
    def task():
        def generate_data(size_in_kb=10):
            return np.zeros(1024 * size_in_kb, dtype=np.uint8)

        a = ""
        for _ in range(100000):
            a = a + random.choice(string.ascii_letters)
        return generate_data(size_in_kb=50)

    @ray.remote(num_cpus=1, max_retries=-1)
    def invoke_nested_task():
        time.sleep(0.8)
        return ray.get(task.remote())

    multiplier = 75
    # For smoke mode, run fewer tasks
    if smoke:
        multiplier = 1
    TOTAL_TASKS = int(total_num_cpus * 2 * multiplier)

    pb = ProgressBar("Chaos test", TOTAL_TASKS, "task")
    results = [invoke_nested_task.remote() for _ in range(TOTAL_TASKS)]
    pb.block_until_complete(results)
    pb.close()

    # Consistency check.
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )
