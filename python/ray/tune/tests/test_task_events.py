import pytest
import sys

import ray
from ray._private.test_utils import (
    run_string_as_driver,
    wait_for_condition,
)
from ray.util.state import list_tasks


@pytest.fixture
def ray_init_4_cpus():
    yield ray.init(num_cpus=4)
    ray.shutdown()


def test_no_missing_parent_task_ids(ray_init_4_cpus):
    """Verify that an e2e Tune workload doesn't have any missing parent_task_ids."""
    script = """
import time

import numpy as np

import ray
import ray.train
from ray import tune

@ray.remote
def train_step_1():
    time.sleep(0.5)
    return 1

def train_function(config):
    for i in range(5):
        loss = config["mean"] * np.random.randn() + ray.get(
            train_step_1.remote())
        ray.train.report(dict(loss=loss, nodes=ray.nodes()))

analysis = tune.run(
    train_function,
    metric="loss",
    mode="min",
    config={
        "mean": tune.grid_search([1, 2, 3, 4, 5]),
    },
    resources_per_trial=tune.PlacementGroupFactory([{
        'CPU': 1.0
    }] + [{
        'CPU': 1.0
    }] * 3),
)
"""

    run_string_as_driver(script)

    def verify():
        tasks = list_tasks()

        task_id_map = {task["task_id"]: task for task in tasks}
        for task in tasks:
            if task["type"] == "DRIVER_TASK":
                continue
            assert task_id_map.get(task["parent_task_id"], None) is not None, task

        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
