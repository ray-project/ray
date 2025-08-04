#!/usr/bin/env python

import argparse
import logging
import os

import ray

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ray.init(address="auto")


@ray.remote
def assert_env_var(prefix, expected_count, expected_value):
    count = 0
    for k, v in os.environ.items():
        if k.startswith(prefix):
            assert v == expected_value
            count += 1
    assert count == expected_count


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_runtime_envs", type=int)
    parser.add_argument("--num_tasks", type=int)
    return parser.parse_known_args()


if __name__ == "__main__":
    args, unknown = parse_script_args()
    tasks = []
    for i in range(args.num_tasks):
        val = f"task{i}"
        task = assert_env_var.options(
            runtime_env={
                "env_vars": {
                    f"STRESS_TEST_{j}": val for j in range(args.num_runtime_envs)
                }
            },
        ).remote("STRESS_TEST_", args.num_runtime_envs, val)
        tasks.append(task)
    ray.get(tasks)
