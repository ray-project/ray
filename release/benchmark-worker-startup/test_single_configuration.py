#!/usr/bin/env python3
"""
Helper file for benchmark_worker_startup.py. This file runs a particular test
configuration.
"""

import argparse
import sys
import time

import ray


@ray.remote
class Actor:
    def run_code(self, should_import_torch: bool):
        if should_import_torch:
            import torch  # noqa: F401


@ray.remote
def task(should_import_torch: bool):
    if should_import_torch:
        import torch  # noqa: F401


def main(
    metrics_actor,
    test_name: str,
    num_runs: int,
    num_tasks_or_actors_per_run: int,
    num_cpus_in_cluster: int,
    num_gpus_in_cluster: int,
    library_to_import: str,
    use_actors: bool,
    with_gpu: bool,
    with_runtime_env: bool,
):

    num_gpus = (num_gpus_in_cluster / num_tasks_or_actors_per_run) if with_gpu else 0
    num_cpus = num_cpus_in_cluster / num_tasks_or_actors_per_run

    print(f"Assigning each task/actor {num_cpus} num_cpus and {num_gpus} num_gpus")
    actor_with_resources = Actor.options(num_gpus=num_gpus, num_cpus=num_cpus)
    task_with_resources = task.options(num_gpus=num_gpus, num_cpus=num_cpus)

    should_import_torch = library_to_import == "torch"
    print(f"should_import_torch: {should_import_torch}")

    fail_if_incorrect_runtime_env(expect_runtime_env=with_runtime_env)

    def with_actors():
        actors = [
            actor_with_resources.remote() for _ in range(num_tasks_or_actors_per_run)
        ]
        ray.get([actor.run_code.remote(should_import_torch) for actor in actors])

    def with_tasks():
        ray.get(
            [
                task_with_resources.remote(should_import_torch)
                for _ in range(num_tasks_or_actors_per_run)
            ]
        )

    func_to_measure = with_actors if use_actors else with_tasks

    for run in range(num_runs):
        print(f"Starting measurement for run {run}")
        start = time.time()
        func_to_measure()
        dur_s = time.time() - start
        ray.get(metrics_actor.submit.remote(test_name, dur_s))


def fail_if_incorrect_runtime_env(expect_runtime_env: bool):
    ctx = ray.runtime_context.get_runtime_context()
    print(f"Found runtime_env={ctx.runtime_env}")
    if expect_runtime_env and ctx.runtime_env == {}:
        raise AssertionError(
            f"Expected a runtime environment but found runtime_env={ctx.runtime_env}"
        )

    if not expect_runtime_env and ctx.runtime_env != {}:
        raise AssertionError(
            f"Expected no runtime environment but found runtime_env={ctx.runtime_env}"
        )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics_actor_name", type=str, required=True)
    parser.add_argument("--metrics_actor_namespace", type=str, required=True)
    parser.add_argument("--test_name", type=str, required=True)
    parser.add_argument("--num_runs", type=int, required=True)
    parser.add_argument("--num_tasks_or_actors_per_run", type=int, required=True)
    parser.add_argument("--num_cpus_in_cluster", type=int, required=True)
    parser.add_argument("--num_gpus_in_cluster", type=int, required=True)
    parser.add_argument(
        "--library_to_import", type=str, required=True, choices=["torch", "none"]
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--with_actors", action="store_true")
    group.add_argument("--with_tasks", action="store_true")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--with_gpu", action="store_true")
    group.add_argument("--without_gpu", action="store_true")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--with_runtime_env", action="store_true")
    group.add_argument("--without_runtime_env", action="store_true")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    metrics_actor = ray.get_actor(
        args.metrics_actor_name,
        args.metrics_actor_namespace,
    )

    sys.exit(
        main(
            metrics_actor,
            args.test_name,
            args.num_runs,
            args.num_tasks_or_actors_per_run,
            args.num_cpus_in_cluster,
            args.num_gpus_in_cluster,
            args.library_to_import,
            args.with_actors,
            args.with_gpu,
            args.with_runtime_env,
        )
    )
