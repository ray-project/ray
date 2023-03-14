#!/usr/bin/env python3

from collections import defaultdict
from dataclasses import dataclass
from ray._private.test_utils import safe_write_to_results_json
from ray.job_submission import JobSubmissionClient, JobStatus
import argparse
import asyncio
import random
import ray
import statistics
import sys


def main(
    num_cpus_in_cluster: int,
    num_tasks_or_actors_per_run: int,
    num_measurements_per_configuration: int,
    with_runtime_env: bool,
):
    metrics_actor_name = "metrics_actor"
    metrics_actor_namespace = "metrics_actor_namespace"
    metrics_actor = MetricsActor.options(  # noqa: F841
        name=metrics_actor_name,
        namespace=metrics_actor_namespace,
    ).remote(
        expected_measurements_per_test=num_measurements_per_configuration,
    )

    run_matrix = generate_test_matrix(
        num_cpus_in_cluster,
        num_tasks_or_actors_per_run,
        num_measurements_per_configuration,
        with_runtime_env,
    )
    print(f"List of tests: {run_matrix}")

    for test in random.sample(list(run_matrix), k=len(run_matrix)):
        print(f"Running test {test}")
        asyncio.run(
            run_and_stream_logs(
                metrics_actor_name,
                metrics_actor_namespace,
                test,
            )
        )


@ray.remote(num_cpus=0)
class MetricsActor:
    def __init__(self, expected_measurements_per_test: int):
        self.measurements = defaultdict(list)
        self.expected_measurements_per_test = expected_measurements_per_test

    def submit(self, test_name: str, latency: float):
        print(f"got latency {latency} s for test {test_name}")
        self.measurements[test_name].append(latency)
        results = self.create_results_dict_from_measurements(
            self.measurements, self.expected_measurements_per_test
        )
        safe_write_to_results_json(results)

        assert (
            len(self.measurements[test_name]) <= self.expected_measurements_per_test
        ), (
            f"Expected {self.measurements[test_name]} to not have more elements than "
            f"{self.expected_measurements_per_test}"
        )

    @staticmethod
    def create_results_dict_from_measurements(
        measurements, expected_measurements_per_test
    ):
        results = {}
        perf_metrics = []

        for test_name, measurements in measurements.items():
            test_summary = {
                "measurements": measurements,
            }

            if len(measurements) == expected_measurements_per_test:
                median = statistics.median(measurements)
                test_summary["p50"] = median
                perf_metrics.append(
                    {
                        "perf_metric_name": f"p50.{test_name}",
                        "perf_metric_value": median,
                        "perf_metric_type": "LATENCY",
                    }
                )

            results[test_name] = test_summary

        results["perf_metrics"] = perf_metrics
        return results


def generate_test_matrix(
    num_cpus_in_cluster: int,
    num_tasks_or_actors_per_run: int,
    num_measurements_per_test: int,
    with_runtime_env_unused: bool,
):

    num_repeated_jobs_or_runs = num_measurements_per_test
    total_num_tasks_or_actors = num_tasks_or_actors_per_run * num_repeated_jobs_or_runs

    num_jobs_per_type = {
        "cold_start": num_repeated_jobs_or_runs,
        "warm_start": 1,
    }

    tests = set()

    for with_tasks in [True, False]:
        for with_runtime_env in [True, False]:
            for with_gpu in [True, False]:
                for num_jobs in num_jobs_per_type.values():
                    num_tasks_or_actors_per_job = total_num_tasks_or_actors // num_jobs
                    num_runs_per_job = (
                        num_tasks_or_actors_per_job // num_tasks_or_actors_per_run
                    )

                    test = TestConfiguration(
                        num_jobs=num_jobs,
                        num_runs_per_job=num_runs_per_job,
                        num_tasks_or_actors_per_run=num_tasks_or_actors_per_run,
                        with_tasks=with_tasks,
                        with_gpu=with_gpu,
                        with_runtime_env=with_runtime_env,
                        expensive_import="torch",
                        num_cpus_in_cluster=num_cpus_in_cluster,
                        num_nodes_in_cluster=1,
                    )
                    tests.add(test)

    return tests


@dataclass(eq=True, frozen=True)
class TestConfiguration:
    num_jobs: int
    num_runs_per_job: int
    num_tasks_or_actors_per_run: int
    with_gpu: bool
    with_tasks: bool
    with_runtime_env: bool
    expensive_import: str
    num_cpus_in_cluster: int
    num_nodes_in_cluster: int

    def __repr__(self):
        with_gpu_str = "with-gpu" if self.with_gpu else "without-gpu"
        executable_unit = "tasks" if self.with_tasks else "actors"
        cold_or_warm_start = "cold" if self.num_jobs > 1 else "warm"
        # This needs more thought.. currently things are all ran as jobs.
        with_runtime_env_str = (
            "with-runtime-env" if self.with_runtime_env else "without-runtime-env"
        )
        single_node_or_multi_node = (
            "single-node" if self.num_nodes_in_cluster == 1 else "multi-node"
        )
        return "_".join(
            [
                f"seconds-to-{cold_or_warm_start}-start-{self.num_tasks_or_actors_per_run}-{self.expensive_import}-{executable_unit}-over-{self.num_cpus_in_cluster}-cpus",  # noqa: E501
                f"{with_gpu_str}",
                f"{single_node_or_multi_node}",
                f"{with_runtime_env_str}",
            ]
        )


async def run_and_stream_logs(
    metrics_actor_name, metrics_actor_namespace, test: TestConfiguration
):
    client = JobSubmissionClient("http://127.0.0.1:8265")

    task_or_actor_arg = "--with_tasks" if test.with_tasks else "--with_actors"
    with_gpu_arg = "--with_gpu" if test.with_gpu else "--without_gpu"

    for i in range(test.num_jobs):
        if not test.with_runtime_env:
            import subprocess

            subprocess.run(
                " ".join(
                    [
                        f"python ./benchmark_single_configuration.py",
                        f"--metrics_actor_name {metrics_actor_name}",
                        f"--metrics_actor_namespace {metrics_actor_namespace}",
                        f"--test_name {test}",
                        f"--num_runs {test.num_runs_per_job} ",
                        f"--num_tasks_or_actors_per_run {test.num_tasks_or_actors_per_run}",
                        f"--num_cpus_in_cluster {test.num_cpus_in_cluster}",
                        f"{task_or_actor_arg}",
                        f"{with_gpu_arg}",
                        f"--library_to_import {test.expensive_import}",
                    ]),
                shell=True,
            )
        else:
            print(f"Running job {i} for {test}")
            job_id = client.submit_job(
                entrypoint=" ".join(
                    [
                        f"python ./benchmark_single_configuration.py",
                        f"--metrics_actor_name {metrics_actor_name}",
                        f"--metrics_actor_namespace {metrics_actor_namespace}",
                        f"--test_name {test}",
                        f"--num_runs {test.num_runs_per_job} ",
                        f"--num_tasks_or_actors_per_run {test.num_tasks_or_actors_per_run}",
                        f"--num_cpus_in_cluster {test.num_cpus_in_cluster}",
                        f"{task_or_actor_arg}",
                        f"{with_gpu_arg}",
                        f"--library_to_import {test.expensive_import}",
                    ]
                ),
                runtime_env={"working_dir": "./"},
            )

            try:
                async for lines in client.tail_job_logs(job_id):
                    print(lines, end="")
            except KeyboardInterrupt:
                print(f"Stopping job {job_id}")
                client.stop_job(job_id)
                raise

            job_status = client.get_job_status(job_id)
            if job_status != JobStatus.SUCCEEDED:
                raise ValueError(
                    f"Job {job_id} was not successful; status is {job_status}"
                )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_cpus_in_cluster", type=int, required=True)
    parser.add_argument("--num_tasks_or_actors_per_run", type=int, required=True)
    parser.add_argument("--num_measurements_per_configuration", type=int, required=True)

    #group = parser.add_mutually_exclusive_group(required=True)
    #group.add_argument("--with_runtime_env", action="store_true")
    #group.add_argument("--without_runtime_env", action="store_true")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sys.exit(
        main(
            args.num_cpus_in_cluster,
            args.num_tasks_or_actors_per_run,
            args.num_measurements_per_configuration,
            args.with_runtime_env,
        )
    )
