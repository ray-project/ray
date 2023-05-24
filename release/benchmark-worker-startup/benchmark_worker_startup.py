#!/usr/bin/env python3

"""
$ ./benchmark_worker_startup.py --help
usage: benchmark_worker_startup.py [-h] --num_gpus_in_cluster
                                   NUM_GPUS_IN_CLUSTER
                                   --num_cpus_in_cluster
                                   NUM_CPUS_IN_CLUSTER
                                   --num_tasks_or_actors_per_run
                                   NUM_TASKS_OR_ACTORS_PER_RUN
                                   --num_measurements_per_configuration
                                   NUM_MEASUREMENTS_PER_CONFIGURATION

This release test measures Ray worker startup time. Specifically, it
measures the time to start N different tasks or actors, where each task or
actor imports a large library (currently PyTorch). N is configurable. The
test runs under a few different configurations: {task, actor} x {runtime
env, no runtime env} x {GPU, no GPU} x {cold start, warm start} x {import
torch, no imports}.

options:
  -h, --help            show this help message and exit
  --num_gpus_in_cluster NUM_GPUS_IN_CLUSTER
                        The number of GPUs in the cluster. This determines
                        how many GPU resources each actor/task requests.
  --num_cpus_in_cluster NUM_CPUS_IN_CLUSTER
                        The number of CPUs in the cluster. This determines
                        how many CPU resources each actor/task requests.
  --num_tasks_or_actors_per_run NUM_TASKS_OR_ACTORS_PER_RUN
                        The number of tasks or actors per 'run'. A run
                        starts this many tasks/actors and consitutes a
                        single measurement. Several runs can be composed
                        within a single job for measure warm start, or
                        spread across different jobs to measure cold start.
  --num_measurements_per_configuration NUM_MEASUREMENTS_PER_CONFIGURATION
                        The number of measurements to record per
                        configuration.

This script uses test_single_configuration.py to run the actual
measurements.
"""

from collections import defaultdict
from dataclasses import dataclass
from ray._private.test_utils import safe_write_to_results_json
from ray.job_submission import JobSubmissionClient, JobStatus
import argparse
import asyncio
import random
import ray
import statistics
import subprocess
import sys


def main(
    num_cpus_in_cluster: int,
    num_gpus_in_cluster: int,
    num_tasks_or_actors_per_run: int,
    num_measurements_per_configuration: int,
):
    """
    Generate test cases, then run them in random order via run_and_stream_logs.
    """
    metrics_actor_name = "metrics_actor"
    metrics_actor_namespace = "metrics_actor_namespace"
    metrics_actor = MetricsActor.options(  # noqa: F841
        name=metrics_actor_name,
        namespace=metrics_actor_namespace,
    ).remote(
        expected_measurements_per_test=num_measurements_per_configuration,
    )

    print_disk_config()

    run_matrix = generate_test_matrix(
        num_cpus_in_cluster,
        num_gpus_in_cluster,
        num_tasks_or_actors_per_run,
        num_measurements_per_configuration,
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
    """
    Actor which tests will report metrics to.
    """

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
        all_measurements, expected_measurements_per_test
    ):
        results = {}
        perf_metrics = []

        for test_name, measurements in all_measurements.items():
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


def print_disk_config():
    print("Getting disk sizes via df -h")
    subprocess.check_call("df -h", shell=True)


def generate_test_matrix(
    num_cpus_in_cluster: int,
    num_gpus_in_cluster: int,
    num_tasks_or_actors_per_run: int,
    num_measurements_per_test: int,
):

    num_repeated_jobs_or_runs = num_measurements_per_test
    total_num_tasks_or_actors = num_tasks_or_actors_per_run * num_repeated_jobs_or_runs

    num_jobs_per_type = {
        "cold_start": num_repeated_jobs_or_runs,
        "warm_start": 1,
    }

    imports_to_try = ["torch", "none"]

    tests = set()

    for with_tasks in [True, False]:
        for with_gpu in [True, False]:
            # Do not run without runtime env. TODO(cade) Infra team added cgroups to
            # default runtime env, need to find some way around that if we want
            # "pure" (non-runtime-env) measurements.
            for with_runtime_env in [True]:
                for import_to_try in imports_to_try:
                    for num_jobs in num_jobs_per_type.values():

                        num_tasks_or_actors_per_job = (
                            total_num_tasks_or_actors // num_jobs
                        )
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
                            import_to_try=import_to_try,
                            num_cpus_in_cluster=num_cpus_in_cluster,
                            num_gpus_in_cluster=num_gpus_in_cluster,
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
    import_to_try: str
    num_cpus_in_cluster: int
    num_gpus_in_cluster: int
    num_nodes_in_cluster: int

    def __repr__(self):
        with_gpu_str = "with_gpu" if self.with_gpu else "without_gpu"
        executable_unit = "tasks" if self.with_tasks else "actors"
        cold_or_warm_start = "cold" if self.num_jobs > 1 else "warm"
        with_runtime_env_str = (
            "with_runtime_env" if self.with_runtime_env else "without_runtime_env"
        )
        single_node_or_multi_node = (
            "single_node" if self.num_nodes_in_cluster == 1 else "multi_node"
        )
        import_torch_or_none = (
            "import_torch" if self.import_to_try == "torch" else "no_import"
        )

        return "-".join(
            [
                f"seconds_to_{cold_or_warm_start}_start_"
                f"{self.num_tasks_or_actors_per_run}_{executable_unit}",
                import_torch_or_none,
                with_gpu_str,
                single_node_or_multi_node,
                with_runtime_env_str,
                f"{self.num_cpus_in_cluster}_CPU_{self.num_gpus_in_cluster}"
                "_GPU_cluster",
            ]
        )


async def run_and_stream_logs(
    metrics_actor_name, metrics_actor_namespace, test: TestConfiguration
):
    """
    Run a particular test configuration by invoking ./test_single_configuration.py.
    """
    client = JobSubmissionClient("http://127.0.0.1:8265")
    entrypoint = generate_entrypoint(metrics_actor_name, metrics_actor_namespace, test)

    for _ in range(test.num_jobs):
        print(f"Running {entrypoint}")

        if not test.with_runtime_env:
            # On non-workspaces, this will run as a job but without a runtime env.
            subprocess.check_call(entrypoint, shell=True)
        else:
            job_id = client.submit_job(
                entrypoint=entrypoint,
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


def generate_entrypoint(
    metrics_actor_name: str, metrics_actor_namespace: str, test: TestConfiguration
):

    task_or_actor_arg = "--with_tasks" if test.with_tasks else "--with_actors"
    with_gpu_arg = "--with_gpu" if test.with_gpu else "--without_gpu"
    with_runtime_env_arg = (
        "--with_runtime_env" if test.with_runtime_env else "--without_runtime_env"
    )
    return " ".join(
        [
            "python ./test_single_configuration.py",
            f"--metrics_actor_name {metrics_actor_name}",
            f"--metrics_actor_namespace {metrics_actor_namespace}",
            f"--test_name {test}",
            f"--num_runs {test.num_runs_per_job} ",
            f"--num_tasks_or_actors_per_run {test.num_tasks_or_actors_per_run}",
            f"--num_cpus_in_cluster {test.num_cpus_in_cluster}",
            f"--num_gpus_in_cluster {test.num_gpus_in_cluster}",
            task_or_actor_arg,
            with_gpu_arg,
            with_runtime_env_arg,
            f"--library_to_import {test.import_to_try}",
        ]
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="This release test measures Ray worker startup time. "
        "Specifically, it measures the time to start N different tasks or"
        " actors, where each task or actor imports a large library ("
        "currently PyTorch). N is configurable.\nThe test runs under a "
        "few different configurations: {task, actor} x {runtime env, "
        "no runtime env} x {GPU, no GPU} x {cold start, warm start} x "
        "{import torch, no imports}.",
        epilog="This script uses test_single_configuration.py to run the "
        "actual measurements.",
    )
    parser.add_argument(
        "--num_gpus_in_cluster",
        type=int,
        required=True,
        help="The number of GPUs in the cluster. This determines how many "
        "GPU resources each actor/task requests.",
    )
    parser.add_argument(
        "--num_cpus_in_cluster",
        type=int,
        required=True,
        help="The number of CPUs in the cluster. This determines how many "
        "CPU resources each actor/task requests.",
    )
    parser.add_argument(
        "--num_tasks_or_actors_per_run",
        type=int,
        required=True,
        help="The number of tasks or actors per 'run'. A run starts this "
        "many tasks/actors and consitutes a single measurement. Several "
        "runs can be composed within a single job for measure warm start, "
        "or spread across different jobs to measure cold start.",
    )
    parser.add_argument(
        "--num_measurements_per_configuration",
        type=int,
        required=True,
        help="The number of measurements to record per configuration.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sys.exit(
        main(
            args.num_cpus_in_cluster,
            args.num_gpus_in_cluster,
            args.num_tasks_or_actors_per_run,
            args.num_measurements_per_configuration,
        )
    )
