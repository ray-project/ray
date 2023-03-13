#!/usr/bin/env python3
"""
Goal:
Provide p50 latency in time-to-user-code for following combinations:
{N=64}
    x {actor, tasks}
    x {tensorflow, pytorch}
    x {CPU-only node, GPU node}
    x {multiple scheduled in the same job, multiple spread over different jobs}

g5.16xlarge

Steps:
* Driver starts N jobs in sequence
    Each job starts M tasks or actors
    Each task or actor imports the binaries

The target will be to execute 64 tasks/actors a number of times and measure the latency.

How will the job report metrics?
The job can dump a line-json to a log file in /tmp
We can run 

i think this benchmark won't show improvement with prestarting workers
so maybe need a sleep? or report first one

Remaining work:
* Write things safely to output json
* Configure as a release test
* Add more libraries?
* Write combinations of tests

./benchmark.py --actors --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0.01 --with_runtime_env
./benchmark.py --actors --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0.01 --without_runtime_env
./benchmark.py --actors --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0 --with_runtime_env
./benchmark.py --actors --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0 --without_runtime_env
./benchmark.py --tasks --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0.01 --with_runtime_env
./benchmark.py --tasks --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0.01 --without_runtime_env
./benchmark.py --tasks --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0 --with_runtime_env
./benchmark.py --tasks --num_cpus_per_task_or_actor 0.25 --num_gpus_per_task_or_actor 0 --without_runtime_env

# Jiajun suggests also to run the same tasks in a single job.
# with GPU

# 1_jobs-5_runs_per_job-64_actors_per_run-240_actors_per_job-with_gpu

"""

import ray
import sys
import asyncio
from ray.job_submission import JobSubmissionClient, JobStatus
import random
import os
from dataclasses import dataclass
from ray._private.test_utils import safe_write_to_results_json
from collections import defaultdict

@ray.remote(num_cpus=0)
class MetricsActor:
    def __init__(self):
        self.results = defaultdict(list)

    def submit(self, test_name: str, latency: float):
        print(f'got latency {latency} s for test {test_name}')
        self.results[test_name].append(latency)

        # TODO conform to correct json structure
        safe_write_to_results_json(self.results)

@dataclass(eq=True, frozen=True)
class Test:
    num_jobs: int
    num_runs_per_job: int
    num_tasks_or_actors_per_run: int
    with_gpu: bool
    with_tasks: bool
    expensive_import: str
    num_cpus_in_cluster: int
    num_nodes_in_cluster: int

    def __repr__(self):
        with_gpu_str = "with-gpu" if self.with_gpu else "without-gpu"
        executable_unit = "tasks" if self.with_tasks else "actors"
        cold_or_warm_start = "cold" if self.num_jobs > 1 else "warm"
        single_node_or_multi_node = 'single-node' if self.num_nodes_in_cluster == 1 else 'multi-node'
        return '_'.join([
                f"time-to-{cold_or_warm_start}-start-{self.num_tasks_or_actors_per_run}-{self.expensive_import}-{executable_unit}-over-{self.num_cpus_in_cluster}-cpus",
                f"{with_gpu_str}",
                f"{single_node_or_multi_node}",
            ])

async def run_and_stream_logs(metrics_actor_name, metrics_actor_namespace, test: Test):
        client = JobSubmissionClient("http://127.0.0.1:8265")
    
        task_or_actor_arg = "--with_tasks" if test.with_tasks else "--with_actors"
        with_gpu_arg = "--with_gpu" if test.with_gpu else "--without_gpu"

        for i in range(test.num_jobs):
            print(f"Running job {i} for {test}")
            job_id = client.submit_job(
                entrypoint=" ".join([
                            f"python ./script.py",
                            f"--metrics_actor_name {metrics_actor_name}",
                            f"--metrics_actor_namespace {metrics_actor_namespace}",
                            f"--test_name {test}",
                            f"--num_runs {test.num_runs_per_job} ",
                            f"--num_tasks_or_actors_per_run {test.num_tasks_or_actors_per_run}",
                            f"--num_cpus_in_cluster {test.num_cpus_in_cluster}",
                            f"{task_or_actor_arg}",
                            f"{with_gpu_arg}",
                            f"--library_to_import {test.expensive_import}",
                        ])
                runtime_env={"working_dir": "./"}
            )

            try:
                async for lines in client.tail_job_logs(job_id):
                    print(lines, end="")
            except KeyboardInterrupt:
                print(f'Stopping job {job_id}')
                client.stop_job(job_id)
                raise
            
            job_status = client.get_job_status(job_id)
            if job_status != JobStatus.SUCCEEDED:
                raise ValueError(f'Job {job_id} was not successful; status is {job_status}')


def generate_test_matrix():
    tests = set()

    num_cpus_in_cluster = os.cpu_count()
    num_repeated_jobs_or_runs = 5
    num_tasks_or_actors_per_run = num_cpus_in_cluster * 1
    total_num_tasks_or_actors = num_tasks_or_actors_per_run * num_repeated_jobs_or_runs

    for num_jobs in [1, num_repeated_jobs_or_runs]:
        for with_gpu in [True, False]:
            for with_tasks in [True, False]:
                num_tasks_or_actors_per_job = total_num_tasks_or_actors // num_jobs
                num_runs_per_job = num_tasks_or_actors_per_job // num_tasks_or_actors_per_run

                test = Test(
                    num_jobs=num_jobs,
                    num_runs_per_job=num_runs_per_job,
                    num_tasks_or_actors_per_run=num_tasks_or_actors_per_run,
                    with_tasks=with_tasks,
                    with_gpu=with_gpu,
                    expensive_import="torch",
                    num_cpus_in_cluster=num_cpus_in_cluster,
                    num_nodes_in_cluster=1,
                )
                tests.add(test)
    
    return tests

def main():
    metrics_actor_name = 'metrics_actor'
    metrics_actor_namespace = 'metrics_actor_namespace'
    metrics_actor = MetricsActor.options(
        name=metrics_actor_name,
        namespace=metrics_actor_namespace,
    ).remote()
    
    run_matrix = generate_test_matrix()

    for test in random.sample(list(run_matrix), k=len(run_matrix)):
        print(f"Running test {test}")
        asyncio.run(run_and_stream_logs(
            metrics_actor_name,
            metrics_actor_namespace,
            test,
        ))

if __name__ == '__main__':
    sys.exit(main())
