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

"""

import ray
from ray.job_submission import JobSubmissionClient, JobStatus
import random
import os
from dataclasses import dataclass

ray.init()

@ray.remote(num_cpus=0)
class MetricsActor:
    def submit(self, test_name: str, latency: float):
        print(f'got latency {latency} s for test {test_name}')

@dataclass(eq=True, frozen=True)
class Test:
    with_gpu: bool
    with_tasks: bool
    num_jobs: int
    total_num_tasks_or_actors: int # redundant?
    num_tasks_or_actors_per_job: int

    def __repr__(self):
        with_gpu_str = "with_gpu" if self.with_gpu else "without_gpu"
        executable_unit = "tasks" if self.with_tasks else "actors"
        tasks_or_actors_str = f"with_{executable_unit}"
        # TODO specify concurrency (the requirement is to specify how many "runs" per job)
        return (f"{self.num_jobs}_jobs-"
                f"{self.num_tasks_or_actors_per_job}_{executable_unit}_per_job-"
                f"{with_gpu_str}")


def run_script(
        test: Test,
        metrics_actor_name: str,
        metrics_actor_namespace: str,
    ):
    
    # TODO need to better specify.
    # Should be specification enough to say how many jobs, how many tasks/actors per job, how many of those are concurrent.
    num_tasks_or_actors_per_run = os.cpu_count()
    
    num_runs = test.total_num_tasks_or_actors // num_tasks_or_actors_per_run
    num_runs_per_job = num_runs // test.num_jobs
    
    import asyncio
    asyncio.run(run_and_stream_logs(
        metrics_actor_name,
        metrics_actor_namespace,
        str(test),
        test.num_jobs,
        num_runs_per_job,
        num_tasks_or_actors_per_run,
        test.with_tasks,
        test.with_gpu,
    ))

async def run_and_stream_logs(metrics_actor_name, metrics_actor_namespace, test_name, num_jobs, num_runs, num_tasks_or_actors_per_run, with_tasks, with_gpu):
        client = JobSubmissionClient("http://127.0.0.1:8265")
    
        task_or_actor_arg = "--with_tasks" if with_tasks else "--with_actors"
        with_gpu_arg = "--with_gpu" if with_gpu else "--without_gpu"


        for i in range(num_jobs):
            print(f"Running job {i} for {test_name}")
            job_id = client.submit_job(
                entrypoint=f"python ./script.py "
                            f"--metrics_actor_name {metrics_actor_name} "
                            f"--metrics_actor_namespace {metrics_actor_namespace} "
                            f"--test_name {test_name} "
                            f"--num_runs {num_runs} "
                            f"--num_tasks_or_actors_per_run {num_tasks_or_actors_per_run} "
                            f"{task_or_actor_arg} "
                            f"{with_gpu_arg}",
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

    total_num_tasks_or_actors = os.cpu_count() * 5

    for with_gpu in [True, False]:
        for with_tasks in [True, False]:
            for num_jobs in [1, 5]:
                test = Test(
                    with_gpu=with_gpu,
                    with_tasks=with_tasks,
                    num_jobs=num_jobs,
                    total_num_tasks_or_actors=total_num_tasks_or_actors,
                    num_tasks_or_actors_per_job=total_num_tasks_or_actors // num_jobs,
                )
                tests.add(test)
    
    return tests

def main():
    name = 'metrics_actor'
    namespace = 'metrics_actor_namespace'
    metrics_actor = MetricsActor.options(
        name=name,
        namespace=namespace,
    ).remote()
    
    run_matrix = generate_test_matrix()

    for test in random.sample(list(run_matrix), k=len(run_matrix)):
        print(test)
        run_script(
            test=test,
            metrics_actor_name=name,
            metrics_actor_namespace=namespace,
        )
    

if __name__ == '__main__':
    main()
