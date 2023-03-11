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

"""

import ray
from ray.job_submission import JobSubmissionClient, JobStatus
import os

ray.init()

@ray.remote(num_cpus=0)
class MetricsActor:
    def submit(self, latency):
        print(f'got latency {latency}')

name = 'metrics_actor'
namespace = 'metrics_actor_namespace'
metrics_actor = MetricsActor.options(
    name=name,
    namespace=namespace,
).remote()

num_runs = 10
num_tasks_or_actors = os.cpu_count()

async def run():
    client = JobSubmissionClient("http://127.0.0.1:8265")
    job_id = client.submit_job(
        entrypoint=f"python script.py --metrics_actor_name {name} --metrics_actor_namespace {namespace} "
                    f"--num_runs {num_runs} --num_tasks_or_actors {num_tasks_or_actors}"
                    f"--with_tasks",
        runtime_env={"working_dir": "./"}
    )
    async for lines in client.tail_job_logs(job_id):
        print(lines, end="")

import asyncio
asyncio.run(run())
