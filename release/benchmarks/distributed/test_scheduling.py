import ray
import argparse
from time import time, sleep
from math import floor
import os
import json


@ray.remote
def simple_task(t):
    sleep(t)


@ray.remote
class SimpleActor:
    def __init__(self, job=None):
        self._job = job

    def ready(self):
        return

    def do_job(self):
        if self._job is not None:
            self._job()


def start_tasks(num_task, num_cpu_per_task, task_duration):
    ray.get(
        [
            simple_task.options(num_cpus=num_cpu_per_task).remote(task_duration)
            for _ in range(num_task)
        ]
    )


def measure(f):
    start = time()
    ret = f()
    end = time()
    return (end - start, ret)


def start_actor(num_actors, num_actors_per_nodes, job):
    resources = {"node": floor(1.0 / num_actors_per_nodes)}
    submission_cost, actors = measure(
        lambda: [
            SimpleActor.options(resources=resources, num_cpus=0).remote(job)
            for _ in range(num_actors)
        ]
    )
    ready_cost, _ = measure(lambda: ray.get([actor.ready.remote() for actor in actors]))
    actor_job_cost, _ = measure(
        lambda: ray.get([actor.do_job.remote() for actor in actors])
    )
    return (submission_cost, ready_cost, actor_job_cost)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Test Scheduling")
    # Task workloads
    parser.add_argument(
        "--total-num-task", type=int, help="Total number of tasks.", required=False
    )
    parser.add_argument(
        "--num-cpu-per-task",
        type=int,
        help="Resources needed for tasks.",
        required=False,
    )
    parser.add_argument(
        "--task-duration-s",
        type=int,
        help="How long does each task execute.",
        required=False,
        default=1,
    )

    # Actor workloads
    parser.add_argument(
        "--total-num-actors", type=int, help="Total number of actors.", required=True
    )
    parser.add_argument(
        "--num-actors-per-nodes",
        type=int,
        help="How many actors to allocate for each nodes.",
        required=True,
    )

    ray.init(address="auto")

    total_cpus_per_node = [node["Resources"]["CPU"] for node in ray.nodes()]
    num_nodes = len(total_cpus_per_node)
    total_cpus = sum(total_cpus_per_node)

    args = parser.parse_args()
    job = None
    if args.total_num_task is not None:
        if args.num_cpu_per_task is None:
            args.num_cpu_per_task = floor(1.0 * total_cpus / args.total_num_task)
        job = lambda: start_tasks(  # noqa: E731
            args.total_num_task, args.num_cpu_per_task, args.task_duration_s
        )

    submission_cost, ready_cost, actor_job_cost = start_actor(
        args.total_num_actors, args.num_actors_per_nodes, job
    )

    output = os.environ.get("TEST_OUTPUT_JSON")

    result = {
        "total_num_task": args.total_num_task,
        "num_cpu_per_task": args.num_cpu_per_task,
        "task_duration_s": args.task_duration_s,
        "total_num_actors": args.total_num_actors,
        "num_actors_per_nodes": args.num_actors_per_nodes,
        "num_nodes": num_nodes,
        "total_cpus": total_cpus,
        "submission_cost": submission_cost,
        "ready_cost": ready_cost,
        "actor_job_cost": actor_job_cost,
        "_runtime": submission_cost + ready_cost + actor_job_cost,
    }

    if output is not None:
        from pathlib import Path

        p = Path(output)
        p.write_text(json.dumps(result))

    print(result)
