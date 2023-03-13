#!/usr/bin/env python3

import ray
import time
import sys
import argparse

@ray.remote
class Actor:
    def run_code(self):
        import torch

@ray.remote
def task():
    import torch

def main(
        metrics_actor,
        test_name: str,
        num_runs: int,
        num_tasks_or_actors_per_run: int,
        num_cpus_in_cluster: int,
        use_actors: bool,
        with_gpu: bool
    ):

    num_gpus = 0.0001 if with_gpu else 0
    num_cpus = (num_cpus_in_cluster / num_tasks_or_actors_per_run) - 0.01

    print(f'Assigning each task/actor {num_cpus} num_cpus and {num_gpus} num_gpus')

    actor_with_resources = Actor.options(num_gpus=num_gpus, num_cpus=num_cpus)
    task_with_resources = task.options(num_gpus=num_gpus, num_cpus=num_cpus)

    def with_actors():
        actors = [actor_with_resources.remote() for _ in range(num_tasks_or_actors_per_run)]
        ray.get([actor.run_code.remote() for actor in actors])

    def with_tasks():
        ray.get([task_with_resources.remote() for _ in range(num_tasks_or_actors_per_run)])

    func_to_measure = with_actors if use_actors else with_tasks
    
    for run in range(num_runs):
        print(f'Starting measurement for run {run}')
        start = time.time()
        func_to_measure()
        dur_s = time.time() - start
        ray.get(metrics_actor.submit.remote(test_name, dur_s))

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metrics_actor_name', type=str, required=True)
    parser.add_argument('--metrics_actor_namespace', type=str, required=True)
    parser.add_argument('--test_name', type=str, required=True)
    parser.add_argument('--num_runs', type=int, required=True)
    parser.add_argument('--num_tasks_or_actors_per_run', type=int, required=True)
    parser.add_argument('--num_cpus_in_cluster', type=int, required=True)
    parser.add_argument('--library_to_import', type=str, required=True, choices=['torch'])

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--with_actors', action='store_true')
    group.add_argument('--with_tasks', action='store_true')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--with_gpu', action='store_true')
    group.add_argument('--without_gpu', action='store_true')

    return parser.parse_args()
    

if __name__ == '__main__':
    args = parse_args()

    metrics_actor = ray.get_actor(
        args.metrics_actor_name,
        args.metrics_actor_namespace,
    )

    sys.exit(main(
        metrics_actor,
        args.test_name,
        args.num_runs,
        args.num_tasks_or_actors_per_run,
        args.num_cpus_in_cluster,
        args.with_actors,
        args.with_gpu,
    ))
