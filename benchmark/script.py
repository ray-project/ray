#!/usr/bin/env python3

import ray

ray.init()

@ray.remote
class Actor:
    def run_code(self):
        import torch

@ray.remote
def task():
    import torch

def main(metrics_actor, test_name, num_runs, num_tasks_or_actors_per_run, use_actors, with_gpu):
    import time
    # TODO GPU

    def with_actors():
        actors = [Actor.remote() for _ in range(num_tasks_or_actors_per_run)]
        ray.get([actor.run_code.remote() for actor in actors])

    def with_tasks():
        ray.get([task.remote() for _ in range(num_tasks_or_actors_per_run)])

    func_to_measure = with_actors if use_actors else with_tasks
    
    for run in range(num_runs):
        print(f'Starting measurement for run {run}')
        start = time.time()
        func_to_measure()
        dur_s = time.time() - start
        ray.get(metrics_actor.submit.remote(test_name, dur_s))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--metrics_actor_name', type=str)
    parser.add_argument('--metrics_actor_namespace', type=str)
    parser.add_argument('--test_name', type=str)
    parser.add_argument('--num_runs', type=int)
    parser.add_argument('--num_tasks_or_actors_per_run', type=int)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--with_actors', action='store_true')
    group.add_argument('--with_tasks', action='store_true')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--with_gpu', action='store_true')
    group.add_argument('--without_gpu', action='store_true')

    args = parser.parse_args()

    metrics_actor = ray.get_actor(
        args.metrics_actor_name,
        args.metrics_actor_namespace,
    )

    import sys

    sys.exit(main(
        metrics_actor,
        args.test_name,
        args.num_runs,
        args.num_tasks_or_actors_per_run,
        args.with_actors,
        args.with_gpu,
    ))
