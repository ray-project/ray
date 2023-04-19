#!/usr/bin/env python3

import ray
import time
import argparse
import os
import asyncio

@ray.remote
def task(scheduling_start_time):
    import torch
    import tensorflow
    import pandas
    import transformers
    import dask
    dur_s = time.time() - scheduling_start_time

    benchmarking_actor = ray.get_actor(name="benchmarking_actor")
    ray.get(benchmarking_actor.report_completion.remote(dur_s))
    while not ray.get(benchmarking_actor.is_done.remote()):
        time.sleep(0.2)

@ray.remote
class Actor:
    def do_work(self, scheduling_start_time):
        import torch
        import tensorflow
        import pandas
        import transformers
        import dask
        dur_s = time.time() - scheduling_start_time

        benchmarking_actor = ray.get_actor(name="benchmarking_actor")
        ray.get(benchmarking_actor.report_completion.remote(dur_s))
        while not ray.get(benchmarking_actor.is_done.remote()):
            time.sleep(0.2)

@ray.remote
class BenchmarkingActor:

    def __init__(self, tasks_or_actors):
        self.tasks_or_actors = tasks_or_actors

    async def run_benchmark(self):
        scheduling_start_time = time.time()
        futures = []

        self.durations = []
        
        # One process used by benchmarking actor.
        self.num_to_run = os.cpu_count() - 1
        print(f'Running {self.num_to_run} {self.tasks_or_actors}')
        self.completed = 0
        if self.tasks_or_actors == "tasks":
            futures = [task.remote(scheduling_start_time) for _ in range(self.num_to_run)]
        else:
            actors = [Actor.remote() for _ in range(self.num_to_run)]
            futures = [a.do_work.remote(scheduling_start_time) for a in actors]
    
        while not (await self.is_done()):
            await asyncio.sleep(0.2)
        await asyncio.gather(*futures)

        self.durations.sort()
        print(f'List of times (seconds): {self.durations}')

    async def report_completion(self, dur_s):
        self.durations.append(round(dur_s, 2))
        print(f'scheduling + imports took {dur_s:.02f}')
        self.completed += 1

    async def is_done(self):
        return self.completed == self.num_to_run

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--tasks', action='store_true')
    group.add_argument('--actors', action='store_false')
    args = parser.parse_args()
    
    tasks_or_actors = "tasks" if args.tasks else "actors"
    benchmark_actor = BenchmarkingActor.options(name="benchmarking_actor").remote(tasks_or_actors)
    
    ray.get(benchmark_actor.run_benchmark.remote())
