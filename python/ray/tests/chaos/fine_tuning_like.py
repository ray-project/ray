"""
Generator data that uses plasma store (XGB, I am thinking 1GB for the first try)
Process data (1~2 seconds, just random processing)
Send data to Y actors (24 actors in the first try)
Actors are sleeping for 30 seconds
Repeat
"""

import argparse
import ray
import numpy as np
import time
import threading
import asyncio
from contextlib import contextmanager


@ray.remote(num_cpus=1)
def read_task():
    # Waste resources
    for _ in range(10):
        np.random.rand(5 * 1024 * 1024)

    # 8 MB * 4 == 32MB
    return [np.random.rand(1024 * 1024) for _ in range(4)]


@ray.remote(num_cpus=1)
def preproc(refs):
    n = np.random.rand(1024 * 1024)
    arrays = ray.get(refs)
    for _ in range(10):
        for arr in arrays:
            n += arr[0]
    return n


@ray.remote(num_cpus=0)
class Coord:
    def __init__(self, actors_in_group):
        self.actors_in_group = actors_in_group

    async def run(self, preproc_refs):
        while True:
            await asyncio.sleep(1)
            refs = []
            for a in self.actors_in_group:
                refs.append(a.ping.remote())
            await asyncio.wait(refs)
            done = True
            for ref in refs:
                finished = await ref
                if not finished:
                    done = False
            if done:
                break


@ray.remote(num_cpus=0)
class FakeFineTuninig:
    def __init__(self):
        self.finished = False

    def ping(self):
        return self.finished

    def fine_tuning(self):
        # Do something for 30 seconds.
        for _ in range(1):
            time.sleep(1)
            for _ in range(10):
                np.random.rand(5 * 1024 * 1024)
        self.finished = True

    def run(self, preproc_refs):
        self.thread = threading.Thread(target=self.fine_tuning)
        self.thread.start()
        return True


@contextmanager
def measure(msg):
    s = time.time()
    print(msg)
    yield
    print("Took", time.time() - s, "seconds.")


def run(num_actors, num_tasks):
    s = time.time()
    print("iteration started")
    # Create an actor group with a coordinator actor.
    with measure("Started actors"):
        actors = []
        print(f"Creating {num_actors} actors")
        actors.extend([FakeFineTuninig.remote() for _ in range(num_actors)])
        ray.get([actor.__ray_ready__.remote() for actor in actors])
    with measure("Start coordination actors"):
        coord = Coord.remote(actors)
        ray.get(coord.__ray_ready__.remote())

    # Prepare num_tasks * 80MB of data
    with measure("Scheduling read tasks"):
        refs = [read_task.remote() for _ in range(num_tasks)]
    # preprocess all refs at the same time.
    with measure("Scheduling preprocessing tasks"):
        preproc_refs = [preproc.remote(refs) for _ in range(num_tasks)]
    # Run fake fine tuninig workload with references
    with measure("Waiting for coordination to finish"):
        for actor in actors:
            actor.run.remote(preproc_refs)
        ray.get(coord.run.remote(preproc_refs))
    print("iteration done. Took ", time.time() - s, "s")
    for actor in actors:
        ray.kill(actor)
    ray.kill(coord)


def main():
    ray.init()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-actors", type=int, default=2, help="Make this many actors"
    )
    parser.add_argument(
        "--num-tasks",
        default=4,
        type=int,
        help="Number of tasks. 80MB * this much of data is generated",
    )
    parser.add_argument(
        "--iteration", type=int, default=5, help="Number of e2e iterations"
    )
    args = parser.parse_args()

    for _ in range(args.iteration):
        run(args.num_actors, args.num_tasks)


main()
