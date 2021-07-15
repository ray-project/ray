import itertools
import json

import ray
from ray.util.queue import Queue, Empty

import time

import numpy as np
import os


def train(config):
    time.sleep(0.1)
    return (os.getpid(), np.random.uniform())


@ray.remote
class NodeTrialRunner:
    def __init__(self, num_workers: int, fn, queue: Queue, timeout: int = 5):
        self.num_workers = num_workers
        self.queue = queue
        self.timeout = timeout

        self.trial_queue = Queue(
            maxsize=num_workers * 2, actor_options=dict(num_cpus=0))

        self.workers = [
            TrialRunner.remote(fn, self.trial_queue)
            for _ in range(num_workers)
        ]

    def run(self):
        futures = [w.run.remote() for w in self.workers]
        start = time.perf_counter()
        while True:
            try:
                config = self.queue.get_nowait()
            except Empty:
                time.sleep(self.timeout)
                if time.perf_counter() - start > self.timeout:
                    break
                time.sleep(0.01)
                continue
            self.trial_queue.put(config)
            start = time.perf_counter()
        return list(itertools.chain.from_iterable(ray.get(futures)))


@ray.remote
class TrialRunner:
    def __init__(self, fn, queue: Queue, timeout: int = 5):
        self.fn = fn
        self.queue = queue
        self.timeout = timeout

    def run(self):
        results = []
        start = time.perf_counter()
        while True:
            try:
                config = self.queue.get_nowait()
            except Empty:
                if time.perf_counter() - start > self.timeout:
                    break
                time.sleep(0.01)
                continue

            result = self.fn(config)
            results.append((config, result))
            start = time.perf_counter()
        return results


def grid_search(config, resolved=None):
    config = config.copy()
    resolved = resolved or {}

    keys = sorted(config.keys())
    key = keys[0]

    vars = config.pop(key)
    for var in vars:
        resolved = resolved.copy()
        resolved[key] = var
        if not config:
            yield resolved
        else:
            for r in grid_search(config, resolved):
                yield r


def main():
    config = {"a": list(range(100 * 100)), "b": list(range(100))}  # 1 M

    num_nodes = len(ray.nodes())
    cpus_per_node = 64

    pgs = [
        ray.util.placement_group([{
            "CPU": cpus_per_node
        }]) for _ in range(num_nodes)
    ]

    config_queue = Queue(maxsize=1600, actor_options=dict(num_cpus=0))

    runners = [
        NodeTrialRunner.options(
            placement_group=pg, placement_group_bundle_index=0).remote(
                num_workers=cpus_per_node, fn=train, queue=config_queue)
        for pg in pgs
    ]

    futures = [runner.run.remote() for runner in runners]

    for config in grid_search(config):
        config_queue.put(config)

    results = ray.get(futures)
    print(results)


if __name__ == "__main__":
    ray.init(address="auto")

    start = time.perf_counter()
    main()
    taken = time.perf_counter() - start

    result = {"time_taken": taken}

    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/tune_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print(f"TOOK {taken:.2f} seconds!")
