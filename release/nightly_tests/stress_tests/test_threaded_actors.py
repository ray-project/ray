import argparse
import json
import logging
import numpy as np
import os
import threading
import time

from ray._private.test_utils import monitor_memory_usage
from ray.data._internal.progress_bar import ProgressBar

from collections import namedtuple
from queue import Queue

import ray

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PiResult = namedtuple("PiResult", ["samples", "pi"])


@ray.remote(num_cpus=0)
class PiCalculator:
    def __init__(self, metadata):
        # -- Read only variables --
        self.metadata = metadata
        self.sample_batch = 1000000
        # -- Variables that are accessed by mulitple threads --
        self.lock = threading.Lock()
        self.result_queue = Queue()
        self.is_running = False

    def ready(self):
        pass

    def run_compute(self):
        self.is_running = True
        sample_cnt = 0
        while self.is_running:
            # Compute pi
            xs = np.random.uniform(low=-1.0, high=1.0, size=self.sample_batch)
            ys = np.random.uniform(low=-1.0, high=1.0, size=self.sample_batch)
            xys = np.stack((xs, ys), axis=-1)
            inside = xs * xs + ys * ys <= 1.0
            xys_inside = xys[inside]
            in_circle = xys_inside.shape[0]
            approx_pi = 4.0 * in_circle / self.sample_batch

            # Put the result to the queue.
            sample_cnt += self.sample_batch
            with self.lock:
                self.result_queue.put(PiResult(samples=sample_cnt, pi=approx_pi))

    def stop(self):
        self.is_running = False

    def get_metadata(self):
        return self.metadata

    def get_pi(self):
        result = None
        while not result:
            with self.lock:
                if not self.result_queue.empty():
                    result = self.result_queue.get(block=False)
                time.sleep(1)
        return result


def start_actors(total_num_actors, num_nodes):
    """Create actors and run the computation loop."""
    total_num_actors = int(total_num_actors)
    actors_per_node = int(total_num_actors / num_nodes)
    start = time.time()
    nodes = []
    # Place an actor per node in round-robin.
    # It is added here to simulate the real user workload.
    while len(nodes) < num_nodes:
        nodes = [
            next((r for r in n["Resources"] if "node" in r), None)
            for n in ray.nodes()
            if n["Alive"]
        ]
        nodes = [n for n in nodes if n is not None]
    pi_actors = [
        PiCalculator.options(resources={n: 0.01}, max_concurrency=10).remote(
            {"meta": 1}
        )
        for n in nodes
        for _ in range(actors_per_node)
    ]
    ray.get([actor.ready.remote() for actor in pi_actors])
    print(f"Took {time.time() - start} to create {total_num_actors} actors")
    # Start the computation loop.
    for actor in pi_actors:
        actor.run_compute.remote()
    return pi_actors


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kill-interval_s", type=float, default=60)
    parser.add_argument("--test-runtime", type=float, default=3000)
    return parser.parse_known_args()


def main():
    """The test simulates the workload with many threaded actors.

    Test is doing 4 things for 1 hour.

    - It first creates actors as many as num_cpus with max_concurrency=10
    - Each actor computes pi and put the result to the queue.
    - Driver keeps getting result & metadata from the actor.
    - Every X seconds, it kills all actors and restarts them.
    """
    ray.init(address="auto")
    args, unknown = parse_script_args()
    num_cpus = ray.cluster_resources()["CPU"]
    num_nodes = sum(1 for n in ray.nodes() if n["Alive"])
    print(f"Total number of actors: {num_cpus}, nodes: {num_nodes}")
    monitor_actor = monitor_memory_usage()

    start = time.time()
    while time.time() - start < args.test_runtime:
        # Step 1: Create actors and start computation loop.
        print("Create actors.")
        actors = start_actors(num_cpus, num_nodes)

        # Step 2: Get the pi result from actors.
        compute_start = time.time()
        print("Start computation.")
        while time.time() - compute_start < args.kill_interval_s:
            # Get the metadata.
            ray.get([actor.get_metadata.remote() for actor in actors])
            # Get the result.
            pb = ProgressBar("Computing Pi", num_cpus)
            results = [actor.get_pi.remote() for actor in actors]
            pb.fetch_until_complete(results)
            pb.close()

        # Step 3: Kill actors.
        print("Kill all actors.")
        for actor in actors:
            ray.kill(actor)

    # Report the result.
    print("PASSED.")
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print("Memory usage with failures.")
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")
    # Report the result.
    ray.get(monitor_actor.stop_run.remote())

    result = {"success": 0}
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps(result))


if __name__ == "__main__":
    main()
