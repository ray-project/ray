import os
import time

import ray

from ray.dag import InputNode, OutputNode
from ray.dag.compiled_dag_node import RayCompiledExecutor


@ray.remote
class Actor(RayCompiledExecutor):
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())
        self.i = init_value

    def inc(self, x):
        print(x)
        return x

    def get(self):
        return self.i


def run_benchmark(num_actors, num_trials):
    init_val = 10
    actors = [Actor.remote(init_val) for _ in range(num_actors)]

    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = OutputNode(out)

    # Warmup.
    for i in range(3):
        refs = dag.execute(b"hello", compiled=True)
        print(ray.get(refs))
        for ref in refs:
            ray.release(ref)

    print("Starting...")
    start = time.time()
    for _ in range(num_trials):
        refs = dag.execute(b"hello", compiled=True)
        ray.get(refs)
        for ref in refs:
            ray.release(ref)
    end = time.time()
    print(f"{num_trials} executed in {end - start}s.")
    print(f"Throughput: {num_trials / (end - start)} rounds/s.")
    print(
        f"Throughput: {num_trials * (args.num_actors + 1) / (end - start)} "
        "total tasks/s."
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-actors",
        default=1,
        type=int,
    )
    parser.add_argument(
        "--num-trials",
        default=1000,
        type=int,
    )

    args = parser.parse_args()
    run_benchmark(args.num_actors, args.num_trials)
