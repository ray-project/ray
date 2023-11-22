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
        self.i += x
        return self.i

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
        ref = dag.execute(1, compiled=True)
        print(ray.get(ref))

    print("Starting...")
    start = time.time()
    for _ in range(num_trials):
        ray.get(dag.execute(1, compiled=True))
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
