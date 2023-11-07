import os
import time

import ray


@ray.remote
class Actor:
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())
        self.i = init_value

    def inc(self, x):
        self.i += x

    def get(self):
        return self.i


@ray.remote
class Gather:
    def __init__(self):
        pass

    def gather(self, *vals):
        return sum(vals)


def run_benchmark(num_actors, num_trials):
    init_val = 10
    # This required a change to cache actor handles in the DAG.
    actors = [Actor.bind(init_val) for _ in range(num_actors)]
    gather_actor = Gather.bind()
    # 1 task for each actor.
    vals = [a.get.bind() for a in actors]
    # Gather results with another actor because Ray DAGs only support one
    # return value right now.
    ret = gather_actor.gather.bind(*vals)

    # Warmup.
    for _ in range(3):
        ref = ret.execute()
        print(ref)
        ray.get(ref) == init_val * num_actors

    print("Starting...")
    start = time.time()
    for _ in range(num_trials):
        ray.get(ret.execute()) == init_val * num_actors
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
