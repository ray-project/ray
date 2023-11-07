import os
import time

import ray
from ray.dag.input_node import InputNode


@ray.remote
class Actor:
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())
        self.i = init_value

    def inc(self, x):
        self.i += x

    def get(self):
        return self.i


def run_benchmark(num_actors, num_trials):
    init_val = 10
    actors = [Actor.remote(init_val) for _ in range(num_actors)]

    with InputNode() as dag_input:
        # 1 task for each actor.
        outputs = [a.inc.bind(dag_input) for a in actors]

    # Current API.
    ray.get([output.execute(1) for output in outputs])
    # This one lets us only ray.put once, but we still need to copy the object
    # to the right actors.
    ray.get([output.execute(ray.put(1)) for output in outputs])

    # This approach is a clean API and lets us execute multiple DAGs at once,
    # but the problem is that it essentially allows the DAG to be changed on
    # each execution.
    with InputNode() as dag_input:
        # 1 task for each actor.
        outputs = [a.inc.bind(dag_input) for a in actors]
    refs = ray.execute_dag(outputs, args=(1, ))
    # Caller can also change the DAG by doing this.
    refs = ray.execute_dag(outputs[1:], args=(1, ))

    # Instead we want to make sure that the exact same DAG is executed each
    # time.
    with InputNode() as dag_input:
        # 1 task for each actor.
        outputs = [a.inc.bind(dag_input) for a in actors]
        dag = MultiOutputNode(outputs)
    refs = dag.execute(1)  # Return a list of ObjectRefs, one for each output.


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
