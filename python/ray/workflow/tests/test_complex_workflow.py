import pytest
import random

import ray
from ray import workflow


def generate_chain(length=10):
    @ray.remote(num_cpus=0.01)
    def inc(n):
        return n + 1

    n = inc.bind(0)
    for _ in range(length):
        n = inc.bind(n)
    return n


def generate_continuation(depth=10):
    @ray.remote(num_cpus=0.01)
    def inc_recur(n, k):
        if k <= 0:
            return n
        return workflow.continuation(inc_recur.bind(n + 1, k - 1))

    return inc_recur.bind(0, depth)


@ray.remote(num_cpus=0.1)
def gather_and_hash(*inputs):
    import hashlib
    import time

    output = hashlib.sha256("-".join(inputs).encode()).hexdigest()
    sleep_duration = int(output, 16) / 2 ** 256 / 100
    time.sleep(sleep_duration)
    return output


def generate_random_dag(node, max_rounds=40):
    random.seed(42)

    max_inputs = int(max_rounds ** 0.5)
    nodes = [node.bind("start")]
    for _ in range(max_rounds):
        n_samples = random.randint(1, min(len(nodes), max_inputs))
        inputs = random.sample(nodes, n_samples)
        nodes.append(node.bind(*inputs))
    return nodes[-1]


def generate_layered_dag(node, width=5, layers=5):
    random.seed(42)

    nodes = [node.bind(f"start_{i}") for i in range(layers)]
    for _ in range(layers - 1):
        new_nodes = []
        for j in range(width):
            random.shuffle(nodes)
            new_nodes.append(node.bind(*nodes))
        nodes = new_nodes
    return node.bind(*nodes)


def test_workflow_with_pressure(workflow_start_regular_shared):
    pressure_level = 10

    dags = [
        generate_chain(),
        generate_continuation(),
        generate_random_dag(gather_and_hash),
        generate_layered_dag(gather_and_hash),
    ]

    ans = ray.get([d.execute() for d in dags])
    outputs = []
    for _ in range(pressure_level):
        for w in dags:
            outputs.append(workflow.run_async(w))

    assert ray.get(outputs) == ans * pressure_level


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
