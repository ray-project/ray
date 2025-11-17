import json
import os
from time import perf_counter

import numpy as np
from tqdm import tqdm

import ray

NUM_NODES = 9
OBJECT_SIZE = 2**32


def test_object_many_to_one():
    @ray.remote(num_cpus=1, resources={"node": 1})
    class Actor:
        def foo(self):
            pass

        def send_objects(self):
            return np.ones(OBJECT_SIZE, dtype=np.uint8)

    actors = [Actor.remote() for _ in range(NUM_NODES)]

    for actor in tqdm(actors, desc="Ensure all actors have started."):
        ray.get(actor.foo.remote())

    start = perf_counter()
    result_refs = []
    for actor in tqdm(actors, desc="Tasks kickoff"):
        result_refs.append(actor.send_objects.remote())

    results = ray.get(result_refs)
    end = perf_counter()

    for result in results:
        assert len(result) == OBJECT_SIZE

    return end - start


def test_object_one_to_many():
    @ray.remote(num_cpus=1, resources={"node": 1})
    class Actor:
        def foo(self):
            pass

        def data_len(self, arr):
            return len(arr)

    actors = [Actor.remote() for _ in range(NUM_NODES)]

    arr = np.ones(OBJECT_SIZE, dtype=np.uint8)
    ref = ray.put(arr)

    for actor in tqdm(actors, desc="Ensure all actors have started."):
        ray.get(actor.foo.remote())

    start = perf_counter()
    result_refs = []
    for actor in tqdm(actors, desc="Tasks kickoff"):
        result_refs.append(actor.data_len.remote(ref))

    results = ray.get(result_refs)
    end = perf_counter()

    for result in results:
        assert result == OBJECT_SIZE

    return end - start


ray.init(address="auto")
many_to_one_duration = test_object_many_to_one()
print(f"many_to_one time: {many_to_one_duration} ({OBJECT_SIZE} B x {NUM_NODES} nodes)")
one_to_many_duration = test_object_one_to_many()
print(f"one_to_many time: {one_to_many_duration} ({OBJECT_SIZE} B x {NUM_NODES} nodes)")


if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        results = {
            "many_to_one_time": many_to_one_duration,
            "one_to_many_time": one_to_many_duration,
            "object_size": OBJECT_SIZE,
            "num_nodes": NUM_NODES,
        }
        results["perf_metrics"] = [
            {
                "perf_metric_name": f"time_many_to_one_{OBJECT_SIZE}_bytes_from_{NUM_NODES}_nodes",
                "perf_metric_value": many_to_one_duration,
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": f"time_one_to_many_{OBJECT_SIZE}_bytes_to_{NUM_NODES}_nodes",
                "perf_metric_value": one_to_many_duration,
                "perf_metric_type": "LATENCY",
            },
        ]

        json.dump(results, out_file)
