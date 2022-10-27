import numpy as np

import ray
import ray.autoscaler.sdk

import json
import os
from time import perf_counter
from tqdm import tqdm

NUM_NODES = 50
OBJECT_SIZE = 2**30


def num_alive_nodes():
    n = 0
    for node in ray.nodes():
        if node["Alive"]:
            n += 1
    return n


def test_object_broadcast():
    assert num_alive_nodes() == NUM_NODES

    @ray.remote(num_cpus=1, resources={"node": 1})
    class Actor:
        def foo(self):
            pass

        def sum(self, arr):
            return np.sum(arr)

    actors = [Actor.remote() for _ in range(NUM_NODES)]

    arr = np.ones(OBJECT_SIZE, dtype=np.uint8)
    ref = ray.put(arr)

    for actor in tqdm(actors, desc="Ensure all actors have started."):
        ray.get(actor.foo.remote())

    result_refs = []
    for actor in tqdm(actors, desc="Broadcasting objects"):
        result_refs.append(actor.sum.remote(ref))

    results = ray.get(result_refs)
    for result in results:
        assert result == OBJECT_SIZE


ray.init(address="auto")
start = perf_counter()
test_object_broadcast()
end = perf_counter()
print(f"Broadcast time: {end - start} ({OBJECT_SIZE} B x {NUM_NODES} nodes)")

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "broadcast_time": end - start,
        "object_size": OBJECT_SIZE,
        "num_nodes": NUM_NODES,
        "success": "1",
    }
    perf_metric_name = f"time_to_broadcast_{OBJECT_SIZE}_bytes_to_{NUM_NODES}_nodes"
    results["perf_metrics"] = [
        {
            "perf_metric_name": perf_metric_name,
            "perf_metric_value": end - start,
            "perf_metric_type": "LATENCY",
        }
    ]
    json.dump(results, out_file)
