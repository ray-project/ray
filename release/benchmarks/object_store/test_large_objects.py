import numpy as np

import ray
import ray.autoscaler.sdk

import json
import os
from time import perf_counter
from tqdm import tqdm


NUM_NODES = 9
OBJECT_SIZE = 2**32


def test_object_ingest():
    @ray.remote(num_cpus=1, resources={"node": 1})
    class Actor:
        def foo(self):
            pass

        def send_objects(self, arr):
            return np.ones(OBJECT_SIZE, dtype=np.uint8)

    actors = [Actor.remote() for _ in range(NUM_NODES)]

    for actor in tqdm(actors, desc="Ensure all actors have started."):
        ray.get(actor.foo.remote())

    start = perf_counter()
    result_refs = []
    for actor in tqdm(actors, desc="Broadcasting objects"):
        result_refs.append(actor.send_objects.remote())

    results = ray.get(result_refs)
    end = perf_counter()

    for result in results:
        assert len(result) == OBJECT_SIZE

    return end - start


def test_object_broadcast():
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
    for actor in tqdm(actors, desc="Broadcasting objects"):
        result_refs.append(actor.data_len.remote(ref))

    results = ray.get(result_refs)
    end = perf_counter()

    for result in results:
        assert result == OBJECT_SIZE

    return end - start


ray.init(address="auto")
ingest_duration = test_object_ingest()
print(f"Ingest time: {ingest_duration} ({OBJECT_SIZE} B x {NUM_NODES} nodes)")
broadcast_duration = test_object_broadcast()
print(f"Broadcast time: {broadcast_duration} ({OBJECT_SIZE} B x {NUM_NODES} nodes)")


if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        results = {
            "ingest_time": ingest_duration,
            "broadcast_time": broadcast_duration,
            "object_size": OBJECT_SIZE,
            "num_nodes": NUM_NODES,
            "success": "1",
        }
        results["perf_metrics"] = [
            {
                "perf_metric_name": f"time_to_ingest_{OBJECT_SIZE}_bytes_from_{NUM_NODES}_nodes",
                "perf_metric_value": ingest_duration,
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": f"time_to_broadcast_{OBJECT_SIZE}_bytes_to_{NUM_NODES}_nodes",
                "perf_metric_value": broadcast_duration,
                "perf_metric_type": "LATENCY",
            },
        ]

        json.dump(results, out_file)
