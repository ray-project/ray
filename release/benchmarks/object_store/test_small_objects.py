import ray
import numpy as np
import time
import os
import json


def test_small_objects_ingest():
    @ray.remote(num_cpus=1)
    class Actor:
        def send(self, _, actor_idx):
            # this size is chosen because it's >100kb so big enough to be stored in plasma
            numpy_arr = np.ones((20, 1024))
            return (numpy_arr, actor_idx)

    actors = [Actor.remote() for _ in range(64)]
    not_ready = []
    for index, actor in enumerate(actors):
        not_ready.append(actor.send.remote(0, index))
    num_messages = 0
    start_time = time.time()
    while time.time() - start_time < 60:
        ready, not_ready = ray.wait(not_ready, num_returns=10)
        for ready_ref in ready:
            _, actor_idx = ray.get(ready_ref)
            not_ready.append(actors[actor_idx].send.remote(0, actor_idx))
        num_messages += 10
    return num_messages / 60


def test_small_objects_broadcast():
    @ray.remote(num_cpus=1)
    class Actor:
        def receive(self, numpy_arr, actor_idx):
            return actor_idx

    actors = [Actor.remote() for _ in range(64)]
    numpy_arr_ref = ray.put(np.ones((20, 1024)))
    not_ready = []

    num_messages = 0
    start_time = time.time()
    for idx, actor in enumerate(actors):
        not_ready.append(actor.receive.remote(numpy_arr_ref, idx))
    while time.time() - start_time < 60:
        ready, not_ready = ray.wait(not_ready, num_returns=10)
        actor_idxs = ray.get(ready)
        for actor_idx in actor_idxs:
            not_ready.append(actors[actor_idx].receive.remote(numpy_arr_ref))
        num_messages += 10
    return num_messages / 60


ray.init(address="auto")
ingest_throughput = test_small_objects_ingest()
print(f"Number of messages per second ingested: {ingest_throughput}")
broadcast_throughput = test_small_objects_broadcast()
print(f"Number of messages per second broadcasted: {broadcast_throughput}")


if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        results = {
            "num_messages_ingested": ingest_throughput,
            "num_messages_broadcasted": broadcast_throughput,
            "success": "1",
        }
        results["perf_metrics"] = [
            {
                "perf_metric_name": "num_small_objects_ingested_per_second",
                "perf_metric_value": ingest_throughput,
                "perf_metric_type": "THROUGHPUT",
            },
            {
                "perf_metric_name": "num_small_objects_broadcasted_per_second",
                "perf_metric_value": broadcast_throughput,
                "perf_metric_type": "THROUGHPUT",
            },
        ]
        json.dump(results, out_file)
