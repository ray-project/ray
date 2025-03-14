import ray
import numpy as np
import pickle
import time
import os
import json


def test_small_objects():
    @ray.remote(num_cpus=1)
    class Actor:
        def send(self, _, actor_idx):
            numpy_arr = np.ones((20, 1024))
            return pickle.dumps((numpy_arr, actor_idx))

    actors = [Actor.remote() for _ in range(64)]
    not_ready = []
    for index, actor in enumerate(actors):
        not_ready.append(actor.send.remote(0, index))
    num_messages = 0
    start_time = time.time()
    while time.time() - start_time < 60:
        ready, not_ready = ray.wait(not_ready, num_returns=10)
        for ready_ref in ready:
            _, actor_idx = pickle.loads(ray.get(ready_ref))
            not_ready.append(actors[actor_idx].send.remote(0, actor_idx))
        num_messages += 10
    return num_messages / 60


ray.init(address="auto")
throughput = test_small_objects()
print(f"Number of messages per second: {throughput}")

if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        results = {
            "num_messages": throughput,
            "success": "1",
        }
        perf_metric_name = "number_of_messages_per_second_from_100_actors"
        results["perf_metrics"] = [
            {
                "perf_metric_name": perf_metric_name,
                "perf_metric_value": throughput,
                "perf_metric_type": "THROUGHPUT",
            }
        ]
        json.dump(results, out_file)
