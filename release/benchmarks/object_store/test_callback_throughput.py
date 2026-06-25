import gc
import json
import os
import threading
import time

import numpy as np

import ray
import ray._private.worker

NUM_WORKERS = 100
OBJECT_SIZE = 1024 * 1024  # 1 MiB, above the 100 KB inlining threshold


@ray.remote(num_cpus=1)
def create_object_on_worker():
    return np.zeros(OBJECT_SIZE, dtype=np.uint8)


def test_callback_throughput(num_refs, timeout_s=60):
    core_worker = ray._private.worker.global_worker.core_worker

    remaining = [num_refs]
    lock = threading.Lock()
    done = threading.Event()

    def on_freed(_id_bytes):
        with lock:
            remaining[0] -= 1
            if remaining[0] == 0:
                done.set()

    refs = [
        create_object_on_worker.options(scheduling_strategy="SPREAD").remote()
        for _ in range(num_refs)
    ]
    ray.wait(refs, num_returns=len(refs))

    assert all(
        core_worker.add_object_out_of_scope_callback(ref, on_freed) for ref in refs
    )

    # Drop all refs at once and measure how long until all callbacks fire.
    refs_dropped_at = time.perf_counter()
    del refs
    gc.collect()

    if not done.wait(timeout=timeout_s):
        fired = num_refs - remaining[0]
        raise TimeoutError(
            f"Only {fired}/{num_refs} callbacks fired within {timeout_s}s"
        )

    settle_time_s = time.perf_counter() - refs_dropped_at
    callbacks_per_sec = num_refs / settle_time_s if settle_time_s > 0 else float("inf")

    print(f"  {num_refs} callbacks in {settle_time_s:.3f}s ({callbacks_per_sec:.0f}/s)")
    return settle_time_s, callbacks_per_sec


ray.init(address="auto")

# Warm up gRPC connections and worker pools.
ray.get(
    [
        create_object_on_worker.options(scheduling_strategy="SPREAD").remote()
        for _ in range(NUM_WORKERS)
    ]
)

settle_1k, throughput_1k = test_callback_throughput(1000)
settle_5k, throughput_5k = test_callback_throughput(5000)

print("\nSummary:")
print(f"  1k: {throughput_1k:.0f}/s (settle: {settle_1k:.3f}s)")
print(f"  5k: {throughput_5k:.0f}/s (settle: {settle_5k:.3f}s)")

if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        results = {
            "settle_1k": settle_1k,
            "settle_5k": settle_5k,
            "throughput_1k": throughput_1k,
            "throughput_5k": throughput_5k,
            "perf_metrics": [
                {
                    "perf_metric_name": "callback_burst_1k_per_second",
                    "perf_metric_value": throughput_1k,
                    "perf_metric_type": "THROUGHPUT",
                },
                {
                    "perf_metric_name": "callback_burst_5k_per_second",
                    "perf_metric_value": throughput_5k,
                    "perf_metric_type": "THROUGHPUT",
                },
            ],
        }
        json.dump(results, out_file)
