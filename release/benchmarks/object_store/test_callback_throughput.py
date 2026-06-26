import gc
import json
import os
import threading
import time

import numpy as np

import ray
import ray._private.worker

NUM_WORKERS = 10
OBJECT_SIZE = 1024 * 1024  # 1 MiB, above the 100 KB inlining threshold


@ray.remote(num_cpus=1)
def produce_block():
    return np.zeros(OBJECT_SIZE, dtype=np.uint8)


@ray.remote(num_cpus=1)
def consume_block(block_ref):
    return len(block_ref)


def test_callback_pipeline(num_blocks, timeout_s=60):
    core_worker = ray._private.worker.global_worker.core_worker

    remaining = [num_blocks]
    lock = threading.Lock()
    done = threading.Event()

    def on_freed(_id_bytes):
        with lock:
            remaining[0] -= 1
            if remaining[0] == 0:
                done.set()

    start = time.perf_counter()

    # Produce blocks (models MapOperator submitting tasks).
    refs = [
        produce_block.options(scheduling_strategy="SPREAD").remote()
        for _ in range(num_blocks)
    ]
    ray.wait(refs, num_returns=len(refs))

    # Register callbacks (models BlockRefCounter.on_block_produced).
    assert all(
        core_worker.add_object_out_of_scope_callback(ref, on_freed) for ref in refs
    )

    # Pass to consumers (models downstream operator receiving blocks).
    consumer_futures = [consume_block.remote(ref) for ref in refs]
    ray.get(consumer_futures)

    # Drop all references (models blocks going out of scope after consumption).
    del refs
    gc.collect()

    if not done.wait(timeout=timeout_s):
        fired = num_blocks - remaining[0]
        raise TimeoutError(
            f"Only {fired}/{num_blocks} callbacks fired within {timeout_s}s"
        )

    total_s = time.perf_counter() - start
    print(f"  {num_blocks} blocks: {total_s:.3f}s")
    return total_s


ray.init(address="auto")

# Warm up gRPC connections and worker pools.
ray.get(
    [
        produce_block.options(scheduling_strategy="SPREAD").remote()
        for _ in range(NUM_WORKERS)
    ]
)

time_100 = test_callback_pipeline(100)
time_1k = test_callback_pipeline(1000)

print("\nSummary:")
print(f"  100 blocks: {time_100:.3f}s")
print(f"  1k blocks: {time_1k:.3f}s")

if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        results = {
            "time_100": time_100,
            "time_1k": time_1k,
            "perf_metrics": [
                {
                    "perf_metric_name": "callback_pipeline_100_blocks_s",
                    "perf_metric_value": time_100,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": "callback_pipeline_1k_blocks_s",
                    "perf_metric_value": time_1k,
                    "perf_metric_type": "LATENCY",
                },
            ],
        }
        json.dump(results, out_file)
