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

    latencies = []
    drop_times = {}
    lock = threading.Lock()
    done = threading.Event()

    def on_freed(id_bytes):
        with lock:
            latencies.append(time.perf_counter() - drop_times[id_bytes])
            if len(latencies) == num_blocks:
                done.set()

    refs = [
        produce_block.options(scheduling_strategy="SPREAD").remote()
        for _ in range(num_blocks)
    ]
    ray.wait(refs, num_returns=len(refs))

    # live_refs keeps each block ref alive until its consumer completes.
    live_refs = {}
    for ref in refs:
        assert core_worker.add_object_out_of_scope_callback(ref, on_freed)
        consumer = consume_block.remote(ref)
        live_refs[consumer] = ref
    del refs

    # Release each ref as its consumer completes.
    pending = list(live_refs.keys())
    while pending:
        done_list, pending = ray.wait(pending, num_returns=1)
        for consumer in done_list:
            ref = live_refs.pop(consumer)
            drop_times[ref.binary()] = time.perf_counter()
            del ref

    if not done.wait(timeout=timeout_s):
        raise TimeoutError(
            f"Only {len(latencies)}/{num_blocks} callbacks fired within {timeout_s}s"
        )

    avg_latency = sum(latencies) / len(latencies)
    print(f"  {num_blocks} blocks: avg={avg_latency:.4f}s")
    return avg_latency


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
