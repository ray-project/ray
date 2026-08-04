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

    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95)]
    print(f"  {num_blocks} blocks: p95={p95:.4f}s")
    return p95


ray.init(address="auto")

# Warm up gRPC connections and worker pools.
ray.get(
    [
        produce_block.options(scheduling_strategy="SPREAD").remote()
        for _ in range(NUM_WORKERS)
    ]
)

p95_100 = test_callback_pipeline(100)
p95_1k = test_callback_pipeline(1000)

print("\nSummary:")
print(f"  100 blocks: p95={p95_100:.4f}s")
print(f"  1k blocks: p95={p95_1k:.4f}s")

if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        results = {
            "p95_100": p95_100,
            "p95_1k": p95_1k,
            "perf_metrics": [
                {
                    "perf_metric_name": "callback_p95_latency_100_blocks_s",
                    "perf_metric_value": p95_100,
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": "callback_p95_latency_1k_blocks_s",
                    "perf_metric_value": p95_1k,
                    "perf_metric_type": "LATENCY",
                },
            ],
        }
        json.dump(results, out_file)
