import json
import os
import threading
import time

import numpy as np

import ray
import ray._private.worker
from ray.data._internal.execution.block_ref_counter import BlockRefCounter

NUM_WORKERS = 10
OBJECT_SIZE = 1024 * 1024  # 1 MiB, above the 100 KB inlining threshold


@ray.remote(num_cpus=1)
def produce_block():
    return np.zeros(OBJECT_SIZE, dtype=np.uint8)


@ray.remote(num_cpus=1)
def consume_block(block):
    return None


def _produce_blocks(num_blocks):
    """Create num_blocks plasma objects spread across the cluster."""
    refs = [
        produce_block.options(scheduling_strategy="SPREAD").remote()
        for _ in range(num_blocks)
    ]
    ray.wait(refs, num_returns=len(refs))
    return refs


def _compute_latencies(fire_times, drop_times, id_binaries):
    """Compute sorted per-callback latencies from fire timestamps.

    drop_times can be a float (single timestamp for all blocks) or a
    dict mapping id_binary -> per-block drop timestamp.
    """
    if isinstance(drop_times, dict):
        latencies = [fire_times[id_b] - drop_times[id_b] for id_b in id_binaries]
    else:
        latencies = [fire_times[id_b] - drop_times for id_b in id_binaries]
    latencies.sort()
    return {
        "p50": latencies[int(len(latencies) * 0.50)],
        "p95": latencies[int(len(latencies) * 0.95)],
        "p99": latencies[int(len(latencies) * 0.99)],
        "max": latencies[-1],
    }


def _make_timing_callback(num_blocks):
    """Create a callback that records fire timestamps and signals completion.

    Returns (callback, fire_times, done_event).
    """
    fire_times = {}
    lock = threading.Lock()
    done = threading.Event()

    def on_freed(id_bytes):
        t = time.perf_counter()
        with lock:
            fire_times[id_bytes] = t
            if len(fire_times) == num_blocks:
                done.set()

    return on_freed, fire_times, done


def test_callback_pipeline(num_blocks, timeout_s=300):
    """Incremental produce-consume-release pipeline.

    Measures p95 latency from ref drop to callback fire,
    with one block released at a time as its consumer completes.
    """
    core_worker = ray._private.worker.global_worker.core_worker
    on_freed, fire_times, done = _make_timing_callback(num_blocks)

    refs = _produce_blocks(num_blocks)

    live_refs = {}
    for ref in refs:
        assert core_worker.add_object_out_of_scope_callback(ref, on_freed)
        live_refs[consume_block.remote(ref)] = ref
    del refs

    # Release each ref as its consumer completes.
    drop_times = {}
    pending = list(live_refs.keys())
    while pending:
        done_list, pending = ray.wait(pending, num_returns=1)
        for consumer in done_list:
            ref = live_refs.pop(consumer)
            drop_times[ref.binary()] = time.perf_counter()
            del ref

    if not done.wait(timeout=timeout_s):
        raise TimeoutError(
            f"Only {len(fire_times)}/{num_blocks} callbacks fired within {timeout_s}s"
        )

    id_binaries = list(fire_times.keys())
    result = _compute_latencies(fire_times, drop_times, id_binaries)
    print(
        f"  {num_blocks} blocks: "
        f"p50={result['p50']:.4f}s p95={result['p95']:.4f}s max={result['max']:.4f}s"
    )
    return result


def test_registration_cost(num_blocks):
    """Measures per-callback registration cost via BlockRefCounter.on_block_produced.

    Includes BRC bookkeeping and the Core API call to register the callback.
    """
    counter = BlockRefCounter()
    refs = _produce_blocks(num_blocks)

    start = time.perf_counter()
    for ref in refs:
        counter.on_block_produced(ref, OBJECT_SIZE, "bench_op")
    elapsed = time.perf_counter() - start

    per_callback_us = (elapsed / num_blocks) * 1e6
    print(
        f"  {num_blocks} registrations: {elapsed:.4f}s total, {per_callback_us:.1f}us each"
    )

    del refs, ref
    return per_callback_us


def test_burst_drop(num_blocks, timeout_s=60):
    """All refs dropped at once, 1 Core API callback per block (no BlockRefCounter).

    Measures time from burst start to each callback firing. The max
    latency approximates total drain time (how long the burst hangs).
    """
    core_worker = ray._private.worker.global_worker.core_worker
    on_freed, fire_times, done = _make_timing_callback(num_blocks)

    refs = _produce_blocks(num_blocks)
    for ref in refs:
        assert core_worker.add_object_out_of_scope_callback(ref, on_freed)

    id_binaries = [ref.binary() for ref in refs]
    drop_time = time.perf_counter()
    del refs, ref

    if not done.wait(timeout=timeout_s):
        raise TimeoutError(
            f"Only {len(fire_times)}/{num_blocks} callbacks fired within {timeout_s}s"
        )

    result = _compute_latencies(fire_times, drop_time, id_binaries)
    print(
        f"  burst {num_blocks} blocks: "
        f"p50={result['p50']:.4f}s p95={result['p95']:.4f}s "
        f"p99={result['p99']:.4f}s max={result['max']:.4f}s"
    )
    return result


def test_burst_drop_per_callback(num_blocks, timeout_s=60):
    """Drops blocks one at a time with per-block timestamps.

    Measures true per-callback latency (each block's drop time to its
    callback fire time). More authentic than test_burst_drop for the
    LIMIT scenario, where the executor drains queues in a loop.
    """
    core_worker = ray._private.worker.global_worker.core_worker
    on_freed, fire_times, done = _make_timing_callback(num_blocks)

    refs = _produce_blocks(num_blocks)
    for ref in refs:
        assert core_worker.add_object_out_of_scope_callback(ref, on_freed)

    # Drop blocks one at a time, capturing per-block drop timestamps.
    id_binaries = []
    drop_times = {}
    for i in range(len(refs)):
        ref = refs[i]
        refs[i] = None
        id_b = ref.binary()
        id_binaries.append(id_b)
        drop_times[id_b] = time.perf_counter()
        del ref
    del refs

    if not done.wait(timeout=timeout_s):
        raise TimeoutError(
            f"Only {len(fire_times)}/{num_blocks} callbacks fired within {timeout_s}s"
        )

    result = _compute_latencies(fire_times, drop_times, id_binaries)
    print(
        f"  per-callback {num_blocks} blocks: "
        f"p50={result['p50']:.4f}s p95={result['p95']:.4f}s "
        f"p99={result['p99']:.4f}s max={result['max']:.4f}s"
    )
    return result


def test_burst_drop_block_ref_counter(num_blocks, timeout_s=60):
    """Burst drop through BlockRefCounter (the real Data-layer path).

    Registers callbacks via on_block_produced (which internally registers
    a Core callback), then registers a second Core callback for timing.
    Both fire on the same single-threaded callback service in registration
    order, so the timing callback's latency includes the BlockRefCounter
    callback that fires before it.
    """
    core_worker = ray._private.worker.global_worker.core_worker
    counter = BlockRefCounter()
    on_freed, fire_times, done = _make_timing_callback(num_blocks)

    refs = _produce_blocks(num_blocks)

    for ref in refs:
        counter.on_block_produced(ref, OBJECT_SIZE, "bench_op")
    for ref in refs:
        assert core_worker.add_object_out_of_scope_callback(ref, on_freed)

    id_binaries = [ref.binary() for ref in refs]
    drop_time = time.perf_counter()
    del refs, ref

    if not done.wait(timeout=timeout_s):
        raise TimeoutError(
            f"Only {len(fire_times)}/{num_blocks} callbacks fired within {timeout_s}s"
        )

    result = _compute_latencies(fire_times, drop_time, id_binaries)
    print(
        f"  burst {num_blocks} blocks (BRC): "
        f"p50={result['p50']:.4f}s p95={result['p95']:.4f}s max={result['max']:.4f}s"
    )
    return result


ray.init(address="auto")

ray.get(
    [
        produce_block.options(scheduling_strategy="SPREAD").remote()
        for _ in range(NUM_WORKERS)
    ]
)

# Let callback service thread drain between tests to avoid interference.
DRAIN_DELAY_S = 1.0

# Scales to test. Higher values reveal whether per-callback cost is constant
# or grows with N (due to GIL contention, queue growth, etc.).
SCALES = [100, 1000, 5000, 10000]


def _run_at_scales(name, test_fn, scales):
    print(f"\n=== {name} ===")
    results = {}
    for n in scales:
        results[n] = test_fn(n)
    time.sleep(DRAIN_DELAY_S)
    return results


reg = _run_at_scales("Registration cost", test_registration_cost, SCALES)
pipeline = _run_at_scales("Incremental pipeline", test_callback_pipeline, SCALES)
burst = _run_at_scales("Burst drop (total drain time)", test_burst_drop, SCALES)
per_cb = _run_at_scales(
    "Burst drop (per-callback latency)", test_burst_drop_per_callback, SCALES
)
brc = _run_at_scales(
    "Burst drop (BlockRefCounter)", test_burst_drop_block_ref_counter, SCALES
)

print("\n=== Scaling summary (p95) ===")
header = "  {:25s}" + " {:>10s}" * len(SCALES)
print(header.format("Test", *[f"{n}" for n in SCALES]))
for name, results in [
    ("Registration (us/cb)", reg),
    ("Burst drain", burst),
    ("Per-callback", per_cb),
    ("BRC", brc),
]:
    vals = []
    for n in SCALES:
        if n not in results:
            vals.append("--")
        elif isinstance(results[n], dict):
            vals.append(f"{results[n]['p95']:.4f}s")
        else:
            vals.append(f"{results[n]:.1f}")
    print(header.format(name, *vals))

print(
    "\n  Pipeline p95: " + ", ".join(f"{pipeline[n]['p95']:.4f}s ({n})" for n in SCALES)
)

if "TEST_OUTPUT_JSON" in os.environ:
    perf_metrics = [
        {
            "perf_metric_name": "callback_p95_latency_1k_blocks_s",
            "perf_metric_value": pipeline[1000]["p95"],
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": "callback_registration_cost_s",
            "perf_metric_value": reg[1000] / 1e6,
            "perf_metric_type": "LATENCY",
        },
    ]
    for n in SCALES:
        perf_metrics.extend(
            [
                {
                    "perf_metric_name": f"callback_burst_drain_p95_{n}_blocks_s",
                    "perf_metric_value": burst[n]["p95"],
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": f"callback_per_callback_p95_{n}_blocks_s",
                    "perf_metric_value": per_cb[n]["p95"],
                    "perf_metric_type": "LATENCY",
                },
                {
                    "perf_metric_name": f"callback_burst_brc_p95_{n}_blocks_s",
                    "perf_metric_value": brc[n]["p95"],
                    "perf_metric_type": "LATENCY",
                },
            ]
        )
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        json.dump({"perf_metrics": perf_metrics}, out_file)
