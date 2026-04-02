"""Benchmark: Out-of-core shuffle capacity for actorless hash shuffle.

Determines how much data the actorless hash shuffle can handle before
spilling degrades performance or causes failures.

Cluster assumption: 16 worker nodes, 8 CPU / 32 GB each.
  - Total memory: 512 GB
  - Object store (50%): 256 GB
  - In-core shuffle limit (x/3): ~85 GB

Tests a single (data_size, num_partitions) combination using synthesized
data (random int columns). Each combination runs as an independent release
test so one OOM does not affect others.

Usage:
    python benchmark_out_of_core_shuffle.py --data-size-gb 50 --num-partitions 200
    python benchmark_out_of_core_shuffle.py --data-size-gb 256 --num-partitions 500
"""

import argparse
import gc
import json
import os
import time
from datetime import datetime

os.environ.setdefault("RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION", "0.5")

import ray
from ray.data.context import ShuffleStrategy

# (data_size_gb, num_partitions) combinations to test.
# Data sizes cover: in-core -> boundary -> mild spill -> heavy spill -> full obj store.
# Partition counts: low (stress per-reducer memory), medium, high (many small shards).
BENCHMARK_MATRIX = [
    # (data_size_gb, num_partitions)
    # --- In-core (50 GB, ~0.6x of limit) ---
    (50, 100),
    (50, 200),
    (50, 500),
    # --- Boundary (85 GB, ~1.0x of limit) ---
    (85, 100),
    (85, 200),
    (85, 500),
    # --- Mild spill (120 GB, ~1.4x of limit) ---
    (120, 200),
    (120, 500),
    # --- Moderate spill (170 GB, ~2.0x of limit) ---
    (170, 200),
    (170, 500),
    # --- Full object store (256 GB, ~3.0x of limit) ---
    (256, 200),
    (256, 500),
]

NUM_KEY_COLUMNS = 1
NUM_VALUE_COLUMNS = 9
KEY_COLUMNS = ["key_0"]

# Each row: NUM_KEY_COLUMNS + NUM_VALUE_COLUMNS int64 columns = 10 * 8 = 80 bytes
BYTES_PER_ROW = (NUM_KEY_COLUMNS + NUM_VALUE_COLUMNS) * 8


def generate_dataset(target_bytes, num_map_tasks=128):
    """Create a synthetic dataset of approximately target_bytes using Ray Data."""
    total_rows = target_bytes // BYTES_PER_ROW
    rows_per_task = total_rows // num_map_tasks

    def make_block(task_idx):
        import pyarrow as pa
        import numpy as np

        rng = np.random.default_rng(seed=int(task_idx))
        columns = {}
        for i in range(NUM_KEY_COLUMNS):
            columns[f"key_{i}"] = rng.integers(
                0, 2**31, size=rows_per_task, dtype=np.int64
            )
        for i in range(NUM_VALUE_COLUMNS):
            columns[f"val_{i}"] = rng.integers(
                0, 2**63, size=rows_per_task, dtype=np.int64
            )
        return pa.table(columns)

    ds = ray.data.from_items(
        list(range(num_map_tasks)), override_num_blocks=num_map_tasks
    )
    ds = ds.map_batches(
        lambda batch: make_block(batch["item"][0]),
        batch_size=1,
        batch_format="pyarrow",
    )
    return ds


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=180, poll_s=5):
    """Wait until object store utilization drops below threshold."""
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        mem = ray.cluster_resources().get("object_store_memory", 1)
        avail = ray.available_resources().get("object_store_memory", 0)
        used_pct = (1 - avail / mem) * 100 if mem > 0 else 0
        if used_pct < threshold_pct:
            return
        print(
            f"    waiting for object store to drain ({used_pct:.0f}% used)...",
            flush=True,
        )
        time.sleep(poll_s)
    print(f"    object store drain timed out after {timeout_s}s", flush=True)


def run_one(data_size_gb, num_partitions, num_map_tasks=128):
    """Run a single shuffle and return timing + status info."""
    target_bytes = int(data_size_gb * 1024**3)

    print(f"  Generating ~{data_size_gb} GB synthetic data...", flush=True)
    ds = generate_dataset(target_bytes, num_map_tasks=num_map_tasks)

    ds.context.shuffle_strategy = ShuffleStrategy.ACTORLESS_HASH_SHUFFLE
    ds.context.override_object_store_memory_limit_fraction = 0.5

    name = f"ooc_{data_size_gb}gb_p{num_partitions}"
    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)
    repartitioned.set_name(name)

    print(
        f"  Shuffling {data_size_gb} GB -> {num_partitions} partitions ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    result = repartitioned.materialize()
    elapsed = time.perf_counter() - start

    num_rows = result.count()
    actual_bytes = result.size_bytes()
    actual_gb = actual_bytes / 1024**3

    print(f"{elapsed:.1f}s ({num_rows:,} rows, {actual_gb:.1f} GB actual)")

    del result, repartitioned, ds
    gc.collect()
    wait_for_object_store_to_drain()

    return {
        "elapsed_s": elapsed,
        "num_rows": num_rows,
        "actual_gb": round(actual_gb, 2),
        "status": "ok",
    }


def main():
    parser = argparse.ArgumentParser(description="Out-of-core shuffle benchmark")
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_ooc_shuffle_results.json",
        help="Output JSON file",
    )
    parser.add_argument(
        "--data-size-gb",
        type=int,
        required=True,
        help="Data size in GB to shuffle",
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        required=True,
        help="Number of output partitions",
    )
    args = parser.parse_args()

    ray.init()

    cluster = ray.cluster_resources()
    total_cpu = cluster.get("CPU", 0)
    total_mem_gb = cluster.get("memory", 0) / 1e9
    total_obj_gb = cluster.get("object_store_memory", 0) / 1e9
    in_core_limit_gb = total_obj_gb / 3

    data_size_gb = args.data_size_gb
    num_partitions = args.num_partitions

    print(
        f"Cluster: {total_cpu:.0f} CPUs, {total_mem_gb:.0f} GB memory, "
        f"{total_obj_gb:.0f} GB object store"
    )
    print(f"Estimated in-core shuffle limit (obj_store/3): {in_core_limit_gb:.0f} GB")
    print(f"Test: {data_size_gb} GB, {num_partitions} partitions")
    print()

    ratio = data_size_gb / in_core_limit_gb if in_core_limit_gb > 0 else float("inf")
    zone = "IN-CORE" if ratio <= 1.0 else "SPILL"
    print(
        f"--- {data_size_gb} GB, {num_partitions} partitions "
        f"({ratio:.1f}x of in-core limit, {zone}) ---"
    )

    info = run_one(data_size_gb, num_partitions)

    result = {
        "timestamp": datetime.now().isoformat(),
        "cluster": {
            "num_cpus": total_cpu,
            "total_memory_gb": round(total_mem_gb, 1),
            "object_store_gb": round(total_obj_gb, 1),
            "in_core_limit_gb": round(in_core_limit_gb, 1),
        },
        "config": {
            "data_size_gb": data_size_gb,
            "num_partitions": num_partitions,
            "ratio_to_in_core_limit": round(ratio, 2),
            "bytes_per_row": BYTES_PER_ROW,
        },
        **info,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nResults written to {args.output}")
    t = info["elapsed_s"]
    tp = f"{info['actual_gb'] / t:.1f} GB/s" if t and t > 0 else "N/A"
    print(f"  {data_size_gb} GB, {num_partitions} partitions: {t:.1f}s, {tp}")


if __name__ == "__main__":
    main()
