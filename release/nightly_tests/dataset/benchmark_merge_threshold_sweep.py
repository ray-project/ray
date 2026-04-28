"""Sweep pre_map_merge_threshold for actorless hash shuffle.

Runs 170 GB / 100 partitions with varying merge thresholds.
20-second cooldown between runs.

Usage:
    python benchmark_merge_threshold_sweep.py
"""

import gc
import json
import time
from datetime import datetime

import ray
from ray.data.context import ShuffleStrategy

KEY_COLUMNS = ["column00"]
APPROX_BYTES_PER_ROW = 145
DATA_SIZE_GB = 170
NUM_PARTITIONS = 100
SF = 1000
PATH = f"s3://ray-benchmark-data/tpch/parquet/sf{SF}/lineitem"

THRESHOLDS = [
    ("256MB", 256 * 1024 * 1024),
    ("512MB", 512 * 1024 * 1024),
    ("1GB", 1 * 1024 * 1024 * 1024),
    ("2GB", 2 * 1024 * 1024 * 1024),
    ("4GB", 4 * 1024 * 1024 * 1024),
]


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=180, poll_s=5):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        mem = ray.cluster_resources().get("object_store_memory", 1)
        avail = ray.available_resources().get("object_store_memory", 0)
        used_pct = (1 - avail / mem) * 100 if mem > 0 else 0
        if used_pct < threshold_pct:
            return
        print(f"    draining object store ({used_pct:.0f}% used)...", flush=True)
        time.sleep(poll_s)
    print(f"    drain timed out after {timeout_s}s", flush=True)


def run_one(threshold_name, threshold_bytes):
    target_rows = int(DATA_SIZE_GB * 1024**3 / APPROX_BYTES_PER_ROW)

    ds = ray.data.read_parquet(PATH).limit(target_rows)
    ds.context.shuffle_strategy = ShuffleStrategy.ACTORLESS_HASH_SHUFFLE
    ds.context.set_config("actorless_shuffle_compaction_strategy", "pre_map_merge")
    ds.context.set_config("actorless_shuffle_pre_map_merge_threshold", threshold_bytes)

    repartitioned = ds.repartition(NUM_PARTITIONS, keys=KEY_COLUMNS)

    print(
        f"  [{threshold_name}] read + shuffle -> {NUM_PARTITIONS}p ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    result = repartitioned.materialize()
    elapsed = time.perf_counter() - start

    num_rows = result.count()
    actual_gb = result.size_bytes() / 1024**3

    print(f"{elapsed:.1f}s ({num_rows:,} rows, {actual_gb:.1f} GB)")

    del result, repartitioned, ds
    gc.collect()
    wait_for_object_store_to_drain()

    return {
        "threshold": threshold_name,
        "threshold_bytes": threshold_bytes,
        "elapsed_s": round(elapsed, 1),
        "num_rows": num_rows,
        "actual_gb": round(actual_gb, 2),
    }


def main():
    ray.init()

    cluster = ray.cluster_resources()
    total_cpu = cluster.get("CPU", 0)
    total_mem_gb = cluster.get("memory", 0) / 1e9
    total_obj_gb = cluster.get("object_store_memory", 0) / 1e9

    print(
        f"Cluster: {total_cpu:.0f} CPUs, {total_mem_gb:.0f} GB memory, "
        f"{total_obj_gb:.0f} GB object store"
    )
    print(
        f"Sweep: {DATA_SIZE_GB} GB, {NUM_PARTITIONS} partitions, "
        f"thresholds={[t[0] for t in THRESHOLDS]}"
    )
    print()

    results = []
    for i, (name, threshold_bytes) in enumerate(THRESHOLDS):
        if i > 0:
            print("  cooldown 20s ...", flush=True)
            time.sleep(20)

        try:
            info = run_one(name, threshold_bytes)
            results.append(info)
        except Exception as e:
            print(f"  [{name}] FAILED: {e}")
            results.append(
                {
                    "threshold": name,
                    "threshold_bytes": threshold_bytes,
                    "elapsed_s": None,
                    "error": str(e),
                }
            )

    output_file = "benchmark_merge_threshold_results.json"
    output = {
        "timestamp": datetime.now().isoformat(),
        "data_size_gb": DATA_SIZE_GB,
        "num_partitions": NUM_PARTITIONS,
        "cluster_cpus": total_cpu,
        "results": results,
    }
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults written to {output_file}")
    print()
    print(f"{'Threshold':<12} {'Time':>8} {'Throughput':>12}")
    print("-" * 34)
    for r in results:
        t = r.get("elapsed_s")
        if t:
            tp = f"{r['actual_gb'] / t:.1f} GB/s"
            print(f"{r['threshold']:<12} {t:>7.1f}s {tp:>12}")
        else:
            print(f"{r['threshold']:<12} {'FAILED':>8}")


if __name__ == "__main__":
    main()
