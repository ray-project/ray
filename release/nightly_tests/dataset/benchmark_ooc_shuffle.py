"""Benchmark: Out-of-core shuffle capacity.

Determines how much data the hash shuffle can handle before spilling
degrades performance or causes failures. Supports both the original
actor-based hash shuffle and the actorless hash shuffle.

Cluster assumption: 16 worker nodes, 8 CPU / 32 GB each.
  - Total memory: 512 GB
  - Object store (50%): 256 GB
  - In-core shuffle limit (x/3): ~85 GB

Data: TPC-H lineitem from S3, limited to target row count.
Times the full read -> shuffle -> materialize pipeline.

Tests a single (data_size, num_partitions, strategy) combination.
Each combination runs as an independent release test so one OOM
does not affect others.

Usage:
    python benchmark_ooc_shuffle.py --data-size-gb 50 --num-partitions 200
    python benchmark_ooc_shuffle.py --data-size-gb 50 --num-partitions 200 --strategy actor
    python benchmark_ooc_shuffle.py --data-size-gb 256 --num-partitions 500 --strategy actorless
"""

import argparse
import gc
import json
import time
from datetime import datetime

import ray
from ray.data.context import ShuffleStrategy

STRATEGY_MAP = {
    "actorless": ShuffleStrategy.ACTORLESS_HASH_SHUFFLE,
    "actor": ShuffleStrategy.HASH_SHUFFLE,
}

KEY_COLUMNS = ["column00"]  # l_orderkey

# Approximate bytes per row for TPC-H lineitem in-memory (Arrow).
APPROX_BYTES_PER_ROW = 145


def pick_sf(data_size_gb):
    """Pick the smallest TPC-H scale factor that has enough data."""
    if data_size_gb <= 70:
        return 100
    return 1000


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


def run_one(data_size_gb, num_partitions, strategy_name="actorless"):
    """Run a single shuffle and return timing + status info."""
    sf = pick_sf(data_size_gb)
    target_rows = int(data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"

    ds = ray.data.read_parquet(path).limit(target_rows)

    ds.context.shuffle_strategy = STRATEGY_MAP[strategy_name]
    if strategy_name == "actorless":
        ds.context.set_config("actorless_shuffle_compaction_strategy", "pre_map_merge")
        ds.context.set_config(
            "actorless_shuffle_pre_map_merge_threshold", 1024 * 1024 * 1024
        )  # 1 GB

    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)

    print(
        f"  [ray] read(sf{sf}, limit {target_rows:,}) + shuffle -> "
        f"{num_partitions} partitions ({strategy_name}) ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    result = repartitioned.materialize()
    elapsed = time.perf_counter() - start

    num_rows = result.count()
    actual_bytes = result.size_bytes()
    actual_gb = actual_bytes / 1024**3

    print(f"{elapsed:.1f}s ({num_rows:,} rows, {actual_gb:.1f} GB)")

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
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["actorless", "actor"],
        default="actorless",
        help="Shuffle strategy: 'actorless' (default) or 'actor' (original)",
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
    strategy_name = args.strategy

    print(
        f"Cluster: {total_cpu:.0f} CPUs, {total_mem_gb:.0f} GB memory, "
        f"{total_obj_gb:.0f} GB object store"
    )
    print(f"Estimated in-core shuffle limit (obj_store/3): {in_core_limit_gb:.0f} GB")
    print(
        f"Test: {data_size_gb} GB, {num_partitions} partitions, strategy={strategy_name}"
    )
    print()

    ratio = data_size_gb / in_core_limit_gb if in_core_limit_gb > 0 else float("inf")
    zone = "IN-CORE" if ratio <= 1.0 else "SPILL"
    print(
        f"--- {data_size_gb} GB, {num_partitions} partitions, {strategy_name} "
        f"({ratio:.1f}x of in-core limit, {zone}) ---"
    )

    info = run_one(data_size_gb, num_partitions, strategy_name=strategy_name)

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
            "strategy": strategy_name,
            "ratio_to_in_core_limit": round(ratio, 2),
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
