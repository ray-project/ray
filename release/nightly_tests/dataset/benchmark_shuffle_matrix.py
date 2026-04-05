"""Benchmark: Hash shuffle for a single (sf, num_partitions, strategy) combination.

Cluster: 32 worker nodes, 8 CPU / 32 GB each (256 CPUs total).

Usage:
    python benchmark_shuffle_matrix.py --sf 10 --num-partitions 1000 --strategy actorless
    python benchmark_shuffle_matrix.py --sf 100 --num-partitions 100 --strategy actor
    python benchmark_shuffle_matrix.py --sf 10 --num-partitions 1000 --strategy actorless_pre_map_merge
"""

import argparse
import gc
import json
import os
import time
from datetime import datetime

# Set object store to 50% of available memory before Ray starts.
os.environ.setdefault("RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION", "0.5")

import ray
from ray.data.context import ShuffleStrategy

STRATEGY_MAP = {
    "actor": (ShuffleStrategy.HASH_SHUFFLE, "actor"),
    "actorless": (ShuffleStrategy.ACTORLESS_HASH_SHUFFLE, "actorless"),
    "actorless_pre_map_merge": (
        ShuffleStrategy.ACTORLESS_HASH_SHUFFLE,
        "actorless_pre_map_merge",
    ),
}

KEY_COLUMNS = ["column00"]  # l_orderkey


def load_dataset(sf):
    """Return a lazy dataset (no materialization)."""
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    return ray.data.read_parquet(path)


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=120, poll_s=5):
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


def run_one(sf, num_partitions, strategy, label):
    """Run a single repartition and return timing + status info."""
    ds = load_dataset(sf)

    ds.context.shuffle_strategy = strategy
    if strategy == ShuffleStrategy.ACTORLESS_HASH_SHUFFLE:
        ds.context.override_object_store_memory_limit_fraction = 0.5

    # Configure pre-map merge when requested.
    if label == "actorless_pre_map_merge":
        ds.context.set_config("actorless_shuffle_compaction_strategy", "pre_map_merge")
        ds.context.set_config(
            "actorless_shuffle_pre_map_merge_threshold", 1024 * 1024 * 1024
        )  # 1 GB
    else:
        ds.context.set_config("actorless_shuffle_compaction_strategy", "none")

    name = f"sf{sf}_{label}_p{num_partitions}"
    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)
    repartitioned.set_name(name)

    print(
        f"  Shuffling sf{sf} -> {num_partitions} partitions ({label}) ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    result = repartitioned.materialize()
    elapsed = time.perf_counter() - start

    num_rows = result.count()
    print(f"{elapsed:.1f}s ({num_rows:,} rows)")

    del result, repartitioned, ds
    gc.collect()
    wait_for_object_store_to_drain()

    return {
        "elapsed_s": elapsed,
        "num_rows": num_rows,
        "status": "ok",
    }


def main():
    parser = argparse.ArgumentParser(description="Shuffle Benchmark")
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_shuffle_results.json",
        help="Output JSON file",
    )
    parser.add_argument(
        "--sf",
        type=int,
        required=True,
        help="TPC-H scale factor (10, 100, 1000)",
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
        choices=list(STRATEGY_MAP.keys()),
        required=True,
        help="Shuffle strategy",
    )
    args = parser.parse_args()

    ray.init()

    cluster = ray.cluster_resources()
    total_cpu = cluster.get("CPU", 0)
    total_mem_gb = cluster.get("memory", 0) / 1e9
    total_obj_gb = cluster.get("object_store_memory", 0) / 1e9

    sf = args.sf
    num_partitions = args.num_partitions
    strategy, label = STRATEGY_MAP[args.strategy]

    print(
        f"Cluster: {total_cpu:.0f} CPUs, {total_mem_gb:.0f} GB memory, "
        f"{total_obj_gb:.0f} GB object store"
    )
    print(f"Test: sf{sf}, {num_partitions} partitions, strategy={label}")
    print()

    info = run_one(sf, num_partitions, strategy, label)

    result = {
        "timestamp": datetime.now().isoformat(),
        "cluster": {
            "num_cpus": total_cpu,
            "total_memory_gb": round(total_mem_gb, 1),
            "object_store_gb": round(total_obj_gb, 1),
        },
        "config": {
            "sf": sf,
            "num_partitions": num_partitions,
            "strategy": label,
        },
        **info,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nResults written to {args.output}")
    t = info["elapsed_s"]
    tp = f"{info['num_rows'] / t / 1e6:.1f} Mrows/s" if t and t > 0 else "N/A"
    print(f"  sf{sf}, {num_partitions} partitions, {label}: {t:.1f}s, {tp}")


if __name__ == "__main__":
    main()
