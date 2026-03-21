"""Benchmark matrix: Actor vs Actorless shuffle across SF x partition combinations.

Cluster: 32 worker nodes, 8 CPU / 32 GB each (256 CPUs total).

Usage:
    python benchmark_shuffle_matrix.py
    python benchmark_shuffle_matrix.py --output results.json
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

BENCHMARK_MATRIX = [
    # (sf, num_partitions)
    (10, 100),
    (10, 500),
    (10, 1000),
    (100, 100),
    (100, 500),
    (100, 1000),
]

STRATEGIES = [
    (ShuffleStrategy.HASH_SHUFFLE, "actor"),
    (ShuffleStrategy.ACTORLESS_HASH_SHUFFLE, "actorless"),
    (ShuffleStrategy.ACTORLESS_HASH_SHUFFLE, "actorless_pre_map_merge"),
]

KEY_COLUMNS = ["column00"]  # l_orderkey


def load_dataset(sf):
    """Read and materialize the dataset once per SF so both strategies share it."""
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    print(f"Loading sf={sf} from S3 ... ", end="", flush=True)
    ds = ray.data.read_parquet(path).materialize()
    print(f"done ({ds.count():,} rows, {ds.num_blocks()} blocks)")
    return ds


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


def run_one(ds, sf, num_partitions, strategy, label):
    """Run a single repartition and return elapsed time and row count."""
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
    print(f"  [{label}] sf={sf}, partitions={num_partitions} ... ", end="", flush=True)

    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)
    repartitioned.set_name(name)

    start = time.perf_counter()
    result = repartitioned.materialize()
    elapsed = time.perf_counter() - start

    num_rows = result.count()
    print(f"{elapsed:.1f}s ({num_rows:,} rows)")

    # Clean up shuffle output and wait for object store to settle.
    del result, repartitioned
    gc.collect()
    wait_for_object_store_to_drain()

    return elapsed, num_rows


def main():
    parser = argparse.ArgumentParser(description="Shuffle Benchmark Matrix")
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_shuffle_results.json",
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help="Number of runs per (sf, partitions, strategy) combo",
    )
    args = parser.parse_args()

    ray.init()

    cluster = ray.cluster_resources()
    print(
        f"Cluster: {cluster.get('CPU', 0):.0f} CPUs, "
        f"{cluster.get('memory', 0) / 1e9:.0f} GB memory"
    )
    print(
        f"Matrix: {len(BENCHMARK_MATRIX)} configs x {len(STRATEGIES)} strategies "
        f"x {args.num_runs} runs"
    )
    print()

    results = []

    def flush_results():
        output = {
            "timestamp": datetime.now().isoformat(),
            "cluster": {
                "num_workers": 32,
                "cpus_per_worker": 8,
                "memory_per_worker_gb": 32,
            },
            "results": results,
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)

    # Group matrix by SF so we load each dataset only once.
    from itertools import groupby

    for sf, group in groupby(BENCHMARK_MATRIX, key=lambda x: x[0]):
        partition_list = [num_partitions for _, num_partitions in group]
        ds = load_dataset(sf)

        for num_partitions in partition_list:
            print(f"--- sf={sf}, partitions={num_partitions} ---")

            for strategy, label in STRATEGIES:
                times = []
                num_rows = 0

                for run in range(args.num_runs):
                    try:
                        elapsed, num_rows = run_one(
                            ds, sf, num_partitions, strategy, label
                        )
                        times.append(elapsed)
                    except Exception as e:
                        print(f"  [{label}] FAILED: {e}")
                        times.append(None)

                valid_times = [t for t in times if t is not None]
                entry = {
                    "sf": sf,
                    "num_partitions": num_partitions,
                    "strategy": label,
                    "num_rows": num_rows,
                    "times": times,
                    "avg_time": sum(valid_times) / len(valid_times)
                    if valid_times
                    else None,
                    "best_time": min(valid_times) if valid_times else None,
                }
                results.append(entry)
                flush_results()

            print()

        # Free the base dataset before loading the next SF.
        del ds
        gc.collect()

    print(f"Results written to {args.output}")

    # Print summary table.
    num_strategies = len(STRATEGIES)
    print()
    header = f"{'SF':>6} {'Partitions':>12}"
    for _, label in STRATEGIES:
        header += f" {label + ' (s)':>20}"
    print(header)
    print("-" * len(header))

    for i in range(0, len(results), num_strategies):
        row_results = results[i : i + num_strategies]
        line = f"{row_results[0]['sf']:>6} {row_results[0]['num_partitions']:>12}"
        for entry in row_results:
            t = entry["avg_time"]
            t_str = f"{t:.1f}" if t else "FAIL"
            line += f" {t_str:>20}"
        print(line)


if __name__ == "__main__":
    main()
