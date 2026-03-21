"""Benchmark: Daft hash shuffle on Ray cluster.

Compare with Ray Data's actor/actorless shuffle using the same
TPC-H lineitem dataset and partition counts.

Cluster: 32 worker nodes, 8 CPU / 32 GB each (256 CPUs total).

Requirements:
    pip install "daft[ray]"

Usage:
    python benchmark_daft_shuffle.py
    python benchmark_daft_shuffle.py --output daft_results.json
"""

import argparse
import gc
import json
import time
from datetime import datetime

import ray

BENCHMARK_MATRIX = [
    # (sf, num_partitions)
    (10, 100),
    (10, 500),
    (10, 1000),
    (100, 100),
    (100, 1000),
]

KEY_COLUMNS = ["column00"]  # l_orderkey


def load_dataset(sf):
    """Read the dataset with Daft and collect it so both runs share data."""
    import daft

    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    print(f"Loading sf={sf} from S3 with Daft ... ", end="", flush=True)
    df = daft.read_parquet(path).collect()
    num_rows = len(df)
    num_partitions = df.num_partitions()
    print(f"done ({num_rows:,} rows, {num_partitions} partitions)")
    return df


def run_one(df, sf, num_partitions):
    """Run a single Daft repartition and return elapsed time and row count."""
    print(f"  [daft] sf={sf}, partitions={num_partitions} ... ", end="", flush=True)

    start = time.perf_counter()
    result = df.repartition(num_partitions, *KEY_COLUMNS).collect()
    elapsed = time.perf_counter() - start

    num_rows = len(result)
    print(f"{elapsed:.1f}s ({num_rows:,} rows)")

    del result
    gc.collect()

    return elapsed, num_rows


def main():
    parser = argparse.ArgumentParser(description="Daft Shuffle Benchmark")
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_daft_results.json",
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help="Number of runs per (sf, partitions) combo",
    )
    args = parser.parse_args()

    # Initialize Ray and Daft.
    ray.init()
    import daft

    daft.set_runner_ray()

    cluster = ray.cluster_resources()
    print(
        f"Cluster: {cluster.get('CPU', 0):.0f} CPUs, "
        f"{cluster.get('memory', 0) / 1e9:.0f} GB memory"
    )
    print(f"Matrix: {len(BENCHMARK_MATRIX)} configs x {args.num_runs} runs")
    print()

    results = []

    def flush_results():
        output = {
            "timestamp": datetime.now().isoformat(),
            "engine": "daft",
            "cluster": {
                "num_workers": 32,
                "cpus_per_worker": 8,
                "memory_per_worker_gb": 32,
            },
            "results": results,
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)

    # Group by SF to load dataset once per SF.
    from itertools import groupby

    for sf, group in groupby(BENCHMARK_MATRIX, key=lambda x: x[0]):
        partition_list = [num_partitions for _, num_partitions in group]
        df = load_dataset(sf)

        for num_partitions in partition_list:
            print(f"--- sf={sf}, partitions={num_partitions} ---")

            times = []
            num_rows = 0

            for run in range(args.num_runs):
                try:
                    elapsed, num_rows = run_one(df, sf, num_partitions)
                    times.append(elapsed)
                except Exception as e:
                    print(f"  [daft] FAILED: {e}")
                    times.append(None)

            valid_times = [t for t in times if t is not None]
            entry = {
                "sf": sf,
                "num_partitions": num_partitions,
                "strategy": "daft",
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

        del df
        gc.collect()

    print(f"Results written to {args.output}")

    # Print summary table.
    print()
    print(f"{'SF':>6} {'Partitions':>12} {'Daft (s)':>12}")
    print("-" * 35)

    for entry in results:
        t = entry["avg_time"]
        t_str = f"{t:.1f}" if t else "FAIL"
        print(f"{entry['sf']:>6} {entry['num_partitions']:>12} {t_str:>12}")


if __name__ == "__main__":
    main()
