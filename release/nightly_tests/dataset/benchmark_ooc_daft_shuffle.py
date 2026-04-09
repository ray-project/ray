"""Benchmark: Out-of-core Daft shuffle on Ray cluster.

Compare with Ray Data's actorless shuffle using the same cluster
configuration and partition counts.

Cluster assumption: 16 worker nodes, 8 CPU / 30 GB each.
  - Total memory: ~480 GB
  - Object store (50%): ~240 GB
  - In-core shuffle limit (x/3): ~80 GB

Data: TPC-H lineitem from S3, limited to target row count.

Requirements:
    pip install "daft[ray]"

Usage:
    python benchmark_ooc_daft_shuffle.py --data-size-gb 50 --num-partitions 200
    python benchmark_ooc_daft_shuffle.py --data-size-gb 256 --num-partitions 1000
"""

import argparse
import gc
import json
import shutil
import time
from datetime import datetime

import ray

KEY_COLUMNS = ["column00"]  # l_orderkey

# Approximate bytes per row for TPC-H lineitem in-memory (Arrow).
# Measured empirically; used to convert --data-size-gb to a row limit.
APPROX_BYTES_PER_ROW = 128

# Local temp dir for shuffle output (forces full materialization).
OUTPUT_DIR = "/tmp/ooc_bench_daft_output"


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


def run_one(data_size_gb, num_partitions):
    """Run a single Daft repartition and return timing + status info."""
    import daft

    sf = pick_sf(data_size_gb)
    target_rows = int(data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    s3_path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"

    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    print(
        f"  [daft] read(sf{sf}, limit {target_rows:,}) + shuffle -> "
        f"{num_partitions} partitions ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    df = daft.read_parquet(s3_path).limit(target_rows)
    # write_parquet() forces full materialization of the shuffle.
    # count_rows() gets optimized away by Daft (skips repartition).
    # collect() pulls all data to driver and OOMs on large datasets.
    result = df.repartition(num_partitions, *KEY_COLUMNS).write_parquet(OUTPUT_DIR)
    elapsed = time.perf_counter() - start

    num_rows = result.to_pydict().get("rows", [0])
    num_rows = sum(num_rows) if isinstance(num_rows, list) else num_rows
    print(f"{elapsed:.1f}s ({num_rows:,} rows)")

    del result
    gc.collect()

    # Clean up output files.
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    wait_for_object_store_to_drain()

    return {
        "elapsed_s": elapsed,
        "num_rows": num_rows,
        "status": "ok",
    }


def main():
    parser = argparse.ArgumentParser(description="Out-of-core Daft shuffle benchmark")
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_ooc_daft_results.json",
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

    import daft

    daft.set_runner_ray()

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
    print(f"Test: {data_size_gb} GB, {num_partitions} partitions, engine=daft")
    print()

    ratio = data_size_gb / in_core_limit_gb if in_core_limit_gb > 0 else float("inf")
    zone = "IN-CORE" if ratio <= 1.0 else "SPILL"
    print(
        f"--- {data_size_gb} GB, {num_partitions} partitions, daft "
        f"({ratio:.1f}x of in-core limit, {zone}) ---"
    )

    info = run_one(data_size_gb, num_partitions)

    result = {
        "timestamp": datetime.now().isoformat(),
        "engine": "daft",
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
        },
        **info,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nResults written to {args.output}")
    t = info["elapsed_s"]
    tp = f"{data_size_gb / t:.1f} GB/s" if t and t > 0 else "N/A"
    print(f"  {data_size_gb} GB, {num_partitions} partitions: {t:.1f}s, {tp}")

    # Shutdown Ray to kill Daft's Swordfish/Flotilla actors which persist
    # after query execution and hold onto cluster resources.
    ray.shutdown()


if __name__ == "__main__":
    main()
