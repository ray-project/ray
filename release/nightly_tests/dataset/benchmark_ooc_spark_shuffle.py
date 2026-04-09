"""Benchmark: Out-of-core Spark (via RayDP) shuffle on Ray cluster.

Compare with Ray Data's actorless shuffle using the same cluster
configuration and partition counts.

Cluster assumption: 16 worker nodes, 8 CPU / 30 GB each.
  - Total memory: ~480 GB
  - Object store (50%): ~240 GB
  - In-core shuffle limit (x/3): ~80 GB

Data: TPC-H lineitem from S3, limited to target row count.

Requirements:
    pip install raydp
    Java must be installed with JAVA_HOME set.

Usage:
    python benchmark_ooc_spark_shuffle.py --data-size-gb 50 --num-partitions 200
    python benchmark_ooc_spark_shuffle.py --data-size-gb 256 --num-partitions 1000
"""

import argparse
import gc
import json
import shutil
import time
from datetime import datetime

import raydp

import ray

KEY_COLUMN = "column00"  # l_orderkey

# Approximate bytes per row for TPC-H lineitem in-memory (Arrow).
# Measured empirically; used to convert --data-size-gb to a row limit.
APPROX_BYTES_PER_ROW = 128

# Local temp dir for shuffle output (forces full materialization).
OUTPUT_DIR = "/tmp/ooc_bench_spark_output"


def pick_sf(data_size_gb):
    """Pick the smallest TPC-H scale factor that has enough data."""
    # SF100 ≈ 600M rows ≈ 75 GB, SF1000 ≈ 6B rows ≈ 750 GB
    if data_size_gb <= 70:
        return 100
    return 1000


def init_spark(executor_cores=8, executor_memory="14GB"):
    """Initialize a Spark session on Ray via RayDP."""
    available_cpus = ray.available_resources().get("CPU", 0)
    usable_cpus = available_cpus - 16
    num_executors = max(1, int(usable_cpus // executor_cores))

    print(
        f"  Spark config: {available_cpus:.0f} available CPUs, "
        f"using {num_executors} executors x {executor_cores} cores",
        flush=True,
    )

    spark = raydp.init_spark(
        app_name="ooc_shuffle_benchmark",
        num_executors=num_executors,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        configs={
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "false",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        },
    )
    return spark


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


def run_one(spark, data_size_gb, num_partitions):
    """Run a single Spark repartition and return timing + status info."""
    sf = pick_sf(data_size_gb)
    target_rows = int(data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    s3a_path = f"s3a://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"

    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    print(
        f"  [spark] read(sf{sf}, limit {target_rows:,}) + shuffle -> "
        f"{num_partitions} partitions ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    df = spark.read.parquet(s3a_path).limit(target_rows)
    result = df.repartition(num_partitions, KEY_COLUMN)
    result.write.parquet(OUTPUT_DIR, mode="overwrite")
    elapsed = time.perf_counter() - start

    print(f"{elapsed:.1f}s")

    del result, df
    gc.collect()

    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    wait_for_object_store_to_drain()

    return {
        "elapsed_s": elapsed,
        "status": "ok",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Out-of-core Spark shuffle benchmark (RayDP)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_ooc_spark_results.json",
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
    print(f"Test: {data_size_gb} GB, {num_partitions} partitions, engine=spark")
    print()

    ratio = data_size_gb / in_core_limit_gb if in_core_limit_gb > 0 else float("inf")
    zone = "IN-CORE" if ratio <= 1.0 else "SPILL"
    print(
        f"--- {data_size_gb} GB, {num_partitions} partitions, spark "
        f"({ratio:.1f}x of in-core limit, {zone}) ---"
    )

    spark = init_spark()

    info = run_one(spark, data_size_gb, num_partitions)

    result = {
        "timestamp": datetime.now().isoformat(),
        "engine": "spark_raydp",
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

    raydp.stop_spark()


if __name__ == "__main__":
    main()
