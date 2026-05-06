"""Benchmark: Spark (via RayDP) hash shuffle on Ray cluster.

Compare with Ray Data and Daft shuffle using the same
TPC-H lineitem dataset and partition counts.

Cluster: 32 worker nodes, 8 CPU / 32 GB each (256 CPUs total).

Requirements:
    pip install raydp
    Java must be installed with JAVA_HOME set.

Usage:
    python benchmark_spark_shuffle.py
    python benchmark_spark_shuffle.py --output spark_results.json
"""

import argparse
import gc
import json
import time
from datetime import datetime

import raydp

import ray

BENCHMARK_MATRIX = [
    # (sf, num_partitions)
    (10, 100),
    (10, 500),
    (10, 1000),
    (100, 100),
    (100, 1000),
]

KEY_COLUMN = "column00"  # l_orderkey


def init_spark(num_executors=32, executor_cores=8, executor_memory="14GB"):
    """Initialize a Spark session on Ray via RayDP."""
    spark = raydp.init_spark(
        app_name="shuffle_benchmark",
        num_executors=num_executors,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        configs={
            # Use all available memory for shuffle.
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "false",
            # S3 access via hadoop-aws.
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        },
    )
    return spark


def load_dataset(spark, sf):
    """Read and cache the dataset so both runs share data."""
    path = f"s3a://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    print(f"Loading sf={sf} from S3 with Spark ... ", end="", flush=True)
    df = spark.read.parquet(path)
    df.cache()
    num_rows = df.count()  # triggers materialization
    print(f"done ({num_rows:,} rows)")
    return df


def run_one(df, sf, num_partitions):
    """Run a single Spark repartition and return elapsed time and row count."""
    print(f"  [spark] sf={sf}, partitions={num_partitions} ... ", end="", flush=True)

    start = time.perf_counter()
    result = df.repartition(num_partitions, KEY_COLUMN)
    # Write to parquet to force full shuffle materialization.
    # cache()+count() can still be optimized by Catalyst.
    result.write.parquet("/tmp/spark_bench_output", mode="overwrite")
    elapsed = time.perf_counter() - start

    num_rows = result.count()
    actual_partitions = result.rdd.getNumPartitions()
    print(f"{elapsed:.1f}s ({num_rows:,} rows, {actual_partitions} partitions)")

    del result
    gc.collect()

    return elapsed, num_rows


def main():
    parser = argparse.ArgumentParser(description="Spark Shuffle Benchmark (RayDP)")
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_spark_results.json",
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help="Number of runs per (sf, partitions) combo",
    )
    args = parser.parse_args()

    ray.init()

    cluster = ray.cluster_resources()
    print(
        f"Cluster: {cluster.get('CPU', 0):.0f} CPUs, "
        f"{cluster.get('memory', 0) / 1e9:.0f} GB memory"
    )
    print(f"Matrix: {len(BENCHMARK_MATRIX)} configs x {args.num_runs} runs")
    print()

    spark = init_spark()

    results = []

    def flush_results():
        output = {
            "timestamp": datetime.now().isoformat(),
            "engine": "spark_raydp",
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
        df = load_dataset(spark, sf)

        for num_partitions in partition_list:
            print(f"--- sf={sf}, partitions={num_partitions} ---")

            times = []
            num_rows = 0

            for run in range(args.num_runs):
                try:
                    elapsed, num_rows = run_one(df, sf, num_partitions)
                    times.append(elapsed)
                except Exception as e:
                    print(f"  [spark] FAILED: {e}")
                    times.append(None)

            valid_times = [t for t in times if t is not None]
            entry = {
                "sf": sf,
                "num_partitions": num_partitions,
                "strategy": "spark_raydp",
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

        df.unpersist()
        gc.collect()

    print(f"Results written to {args.output}")

    # Print summary table.
    print()
    print(f"{'SF':>6} {'Partitions':>12} {'Spark (s)':>12}")
    print("-" * 35)

    for entry in results:
        t = entry["avg_time"]
        t_str = f"{t:.1f}" if t else "FAIL"
        print(f"{entry['sf']:>6} {entry['num_partitions']:>12} {t_str:>12}")

    raydp.stop_spark()


if __name__ == "__main__":
    main()
