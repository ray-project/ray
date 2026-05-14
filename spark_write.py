"""Spark shuffle benchmark with write-to-parquet.

Uses file-prefix selection (not limit()) to control data size.
Writes output to local disk as parquet.
"""
import argparse
import gc
import json
import math
import shutil
import time
from datetime import datetime

import raydp

import ray

KEY_COLUMN = "column00"
APPROX_BYTES_PER_ROW = 145
OUTPUT_DIR = "/tmp/ooc_bench_spark_output"

TPCH_LINEITEM_TOTAL_GB = {
    100: 75,
    1000: 750,
    10000: 8100,
}


def pick_sf(data_size_gb):
    if data_size_gb <= 70:
        return 100
    if data_size_gb <= 700:
        return 1000
    return 10000


def init_spark(executor_cores=8):
    cluster = ray.cluster_resources()
    available_cpus = cluster.get("CPU", 0)
    total_memory_bytes = cluster.get("memory", 0)

    # One executor per worker node, reserve 1 node for Ray head + system.
    num_executors = max(1, int(available_cpus // executor_cores) - 1)

    # Derive executor memory from available node memory, excluding the
    # object store which also lives in physical RAM.
    # cluster.get("memory") is Ray's non-object-store heap allocation.
    # Subtract object store per node, leave 20% for OS/Ray workers,
    # divide by 1.1 to account for JVM off-heap overhead.
    est_num_nodes = max(1, round(available_cpus / executor_cores))
    ray_heap_per_node_gb = total_memory_bytes / 1e9 / est_num_nodes
    obj_store_per_node_gb = cluster.get("object_store_memory", 0) / 1e9 / est_num_nodes
    available_per_node_gb = ray_heap_per_node_gb - obj_store_per_node_gb
    executor_memory_gb = max(4, int(available_per_node_gb * 0.80 / 1.1))
    executor_memory = f"{executor_memory_gb}GB"

    print(
        f"  Spark config: {available_cpus:.0f} CPUs, "
        f"{num_executors} executors × {executor_cores} cores × {executor_memory}",
        flush=True,
    )
    spark = raydp.init_spark(
        app_name="ooc_shuffle_benchmark",
        num_executors=num_executors,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        configs={
            "spark.sql.adaptive.enabled": "true",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        },
    )
    return spark


def list_lineitem_files(spark, sf):
    sc = spark.sparkContext
    s3a_path = f"s3a://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    hpath = sc._jvm.org.apache.hadoop.fs.Path(s3a_path)
    fs = hpath.getFileSystem(sc._jsc.hadoopConfiguration())
    statuses = list(fs.listStatus(hpath))
    files = []
    for st in statuses:
        name = st.getPath().getName()
        if name.endswith(".parquet") or name.endswith(".pq"):
            files.append((st.getPath().toString(), int(st.getLen())))
    files.sort(key=lambda pair: pair[0])
    return files


def select_files_for_size(files, sf, data_size_gb):
    if not files:
        raise RuntimeError("No parquet files listed")
    total_gb = TPCH_LINEITEM_TOTAL_GB.get(sf)
    if total_gb is None or data_size_gb >= total_gb:
        return [path for path, _ in files], total_gb or 0.0
    fraction = data_size_gb / total_gb
    n = max(1, math.ceil(len(files) * fraction))
    selected = [path for path, _ in files[:n]]
    approx_gb = total_gb * (n / len(files))
    return selected, approx_gb


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=180, poll_s=5):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        mem = ray.cluster_resources().get("object_store_memory", 1)
        avail = ray.available_resources().get("object_store_memory", 0)
        used_pct = (1 - avail / mem) * 100 if mem > 0 else 0
        if used_pct < threshold_pct:
            return
        time.sleep(poll_s)


def run_one(spark, data_size_gb, num_partitions):
    sf = pick_sf(data_size_gb)
    files = list_lineitem_files(spark, sf)
    selected, approx_gb = select_files_for_size(files, sf, data_size_gb)

    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    print(
        f"  [spark] sf{sf}: read {len(selected)} of {len(files)} parquet "
        f"files (~{approx_gb:.0f} GB requested {data_size_gb} GB) + "
        f"shuffle -> {num_partitions} partitions (write parquet) ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    df = spark.read.parquet(*selected)
    df.repartition(num_partitions, KEY_COLUMN).write.parquet(
        OUTPUT_DIR, mode="overwrite"
    )
    elapsed = time.perf_counter() - start

    print(f"{elapsed:.1f}s")

    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    gc.collect()
    wait_for_object_store_to_drain()

    return {
        "elapsed_s": elapsed,
        "num_files_read": len(selected),
        "num_files_total": len(files),
        "approx_in_memory_gb": round(approx_gb, 1),
        "status": "ok",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Spark shuffle + write parquet benchmark"
    )
    parser.add_argument("--output", type=str, default="spark_result.json")
    parser.add_argument("--data-size-gb", type=int, required=True)
    parser.add_argument("--num-partitions", type=int, required=True)
    args = parser.parse_args()

    ray.init()
    cluster = ray.cluster_resources()
    total_cpu = cluster.get("CPU", 0)
    total_obj_gb = cluster.get("object_store_memory", 0) / 1e9

    print(f"Cluster: {total_cpu:.0f} CPUs, {total_obj_gb:.0f} GB object store")
    print(
        f"Test: {args.data_size_gb} GB, {args.num_partitions} partitions, engine=spark"
    )

    spark = init_spark()
    info = run_one(spark, args.data_size_gb, args.num_partitions)

    result = {
        "timestamp": datetime.now().isoformat(),
        "engine": "spark_raydp",
        "cluster": {"num_cpus": total_cpu, "object_store_gb": round(total_obj_gb, 1)},
        "config": {
            "data_size_gb": args.data_size_gb,
            "num_partitions": args.num_partitions,
        },
        **info,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Written to {args.output}")

    raydp.stop_spark()


if __name__ == "__main__":
    main()
