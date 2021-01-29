import ray
import ray.autoscaler.sdk
import glob
import csv
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import raydp
import os.path

import time


def load_dataset(spark, nbytes, npartitions, sort, base):
    num_bytes_per_partition = nbytes // npartitions
    filenames = []

    @ray.remote
    def load(i):
        filename = os.path.join(base, "df-{}-{}-{}.parquet".format("sort" if sort else "groupby", num_bytes_per_partition, i))
        print("Partition file", filename)
        if not os.path.exists(filename):
            if sort:
                nrows = num_bytes_per_partition // 8
                print("Allocating dataset with {} rows".format(nrows))
                dataset = pd.DataFrame(np.random.randint(0, np.iinfo(np.int64).max, size=(nrows, 1), dtype=np.int64), columns=['a'])
            else:
                nrows = num_bytes_per_partition // (8 * 2)
                print("Allocating dataset with {} rows".format(nrows))
                dataset = pd.DataFrame(np.random.randint(0, 100, size=(nrows, 2), dtype=np.int64), columns=['a', 'b'])
            print("Done allocating")
            dataset.to_parquet(filename, flavor="spark")
            print("Done writing to disk")
        return filename

    for i in range(npartitions):
        filenames.append(load.remote(i))
    ray.wait(filenames, num_returns=len(filenames))

    return spark.read.parquet(os.path.join(base, "df-{}-{}-*.parquet".format("sort" if sort else "groupby", num_bytes_per_partition)))


def trial(spark, nbytes, n_partitions, sort, generate_only, base):
    df = load_dataset(spark, nbytes, n_partitions, sort, base)

    if generate_only:
        return

    times = []
    start = time.time()
    for i in range(10):
        print("Trial {} start".format(i))
        trial_start = time.time()

        if sort:
            df.sort("a").head(10)
        else:
            df.groupBy("b").avg("a").collect()

        trial_end = time.time()
        duration = trial_end - trial_start
        times.append(duration)
        print("Trial {} done after {}".format(i, duration))

        if time.time() - start > 10800:
            break
    return times


def num_alive_nodes():
    n = 0
    for node in ray.nodes():
        if node["Alive"]:
            n += 1
    return n


def scale_to(target):
    while num_alive_nodes() != target:
        ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
        print(f"Current # nodes: {num_alive_nodes()}, target: {target}")
        print("Waiting ...")
        time.sleep(5)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--nbytes", type=int, default=1_000_000)
    parser.add_argument("--npartitions", type=int, default=100, required=False)
    # Max partition size is 1GB.
    parser.add_argument("--max-partition-size", type=int, default=1000_000_000, required=False)
    parser.add_argument("--num-nodes", type=int, default=1)
    parser.add_argument("--num-executors", type=int, default=4)
    parser.add_argument("--cores-per-executor", type=int, default=1)
    parser.add_argument("--memory-per-executor", type=int, default=2)
    parser.add_argument("--sort", action="store_true")
    parser.add_argument("--timeline", action="store_true")
    parser.add_argument("--spark-only", action="store_true")
    parser.add_argument("--cluster", action="store_true")
    parser.add_argument("--s3", action="store_true")
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--clear-old-data", action="store_true")
    parser.add_argument("--spark-local-dir", type=str, default="/tmp")
    parser.add_argument("--spark-executor-memory", type=str, default="1g")
    parser.add_argument("--spark-python-worker-memory", type=str, default="512m")
    parser.add_argument("--spark-memory-fraction", type=float, default=0.6)
    args = parser.parse_args()

    if not args.spark_only:
        print("Starting Ray...")
        if args.cluster:
            ray.init(address="auto")
        else:
            ray.init()
            #ray.init(_system_config={
            #        "max_io_workers": 4,
            #        "object_spilling_config": json.dumps(
            #            {"type": "filesystem", "params": {"directory_path": "/tmp/spill"}},
            #            separators=(",", ":")
            #        )
            #    })

        # Wait until Ray has started and scaled before initializing Spark on Ray.
        scale_to(args.num_nodes)

        app_name = "Shuffle Benchmark on RayDP"
        num_executors = args.num_executors
        cores_per_executor = args.cores_per_executor
        memory_per_executor = f"{args.memory_per_executor}GB"
        config = {
            "spark.driver.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",
            "spark.executor.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",
            "spark.local.dir": args.spark_local_dir,
            "spark.executor.memory": args.spark_executor_memory,
            "spark.python.worker.memory": args.spark_python_worker_memory,
            "spark.memory.fraction": args.spark_memory_fraction,
        }
        if args.s3:
            config["spark.driver.extraJavaOptions"] += " -Dcom.amazonaws.services.s3.enableV4=true"
            config["spark.executor.extraJavaOptions"] += " -Dcom.amazonaws.services.s3.enableV4=true"
            config.update({
                # "spark.driver.extraClassPath": "/opt/spark/jars/hadoop-aws-2.7.4.jar:/opt/spark/jars/aws-java-sdk-1.7.4.jar:/opt/spark/jars/hadoop-common-2.7.4.jar:/opt/spark/jars/joda-time-2.3.jar",
                # "spark.executor.extraClassPath": "/opt/spark/jars/hadoop-aws-2.7.4.jar:/opt/spark/jars/aws-java-sdk-1.7.4.jar:/opt/spark/jars/hadoop-common-2.7.4.jar:/opt/spark/jars/joda-time-2.3.jar",
                # "spark.driver.extraClassPath": "/opt/spark/jars/*",
                # FIXME(Clark): The Spark-on-Ray app master or executor backend is currently
                # choking on user-defined executor class paths, causing the cluster to hang
                # indefinitely. We should fix this, but use spark.jars until it is fixed.
                # "spark.executor.extraClassPath": "/opt/spark/jars/*",
                # "spark.jars": "/opt/spark/jars/*",
                "spark.jars": "/opt/spark/jars/hadoop-aws-2.7.4.jar,/opt/spark/jars/aws-java-sdk-1.7.4.jar,/opt/spark/jars/hadoop-common-2.7.4.jar,/opt/spark/jars/joda-time-2.3.jar",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.endpoint": "s3.us-east-2.amazonaws.com",
                # "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": 2,
                # "spark.sepculation": False,
            })
        print("Initializing Spark on Ray...")
        spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor, config)
    else:
        print("Creating Spark session...")
        # NOTE: We still initialize a local Ray cluster for parallel data generation.
        ray.init()
        spark = (SparkSession.builder
                    .master("local")
                    .appName("Shuffle Benchmark on Spark")
                    .getOrCreate())

    system = "Spark" if args.spark_only else "RayDP"
    print(f"Running Spark version {spark.version}")
    if args.s3:
        base = "s3a://raydp-shuffle-benchmarks/data"
    elif args.cluster:
        base = "/tmp/benchmark_scratch"
    else:
        base = "data"

    if args.clear_old_data:
        print(f"Clearing old data from {base}.")
        files = glob.glob(os.path.join(base, "*.parquet"))
        for f in files:
            os.remove(f)

    print("Starting warmup trials...")
    print(system, trial(spark, 1000, 10, args.sort, args.generate_only, base))
    print("Warmup done.")

    npartitions = args.npartitions
    if args.nbytes // npartitions > args.max_partition_size:
        npartitions = args.nbytes // args.max_partition_size

    print("Starting real trials...")
    output = trial(spark, args.nbytes, npartitions, args.sort, args.generate_only, base)
    print("Trials done.")
    print("{} mean over {} trials: {} +- {}".format(system, len(output), np.mean(output), np.std(output)))

    if args.cluster:
        outfile = "/tmp/raydp_benchmark_output.csv"
    else:
        outfile = "output.csv"
    write_header = not os.path.exists(outfile) or os.path.getsize(outfile) == 0
    with open(outfile, "a+") as csvfile:
        fieldnames = ["system", "operation", "num_nodes", "nbytes", "npartitions", "duration"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        row = {
                "operation": "sort" if args.sort else "groupby",
                "num_nodes": args.num_nodes,
                "nbytes": args.nbytes,
                "npartitions": npartitions,
                }
        for output in output:
            row["system"] = system
            row["duration"] = output
            writer.writerow(row)

    if args.timeline:
        time.sleep(1)
        ray.timeline(filename="raydp.json")
