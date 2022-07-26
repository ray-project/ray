import csv
import json
import os.path
import time

import boto3
import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client


def load_dataset(client, data_dir, s3_bucket, nbytes, npartitions):
    num_bytes_per_partition = nbytes // npartitions
    filenames = []

    @dask.delayed
    def generate_s3_file(i, data_dir, s3_bucket):
        s3 = boto3.client("s3")
        key = "df-{}-{}.parquet.gzip".format(num_bytes_per_partition, i)
        contents = s3.list_objects(Bucket=s3_bucket, Prefix=key)
        for obj in contents.get("Contents", []):
            if obj["Key"] == key:
                print(f"S3 partition {i} exists")
                return

        filename = os.path.join(data_dir, key)
        if not os.path.exists(filename):
            print("Generating partition", filename)
            nrows = num_bytes_per_partition // 8
            dataset = pd.DataFrame(
                np.random.randint(
                    0, np.iinfo(np.int64).max, size=(nrows, 1), dtype=np.int64
                ),
                columns=["a"],
            )
            dataset.to_parquet(filename, compression="gzip")
        print("Writing partition to S3", filename)
        with open(filename, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=key, Body=f)

    x = []
    for i in range(npartitions):
        x.append(generate_s3_file(i, data_dir, s3_bucket))
    # from ray.util.dask import ProgressBarCallback
    # with ProgressBarCallback():
    # dask.compute(x, _ray_enable_progress_bar=True)
    dask.compute(x)

    filenames = [
        f"s3://{s3_bucket}/df-{num_bytes_per_partition}-{i}.parquet.gzip"
        for i in range(npartitions)
    ]

    df = None
    max_retry = 3
    retry = 0
    while retry < max_retry:
        try:
            df = dd.read_parquet(filenames)
            break
        except FileNotFoundError as e:
            print(f"Failed to load a file. {e}")
            # Wait a little bit before retrying.
            time.sleep(30)
            retry += 1

    return df


def load_dataset_files(client, data_dir, file_path, nbytes, npartitions):
    num_bytes_per_partition = nbytes // npartitions
    filenames = []

    @dask.delayed
    def generate_file(i, data_dir, file_path):
        key = "{}/df-{}-{}.parquet.gzip".format(file_path, num_bytes_per_partition, i)
        from os import path

        if path.exists(key):
            print(f"The file {key} already exists. Do nothing")
            return

        filename = os.path.join(data_dir, key)
        if not os.path.exists(filename):
            print("Generating partition", filename)
            nrows = num_bytes_per_partition // 8
            dataset = pd.DataFrame(
                np.random.randint(
                    0, np.iinfo(np.int64).max, size=(nrows, 1), dtype=np.int64
                ),
                columns=["a"],
            )
            dataset.to_parquet(filename, compression="gzip")
        print("Writing partition to a file", filename)

    x = []
    for i in range(npartitions):
        x.append(generate_file(i, data_dir, file_path))
    # from ray.util.dask import ProgressBarCallback
    # with ProgressBarCallback():
    #     dask.compute(x, _ray_enable_progress_bar=True)
    dask.compute(x)

    filenames = [
        f"{file_path}/df-{num_bytes_per_partition}-{i}.parquet.gzip"
        for i in range(npartitions)
    ]
    df = dd.read_parquet(filenames)
    return df


def trial(
    client,
    data_dir,
    nbytes,
    n_partitions,
    generate_only,
    s3_bucket=None,
    file_path=None,
):
    if s3_bucket:
        df = load_dataset(client, data_dir, s3_bucket, nbytes, n_partitions)
    elif file_path:
        df = load_dataset_files(client, data_dir, file_path, nbytes, n_partitions)

    if generate_only:
        return []

    times = []
    start = time.time()
    for i in range(10):
        print("Trial {} start".format(i))
        trial_start = time.time()

        # from ray.util.dask import ProgressBarCallback
        # with ProgressBarCallback():
        #     print(
        #         df.set_index("a", shuffle="tasks",
        #                      max_branch=float("inf")).compute(
        #                          _ray_enable_progress_bar=True).head(10))
        print(
            df.set_index("a", shuffle="tasks", max_branch=float("inf")).head(
                10, npartitions=-1
            )
        )

        trial_end = time.time()
        duration = trial_end - trial_start
        times.append(duration)
        print("Trial {} done after {}".format(i, duration))

        if time.time() - start > 60:
            break
    return times


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--nbytes", type=int, default=1_000_000)
    parser.add_argument("--npartitions", type=int, default=100, required=False)
    # Max partition size is 1GB.
    parser.add_argument(
        "--max-partition-size", type=int, default=1000_000_000, required=False
    )
    parser.add_argument("--num-nodes", type=int, default=1)
    parser.add_argument("--dask-tasks", action="store_true")
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--ray", action="store_true")
    parser.add_argument("--data-dir", default="/home/ubuntu/dask-benchmarks")
    parser.add_argument("--s3-bucket")
    parser.add_argument("--file-path")

    parser.add_argument("--dask-nprocs", type=int, default=0)
    parser.add_argument("--dask-nthreads", type=int, default=0)
    parser.add_argument("--dask-memlimit", type=int, default=0)
    args = parser.parse_args()
    assert not (args.s3_bucket and args.file_path), "Provide S3 bucket or file path."
    if args.ray:
        import ray

        ray.init(address="auto")
        from ray.util.dask import dataframe_optimize, ray_dask_get

        dask.config.set(scheduler=ray_dask_get, dataframe_optimize=dataframe_optimize)
        client = None
    else:
        assert args.dask_nprocs != -0
        assert args.dask_nthreads != -0
        assert args.dask_memlimit != -0

        if args.dask_tasks:
            print("Using task-based Dask shuffle")
            dask.config.set(shuffle="tasks")
        else:
            print("Using disk-based Dask shuffle")

        client = Client("localhost:8786")

    print(
        trial(
            client,
            args.data_dir,
            1000,
            10,
            args.generate_only,
            s3_bucket=args.s3_bucket,
            file_path=args.file_path,
        )
    )
    print("WARMUP DONE")

    npartitions = args.npartitions
    if args.nbytes // npartitions > args.max_partition_size:
        npartitions = args.nbytes // args.max_partition_size

    duration = []
    print("Start the main trial")
    output = trial(
        client,
        args.data_dir,
        args.nbytes,
        npartitions,
        args.generate_only,
        s3_bucket=args.s3_bucket,
        file_path=args.file_path,
    )
    print(
        "mean over {} trials: {} +- {}".format(
            len(output), np.mean(output), np.std(output)
        )
    )

    print(ray._private.internal_api.memory_summary(stats_only=True))
    duration = np.mean(output)

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"duration": duration, "success": 1}))

    write_header = (
        not os.path.exists("output.csv") or os.path.getsize("output.csv") == 0
    )
    with open("output.csv", "a+") as csvfile:
        fieldnames = [
            "num_nodes",
            "nbytes",
            "npartitions",
            "dask_tasks",
            "dask_nprocs",
            "dask_nthreads",
            "dask_memlimit",
            "duration",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        row = {
            "num_nodes": args.num_nodes,
            "nbytes": args.nbytes,
            "npartitions": npartitions,
            "dask_tasks": args.dask_tasks,
            "dask_nprocs": args.dask_nprocs,
            "dask_nthreads": args.dask_nthreads,
            "dask_memlimit": args.dask_memlimit,
        }
        for output in output:
            row["duration"] = output
            writer.writerow(row)
