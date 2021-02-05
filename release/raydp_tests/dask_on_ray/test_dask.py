import glob
import ray
import dask
import dask.dataframe as dd
import pandas as pd
import numpy as np
from ray.util.dask import ray_dask_get_sync, dataframe_optimize
import os.path
import json
import csv

from dask.distributed import Client
# import cProfile, pstats, io

import time

# DATA_DIR = "~/dask-on-ray-data"
DATA_DIR = "obj-data"


def load_dataset(nbytes, npartitions, sort):
    num_bytes_per_partition = nbytes // npartitions
    filenames = []

    @ray.remote
    def foo(i):
        filename = "df-{}-{}-{}.parquet.gzip".format(
            "sort" if sort else "groupby", num_bytes_per_partition, i)
        filename = os.path.join(DATA_DIR, filename)
        print("Partition file", filename)
        if not os.path.exists(filename):
            if sort:
                nrows = num_bytes_per_partition // 8
                print("Allocating dataset with {} rows".format(nrows))
                dataset = pd.DataFrame(
                    np.random.randint(
                        0,
                        np.iinfo(np.int64).max,
                        size=(nrows, 1),
                        dtype=np.int64),
                    columns=['a'])
            else:
                nrows = num_bytes_per_partition // (8 * 2)
                print("Allocating dataset with {} rows".format(nrows))
                dataset = pd.DataFrame(
                    np.random.randint(0, 100, size=(nrows, 2), dtype=np.int64),
                    columns=['a', 'b'])
            print("Done allocating")
            dataset.to_parquet(filename, compression='gzip')
            print("Done writing to disk")
        return filename

    for i in range(npartitions):
        filenames.append(foo.remote(i))
    filenames = ray.get(filenames)

    df = dd.read_parquet(filenames)
    import time
    print("parquet read")
    time.sleep(5)
    print("sleep done.")
    return df


def trial(nbytes, n_partitions, sort, generate_only, custom_shuffle_optimization):
    df = load_dataset(nbytes, n_partitions, sort)
    # pr = cProfile.Profile()
    if generate_only:
        return

    times = []
    start = time.time()
    for i in range(1):
        print("Trial {} start".format(i))
        trial_start = time.time()

        if sort:
            # pr.enable()
            if custom_shuffle_optimization:
                with dask.config.set(dataframe_optimize=dataframe_optimize):
                    a = df.set_index('a', shuffle='tasks', max_branch=n_partitions)
                    # a.visualize(filename=f'a-{i}.svg')
                    a.head(10, npartitions=-1)
            else:
                a = df.set_index('a', shuffle='tasks', max_branch=n_partitions)
                # a.visualize(filename=f'a-{i}.svg')
                a.head(10, npartitions=-1)
            # pr.disable()
        else:
            df.groupby('b').a.mean().compute()

        trial_end = time.time()
        duration = trial_end - trial_start
        times.append(duration)
        print("Trial {} done after {}".format(i, duration))
        # sortby = SortKey.CUMULATIVE
        # ps = pstats.Stats(pr).sort_stats(sortby)
        # ps.dump_stats("ray_profile_data")

        if time.time() - start > 60 and i > 0:
            break
    return times


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--nbytes", type=int, default=1_000_000)
    parser.add_argument("--npartitions", type=int, default=20, required=False)
    # Max partition size is 1GB.
    parser.add_argument(
        "--max-partition-size", type=int, default=1000_000_000, required=False)
    parser.add_argument("--num-nodes", type=int, default=1)
    parser.add_argument("--sort", action="store_true")
    parser.add_argument("--timeline", action="store_true")
    parser.add_argument("--dask", action="store_true")
    parser.add_argument("--ray", action="store_true")
    parser.add_argument("--dask-tasks", action="store_true")
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--clear-old-data", action="store_true")
    parser.add_argument("--custom-shuffle-optimization", action="store_true")
    args = parser.parse_args()

    if args.clear_old_data:
        print(f"Clearing old data from {DATA_DIR}.")
        files = glob.glob(os.path.join(DATA_DIR, "*.parquet.gzip"))
        for f in files:
            os.remove(f)

    if args.ray:
        args.dask_tasks = True

    if args.dask_tasks:
        print("Using task-based Dask shuffle")
        dask.config.set(shuffle='tasks')
    else:
        print("Using disk-based Dask shuffle")

    if args.dask:
        # client = Client()
        # ray.init(address='auto')
        client = Client('127.0.0.1:8786')
        ray.init()
    if args.ray:
        # ray.init(address="auto")
        # ray.init()
        ray.init(
            num_cpus=16,
            _system_config={
                "max_io_workers": 1,
                "object_spilling_config": json.dumps(
                    {
                        "type": "filesystem",
                        "params": {
                            "directory_path": "/tmp/spill"
                        }
                    },
                    separators=(",", ":"))
            })
        dask.config.set(scheduler=ray_dask_get_sync)

    system = "dask" if args.dask else "ray"

    print(system, trial(1000, 10, args.sort, args.generate_only, args.custom_shuffle_optimization))
    print("WARMUP DONE")

    npartitions = args.npartitions
    if args.nbytes // npartitions > args.max_partition_size:
        npartitions = args.nbytes // args.max_partition_size

    output = trial(args.nbytes, npartitions, args.sort, args.generate_only, args.custom_shuffle_optimization)
    print("{} mean over {} trials: {} +- {}".format(system, len(output),
                                                    np.mean(output),
                                                    np.std(output)))

    write_header = not os.path.exists("output.csv") or os.path.getsize(
        "output.csv") == 0
    with open("output.csv", "a+") as csvfile:
        fieldnames = [
            "system", "operation", "num_nodes", "nbytes", "npartitions",
            "duration"
        ]
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
        ray.timeline(filename="dask.json")
