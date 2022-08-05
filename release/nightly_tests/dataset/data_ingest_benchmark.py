import numpy as np
import json
import os
import sys
import time
import argparse

import ray
from ray.data import DatasetPipeline

import pandas as pd

GiB = 1024 * 1024 * 1024


@ray.remote
class ConsumingActor:
    def __init__(self, rank):
        self._rank = rank

    def consume(self, split):
        DoConsume(split, self._rank)


def DoConsume(split, rank):
    prefetch_blocks = 1
    batch_size = 4096
    num_epochs = 2

    start = time.perf_counter()
    epochs_read, batches_read, bytes_read = 0, 0, 0
    batch_delays = []

    def generate_epochs(data, epochs: int):
        if isinstance(data, DatasetPipeline):
            for epoch in data.iter_epochs(epochs):
                yield epoch
        else:
            # Dataset
            for _ in range(epochs):
                yield data

    for epoch_data in generate_epochs(split, num_epochs):
        epochs_read += 1
        batch_start = time.perf_counter()
        for batch in epoch_data.iter_batches(
            prefetch_blocks=prefetch_blocks, batch_size=batch_size
        ):
            batch_delay = time.perf_counter() - batch_start
            batch_delays.append(batch_delay)
            batches_read += 1
            if isinstance(batch, pd.DataFrame):
                bytes_read += int(batch.memory_usage(index=True, deep=True).sum())
            elif isinstance(batch, np.ndarray):
                bytes_read += batch.nbytes
            else:
                # NOTE: This isn't recursive and will just return the size of
                # the object pointers if list of non-primitive types.
                bytes_read += sys.getsizeof(batch)
            batch_start = time.perf_counter()
    delta = time.perf_counter() - start

    print("Time to read all data", delta, "seconds")
    print(
        "P50/P95/Max batch delay (s)",
        np.quantile(batch_delays, 0.5),
        np.quantile(batch_delays, 0.95),
        np.max(batch_delays),
    )
    print("Num epochs read", epochs_read)
    print("Num batches read", batches_read)
    print("Num bytes read", round(bytes_read / (1024 * 1024), 2), "MiB")
    print("Mean throughput", round(bytes_read / (1024 * 1024) / delta, 2), "MiB/s")

    if rank == 0:
        print("Ingest stats from rank=0:\n\n{}".format(split.stats()))


def make_ds(size_gb: int):
    # Dataset of 10KiB tensor records.
    total_size = 1024 * 1024 * 1024 * size_gb
    record_dim = 1280
    record_size = record_dim * 8
    num_records = int(total_size / record_size)
    dataset = ray.data.range_tensor(num_records, shape=(record_dim,), parallelism=200)
    print("Created dataset", dataset, "of size", dataset.size_bytes())
    return dataset


def run_ingest_bulk(dataset, num_workers):
    consumers = [
        ConsumingActor.options(scheduling_strategy="SPREAD").remote(i)
        for i in range(num_workers)
    ]
    ds = dataset.map_batches(lambda df: df * 2)
    splits = ds.split(num_workers, equal=True, locality_hints=consumers)
    future = [consumers[i].consume.remote(s) for i, s in enumerate(splits)]
    ray.get(future)

    # Example ballpark number for transformation (6s):
    # Read->Map_Batches: 201/201 [00:06<00:00, 28.90it/s]

    # Example ballpark number for consumption i.e. at an actor (consumer):
    # Fast ones:
    # Time to read all data 6.060172239998792 seconds
    # P50/P95/Max batch delay (s) 0.011000780499671237 0.013028981001298234 0.11437869699875591  # noqa: E501
    # Num epochs read 2
    # Num batches read 512
    # Num bytes read 20480.0 MiB
    # Mean throughput 3379.44 MiB/s
    # Slow ones:
    # Time to read all data 39.7250169550025 seconds
    # P50/P95/Max batch delay (s) 0.010788186998979654 0.027017505450021396 2.936176807997981  # noqa: E501
    # Num epochs read 2
    # Num batches read 512
    # Num bytes read 20480.0 MiB
    # Mean throughput 515.54 MiB/s

    # Example ballpark number of total time:
    # success! total time 62.37753415107727


def run_ingest_streaming(dataset, num_workers):
    consumers = [
        ConsumingActor.options(scheduling_strategy="SPREAD").remote(i)
        for i in range(num_workers)
    ]
    p = (
        dataset.window(bytes_per_window=40 * GiB)
        .repeat()
        .map_batches(lambda df: df * 2)
    )
    splits = p.split(num_workers, equal=True, locality_hints=consumers)
    future = [consumers[i].consume.remote(s) for i, s in enumerate(splits)]
    ray.get(future)

    # Example ballpark number for a window:
    # == Pipeline Window 12 ==
    # Stage 1 read->map_batches: 40/40 blocks executed in 4.1s
    # * Remote wall time: 1.42s min, 2.63s max, 1.57s mean, 62.7s total
    # * Remote cpu time: 1.42s min, 2.59s max, 1.56s mean, 62.38s total
    # * Peak heap memory usage (MiB): 3252116000.0 min, 12829140000.0 max, 10597707000 mean  # noqa: E501
    # * Output num rows: 104857 min, 104857 max, 104857 mean, 4194280 total
    # * Output size bytes: 1074155212 min, 1074155212 max, 1074155212 mean, 42966208480 total  # noqa: E501
    # * Tasks per node: 1 min, 3 max, 2 mean; 20 nodes used

    # Example ballpark number for an actor (consumer):
    # Time to read all data 42.57252279000022 seconds
    # P50/P95/Max batch delay (s) 0.01082486700033769 0.012740581999969434 4.104724623000948  # noqa: E501
    # Num epochs read 2
    # Num batches read 512
    # Num bytes read 20480.0 MiB
    # Mean throughput 481.06 MiB/s

    # Example ballpark number of total time:
    # success! total time 61.76846528053284


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--dataset-size-gb", type=int, default=200)
    parser.add_argument("--streaming", action="store_true", default=False)
    args = parser.parse_args()

    start = time.time()
    ds = make_ds(args.dataset_size_gb)
    if args.streaming:
        run_ingest_streaming(ds, args.num_workers)
    else:
        run_ingest_bulk(ds, args.num_workers)

    delta = time.time() - start
    print(f"success! total time {delta}")
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "w") as f:
        f.write(json.dumps({"ingest_time": delta, "success": 1}))
