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


@ray.remote(num_cpus=0.5)
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

    # Example ballpark number for transformation (5s):
    # Read->Map_Batches: 201/201 Time to read all data 5.001230175999922 seconds

    # Example ballpark number for consumption i.e. at an actor (consumer):
    # Time to read all data 5.275932452000006 seconds
    # P50/P95/Max batch delay (s) 0.010558151499992618 0.010944704699983276 0.04179979600007755  # noqa: E501
    # Num epochs read 2
    # Num batches read 512
    # Num bytes read 20480.0 MiB
    # Mean throughput 3881.78 MiB/s

    # Example total time:
    # success! total time 13.813468217849731


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

    # Example ballpark numbers:
    # == Pipeline Window 10 ==
    # Stage 1 read->map_batches: 40/40 blocks executed in 1.98s
    # * Remote wall time: 1.38s min, 1.66s max, 1.46s mean, 58.26s total
    # * Remote cpu time: 1.38s min, 1.7s max, 1.46s mean, 58.33s total
    # * Peak heap memory usage (MiB): 6533908000.0 min, 10731508000.0 max, 9710443300 mean  # noqa: E501
    # * Output num rows: 104857 min, 104857 max, 104857 mean, 4194280 total
    # * Output size bytes: 1074155212 min, 1074155212 max, 1074155212 mean, 42966208480 total  # noqa: E501
    # * Tasks per node: 2 min, 2 max, 2 mean; 20 nodes used

    # Example actor (consumer):
    # Time to read all data 25.58030511100003 seconds
    # P50/P95/Max batch delay (s) 0.010486626999977489 0.012674414999997904 2.0688196870000866  # noqa: E501
    # Num epochs read 2
    # Num batches read 512
    # Num bytes read 20480.0 MiB
    # Mean throughput 800.62 MiB/s

    # Example total time:
    # success! total time 27.822711944580078


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
