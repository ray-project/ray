import numpy as np
import sys
import time
import argparse

from benchmark import Benchmark
import ray
from ray.data import Dataset

import pandas as pd
import torch

GiB = 1024 * 1024 * 1024


@ray.remote
class ConsumingActor:
    def __init__(self, rank):
        self._rank = rank

    def consume(
        self,
        split,
        use_gpu=False,
        max_bytes_to_read=None,
    ):
        do_consume(
            split,
            self._rank,
            use_gpu,
            max_bytes_to_read,
        )

    def get_location(self):
        return ray.get_runtime_context().get_node_id()


def do_consume(
    split,
    rank,
    use_gpu=False,
    max_bytes_to_read=None,
):
    prefetch_batches = 1
    batch_size = 4096
    num_epochs = 1

    start = time.perf_counter()
    epochs_read, batches_read, bytes_read = 0, 0, 0
    batch_delays = []

    def generate_epochs(data, epochs: int):
        if isinstance(data, Dataset):
            # Dataset
            for _ in range(epochs):
                yield data
        else:
            # StreamSplitDataIterator
            yield data

    for epoch_data in generate_epochs(split, num_epochs):
        epochs_read += 1
        batch_start = time.perf_counter()

        if not use_gpu:
            batch_iterator = epoch_data.iter_batches(
                prefetch_batches=prefetch_batches, batch_size=batch_size
            )
        else:
            batch_iterator = epoch_data.iter_torch_batches(
                prefetch_batches=prefetch_batches,
                batch_size=batch_size,
                device="cuda",
            )

        for batch in batch_iterator:
            batch_delay = time.perf_counter() - batch_start
            batch_delays.append(batch_delay)
            batches_read += 1
            if isinstance(batch, pd.DataFrame):
                bytes_read += int(batch.memory_usage(index=True, deep=True).sum())
            elif isinstance(batch, np.ndarray):
                bytes_read += batch.nbytes
            elif isinstance(batch, dict) and isinstance(
                batch.get("data"), torch.Tensor
            ):
                tensor = batch["data"]
                bytes_read += tensor.element_size() * tensor.nelement()
            else:
                # NOTE: This isn't recursive and will just return the size of
                # the object pointers if list of non-primitive types.
                bytes_read += sys.getsizeof(batch)
            batch_start = time.perf_counter()
            if max_bytes_to_read is not None:
                if bytes_read >= max_bytes_to_read:
                    break
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


def make_ds(size_gb: int, parallelism: int = -1):
    # Dataset of 10KiB tensor records.
    total_size = 1024 * 1024 * 1024 * size_gb
    record_dim = 1024
    record_size = record_dim * 8
    num_records = int(total_size / record_size)
    dataset = ray.data.range_tensor(
        num_records, shape=(record_dim,), override_num_blocks=parallelism
    )
    print("Created dataset", dataset, "of size", dataset.size_bytes())
    return dataset


def run_ingest_streaming(dataset_size_gb, num_workers, use_gpu, early_stop):
    ds = make_ds(dataset_size_gb)
    resources = {"num_cpus": 0.5}
    if use_gpu:
        resources["num_gpus"] = 0.5
    consumers = [
        ConsumingActor.options(scheduling_strategy="SPREAD", **resources).remote(i)
        for i in range(num_workers)
    ]
    locality_hints = ray.get([actor.get_location.remote() for actor in consumers])
    ds = ds.map_batches(lambda df: df * 2, batch_format="pandas")
    splits = ds.streaming_split(num_workers, equal=True, locality_hints=locality_hints)
    max_bytes_to_read = None
    if early_stop:
        max_bytes_to_read = dataset_size_gb * GiB // num_workers // 2
    # Early stop when we've read half the dataset.
    future = [
        consumers[i].consume.remote(
            s,
            use_gpu,
            max_bytes_to_read,
        )
        for i, s in enumerate(splits)
    ]
    ray.get(future)


def run_ingest_bulk(dataset_size_gb, num_workers):
    ds = make_ds(dataset_size_gb, parallelism=200)
    consumers = [
        ConsumingActor.options(scheduling_strategy="SPREAD", num_cpus=0.5).remote(i)
        for i in range(num_workers)
    ]
    ds = ds.map_batches(lambda df: df * 2, batch_format="pandas")
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--dataset-size-gb", type=int, default=200)
    parser.add_argument("--streaming", action="store_true", default=False)
    parser.add_argument("--use-gpu", action="store_true", default=False)
    parser.add_argument("--early-stop", action="store_true", default=False)
    args = parser.parse_args()

    benchmark = Benchmark("streaming-data-ingest")
    if args.streaming:
        benchmark.run_fn(
            "streaming-ingest",
            run_ingest_streaming,
            args.dataset_size_gb,
            args.num_workers,
            args.use_gpu,
            args.early_stop,
        )
    else:
        benchmark.run_fn(
            "bulk-ingest",
            run_ingest_bulk,
            args.dataset_size_gb,
            args.num_workers,
        )

    benchmark.write_result()
