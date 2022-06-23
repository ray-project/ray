#!/usr/bin/env python

import sys
import time
from typing import Optional

import numpy as np

import ray
from ray import train
from ray.air.config import DatasetConfig
from ray.data.preprocessors import BatchMapper, Chain
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DummyTrainer(DataParallelTrainer):
    """A Trainer that does nothing except read the data for a given period of time.

    It prints out as much debugging statistics as possible.

    This is useful for debugging data ingest problem. This trainer supports normal
    scaling options same as any other Trainer (e.g., num_workers, use_gpu).
    """

    def __init__(
        self,
        *args,
        scaling_config: dict = None,
        runtime_seconds: int = 30,
        prefetch_blocks: int = 1,
        batch_size: Optional[int] = None,
        **kwargs
    ):
        if not scaling_config:
            scaling_config = {"num_workers": 1}
        super().__init__(
            train_loop_per_worker=DummyTrainer.make_train_loop(
                runtime_seconds, prefetch_blocks, batch_size
            ),
            *args,
            scaling_config=scaling_config,
            **kwargs
        )

    def preprocess_datasets(self):
        print("Starting dataset preprocessing")
        start = time.perf_counter()
        super().preprocess_datasets()
        print("Preprocessed datasets in", time.perf_counter() - start, "seconds")
        if self.preprocessor:
            print("Preprocessor", self.preprocessor)
            print(
                "Preprocessor transform stats:\n\n{}".format(
                    self.preprocessor.transform_stats()
                )
            )

    @staticmethod
    def make_train_loop(
        runtime_seconds: int, prefetch_blocks: int, batch_size: Optional[int]
    ):
        """Make a debug train loop that runs for the given amount of runtime."""

        def train_loop_per_worker():
            import pandas as pd

            rank = train.world_rank()
            data_shard = train.get_dataset_shard("train")
            start = time.perf_counter()
            num_epochs, num_batches, num_bytes = 0, 0, 0
            batch_delays = []

            print("Starting train loop on worker", rank)
            while time.perf_counter() - start < runtime_seconds:
                num_epochs += 1
                batch_start = time.perf_counter()
                for batch in data_shard.iter_batches(
                    prefetch_blocks=prefetch_blocks, batch_size=batch_size
                ):
                    batch_delay = time.perf_counter() - batch_start
                    batch_delays.append(batch_delay)
                    num_batches += 1
                    if isinstance(batch, pd.DataFrame):
                        num_bytes += int(
                            batch.memory_usage(index=True, deep=True).sum()
                        )
                    elif isinstance(batch, np.ndarray):
                        num_bytes += batch.nbytes
                    else:
                        # NOTE: This isn't recursive and will just return the size of
                        # the object pointers if list of non-primitive types.
                        num_bytes += sys.getsizeof(batch)
                    train.report(
                        bytes_read=num_bytes,
                        num_batches=num_batches,
                        num_epochs=num_epochs,
                        batch_delay=batch_delay,
                    )
                    batch_start = time.perf_counter()
            delta = time.perf_counter() - start

            print("Time to read all data", delta, "seconds")
            print(
                "P50/P95/Max batch delay (s)",
                np.quantile(batch_delays, 0.5),
                np.quantile(batch_delays, 0.95),
                np.max(batch_delays),
            )
            print("Num epochs read", num_epochs)
            print("Num batches read", num_batches)
            print("Num bytes read", round(num_bytes / (1024 * 1024), 2), "MiB")
            print(
                "Mean throughput", round(num_bytes / (1024 * 1024) / delta, 2), "MiB/s"
            )

            if rank == 0:
                print("Ingest stats from rank=0:\n\n{}".format(data_shard.stats()))

        return train_loop_per_worker


if __name__ == "__main__":
    # Generate a synthetic dataset of ~10GiB of float64 data. The dataset is sharded
    # into 100 blocks (parallelism=100).
    dataset = ray.data.range_tensor(50000, shape=(80, 80, 4), parallelism=100)

    # An example preprocessor chain that just scales all values by 4.0 in two stages.
    preprocessor = Chain(
        BatchMapper(lambda df: df * 2),
        BatchMapper(lambda df: df * 2),
    )

    # Setup the dummy trainer that prints ingest stats.
    # Run and print ingest stats.
    trainer = DummyTrainer(
        scaling_config={"num_workers": 1, "use_gpu": False},
        datasets={"train": dataset},
        preprocessor=preprocessor,
        runtime_seconds=30,  # Stop after this amount or time or 1 epoch is read.
        prefetch_blocks=1,  # Number of blocks to prefetch when reading data.
        dataset_config={"valid": DatasetConfig(transform=False)},
        batch_size=None,
    )
    print("Dataset config", trainer.get_dataset_config())
    trainer.fit()

    # Print memory stats (you can also use "ray memory --stats-only" to monitor this
    # during the middle of the run.
    try:
        print(
            "Memory stats at end of ingest:\n\n{}".format(
                ray._private.internal_api.memory_summary(stats_only=True)
            )
        )
    except Exception:
        print("Error getting Ray memory stats")
