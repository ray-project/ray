#!/usr/bin/env python

import sys
import time
from typing import Optional

import numpy as np

import ray
from ray.air import session
from ray.air.config import DatasetConfig, ScalingConfig
from ray.data import Dataset, DatasetIterator, Preprocessor
from ray.data.preprocessors import BatchMapper, Chain
from ray.train._internal.dataset_spec import DataParallelIngestSpec
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DummyTrainer(DataParallelTrainer):
    """A Trainer that does nothing except read the data for a given number of epochs.

    It prints out as much debugging statistics as possible.

    This is useful for debugging data ingest problem. This trainer supports normal
    scaling options same as any other Trainer (e.g., num_workers, use_gpu).
    """

    def __init__(
        self,
        *args,
        scaling_config: Optional[ScalingConfig] = None,
        num_epochs: int = 1,
        prefetch_blocks: int = 1,
        batch_size: Optional[int] = 4096,
        **kwargs
    ):
        if not scaling_config:
            scaling_config = ScalingConfig(num_workers=1)
        super().__init__(
            train_loop_per_worker=DummyTrainer.make_train_loop(
                num_epochs, prefetch_blocks, batch_size
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
        num_epochs: int, prefetch_blocks: int, batch_size: Optional[int]
    ):
        """Make a debug train loop that runs for the given amount of epochs."""

        def train_loop_per_worker():
            import pandas as pd

            rank = session.get_world_rank()
            data_shard = session.get_dataset_shard("train")
            start = time.perf_counter()
            epochs_read, batches_read, bytes_read = 0, 0, 0
            batch_delays = []

            print("Starting train loop on worker", rank)
            for epoch in range(num_epochs):
                epochs_read += 1
                batch_start = time.perf_counter()
                for batch in data_shard.iter_batches(
                    prefetch_blocks=prefetch_blocks, batch_size=batch_size
                ):
                    batch_delay = time.perf_counter() - batch_start
                    batch_delays.append(batch_delay)
                    batches_read += 1
                    if isinstance(batch, pd.DataFrame):
                        bytes_read += int(
                            batch.memory_usage(index=True, deep=True).sum()
                        )
                    elif isinstance(batch, np.ndarray):
                        bytes_read += batch.nbytes
                    else:
                        # NOTE: This isn't recursive and will just return the size of
                        # the object pointers if list of non-primitive types.
                        bytes_read += sys.getsizeof(batch)
                    session.report(
                        dict(
                            bytes_read=bytes_read,
                            batches_read=batches_read,
                            epochs_read=epochs_read,
                            batch_delay=batch_delay,
                        )
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
            print("Num epochs read", epochs_read)
            print("Num batches read", batches_read)
            print("Num bytes read", round(bytes_read / (1024 * 1024), 2), "MiB")
            print(
                "Mean throughput", round(bytes_read / (1024 * 1024) / delta, 2), "MiB/s"
            )

            if rank == 0:
                print("Ingest stats from rank=0:\n\n{}".format(data_shard.stats()))

        return train_loop_per_worker


@DeveloperAPI
def make_local_dataset_iterator(
    dataset: Dataset,
    preprocessor: Preprocessor,
    dataset_config: DatasetConfig,
) -> DatasetIterator:
    """A helper function to create a local
    :py:class:`DatasetIterator <ray.data.DatasetIterator>`,
    like the one returned by :meth:`~ray.air.session.get_dataset_shard`.

    This function should only be used for development and debugging. It will
    raise an exception if called by a worker instead of the driver.

    Args:
        dataset: The input Dataset.
        preprocessor: The preprocessor that will be applied to the input dataset.
        dataset_config: The dataset config normally passed to the trainer.
    """
    runtime_context = ray.runtime_context.get_runtime_context()
    if runtime_context.worker.mode == ray._private.worker.WORKER_MODE:
        raise RuntimeError(
            "make_local_dataset_iterator should only be used by the driver "
            "for development and debugging. To consume a dataset from a "
            "worker or AIR trainer, see "
            "https://docs.ray.io/en/latest/ray-air/check-ingest.html."
        )

    dataset_config = dataset_config.fill_defaults()
    spec = DataParallelIngestSpec({"train": dataset_config})
    spec.preprocess_datasets(preprocessor, {"train": dataset})
    training_worker_handles = [None]
    it = spec.get_dataset_shards(training_worker_handles)[0]["train"]
    return it


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-epochs", "-e", type=int, default=1, help="Number of epochs to read."
    )
    parser.add_argument(
        "--prefetch-blocks",
        "-b",
        type=int,
        default=1,
        help="Number of blocks to prefetch when reading data.",
    )

    args = parser.parse_args()

    # Generate a synthetic dataset of ~10GiB of float64 data. The dataset is sharded
    # into 100 blocks (parallelism=100).
    dataset = ray.data.range_tensor(50000, shape=(80, 80, 4), parallelism=100)

    # An example preprocessor chain that just scales all values by 4.0 in two stages.
    preprocessor = Chain(
        BatchMapper(lambda df: df * 2, batch_format="pandas"),
        BatchMapper(lambda df: df * 2, batch_format="pandas"),
    )

    # Setup the dummy trainer that prints ingest stats.
    # Run and print ingest stats.
    trainer = DummyTrainer(
        scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
        datasets={"train": dataset},
        preprocessor=preprocessor,
        num_epochs=args.num_epochs,
        prefetch_blocks=args.prefetch_blocks,
        dataset_config={"train": DatasetConfig()},
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
