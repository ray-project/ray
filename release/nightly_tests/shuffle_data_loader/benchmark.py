import argparse
import asyncio
import collections
import contextlib
import glob
import json
import os
import timeit
from typing import List

import numpy as np

import ray
from ray_shuffling_data_loader.shuffle import shuffle, BatchConsumer
from ray_shuffling_data_loader.stats import (
    TrialStatsCollector,
    ObjectStoreStatsCollector,
    process_stats,
    human_readable_size,
)

from ray_shuffling_data_loader.data_generation import generate_data

BATCHQUEUE_ACTOR_NAME = "BatchQueue"
DEFAULT_DATA_DIR = "/mnt/disk0/benchmark_scratch"
DEFAULT_STATS_DIR = "./results"

DEFAULT_UTILIZATION_SAMPLE_PERIOD = 5.0


@ray.remote(num_cpus=0)
class Consumer:
    def __init__(self, rank, num_epochs, max_concurrent_epochs, stats_collector=None):
        self._rank = rank
        self._num_epochs = num_epochs
        self._max_epochs = max_concurrent_epochs
        self._curr_epochs = collections.deque()
        self._epoch_done_evs = [asyncio.Event() for _ in range(num_epochs)]
        self._stats_collector = stats_collector

    async def new_epoch(self, epoch):
        if len(self._curr_epochs) == self._max_epochs:
            first_epoch = self._curr_epochs.popleft()
            await self._epoch_done_evs[first_epoch].wait()
        self._curr_epochs.append(epoch)
        print(f"Starting epoch {epoch} on consumer {self._rank}.")

    def consume(self, epoch, batch):
        print(f"Consuming batch on consumer {self._rank} for epoch {epoch}.")
        if self._stats_collector is not None:
            self._stats_collector.consume_batch.remote(epoch, len(batch))

    def producer_done(self, epoch):
        if self._stats_collector is not None:
            self._stats_collector.consume_done.remote(epoch)
        self._epoch_done_evs[epoch].set()
        print(f"Epoch {epoch} done on consumer {self._rank}.")

    async def wait_until_all_epochs_done(self):
        await self._epoch_done_evs[self._num_epochs - 1].wait()

    def ready(self):
        pass


class BatchConsumer(BatchConsumer):
    def __init__(
        self, num_trainers, num_epochs, pg, max_concurrent_epochs, stats_collector=None
    ):
        self._consumers = [
            Consumer.options(placement_group=pg).remote(
                rank, num_epochs, max_concurrent_epochs, stats_collector
            )
            for rank in range(num_trainers)
        ]

    def consume(self, rank: int, epoch: int, batches: List[ray.ObjectRef]):
        if batches is not None:
            for batch in batches:
                self._consumers[rank].consume.remote(epoch, batch)

    def producer_done(self, rank: int, epoch: int):
        self._consumers[rank].producer_done.remote(epoch)

    def wait_until_ready(self, epoch: int):
        ray.get([consumer.new_epoch.remote(epoch) for consumer in self._consumers])

    def wait_until_all_epochs_done(self):
        ray.get(
            [
                consumer.wait_until_all_epochs_done.remote()
                for consumer in self._consumers
            ]
        )

    def actors_ready(self):
        ray.get([consumer.ready.remote() for consumer in self._consumers])

    def get_stats(self):
        (
            consume_times,
            time_to_consumes,
            consume_start_times,
            consume_end_times,
        ) = tuple(
            list(zip(*stats_))
            for stats_ in zip(
                *ray.get([consumer.get_stats.remote() for consumer in self._consumers])
            )
        )
        consume_stage_durations = []
        for start_times, end_times in zip(consume_start_times, consume_end_times):
            consume_stage_durations.append(max(end_times) - min(start_times))
        return consume_times, time_to_consumes, consume_stage_durations


def run_trials(
    num_epochs,
    filenames,
    num_reducers,
    num_trainers,
    max_concurrent_epochs,
    utilization_sample_period,
    collect_stats=True,
    num_trials=None,
    trials_timeout=None,
):
    """
    Run shuffling trials.
    """
    print("Using from-memory shuffler.")
    all_stats = []
    pg = ray.util.placement_group(
        [{"CPU": 0.1} for _ in range(num_trainers)], strategy="SPREAD"
    )
    ray.get(pg.ready())
    if collect_stats:
        stats_collector = TrialStatsCollector.remote(
            num_epochs, len(filenames), num_reducers, num_trainers
        )
        object_store_stats_collector = ObjectStoreStatsCollector(
            utilization_sample_period
        )
    else:
        stats_collector = None
        try:
            object_store_stats_collector = contextlib.nullcontext()
        except AttributeError:
            # Python 3.6 doesn't support nullcontext().
            object_store_stats_collector = contextlib.suppress()
    batch_consumer = BatchConsumer(
        num_trainers, num_epochs, pg, max_concurrent_epochs, stats_collector
    )
    # Wait until batch consumer actors have been created.
    batch_consumer.actors_ready()
    if num_trials is not None:
        for trial in range(num_trials):
            print(f"Starting trial {trial}.")
            with object_store_stats_collector:
                duration = shuffle(
                    filenames,
                    batch_consumer,
                    num_epochs,
                    num_reducers,
                    num_trainers,
                    stats_collector,
                )
            print(f"Trial {trial} done after {duration} seconds.")
            if collect_stats:
                stats = ray.get(stats_collector.get_stats.remote())
                store_stats = object_store_stats_collector.get_stats()
            else:
                stats = duration
                store_stats = None
            all_stats.append((stats, store_stats))
    elif trials_timeout is not None:
        start = timeit.default_timer()
        trial = 0
        while timeit.default_timer() - start < trials_timeout:
            print(f"Starting trial {trial}.")
            with object_store_stats_collector:
                duration = shuffle(
                    filenames,
                    batch_consumer,
                    num_epochs,
                    num_reducers,
                    num_trainers,
                    stats_collector,
                )
            print(f"Trial {trial} done after {duration} seconds.")
            if collect_stats:
                stats = ray.get(stats_collector.get_stats.remote())
                store_stats = object_store_stats_collector.get_stats()
            else:
                stats = duration
                store_stats = None
            all_stats.append((stats, store_stats))
            trial += 1
    else:
        raise ValueError("One of num_trials and trials_timeout must be specified")
    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shuffling data loader")
    parser.add_argument("--num-rows", type=int, default=4 * (10 ** 11))
    parser.add_argument("--num-files", type=int, default=100)
    parser.add_argument("--max-row-group-skew", type=float, default=0.0)
    parser.add_argument("--num-row-groups-per-file", type=int, default=1)
    parser.add_argument("--num-reducers", type=int, default=5)
    parser.add_argument("--num-trainers", type=int, default=5)
    parser.add_argument("--num-epochs", type=int, default=10)
    parser.add_argument("--max-concurrent-epochs", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--num-trials", type=int, default=None)
    parser.add_argument("--trials-timeout", type=int, default=None)
    parser.add_argument(
        "--utilization-sample-period",
        type=float,
        default=DEFAULT_UTILIZATION_SAMPLE_PERIOD,
    )
    parser.add_argument("--cluster", action="store_true")
    parser.add_argument("--data-dir", type=str, default=DEFAULT_DATA_DIR)
    parser.add_argument("--stats-dir", type=str, default=DEFAULT_STATS_DIR)
    parser.add_argument("--clear-old-data", action="store_true")
    parser.add_argument("--use-old-data", action="store_true")
    parser.add_argument("--no-stats", action="store_true")
    parser.add_argument("--no-epoch-stats", action="store_true")
    parser.add_argument("--no-consumer-stats", action="store_true")
    parser.add_argument("--overwrite-stats", action="store_true")
    parser.add_argument("--unique-stats", action="store_true")
    args = parser.parse_args()

    if args.num_row_groups_per_file < 1:
        raise ValueError("Must have at least one row group per file.")

    num_trials = args.num_trials
    trials_timeout = args.trials_timeout
    if num_trials is not None and trials_timeout is not None:
        raise ValueError(
            "Only one of --num-trials and --trials-timeout should be " "specified."
        )

    if num_trials is None and trials_timeout is None:
        num_trials = 3

    if args.clear_old_data and args.use_old_data:
        raise ValueError(
            "Only one of --clear-old-data and --use-old-data should be " "specified."
        )

    data_dir = args.data_dir
    if args.clear_old_data:
        print(f"Clearing old data from {data_dir}.")
        files = glob.glob(os.path.join(data_dir, "*.parquet.snappy"))
        for f in files:
            os.remove(f)

    if args.cluster:
        print("Connecting to an existing Ray cluster.")
        ray.init(address="auto")
    else:
        print("Starting a new local Ray cluster.")
        ray.init(resources={"resources": 100})

    num_rows = args.num_rows
    num_row_groups_per_file = args.num_row_groups_per_file
    num_files = args.num_files
    max_row_group_skew = args.max_row_group_skew
    if not args.use_old_data:
        print(
            f"Generating {num_rows} rows over {num_files} files, with "
            f"{num_row_groups_per_file} row groups per file and at most "
            f"{100 * max_row_group_skew:.1f}% row group skew."
        )
        filenames, num_bytes = generate_data(
            num_rows, num_files, num_row_groups_per_file, max_row_group_skew, data_dir
        )
        print(
            f"Generated {len(filenames)} files containing {num_rows} rows "
            f"with {num_row_groups_per_file} row groups per file, totalling "
            f"{human_readable_size(num_bytes)}."
        )
    else:
        filenames = [
            os.path.join(data_dir, f"input_data_{file_index}.parquet.snappy")
            for file_index in range(num_files)
        ]
        print("Not generating input data, using existing data instead.")

    num_reducers = args.num_reducers
    num_trainers = args.num_trainers
    batch_size = args.batch_size

    num_epochs = args.num_epochs
    max_concurrent_epochs = args.max_concurrent_epochs
    if max_concurrent_epochs is None or max_concurrent_epochs > num_epochs:
        max_concurrent_epochs = num_epochs
    assert max_concurrent_epochs > 0

    utilization_sample_period = args.utilization_sample_period

    # TODO(Clark): Add warmup trials.

    print("\nRunning real trials.")
    if num_trials is not None:
        print(
            f"Running {num_trials} shuffle trials with {num_epochs} epochs, "
            f"{num_reducers} reducers, {num_trainers} trainers, and a batch "
            f"size of {batch_size} over {num_rows} rows."
        )
    else:
        print(
            f"Running {trials_timeout} seconds of shuffle trials with "
            f"{num_epochs} epochs, {num_reducers} reducers, {num_trainers} "
            f"trainers, and a batch size of {batch_size} over {num_rows} "
            "rows."
        )
    print(
        f"Shuffling will be pipelined with at most "
        f"{max_concurrent_epochs} concurrent epochs."
    )
    collect_stats = not args.no_stats
    all_stats = run_trials(
        num_epochs,
        filenames,
        num_reducers,
        num_trainers,
        max_concurrent_epochs,
        utilization_sample_period,
        collect_stats,
        num_trials,
        trials_timeout,
    )

    if collect_stats:
        process_stats(
            all_stats,
            args.overwrite_stats,
            args.stats_dir,
            args.no_epoch_stats,
            args.no_consumer_stats,
            args.unique_stats,
            num_rows,
            num_files,
            num_row_groups_per_file,
            batch_size,
            num_reducers,
            num_trainers,
            num_epochs,
            max_concurrent_epochs,
        )
    else:
        print("Shuffle trials done, no detailed stats collected.")
        times, _ = zip(*all_stats)
        mean = np.mean(times)
        std = np.std(times)
        throughput_std = np.std([num_epochs * num_rows / time for time in times])
        batch_throughput_std = np.std(
            [(num_epochs * num_rows / batch_size) / time for time in times]
        )
        print(f"\nMean over {len(times)} trials: {mean:.3f}s +- {std}")
        print(
            f"Mean throughput over {len(times)} trials: "
            f"{num_epochs * num_rows / mean:.2f} rows/s +- "
            f"{throughput_std:.2f}"
        )
        print(
            f"Mean batch throughput over {len(times)} trials: "
            f"{(num_epochs * num_rows / batch_size) / mean:.2f} batches/s "
            f"+- {batch_throughput_std:.2f}"
        )

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"success": 1}))
