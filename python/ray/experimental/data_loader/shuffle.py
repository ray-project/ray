import time
import timeit
import threading
from typing import Callable, List, Iterable, Union

import pandas as pd
import numpy as np
from smart_open import open

import ray
from ray.experimental.data_loader.stats import (
    TrialStatsCollector, collect_store_stats, TrialStats)

#
# In-memory shuffling, loads data from disk once per epoch.
#


def shuffle_from_memory_with_stats(
        filenames: List[str],
        batch_consumer: Callable[[int, int, Iterable[pd.DataFrame]], None],
        num_epochs: int, num_reducers: int, num_trainers: int,
        max_concurrent_epochs: int,
        utilization_sample_period: float) -> (TrialStats, List):
    """
    Shuffle the provided dataset every epoch.
    """
    stats = None
    store_stats = []
    done_event = threading.Event()
    store_stats_collector_thread = threading.Thread(
        target=collect_store_stats,
        args=(store_stats, done_event, utilization_sample_period))
    try:
        store_stats_collector_thread.start()

        print(f"Doing {num_epochs} epochs of shuffling.")

        stats = shuffle_from_memory(
            filenames,
            batch_consumer,
            num_epochs,
            num_reducers,
            num_trainers,
            max_concurrent_epochs,
            collect_stats=True)
    finally:
        # Signal store stats collector thread that we're done, join the
        # thread.
        done_event.set()
        store_stats_collector_thread.join()

    return stats, store_stats


def shuffle_from_memory_no_stats(
        filenames: List[str],
        batch_consumer: Callable[[int, int, Iterable[pd.DataFrame]], None],
        num_epochs: int, num_reducers: int, num_trainers: int,
        max_concurrent_epochs: int,
        utilization_sample_period: float) -> (float, None):
    """
    Shuffle the provided dataset every epoch.
    """
    print(f"Doing {num_epochs} epochs of shuffling.")
    duration = shuffle_from_memory(
        filenames,
        batch_consumer,
        num_epochs,
        num_reducers,
        num_trainers,
        max_concurrent_epochs,
        collect_stats=False)
    return duration, None


def shuffle_from_memory(
        filenames: List[str],
        batch_consumer: Callable[[int, int, Iterable[pd.DataFrame]], None],
        num_epochs: int,
        num_reducers: int,
        num_trainers: int,
        max_concurrent_epochs: int,
        collect_stats: bool = True) -> Union[TrialStats, float]:
    if collect_stats:
        stats_collector = TrialStatsCollector.remote(
            num_epochs, len(filenames), num_reducers, num_trainers)
    else:
        stats_collector = None

    start = timeit.default_timer()

    # A list containing reducer output refs for all in-progress epochs.
    in_progress = []
    # We wait for reducer outputs in num_trainers batches given that trainers
    # will consume the reducer outputs in lockstep for a training step, so
    # num_trainers reducer outputs should be released at ~the same time.
    # TODO(Clark): Tweak this heuristic.
    wait_batch = num_trainers
    num_done = 0
    for epoch_idx in range(num_epochs):
        # Throttle epoch pipelining.
        # Get the number of epochs currently in progress.
        num_in_progress_epochs = len(in_progress) // num_reducers
        # Get the number of epochs whose finishing we need to wait for before
        # starting another epoch.
        epochs_to_wait_for = 1 + num_in_progress_epochs - max_concurrent_epochs
        if epochs_to_wait_for > 0:
            # Convert the number of epochs we need to wait for to the number of
            # reducers that we need to wait for.
            reducers_to_wait_for = epochs_to_wait_for * num_reducers
            print(f"Throttling on epoch {epoch_idx}, waiting for "
                  f"{epochs_to_wait_for} epochs, {num_in_progress_epochs} in "
                  "progress.")
            # We wait on the reducers from the first epochs_to_wait_for epochs,
            # ensuring that we give earlier straggler epochs all of the
            # resources they need to finish since epochs are consumed in order.
            refs_to_wait_for = in_progress[:reducers_to_wait_for]
            in_progress = in_progress[reducers_to_wait_for:]
            start_throttle = timeit.default_timer()
            # While throttling, we wait for these refs in num_trainers batches
            # in order to more aggressively free the associated reducer objects
            # from the object store.
            while refs_to_wait_for:
                new_done, refs_to_wait_for = ray.wait(
                    refs_to_wait_for,
                    num_returns=wait_batch,
                    fetch_local=False)
                num_done += wait_batch
                del new_done
            time = timeit.default_timer() - start
            throughput = num_done / time
            print(f"Throughput after throttle: {throughput:.2f} reducer"
                  " chunks/sec")
            if stats_collector is not None:
                stats_collector.epoch_throttle_done.remote(
                    epoch_idx,
                    timeit.default_timer() - start_throttle)

        epoch_reducers = shuffle_from_memory_epoch(
            epoch_idx, filenames, batch_consumer, num_reducers, num_trainers,
            start, stats_collector)
        in_progress.extend(epoch_reducers)

    print(f"Waiting until last {len(in_progress)} reducer chunks are " "done.")
    # Block until all epochs are done.
    while in_progress:
        new_done, in_progress = ray.wait(
            in_progress, num_returns=wait_batch, fetch_local=False)
        del new_done

    print("All epochs done.")

    end = timeit.default_timer()

    if stats_collector is not None:
        stats_collector.trial_done.remote(end - start)

        return ray.get(stats_collector.get_stats.remote())
    else:
        return end - start


def shuffle_from_memory_epoch(
        epoch: int, filenames: List[str],
        batch_consumer: Callable[[int, int, Iterable[pd.DataFrame]], None],
        num_reducers: int, num_trainers: int, trial_start: float,
        stats_collector: Union[TrialStatsCollector, None]) -> None:
    if stats_collector is not None:
        stats_collector.epoch_start.remote(epoch)
    reducers_partitions = []
    for filename in filenames:
        file_reducer_parts = shuffle_map.options(
            num_returns=num_reducers).remote(filename, num_reducers,
                                             stats_collector, epoch)
        if not isinstance(file_reducer_parts, list):
            file_reducer_parts = [file_reducer_parts]
        reducers_partitions.append(file_reducer_parts)

    shuffled = []
    for reducer_idx, reducer_partitions in enumerate(
            zip(*reducers_partitions)):
        consumer_batches = shuffle_reduce.remote(reducer_idx, stats_collector,
                                                 epoch, *reducer_partitions)
        shuffled.append(consumer_batches)
    for trainer_idx, batches in enumerate(
            np.array_split(shuffled, num_trainers)):
        consume(trainer_idx, batch_consumer, trial_start, stats_collector,
                epoch, list(batches))
        # Signal to all batch consumers that we're done producing batches for
        # this epoch.
        batch_consumer(trainer_idx, epoch, None)
    return shuffled


@ray.remote
def shuffle_map(filename: str, num_reducers: int,
                stats_collector: Union[TrialStatsCollector, None],
                epoch: int) -> List[List[ray.ObjectRef]]:
    if stats_collector is not None:
        stats_collector.map_start.remote(epoch)
    start = timeit.default_timer()
    # Load file.
    rows = pd.read_parquet(open(filename, "rb"))
    assert len(rows) > num_reducers
    end_read = timeit.default_timer()

    # Create random reducer assignment.
    reducer_assignment = np.random.randint(num_reducers, size=len(rows))
    # Partition the rows into a partition per reducer.
    reducer_parts = []
    for reducer_idx in range(num_reducers):
        reducer_part = rows[reducer_assignment == reducer_idx]
        reducer_parts.append(reducer_part)
    if len(reducer_parts) == 1:
        reducer_parts = reducer_parts[0]
    duration = timeit.default_timer() - start
    read_duration = end_read - start
    if stats_collector is not None:
        stats_collector.map_done.remote(epoch, duration, read_duration)
    return reducer_parts


#
# Shared shuffle stages.
#


@ray.remote
def shuffle_reduce(reduce_index: int,
                   stats_collector: Union[TrialStatsCollector, None],
                   epoch: int, *chunks: pd.DataFrame) -> List[pd.DataFrame]:
    if stats_collector is not None:
        stats_collector.reduce_start.remote(epoch)
    start = timeit.default_timer()
    # Concatenate chunks from all mapper partitions.
    batch = pd.concat(chunks)
    # Shuffle the batch.
    batch = batch.sample(frac=1)
    if len(batch) == 1:
        batch = batch[0]
    duration = timeit.default_timer() - start
    if stats_collector is not None:
        stats_collector.reduce_done.remote(epoch, duration)
    return batch


def consume(trainer_idx: int,
            batch_consumer: Callable[[int, int, Iterable[pd.DataFrame]], None],
            trial_start: float,
            stats_collector: Union[TrialStatsCollector, None], epoch: int,
            batches: List[ray.ObjectRef]) -> None:
    print(f"Sending to consumer {trainer_idx} in epoch {epoch}")
    if stats_collector is not None:
        stats_collector.consume_start.remote(epoch)
    start = timeit.default_timer()
    trial_time_to_consume = start - trial_start
    # TODO(Clark): Confirm that epochs are consumed in order, e.g. that batches
    # from epoch 2 aren't interleaved with batches from epoch 1.
    batch_consumer(trainer_idx, epoch, batches)
    end = timeit.default_timer()
    duration = end - start
    if stats_collector is not None:
        stats_collector.consume_done.remote(epoch, duration,
                                            trial_time_to_consume)


#
# (LEGACY) Disk-based shuffle, loads full file from disk in each round.
#


def shuffle_from_disk(num_epochs, num_rounds, filenames, num_trainers,
                      max_concurrent_epochs, max_concurrent_rounds,
                      utilization_sample_period):
    # TODO(Clark): Add a stats collector per round? Could result in exhausting
    # the Ray cluster with stats collecting actors.
    stats_collector = TrialStatsCollector.remote(
        num_epochs, len(filenames), num_trainers, num_trainers, num_rounds)

    store_stats = []
    done_event = threading.Event()
    store_stats_collector_thread = threading.Thread(
        target=collect_store_stats,
        args=(store_stats, done_event, utilization_sample_period))
    store_stats_collector_thread.start()

    print(f"Doing {num_rounds} shuffle rounds.")

    start = timeit.default_timer()

    in_progress = []
    for epoch_idx in range(num_epochs):
        seed = time.time()
        epoch = shuffle_from_disk_epoch.remote(
            epoch_idx, filenames, num_rounds, num_trainers,
            max_concurrent_rounds, seed, stats_collector)
        in_progress.append(epoch)
        # Throttle epoch pipelining.
        epochs_to_wait_for = len(in_progress) - max_concurrent_epochs
        if epochs_to_wait_for > 0:
            start_throttle = timeit.default_timer()
            _, in_progress = ray.wait(
                in_progress, num_returns=epochs_to_wait_for)
            stats_collector.epoch_throttle_done.remote(
                epoch_idx,
                timeit.default_timer() - start_throttle)

    # Block until all consumers are done.
    if in_progress:
        ray.wait(in_progress, num_returns=len(in_progress))

    end = timeit.default_timer()

    stats_collector.trial_done.remote(end - start)

    # Signal store stats collector thread that we're done, join the thread.
    done_event.set()
    store_stats_collector_thread.join()

    stats = ray.get(stats_collector.get_stats.remote())

    return stats, store_stats


@ray.remote
def shuffle_from_disk_epoch(epoch, filenames, num_rounds, num_trainers,
                            max_concurrent_rounds, seed, stats_collector):
    start = timeit.default_timer()

    max_concurrent_consumers = num_trainers * max_concurrent_rounds
    in_progress = []
    # TODO(Clark): Move to streaming implementation.
    for round_index in range(num_rounds):
        chunks = []
        for filename in filenames:
            chunk = shuffle_select_from_disk.options(
                num_returns=num_trainers).remote(filename, num_trainers, seed,
                                                 num_rounds, epoch,
                                                 round_index, stats_collector)
            if not isinstance(chunk, list):
                chunk = [chunk]
            chunks.append(chunk)

        shuffled = [
            shuffle_reduce.remote(
                j, stats_collector, epoch, round_index,
                *[chunks[i][j] for i in range(len(filenames))])
            for j in range(num_trainers)
        ]
        in_progress.extend([
            consume.remote(batch, start, stats_collector, epoch, round_index)
            for batch in shuffled
        ])
        # Throttle round pipelining.
        consumers_to_wait_for = len(in_progress) - max_concurrent_consumers
        if consumers_to_wait_for > 0:
            start_throttle = timeit.default_timer()
            _, in_progress = ray.wait(
                in_progress, num_returns=consumers_to_wait_for)
            stats_collector.round_throttle_done(
                epoch, round_index,
                timeit.default_timer() - start_throttle)

    # Block until all consumers are done.
    if in_progress:
        ray.wait(in_progress, num_returns=len(in_progress))

    end = timeit.default_timer()

    ray.wait(
        [stats_collector.epoch_done.remote(epoch, end - start)], num_returns=1)


@ray.remote
def shuffle_select_from_disk(filename, num_reducers, seed, num_rounds, epoch,
                             round_index, stats_collector):
    stats_collector.map_start.remote(round_index)
    start = timeit.default_timer()
    # Load file.
    rows = pd.read_parquet(filename)
    end_read = timeit.default_timer()

    # Select rows based on our map index and the random seed.
    rows = rows.sample(frac=1, random_state=seed)
    rows = np.array_split(rows, num_rounds)[round_index]

    # Return a list of chunks, one for each reducer.
    split = np.array_split(rows, num_reducers)
    if len(split) == 1:
        split = split[0]
    duration = timeit.default_timer() - start
    read_duration = end_read - start
    stats_collector.map_done.remote(round_index, duration, read_duration)
    return split


def chunk_it(iterable, n):
    chunked = [[] for _ in range(n)]
    for idx, item in enumerate(iterable):
        chunked[idx % n].append(item)
    return chunked


def chunk_it_contiguous(iterable, n):
    chunked = []
    for idx, item in enumerate(iterable):
        try:
            sub_chunked = chunked[idx // n]
        except IndexError:
            sub_chunked = []
            chunked.append(sub_chunked)
        sub_chunked.append(item)
    return chunked


def split_num(n, k):
    return [n // k] * (k - (n % k)) + [n // k + 1] * (n % k)
