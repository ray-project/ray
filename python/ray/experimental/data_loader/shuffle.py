import itertools
import time
import timeit
import threading
from typing import Callable, List, Iterable, Union

import pandas as pd
import numpy as np
from smart_open import open

import ray
from ray.experimental.data_loader.stats import (
    TrialStatsCollector, TrialStatsCollectorFromMemory, collect_store_stats,
    TrialStatsFromMemory)


#
# In-memory shuffling, loads data from disk once per epoch.
#


def shuffle_from_memory_with_stats(
        filenames: List[str],
        batch_consumer: Callable[[int, Iterable[pd.DataFrame]], None],
        num_epochs: int,
        num_rounds: int,
        num_reducers: int,
        num_trainers: int,
        max_concurrent_epochs: int,
        utilization_sample_period: float) -> (TrialStatsFromMemory, List):
    """
    Shuffle the provided dataset every epoch, using round-based shuffling that
    caches the input dataset in cluster memory at the beginning of each epoch.
    """
    stats = None
    store_stats = []
    done_event = threading.Event()
    store_stats_collector_thread = threading.Thread(
        target=collect_store_stats,
        args=(store_stats, done_event, utilization_sample_period))
    try:
        store_stats_collector_thread.start()

        print(
            f"Doing {num_epochs} epochs each with {num_rounds} shuffle "
            "rounds.")

        stats = shuffle_from_memory(
            filenames,
            batch_consumer,
            num_epochs,
            num_rounds,
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
        batch_consumer: Callable[[int, Iterable[pd.DataFrame]], None],
        num_epochs: int,
        num_rounds: int,
        num_reducers: int,
        num_trainers: int,
        max_concurrent_epochs: int,
        utilization_sample_period: float) -> (float, None):
    """
    Shuffle the provided dataset every epoch, using round-based shuffling that
    caches the input dataset in cluster memory at the beginning of each epoch.
    """
    print(f"Doing {num_epochs} epochs each with {num_rounds} shuffle rounds.")
    duration = shuffle_from_memory(
        filenames,
        batch_consumer,
        num_epochs,
        num_rounds,
        num_reducers,
        num_trainers,
        max_concurrent_epochs,
        collect_stats=False)
    return duration, None


def shuffle_from_memory(
        filenames: List[str],
        batch_consumer: Callable[[int, Iterable[pd.DataFrame]], None],
        num_epochs: int,
        num_rounds: int,
        num_reducers: int,
        num_trainers: int,
        max_concurrent_epochs: int,
        collect_stats: bool = True) -> Union[TrialStatsFromMemory, float]:
    if collect_stats:
        stats_collector = TrialStatsCollectorFromMemory.remote(
            num_epochs, len(filenames), num_reducers, num_trainers, num_rounds)
    else:
        stats_collector = None

    start = timeit.default_timer()

    in_progress = []
    num_done = 0
    reducer_wait_batch = 1
    for epoch_idx in range(num_epochs):
        # Throttle epoch pipelining.
        # TODO(Clark): Find a way to more aggressively delete object
        # references without slow wait loops.
        reducers_to_wait_for = (
            len(in_progress) - (
                (max_concurrent_epochs - 1) * num_rounds * num_reducers *
                num_trainers))
        if reducers_to_wait_for > 0:
            print(f"Throttling on epoch {epoch_idx}, waiting for "
                  f"{reducers_to_wait_for} reducer-trainer chunks, "
                  f"{len(in_progress)} in progress.")
            start_throttle = timeit.default_timer()
            while reducers_to_wait_for > 0:
                new_done, in_progress = ray.wait(
                    in_progress, num_returns=reducer_wait_batch)
                num_done += len(new_done)
                del new_done
                reducers_to_wait_for -= reducer_wait_batch
            time = timeit.default_timer() - start
            throughput = num_done / time
            print(f"Throughput after throttle: {throughput:.2f} reducer "
                  "trainer chunks/sec")
            if stats_collector is not None:
                stats_collector.epoch_throttle_done.remote(
                    epoch_idx, timeit.default_timer() - start_throttle)

        epoch_reducers = shuffle_from_memory_epoch(
            epoch_idx,
            filenames,
            batch_consumer,
            num_rounds,
            num_reducers,
            num_trainers,
            start,
            stats_collector)
        in_progress.extend(epoch_reducers)

    print(f"Waiting until last {len(in_progress)} reducer-trainer chunks are "
          "done.")
    # Block until all epochs are done.
    wait_batch = max(len(in_progress) // 10, 1)
    while in_progress:
        done, in_progress = ray.wait(in_progress, num_returns=wait_batch)
        del done

    print("All epochs done.")

    end = timeit.default_timer()

    # Signal to all batch consumers that we're done producing batches.
    for consumer_idx in range(num_trainers):
        batch_consumer(consumer_idx, None)

    if stats_collector is not None:
        stats_collector.trial_done.remote(end - start)

        return ray.get(stats_collector.get_stats.remote())
    else:
        return end - start


def shuffle_from_memory_epoch(
        epoch: int,
        filenames: List[str],
        batch_consumer: Callable[[int, Iterable[pd.DataFrame]], None],
        num_rounds: int,
        num_reducers: int,
        num_trainers: int,
        trial_start: float,
        stats_collector: Union[TrialStatsCollectorFromMemory, None]) -> None:
    rounds_of_partitions = shuffle_map(
        filenames, num_rounds, num_reducers, stats_collector, epoch)
    if stats_collector is not None:
        stats_collector.epoch_start.remote(epoch)

    epoch_reducers = []
    # TODO(Clark): Move to streaming implementation.
    # TODO(Clark): Make mapper, reducer, and trainer IDs stable in the stats
    # across rounds, epochs, and trials?
    for round_index, round_partitions in enumerate(rounds_of_partitions):
        round_reducers = shuffle_from_memory_round(
            batch_consumer,
            num_reducers,
            num_trainers,
            epoch,
            round_index,
            trial_start,
            stats_collector,
            *round_partitions)

        epoch_reducers.extend(round_reducers)

    return epoch_reducers


def shuffle_map(
        filenames: List[str],
        num_rounds: int,
        num_reducers: int,
        stats_collector: Union[TrialStatsCollectorFromMemory, None],
        epoch: int) -> List[List[ray.ObjectRef]]:
    # TODO(Clark): In each epoch, we're currently loading the full dataset
    # from disk, shuffling each file, and partitioning these files into rounds.
    # We should optimize this so we aren't having to do a disk load at the
    # beginning of each epoch.
    # One option would be to read the files into the object store at the
    # beginning of training, chunked by round partitions, and only select from
    # the desired round partition in each round, shuffle within each round
    # partition, and split to reducers. At the beginning of each epoch, the
    # round partitions could be globally shuffled.
    reducer_partitions = []
    for filename in filenames:
        file_reducer_parts = shuffle_map_file.options(
            num_returns=num_rounds * num_reducers).remote(
                filename, num_rounds, num_reducers,
                stats_collector, epoch)
        if not isinstance(file_reducer_parts, list):
            file_reducer_parts = [file_reducer_parts]
        file_reducer_parts = chunk_it_contiguous(
            file_reducer_parts, num_reducers)
        assert len(file_reducer_parts) == num_rounds
        reducer_partitions.append(file_reducer_parts)
    return list(zip(*reducer_partitions))


@ray.remote
def shuffle_map_file(
        filename: str,
        num_rounds: int,
        num_reducers: int,
        stats_collector: Union[TrialStatsCollectorFromMemory, None],
        epoch: int) -> List[List[ray.ObjectRef]]:
    if stats_collector is not None:
        stats_collector.cache_map_start.remote(epoch)
    start = timeit.default_timer()
    # Load file.
    rows = pd.read_parquet(open(filename, "rb"))
    end_read = timeit.default_timer()

    # Create random round assignment.
    round_assignment = np.random.randint(num_rounds, size=len(rows))
    # Partition the rows into a partition per reducer per round.
    reducer_parts = []
    for round_idx in range(num_rounds):
        round_part = rows[round_assignment == round_idx]
        reducer_assignment = np.random.randint(
            num_reducers, size=len(round_part))
        for reducer_idx in range(num_reducers):
            reducer_part = round_part[reducer_assignment == reducer_idx]
            reducer_parts.append(reducer_part)
    if len(reducer_parts) == 1:
        reducer_parts = reducer_parts[0]
    duration = timeit.default_timer() - start
    read_duration = end_read - start
    if stats_collector is not None:
        stats_collector.cache_map_done.remote(epoch, duration, read_duration)
    return reducer_parts


def shuffle_from_memory_round(
        batch_consumer: Callable[[int, Iterable[pd.DataFrame]], None],
        num_reducers: int,
        num_trainers: int,
        epoch: int,
        round_index: int,
        trial_start: float,
        stats_collector: Union[TrialStatsCollectorFromMemory, None],
        *reducers_partitions: List[List[pd.DataFrame]]) -> None:
    if stats_collector is not None:
        stats_collector.round_start.remote(epoch, round_index)
    shuffled = []
    # reducers_partitions is a 2D (num_files, max(num_reducers, num_files))
    # array of reducer partitions; we chunk it into a (num_files, num_reducers)
    # array of reducer partitions, unzip it into a (num_reducers, num_files)
    # array of reducer partitions, and feed each set of reducer partitions into
    # a reducer.
    for reducer_idx, reducer_partitions in enumerate(zip(
            *[
                chunk_it(file_part, num_reducers)
                for file_part in reducers_partitions])):
        consumer_batches = shuffle_reduce.options(
            num_returns=num_trainers).remote(
                reducer_idx,
                num_trainers,
                stats_collector,
                epoch,
                round_index,
                *itertools.chain.from_iterable(reducer_partitions))
        if not isinstance(consumer_batches, list):
            consumer_batches = [consumer_batches]
        shuffled.append(consumer_batches)
    for trainer_idx, batches in enumerate(
        zip(
            *[
                chunk_it(reducer_part, num_trainers)
                for reducer_part in shuffled])):
        consume(
            trainer_idx, batch_consumer, trial_start, stats_collector, epoch,
            round_index, list(itertools.chain.from_iterable(batches)))
    shuffled = list(itertools.chain.from_iterable(shuffled))
    return shuffled


#
# Shared shuffle stages.
#


@ray.remote
def shuffle_reduce(
        reduce_index: int, num_trainers: int,
        stats_collector: Union[TrialStatsCollector, None], epoch: int,
        round_index: int, *chunks: pd.DataFrame) -> List[pd.DataFrame]:
    if stats_collector is not None:
        stats_collector.reduce_start.remote(epoch, round_index)
    start = timeit.default_timer()
    # Concatenate chunks from all mapper partitions.
    batch = pd.concat(chunks)
    # Shuffle the batch.
    batch = batch.sample(frac=1)
    # Return a list of batches, one for each trainer.
    batch = np.array_split(batch, num_trainers)
    if len(batch) == 1:
        batch = batch[0]
    duration = timeit.default_timer() - start
    if stats_collector is not None:
        stats_collector.reduce_done.remote(epoch, round_index, duration)
    return batch


def consume(
        trainer_idx: int,
        batch_consumer: Callable[[int, Iterable[pd.DataFrame]], None],
        trial_start: float, stats_collector: Union[TrialStatsCollector, None],
        epoch: int, round_index: int, batches: List[ray.ObjectRef]) -> None:
    print(f"Sending to consumer {trainer_idx} in epoch {epoch}, round "
          f"{round_index}")
    if stats_collector is not None:
        stats_collector.consume_start.remote(epoch, round_index)
    start = timeit.default_timer()
    trial_time_to_consume = start - trial_start
    batch_consumer(trainer_idx, batches)
    end = timeit.default_timer()
    duration = end - start
    if stats_collector is not None:
        stats_collector.consume_done.remote(
            epoch, round_index, duration, trial_time_to_consume)


#
# (LEGACY) Disk-based shuffle, loads full file from disk in each round.
#


def shuffle_from_disk(
        num_epochs,
        num_rounds,
        filenames,
        num_trainers,
        max_concurrent_epochs,
        max_concurrent_rounds,
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
            epoch_idx,
            filenames,
            num_rounds,
            num_trainers,
            max_concurrent_rounds,
            seed,
            stats_collector)
        in_progress.append(epoch)
        # Throttle epoch pipelining.
        epochs_to_wait_for = len(in_progress) - max_concurrent_epochs
        if epochs_to_wait_for > 0:
            start_throttle = timeit.default_timer()
            _, in_progress = ray.wait(
                in_progress, num_returns=epochs_to_wait_for)
            stats_collector.epoch_throttle_done.remote(
                epoch_idx, timeit.default_timer() - start_throttle)

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
def shuffle_from_disk_epoch(
        epoch,
        filenames,
        num_rounds,
        num_trainers,
        max_concurrent_rounds,
        seed,
        stats_collector):
    start = timeit.default_timer()

    max_concurrent_consumers = num_trainers * max_concurrent_rounds
    in_progress = []
    # TODO(Clark): Move to streaming implementation.
    for round_index in range(num_rounds):
        chunks = []
        for filename in filenames:
            chunk = shuffle_select_from_disk.options(
                num_returns=num_trainers).remote(
                    filename,
                    num_trainers,
                    seed,
                    num_rounds,
                    epoch,
                    round_index,
                    stats_collector)
            if not isinstance(chunk, list):
                chunk = [chunk]
            chunks.append(chunk)

        shuffled = [
            shuffle_reduce.remote(
                j,
                stats_collector,
                epoch,
                round_index,
                *[chunks[i][j] for i in range(len(filenames))])
            for j in range(num_trainers)]
        in_progress.extend([
            consume.remote(batch, start, stats_collector, epoch, round_index)
            for batch in shuffled])
        # Throttle round pipelining.
        consumers_to_wait_for = len(in_progress) - max_concurrent_consumers
        if consumers_to_wait_for > 0:
            start_throttle = timeit.default_timer()
            _, in_progress = ray.wait(
                in_progress, num_returns=consumers_to_wait_for)
            stats_collector.round_throttle_done(
                epoch, round_index, timeit.default_timer() - start_throttle)

    # Block until all consumers are done.
    if in_progress:
        ray.wait(in_progress, num_returns=len(in_progress))

    end = timeit.default_timer()

    ray.wait(
        [stats_collector.epoch_done.remote(epoch, end - start)], num_returns=1)


@ray.remote
def shuffle_select_from_disk(
        filename, num_reducers, seed, num_rounds, epoch, round_index,
        stats_collector):
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
