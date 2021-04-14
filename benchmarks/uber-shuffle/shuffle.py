import time
import timeit

import pandas as pd
import numpy as np

import ray

from stats import TrialStatsCollector, TrialStatsCollectorFromMemory


#
# In-memory shuffling, loads data from disk once per epoch.
#


def shuffle_from_memory(
        num_epochs,
        num_rounds,
        filenames,
        num_trainers,
        batch_size,
        batches_per_round,
        num_rows,
        max_concurrent_rounds,
        max_concurrent_epochs,
        ):
    # v = Validator.remote(filenames)
    # num_expected_rows = ray.get(v.get_num_expected_rows.remote())
    # print("Expecting", num_expected_rows, "rows")

    stats_collector = TrialStatsCollectorFromMemory.remote(
        num_epochs, len(filenames), num_trainers, num_trainers, num_rounds)

    print(f"Doing {num_epochs} epochs each with {num_rounds} shuffle rounds.")

    # TODO(Clark): Make mapper, reducer, and trainer IDs stable in the stats
    # across rounds, epochs, and trials?

    start = timeit.default_timer()

    in_progress = []
    for epoch_idx in range(num_epochs):
        # Throttle epoch pipelining.
        epochs_to_wait_for = len(in_progress) - max_concurrent_epochs + 1
        if epochs_to_wait_for > 0:
            print(f"Throttling on epoch {epoch_idx}, waiting for "
                  f"{epochs_to_wait_for} epochs.")
            start_throttle = timeit.default_timer()
            _, in_progress = ray.wait(
                in_progress, num_returns=epochs_to_wait_for)
            stats_collector.epoch_throttle_done.remote(
                epoch_idx, timeit.default_timer() - start_throttle)

        epoch = shuffle_from_memory_epoch.remote(
            epoch_idx,
            filenames,
            num_rounds,
            num_trainers,
            batches_per_round,
            max_concurrent_rounds,
            stats_collector)
        in_progress.append(epoch)

    # Block until all consumers are done.
    if in_progress:
        ray.wait(in_progress, num_returns=len(in_progress))

    end = timeit.default_timer()

    stats_collector.trial_done.remote(end - start)

    stats = ray.get(stats_collector.get_stats.remote())

    # ray.get(v.check.remote(batches_per_round, *final_shuffled))

    return stats


@ray.remote
def shuffle_from_memory_epoch(
        epoch,
        filenames,
        num_rounds,
        num_trainers,
        batches_per_round,
        max_concurrent_rounds,
        stats_collector):
    start = timeit.default_timer()
    rounds_of_partitions = cache_in_memory(
        filenames, num_rounds, stats_collector, epoch)

    max_concurrent_consumers = num_trainers * max_concurrent_rounds
    in_progress = []
    # TODO(Clark): Move to streaming implementation.
    for round_index, round_partitions in enumerate(rounds_of_partitions):
        # TODO(Clark): Launch each shuffle round in its own Ray task?
        # This would simplify the pipelining and throttling logic.

        # Throttle round pipelining.
        consumers_to_wait_for = len(in_progress) - max_concurrent_consumers + 1
        if consumers_to_wait_for > 0:
            print(f"Throttling on round {round_index}, waiting for "
                  f"{consumers_to_wait_for} consumers.")
            start_throttle = timeit.default_timer()
            _, in_progress = ray.wait(
                in_progress, num_returns=consumers_to_wait_for)
            stats_collector.round_throttle_done.remote(
                epoch, round_index, timeit.default_timer() - start_throttle)

        chunks = []
        for round_partition in round_partitions:
            chunk = shuffle_select_from_memory.options(
                num_returns=num_trainers).remote(
                    round_partition, num_trainers,
                    stats_collector, epoch, round_index)
            if not isinstance(chunk, list):
                chunk = [chunk]
            chunks.append(chunk)
        shuffled = [
            shuffle_reduce.remote(
                j,
                batches_per_round,
                stats_collector,
                epoch,
                round_index,
                *[chunks[i][j] for i in range(len(round_partitions))])
            for j in range(num_trainers)]
        in_progress.extend([
            consume.remote(batch, start, stats_collector, epoch, round_index)
            for batch in shuffled])

    # Block until all consumers are done.
    if in_progress:
        ray.get(in_progress)

    end = timeit.default_timer()
    duration = end - start
    stats_collector.epoch_done.remote(epoch, duration)
    print(f"Epoch {epoch} done after {duration} seconds.")


def cache_in_memory(filenames, num_rounds, stats_collector, epoch):
    # TODO(Clark): Support num_mapers = k * num_files for k > 1.
    # TODO(Clark): Support num_mapers > num_files.
    # TODO(Clark): In each epoch, we're currently loading the full dataset
    # from disk, shuffling each file, and partitioning these files into rounds.
    # We should optimize this so we aren't having to do a disk load at the
    # beginning of each epoch.
    # One option would be to read the files into the object store at the
    # beginning of training, chunked by round partitions, and only select from
    # the desired round partition in each round, shuffle within each round
    # partition, and split to reducers. At the beginning of each epoch, the
    # round partitions could be globally shuffled.
    # This doesn't matter as much if we're also pipelining epochs.
    round_partitions = []
    for filename in filenames:
        rounds = cache_round_partitions.options(
            num_returns=num_rounds).remote(filename, num_rounds,
                                           stats_collector, epoch)
        if not isinstance(rounds, list):
            rounds = [rounds]
        round_partitions.append(rounds)
    return list(zip(*round_partitions))


@ray.remote
def cache_round_partitions(filename, num_rounds, stats_collector, epoch):
    stats_collector.cache_map_start.remote(epoch)
    start = timeit.default_timer()
    # Load file.
    rows = pd.read_parquet(filename)
    end_read = timeit.default_timer()

    # Shuffle rows.
    rows = rows.sample(frac=1)
    # Partition the rows into a partition per round.
    split = np.array_split(rows, num_rounds)
    if len(split) == 1:
        split = split[0]
    duration = timeit.default_timer() - start
    read_duration = end_read - start
    stats_collector.cache_map_done.remote(epoch, duration, read_duration)
    return split


@ray.remote
def shuffle_select_from_memory(
        rows, num_reducers, stats_collector, epoch, round_idx):
    stats_collector.map_start.remote(epoch, round_idx)
    start = timeit.default_timer()
    # Return a list of chunks, one for each reducer.
    split = np.array_split(rows, num_reducers)
    if len(split) == 1:
        split = split[0]
    duration = timeit.default_timer() - start
    stats_collector.map_done.remote(epoch, round_idx, duration)
    return split


#
# Shared shuffle stages.
#


@ray.remote
def shuffle_reduce(reduce_index, batches_per_round, stats_collector,
                   epoch, round_index, *chunks):
    stats_collector.reduce_start.remote(epoch, round_index)
    start = timeit.default_timer()
    # Concatenate and shuffle all rows in the chunks.
    batch = pd.concat(chunks)
    batch = batch.sample(frac=1)
    if batches_per_round > 1:
        batch = np.array_split(batch, batches_per_round)
    duration = timeit.default_timer() - start
    stats_collector.reduce_done.remote(epoch, round_index, duration)
    return batch


@ray.remote
def consume(chunk, start_time, stats_collector, epoch, round_index):
    print(f"Epoch: {epoch}, Round: {round_index}")
    stats_collector.consume.remote(
        epoch, round_index, timeit.default_timer() - start_time)


#
# Disk-based shuffle, loads full file from disk in each round.
#


def shuffle_from_disk(
        num_epochs,
        num_rounds,
        filenames,
        num_trainers,
        batch_size,
        batches_per_round,
        num_rows,
        max_concurrent_epochs,
        max_concurrent_rounds):
    # v = Validator.remote(filenames)
    # num_expected_rows = ray.get(v.get_num_expected_rows.remote())
    # print("Expecting", num_expected_rows, "rows")

    # TODO(Clark): Add a stats collector per round? Could result in exhausting
    # the Ray cluster with stats collecting actors.
    stats_collector = TrialStatsCollector.remote(
        num_epochs, len(filenames), num_trainers, num_trainers, num_rounds)

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
            batches_per_round,
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

    ray.wait(
        [stats_collector.trial_done.remote(end - start)], num_returns=1)

    stats = ray.get(stats_collector.get_stats.remote())

    # ray.get(v.check.remote(batches_per_round, *final_shuffled))

    return stats


@ray.remote
def shuffle_from_disk_epoch(
        epoch,
        filenames,
        num_rounds,
        num_trainers,
        batches_per_round,
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
                batches_per_round,
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
