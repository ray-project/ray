import argparse
import csv
from dataclasses import dataclass
import glob
import math
import os
import timeit
from typing import List

import pandas as pd
import numpy as np
import ray


# TODOs:
# - Instrument profiling:
#   - Get some basic metrics: disk read time, shuffle time between map and
#     reduce tasks, average map/reduce task duration.
# - Plot the results.
# - Run on a large machine with the full dataset size
# - [DONE] Compute number of rounds based on batch size.
# - Scale past memory capacity of the cluster (long-term)

# Dataset info:
#
# 100 row groups
# 4M rows/group
#  - Size of row group can vary from 30k to 4M
# ~52GB total, on disk, snappy compressed
# 256k - 512k rows/batch
#  - batch size could be as small as 50k
#  - has target in bytes
#  - is arrived at iteratively, can vary across models
# 4M rows/group, 256k rows/batch -> 170MB/file


DEFAULT_DATA_DIR = "/mnt/disk0/benchmark_scratch"
DEFAULT_STATS_DIR = "."


@dataclass
class StageStats:
    task_durations: List[float]
    stage_duration: float


@dataclass
class MapStats(StageStats):
    read_durations: List[float]


@dataclass
class ReduceStats(StageStats):
    pass


@dataclass
class ConsumeStats:
    consume_times: List[float]


@dataclass
class RoundStats:
    map_stats: MapStats
    reduce_stats: ReduceStats
    consume_stats: ConsumeStats


@dataclass
class FromMemoryMapStats(StageStats):
    pass


@dataclass
class FromMemoryRoundStats:
    map_stats: FromMemoryMapStats
    reduce_stats: ReduceStats
    consume_stats: ConsumeStats


@dataclass
class EpochStats:
    round_stats: List[RoundStats]
    duration: float


@dataclass
class CacheMapStats(StageStats):
    read_durations: List[float]


@dataclass
class EpochStatsFromMemoryShuffle:
    round_stats: List[FromMemoryRoundStats]
    duration: float
    cache_map_stats: CacheMapStats


class RoundStatsCollector_:
    def __init__(self, num_maps, num_reduces):
        self._num_maps = num_maps
        self._num_reduces = num_reduces
        self._maps_started = 0
        self._maps_done = 0
        self._map_durations = []
        self._read_durations = []
        self._reduces_started = 0
        self._reduces_done = 0
        self._reduce_durations = []
        self._consume_times = []
        self._map_stage_start_time = None
        self._reduce_stage_start_time = None
        self._map_stage_duration = None
        self._reduce_stage_duration = None

    def map_start(self):
        if self._maps_started == 0:
            self._map_stage_start(timeit.default_timer())
        self._maps_started += 1

    def map_done(self, duration, read_duration):
        self._maps_done += 1
        self._map_durations.append(duration)
        self._read_durations.append(read_duration)
        if self._maps_done == self._num_maps:
            self._map_stage_done(timeit.default_timer())

    def reduce_start(self):
        if self._reduces_started == 0:
            self._reduce_stage_start(timeit.default_timer())
        self._reduces_started += 1

    def reduce_done(self, duration):
        self._reduces_done += 1
        self._reduce_durations.append(duration)
        if self._reduces_done == self._num_reduces:
            self._reduce_stage_done(timeit.default_timer())

    def consume(self, consume_time):
        self._consume_times.append(consume_time)

    def get_consume_times(self):
        assert len(self._consume_times) == self._num_reduces
        return self._consume_times

    def get_progress(self):
        return (
            self._maps_started, self._maps_done,
            self._reduces_started, self._reduces_done)

    def get_stage_task_durations(self):
        # TODO(Clark): Yield until these conditions are true?
        assert len(self._map_durations) == self._num_maps
        assert len(self._reduce_durations) == self._num_reduces
        return self._map_durations, self._reduce_durations

    def _map_stage_start(self, start_time):
        self._map_stage_start_time = start_time

    def _map_stage_done(self, end_time):
        assert self._map_stage_start_time is not None
        self._map_stage_duration = end_time - self._map_stage_start_time

    def _reduce_stage_start(self, start_time):
        self._reduce_stage_start_time = start_time

    def _reduce_stage_done(self, end_time):
        assert self._reduce_stage_start_time is not None
        self._reduce_stage_duration = end_time - self._reduce_stage_start_time

    def get_stage_durations(self):
        # TODO(Clark): Yield until these conditions are true?
        assert self._map_stage_duration is not None
        assert self._reduce_stage_duration is not None
        return self._map_stage_duration, self._reduce_stage_duration

    def get_map_stats(self):
        assert len(self._map_durations) == self._num_maps
        assert self._map_stage_duration is not None
        return MapStats(
            self._map_durations,
            self._map_stage_duration,
            self._read_durations)

    def get_reduce_stats(self):
        assert len(self._reduce_durations) == self._num_reduces
        assert self._reduce_stage_duration is not None
        return ReduceStats(self._reduce_durations, self._reduce_stage_duration)

    def get_consume_stats(self):
        assert len(self._consume_times) == self._num_reduces
        return ConsumeStats(self._consume_times)

    def get_stats(self):
        # TODO(Clark): Yield until these conditions are true?
        assert self._maps_done == self._num_maps
        assert self._reduces_done == self._num_reduces
        return RoundStats(
            self.get_map_stats(),
            self.get_reduce_stats(),
            self.get_consume_stats())


class FromMemoryRoundStatsCollector_(RoundStatsCollector_):
    def map_done(self, duration):
        self._maps_done += 1
        self._map_durations.append(duration)
        if self._maps_done == self._num_maps:
            self._map_stage_done(timeit.default_timer())

    def get_map_stats(self):
        assert self._map_stage_duration is not None
        assert len(self._map_durations) == self._num_maps
        return FromMemoryMapStats(
            self._map_durations, self._map_stage_duration)

    def get_stats(self):
        # TODO(Clark): Yield until these conditions are true?
        assert self._maps_done == self._num_maps
        assert self._reduces_done == self._num_reduces
        return FromMemoryRoundStats(
            self.get_map_stats(),
            self.get_reduce_stats(),
            self.get_consume_stats())


class EpochStatsCollector_:
    def __init__(self, num_maps, num_reduces, num_rounds):
        self._collectors = [
            RoundStatsCollector_(num_maps, num_reduces)
            for _ in range(num_rounds)]
        self._duration = None

    def epoch_done(self, duration):
        self._duration = duration

    def map_start(self, round_idx):
        self._collectors[round_idx].map_start()

    def map_done(self, round_idx, duration, read_duration):
        self._collectors[round_idx].map_done(duration, read_duration)

    def reduce_start(self, round_idx):
        self._collectors[round_idx].reduce_start()

    def reduce_done(self, round_idx, duration):
        self._collectors[round_idx].reduce_done(duration)

    def consume(self, round_idx, consume_time):
        self._collectors[round_idx].consume(consume_time)

    def get_consume_times(self, round_idx):
        self._collectors[round_idx].get_consume_times()

    def get_progress(self, round_idx):
        self._collectors[round_idx].get_progress()

    def get_stage_task_durations(self, round_idx):
        self._collectors[round_idx].get_stage_task_durations()

    def get_stage_durations(self, round_idx):
        self._collectors[round_idx].get_stage_durations()

    def get_round_stats(self, round_idx):
        self._collectors[round_idx].get_stats()

    def get_stats(self):
        assert self._duration is not None
        return EpochStats(
            [
                collector.get_stats()
                for collector in self._collectors],
            self._duration)


class FromMemoryEpochStatsCollector_(EpochStatsCollector_):
    def __init__(self, num_maps, num_reduces, num_rounds):
        self._collectors = [
            FromMemoryRoundStatsCollector_(num_maps, num_reduces)
            for _ in range(num_rounds)]
        self._duration = None
        self._num_cache_maps = num_maps
        self._cache_maps_started = 0
        self._cache_maps_done = 0
        self._cache_map_durations = []
        self._read_durations = []
        self._cache_map_stage_start_time = None
        self._cache_map_stage_duration = None

    def map_done(self, round_idx, duration):
        self._collectors[round_idx].map_done(duration)

    def cache_map_start(self):
        if self._cache_maps_started == 0:
            self._cache_map_stage_start(timeit.default_timer())
        self._cache_maps_started += 1

    def cache_map_done(self, duration, read_duration):
        self._cache_maps_done += 1
        self._cache_map_durations.append(duration)
        self._read_durations.append(read_duration)
        if self._cache_maps_done == self._num_cache_maps:
            self._cache_map_stage_done(timeit.default_timer())

    def _cache_map_stage_start(self, start_time):
        self._cache_map_stage_start_time = start_time

    def _cache_map_stage_done(self, end_time):
        assert self._cache_map_stage_start_time is not None
        self._cache_map_stage_duration = (
            end_time - self._cache_map_stage_start_time)

    def get_cache_map_task_durations(self):
        # TODO(Clark): Yield until this condition is true?
        assert len(self._cache_map_durations) == self._num_cache_maps
        return self._cache_map_durations

    def get_cache_map_stage_duration(self):
        # TODO(Clark): Yield until this condition is true?
        assert self._cache_map_stage_duration is not None
        return self._cache_map_stage_duration

    def get_cache_map_stats(self):
        # TODO(Clark): Yield until this condition is true?
        assert len(self._cache_map_durations) == self._num_cache_maps
        assert self._cache_map_stage_duration is not None
        return CacheMapStats(
            self._cache_map_durations,
            self._cache_map_stage_duration,
            self._read_durations)

    def get_progress(self):
        return self._cache_maps_started, self._cache_maps_done

    def get_stats(self):
        # TODO(Clark): Yield until this condition is true?
        assert self._cache_maps_done == self._num_cache_maps
        return EpochStatsFromMemoryShuffle(
            [
                collector.get_stats()
                for collector in self._collectors],
            self._duration,
            self.get_cache_map_stats())


EpochStatsCollector = ray.remote(EpochStatsCollector_)
FromMemoryEpochStatsCollector = ray.remote(FromMemoryEpochStatsCollector_)


def human_readable_size(num, precision=1, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0 or unit == "Zi":
            break
        num /= 1024.0
    return f"{num:.{precision}f}{unit}{suffix}"


UNITS = ["", "K", "M", "B", "T", "Q"]


def human_readable_big_num(num):
    idx = int(math.log10(num) // 3)
    unit = UNITS[idx]
    new_num = num / 10 ** (3 * idx)
    if new_num % 1 == 0:
        return f"{int(new_num)}{unit}"
    else:
        return f"{new_num:.1f}{unit}"


def generate_data(
        num_row_groups,
        num_rows_per_group,
        num_row_groups_per_file,
        data_dir):
    results = []
    for file_index, global_row_index in enumerate(
            range(
                0,
                num_row_groups * num_rows_per_group,
                num_rows_per_group * num_row_groups_per_file)):
        results.append(
            generate_file.remote(
                file_index,
                global_row_index,
                num_rows_per_group,
                num_row_groups_per_file))
    filenames, data_sizes = zip(*ray.get(results))
    return filenames, sum(data_sizes)


@ray.remote
def generate_file(
        file_index,
        global_row_index,
        num_rows_per_group,
        num_row_groups_per_file):
    # TODO(Clark): Optimize this data generation to reduce copies and
    # progressively write smaller buffers to the Parquet file.
    buffs = []
    for group_index in range(num_row_groups_per_file):
        buffs.append(
            generate_row_group(
                group_index,
                global_row_index + group_index * num_rows_per_group,
                num_rows_per_group))
    df = pd.concat(buffs)
    data_size = df.memory_usage(deep=True).sum()
    filename = os.path.join(
        data_dir, f"input_data_{file_index}.parquet.gzip")
    df.to_parquet(
        filename,
        engine="pyarrow",
        compression="gzip",
        row_group_size=num_rows_per_group)
    return filename, data_size


def generate_row_group(group_index, global_row_index, num_rows_in_group):
    buffer = {
        "key": np.array(
            range(global_row_index, global_row_index + num_rows_in_group)),
        "embeddings_name0": np.random.randint(
            0, 2385, num_rows_in_group, dtype=np.long),
        "embeddings_name1": np.random.randint(
            0, 201, num_rows_in_group, dtype=np.long),
        "embeddings_name2": np.random.randint(
            0, 201, num_rows_in_group, dtype=np.long),
        "embeddings_name3": np.random.randint(
            0, 6, num_rows_in_group, dtype=np.long),
        "embeddings_name4": np.random.randint(
            0, 19, num_rows_in_group, dtype=np.long),
        "embeddings_name5": np.random.randint(
            0, 1441, num_rows_in_group, dtype=np.long),
        "embeddings_name6": np.random.randint(
            0, 201, num_rows_in_group, dtype=np.long),
        "embeddings_name7": np.random.randint(
            0, 22, num_rows_in_group, dtype=np.long),
        "embeddings_name8": np.random.randint(
            0, 156, num_rows_in_group, dtype=np.long),
        "embeddings_name9": np.random.randint(
            0, 1216, num_rows_in_group, dtype=np.long),
        "embeddings_name10": np.random.randint(
            0, 9216, num_rows_in_group, dtype=np.long),
        "embeddings_name11": np.random.randint(
            0, 88999, num_rows_in_group, dtype=np.long),
        "embeddings_name12": np.random.randint(
            0, 941792, num_rows_in_group, dtype=np.long),
        "embeddings_name13": np.random.randint(
            0, 9405, num_rows_in_group, dtype=np.long),
        "embeddings_name14": np.random.randint(
            0, 83332, num_rows_in_group, dtype=np.long),
        "embeddings_name15": np.random.randint(
            0, 828767, num_rows_in_group, dtype=np.long),
        "embeddings_name16": np.random.randint(
            0, 945195, num_rows_in_group, dtype=np.long),
        "one_hot0": np.random.randint(0, 3, num_rows_in_group, dtype=np.long),
        "one_hot1": np.random.randint(0, 50, num_rows_in_group, dtype=np.long),
        "labels": np.random.rand(num_rows_in_group),
    }

    return pd.DataFrame(buffer)


@ray.remote
class Validator:
    def __init__(self, filenames):
        self.filenames = filenames
        self.num_expected_rows = None

    def get_num_expected_rows(self):
        if self.num_expected_rows is None:
            self.num_expected_rows = sum(
                len(pd.read_parquet(f)) for f in self.filenames)
        return self.num_expected_rows

    def check(self, batches_per_round, *chunks):
        if batches_per_round > 1:
            # Flatten the batches.
            chunks = [chunk for chunk_list in chunks for chunk in chunk_list]
        shuffled = pd.concat(chunks)
        num_expected_rows = self.get_num_expected_rows()
        assert num_expected_rows == len(shuffled)
        assert (
            list(shuffled["key"]) != list(range(num_expected_rows)) and
            set(shuffled["key"]) == set(range(num_expected_rows)))


#
# Disk-based shuffle, loads full file from disk in each round.
#


@ray.remote
def shuffle_select_from_disk(
        filename, num_reducers, seed, round_index, num_rounds,
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


@ray.remote
def shuffle_reduce(reduce_index, batches_per_round, stats_collector,
                   round_index, *chunks):
    stats_collector.reduce_start.remote(round_index)
    start = timeit.default_timer()
    # Concatenate and shuffle all rows in the chunks.
    batch = pd.concat(chunks)
    batch = batch.sample(frac=1)
    if batches_per_round > 1:
        batch = np.array_split(batch, batches_per_round)
    duration = timeit.default_timer() - start
    stats_collector.reduce_done.remote(round_index, duration)
    return batch


@ray.remote
def consume(chunk, start_time, stats_collector, round_index):
    print("Round:", round_index)
    stats_collector.consume.remote(
        round_index, timeit.default_timer() - start_time)


def shuffle_from_disk(
        filenames, num_trainers, batch_size, batches_per_round, num_rows):
    # v = Validator.remote(filenames)
    # num_expected_rows = ray.get(v.get_num_expected_rows.remote())
    # print("Expecting", num_expected_rows, "rows")

    # Calculate the number of shuffle rounds.
    # TODO(Clark): Handle uneven rounds (remainders).
    num_rounds = max(
        num_rows / num_trainers / batch_size / batches_per_round, 1)
    # Assert even division (no remainders, uneven rounds).
    assert num_rounds % 1 == 0
    num_rounds = int(num_rounds)

    # TODO(Clark): Add a stats collector per round? Could result in exhausting
    # the Ray cluster with stats collecting actors.
    stats_collector = EpochStatsCollector.remote(
        len(filenames), num_trainers, num_rounds)

    print(f"Doing {num_rounds} shuffle rounds.")

    start = timeit.default_timer()

    seed = 0
    consumers = []
    # TODO(Clark): Move to streaming implementation.
    for round_index in range(num_rounds):
        chunks = []
        for filename in filenames:
            chunk = shuffle_select_from_disk.options(
                num_returns=num_trainers).remote(
                    filename, num_trainers, seed, round_index, num_rounds,
                    stats_collector)
            if not isinstance(chunk, list):
                chunk = [chunk]
            chunks.append(chunk)

        shuffled = [
            shuffle_reduce.remote(
                j,
                batches_per_round,
                stats_collector,
                round_index,
                *[chunks[i][j] for i in range(len(filenames))])
            for j in range(num_trainers)]
        consumers.extend([
            consume.remote(batch, start, stats_collector, round_index)
            for batch in shuffled])

    # Block until all consumers are done.
    ray.get(consumers)

    end = timeit.default_timer()

    ray.wait([stats_collector.epoch_done.remote(end - start)], num_returns=1)

    stats = ray.get(stats_collector.get_stats.remote())

    # ray.get(v.check.remote(batches_per_round, *final_shuffled))

    return stats


#
# In-memory shuffling, loads data from disk once per epoch.
#


@ray.remote
def shuffle_select_from_memory(rows, num_reducers, stats_collector, round_idx):
    stats_collector.map_start.remote(round_idx)
    start = timeit.default_timer()
    # Return a list of chunks, one for each reducer.
    split = np.array_split(rows, num_reducers)
    if len(split) == 1:
        split = split[0]
    duration = timeit.default_timer() - start
    stats_collector.map_done.remote(round_idx, duration)
    return split


@ray.remote
def cache_round_partitions(filename, num_rounds, stats_collector):
    stats_collector.cache_map_start.remote()
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
    stats_collector.cache_map_done.remote(duration, read_duration)
    return split


def cache_in_memory(filenames, num_rounds, stats_collector):
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
                                           stats_collector)
        if not isinstance(rounds, list):
            rounds = [rounds]
        round_partitions.append(rounds)
    return list(zip(*round_partitions))


def shuffle_from_memory(
        filenames, num_trainers, batch_size, batches_per_round, num_rows):
    # v = Validator.remote(filenames)
    # num_expected_rows = ray.get(v.get_num_expected_rows.remote())
    # print("Expecting", num_expected_rows, "rows")

    # Calculate the number of shuffle rounds.
    # TODO(Clark): Handle uneven rounds (remainders).
    num_rounds = max(
        num_rows / num_trainers / batch_size / batches_per_round, 1)
    # Assert even division (no remainders, uneven rounds).
    assert num_rounds % 1 == 0
    num_rounds = int(num_rounds)

    stats_collector = FromMemoryEpochStatsCollector.remote(
        len(filenames), num_trainers, num_rounds)

    print(f"Doing {num_rounds} shuffle rounds.")

    start = timeit.default_timer()

    rounds_of_partitions = cache_in_memory(
        filenames, num_rounds, stats_collector)

    consumers = []
    # TODO(Clark): Move to streaming implementation.
    for round_index, round_partitions in enumerate(rounds_of_partitions):
        chunks = []
        for round_partition in round_partitions:
            chunk = shuffle_select_from_memory.options(
                num_returns=num_trainers).remote(
                    round_partition, num_trainers,
                    stats_collector, round_index)
            if not isinstance(chunk, list):
                chunk = [chunk]
            chunks.append(chunk)
        shuffled = [
            shuffle_reduce.remote(
                j,
                batches_per_round,
                stats_collector,
                round_index,
                *[chunks[i][j] for i in range(len(round_partitions))])
            for j in range(num_trainers)]
        consumers.extend([
            consume.remote(batch, start, stats_collector, round_index)
            for batch in shuffled])

    # Block until all consumers are done.
    ray.get(consumers)

    end = timeit.default_timer()

    ray.wait([stats_collector.epoch_done.remote(end - start)], num_returns=1)

    stats = ray.get(stats_collector.get_stats.remote())

    # ray.get(v.check.remote(batches_per_round, *final_shuffled))

    return stats


def run_trials(
        filenames,
        num_trainers,
        batch_size,
        batches_per_round,
        num_rows,
        use_from_disk_shuffler=False,
        num_trials=None,
        trials_timeout=None):
    if use_from_disk_shuffler:
        print(
            "Using from-disk shuffler that loads data from disk each round.")
        shuffle = shuffle_from_disk
    else:
        print(
            "Using from-memory shuffler that caches data in memory between "
            "rounds.")
        shuffle = shuffle_from_memory
    all_stats = []
    if num_trials is not None:
        print(f"Running {num_trials} shuffle trials with {num_trainers} "
              f"trainers and a batch size of {batch_size} over {num_rows} "
              f"rows, with {batches_per_round} batches per round.")
        for trial in range(num_trials):
            print(f"Starting trial {trial}.")
            stats = shuffle(
                filenames,
                num_trainers,
                batch_size,
                batches_per_round,
                num_rows)
            print(f"Trial {trial} done after {stats.duration} seconds.")
            all_stats.append(stats)
    elif trials_timeout is not None:
        print(f"Running {trials_timeout} seconds of shuffle trials with "
              f"{num_trainers} trainers and a {batch_size} batch_size over "
              f"{num_rows} rows.")
        start = timeit.default_timer()
        trial = 0
        while timeit.default_timer() - start < trials_timeout:
            print(f"Starting trial {trial}.")
            stats = shuffle(
                filenames,
                num_trainers,
                batch_size,
                batches_per_round,
                num_rows)
            print(f"Trial {trial} done after {stats.duration} seconds.")
            all_stats.append(stats)
            trial += 1
    else:
        raise ValueError(
            "One of num_trials and trials_timeout must be specified")
    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Shuffling per-epoch data loader")
    parser.add_argument("--num-rows-per-group", type=int, default=100)
    parser.add_argument("--num-row-groups", type=int, default=100)
    parser.add_argument("--num-row-groups-per-file", type=int, default=1)
    parser.add_argument("--num-trainers", type=int, default=5)
    parser.add_argument("--num-trials", type=int, default=None)
    parser.add_argument("--trials-timeout", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batches-per-round", type=int, default=1)
    parser.add_argument("--use-from-disk-shuffler", action="store_true")
    parser.add_argument("--cluster", action="store_true")
    parser.add_argument("--data-dir", type=str, default=DEFAULT_DATA_DIR)
    parser.add_argument("--stats-dir", type=str, default=DEFAULT_STATS_DIR)
    parser.add_argument("--clear-old-data", action="store_true")
    parser.add_argument("--use-old-data", action="store_true")
    parser.add_argument("--no-round-stats", action="store_true")
    parser.add_argument("--no-consume-stats", action="store_true")
    parser.add_argument("--overwrite-stats", action="store_true")
    args = parser.parse_args()

    if args.num_row_groups_per_file < 1:
        raise ValueError("Must have at least one row group per file.")

    num_trials = args.num_trials
    trials_timeout = args.trials_timeout
    if num_trials is not None and trials_timeout is not None:
        raise ValueError(
            "Only one of --num-trials and --trials-timeout should be "
            "specified.")

    if num_trials is None and trials_timeout is None:
        num_trials = 3

    if args.clear_old_data and args.use_old_data:
        raise ValueError(
            "Only one of --clear-old-data and --use-old-data should be "
            "specified.")

    data_dir = args.data_dir
    if args.clear_old_data:
        print(f"Clearing old data from {data_dir}.")
        files = glob.glob(os.path.join(data_dir, "*.parquet.gzip"))
        for f in files:
            os.remove(f)

    if args.cluster:
        print("Connecting to an existing Ray cluster.")
        ray.init(address="auto")
    else:
        print("Starting a new local Ray cluster.")
        ray.init()

    num_row_groups = args.num_row_groups
    num_rows_per_group = args.num_rows_per_group
    num_row_groups_per_file = args.num_row_groups_per_file
    if not args.use_old_data:
        print(
            f"Generating {num_row_groups} row groups with "
            f"{num_row_groups_per_file} row groups per file and each with "
            f"{num_rows_per_group} rows.")
        filenames, num_bytes = generate_data(
            num_row_groups,
            num_rows_per_group,
            num_row_groups_per_file,
            data_dir)
        print(
            f"Generated {len(filenames)} files each containing "
            f"{num_row_groups_per_file} row groups, where each row group "
            f"contains {num_rows_per_group} rows, totalling "
            f"{human_readable_size(num_bytes)}.")
    else:
        num_files = num_row_groups / num_row_groups_per_file
        assert num_files % 1 == 0
        num_files = int(num_files)
        filenames = [
            os.path.join(
                data_dir,
                f"input_data_{file_index}.parquet.gzip")
            for file_index in range(num_files)]
        print("Not generating input data, using existing data instead.")

    num_trainers = args.num_trainers
    batch_size = args.batch_size
    batches_per_round = args.batches_per_round
    num_rows = num_row_groups * num_rows_per_group
    num_rounds = max(
        num_rows / num_trainers / batch_size / batches_per_round, 1)
    # Assert even division (no remainders, uneven rounds).
    assert num_rounds % 1 == 0
    num_rounds = int(num_rounds)

    # warmup_trials = 2
    # print(f"\nRunning {warmup_trials} warmup trials.")
    # times = run_trials(
    #     filenames,
    #     num_trainers,
    #     batch_size,
    #     batches_per_round,
    #     num_rows,
    #     warmup_trials)

    print("\nRunning real trials.")
    use_from_disk_shuffler = args.use_from_disk_shuffler
    all_stats = run_trials(
        filenames,
        num_trainers,
        batch_size,
        batches_per_round,
        num_rows,
        use_from_disk_shuffler,
        num_trials,
        trials_timeout)

    times = [stats.duration for stats in all_stats]
    mean = np.mean(times)
    std = np.std(times)
    throughput_std = np.std([num_rows / time for time in times])
    batch_throughput_std = np.std([
        (num_rows / batch_size) / time for time in times])
    print(f"\nMean over {len(times)} trials: {mean:.3f}s +- {std}")
    print(f"Mean throughput over {len(times)} trials: "
          f"{num_rows / mean:.2f} rows/s +- {throughput_std:.2f}")
    print(f"Mean batch throughput over {len(times)} trials: "
          f"{(num_rows / batch_size) / mean:.2f} batches/s +- "
          f"{batch_throughput_std:.2f}")

    shuffle_type = (
        "from_disk" if use_from_disk_shuffler else "from_memory")
    overwrite_stats = args.overwrite_stats
    write_mode = "w+" if overwrite_stats else "a+"
    stats_dir = args.stats_dir
    hr_num_row_groups = human_readable_big_num(num_row_groups)
    hr_num_rows_per_group = human_readable_big_num(num_rows_per_group)
    hr_batch_size = human_readable_big_num(batch_size)
    filename = (
        f"trial_stats_{shuffle_type}_{hr_num_row_groups}_"
        f"{hr_num_rows_per_group}_{hr_batch_size}.csv")
    filename = os.path.join(stats_dir, filename)
    write_header = (
        overwrite_stats or not os.path.exists(filename) or
        os.path.getsize(filename) == 0)
    print(f"Writing out trial stats to {filename}.")
    # TODO(Clark): Add per-mapper, per-reducer, and per-trainer stat CSVs.
    with open(filename, write_mode) as f:
        fieldnames = [
            "shuffle_type",
            "row_groups_per_file",
            "num_trainers",
            "batches_per_round",
            "num_rounds",
            "trial",
            "duration",
            "row_throughput",
            "batch_throughput",
            "avg_map_stage_duration",  # across rounds
            "std_map_stage_duration",  # across rounds
            "max_map_stage_duration",  # across rounds
            "min_map_stage_duration",  # across rounds
            "avg_reduce_stage_duration",  # across rounds
            "std_reduce_stage_duration",  # across rounds
            "max_reduce_stage_duration",  # across rounds
            "min_reduce_stage_duration",  # across rounds
            "avg_map_task_duration",  # across rounds and mappers
            "std_map_task_duration",  # across rounds and mappers
            "max_map_task_duration",  # across rounds and mappers
            "min_map_task_duration",  # across rounds and mappers
            "avg_reduce_task_duration",  # across rounds and reducers
            "std_reduce_task_duration",  # across rounds and reducers
            "max_reduce_task_duration",  # across rounds and reducers
            "min_reduce_task_duration",  # across rounds and reducers
            "avg_time_to_consume",  # across rounds and consumers
            "std_time_to_consume",  # across rounds and consumers
            "max_time_to_consume",  # across rounds and consumers
            "min_time_to_consume"]  # across rounds and consumers
        if use_from_disk_shuffler:
            fieldnames += [
                "avg_read_duration",  # across rounds and mappers
                "std_read_duration",  # across rounds and mappers
                "max_read_duration",  # across rounds and mappers
                "min_read_duration"]  # across rounds and mappers
        else:
            fieldnames += [
                "cache_map_stage_duration",
                "avg_cache_map_task_duration",  # across mappers
                "std_cache_map_task_duration",  # across mappers
                "max_cache_map_task_duration",  # across mappers
                "min_cache_map_task_duration",  # across mappers
                "avg_read_duration",  # across rounds
                "std_read_duration",  # across rounds
                "max_read_duration",  # across rounds
                "min_read_duration"]  # across rounds
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        row = {
            "shuffle_type": shuffle_type,
            "row_groups_per_file": num_row_groups_per_file,
            "num_trainers": num_trainers,
            "batches_per_round": batches_per_round,
            "num_rounds": num_rounds,
        }
        for trial, stats in enumerate(all_stats):
            row["trial"] = trial
            row["duration"] = stats.duration
            row["row_throughput"] = num_rows / stats.duration
            row["batch_throughput"] = num_rows / batch_size / stats.duration

            # Get the stats from each round.
            map_task_durations = []
            map_stage_durations = []
            reduce_task_durations = []
            reduce_stage_durations = []
            read_durations = []
            consume_times = []
            for round_stats in stats.round_stats:
                map_stats = round_stats.map_stats
                map_stage_durations.append(map_stats.stage_duration)
                for duration in map_stats.task_durations:
                    map_task_durations.append(duration)
                if use_from_disk_shuffler:
                    for duration in map_stats.read_durations:
                        read_durations.append(duration)
                reduce_stats = round_stats.reduce_stats
                reduce_stage_durations.append(reduce_stats.stage_duration)
                for duration in reduce_stats.task_durations:
                    reduce_task_durations.append(duration)
                for consume_time in round_stats.consume_stats.consume_times:
                    consume_times.append(consume_time)

            # Calculate the trial stats.
            row["avg_map_stage_duration"] = np.mean(map_stage_durations)
            row["std_map_stage_duration"] = np.std(map_stage_durations)
            row["max_map_stage_duration"] = np.max(map_stage_durations)
            row["min_map_stage_duration"] = np.min(map_stage_durations)
            row["avg_reduce_stage_duration"] = np.mean(reduce_stage_durations)
            row["std_reduce_stage_duration"] = np.std(reduce_stage_durations)
            row["max_reduce_stage_duration"] = np.max(reduce_stage_durations)
            row["min_reduce_stage_duration"] = np.min(reduce_stage_durations)
            row["avg_map_task_duration"] = np.mean(map_task_durations)
            row["std_map_task_duration"] = np.std(map_task_durations)
            row["max_map_task_duration"] = np.max(map_task_durations)
            row["min_map_task_duration"] = np.min(map_task_durations)
            row["avg_reduce_task_duration"] = np.mean(reduce_task_durations)
            row["std_reduce_task_duration"] = np.std(reduce_task_durations)
            row["max_reduce_task_duration"] = np.max(reduce_task_durations)
            row["min_reduce_task_duration"] = np.min(reduce_task_durations)
            row["avg_time_to_consume"] = np.mean(consume_times)
            row["std_time_to_consume"] = np.std(consume_times)
            row["max_time_to_consume"] = np.max(consume_times)
            row["min_time_to_consume"] = np.min(consume_times)
            if not use_from_disk_shuffler:
                cache_map_stats = stats.cache_map_stats
                row["cache_map_stage_duration"] = (
                    cache_map_stats.stage_duration)
                row["avg_cache_map_task_duration"] = np.mean(
                    cache_map_stats.task_durations)
                row["std_cache_map_task_duration"] = np.std(
                    cache_map_stats.task_durations)
                row["max_cache_map_task_duration"] = np.max(
                    cache_map_stats.task_durations)
                row["min_cache_map_task_duration"] = np.min(
                    cache_map_stats.task_durations)
                for duration in cache_map_stats.read_durations:
                    read_durations.append(duration)
            row["avg_read_duration"] = np.mean(read_durations)
            row["std_read_duration"] = np.std(read_durations)
            row["max_read_duration"] = np.max(read_durations)
            row["min_read_duration"] = np.min(read_durations)
            writer.writerow(row)

    if not args.no_round_stats:
        # TODO(Clark): Add per-round granularity for stats.
        filename = (
            f"round_stats_{shuffle_type}_{hr_num_row_groups}_"
            f"{hr_num_rows_per_group}_{hr_batch_size}.csv")
        filename = os.path.join(stats_dir, filename)
        write_header = (
            overwrite_stats or not os.path.exists(filename) or
            os.path.getsize(filename) == 0)
        print(f"Writing out round stats to {filename}.")
        # TODO(Clark): Add per-mapper, per-reducer, and per-trainer stat CSVs.
        with open(filename, write_mode) as f:
            fieldnames = [
                "shuffle_type",
                "row_groups_per_file",
                "num_trainers",
                "batches_per_round",
                "num_rounds",
                "trial",
                "round",
                "map_stage_duration",
                "reduce_stage_duration",
                "avg_map_task_duration",  # across mappers
                "std_map_task_duration",  # across mappers
                "max_map_task_duration",  # across mappers
                "min_map_task_duration",  # across mappers
                "avg_reduce_task_duration",  # across reducers
                "std_reduce_task_duration",  # across reducers
                "max_reduce_task_duration",  # across reducers
                "min_reduce_task_duration",  # across reducers
                "avg_time_to_consume",  # across consumers
                "std_time_to_consume",  # across consumers
                "max_time_to_consume",  # across consumers
                "min_time_to_consume"]  # across consumers
            if use_from_disk_shuffler:
                fieldnames += [
                    "avg_read_duration",  # across mappers
                    "std_read_duration",  # across mappers
                    "max_read_duration",  # across mappers
                    "min_read_duration"]  # across mappers
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            row = {
                "shuffle_type": shuffle_type,
                "row_groups_per_file": num_row_groups_per_file,
                "num_trainers": num_trainers,
                "batches_per_round": batches_per_round,
                "num_rounds": num_rounds,
            }
            for trial, trial_stats in enumerate(all_stats):
                row["trial"] = trial
                for round_idx, stats in enumerate(trial_stats.round_stats):
                    row["round"] = round_idx
                    row["map_stage_duration"] = stats.map_stats.stage_duration
                    row["reduce_stage_duration"] = (
                        stats.reduce_stats.stage_duration)
                    row["avg_map_task_duration"] = np.mean(
                        stats.map_stats.task_durations)
                    row["std_map_task_duration"] = np.std(
                        stats.map_stats.task_durations)
                    row["max_map_task_duration"] = np.max(
                        stats.map_stats.task_durations)
                    row["min_map_task_duration"] = np.min(
                        stats.map_stats.task_durations)
                    row["avg_reduce_task_duration"] = np.mean(
                        stats.reduce_stats.task_durations)
                    row["std_reduce_task_duration"] = np.std(
                        stats.reduce_stats.task_durations)
                    row["max_reduce_task_duration"] = np.max(
                        stats.reduce_stats.task_durations)
                    row["min_reduce_task_duration"] = np.min(
                        stats.reduce_stats.task_durations)
                    row["avg_time_to_consume"] = np.mean(
                        stats.consume_stats.consume_times)
                    row["std_time_to_consume"] = np.std(
                        stats.consume_stats.consume_times)
                    row["max_time_to_consume"] = np.max(
                        stats.consume_stats.consume_times)
                    row["min_time_to_consume"] = np.min(
                        stats.consume_stats.consume_times)
                    if use_from_disk_shuffler:
                        row["avg_read_duration"] = np.mean(
                            stats.map_stats.read_durations)
                        row["std_read_duration"] = np.std(
                            stats.map_stats.read_durations)
                        row["max_read_duration"] = np.max(
                            stats.map_stats.read_durations)
                        row["min_read_duration"] = np.min(
                            stats.map_stats.read_durations)
                    writer.writerow(row)
    if not args.no_consume_stats:
        print("Writing out consume stats.")
        # TODO(Clark): Add per-round granularity for stats.
        filename = (
            f"consume_stats_{shuffle_type}_{hr_num_row_groups}_"
            f"{hr_num_rows_per_group}_{hr_batch_size}.csv")
        filename = os.path.join(stats_dir, filename)
        write_header = (
            overwrite_stats or not os.path.exists(filename) or
            os.path.getsize(filename) == 0)
        with open(filename, write_mode) as f:
            fieldnames = [
                "shuffle_type",
                "row_groups_per_file",
                "num_trainers",
                "batches_per_round",
                "num_rounds",
                "trial",
                "round",
                "consumer",
                "consume_time"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            row = {
                "shuffle_type": shuffle_type,
                "row_groups_per_file": num_row_groups_per_file,
                "num_trainers": num_trainers,
                "batches_per_round": batches_per_round,
                "num_rounds": num_rounds,
            }
            for trial, trial_stats in enumerate(all_stats):
                row["trial"] = trial
                for round_idx, stats in enumerate(trial_stats.round_stats):
                    row["round"] = round_idx
                    for consumer, consume_time in enumerate(
                            stats.consume_stats.consume_times):
                        row["consumer"] = consumer
                        row["consume_time"] = consume_time
                        writer.writerow(row)
