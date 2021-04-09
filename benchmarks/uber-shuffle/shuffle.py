import argparse
import glob
import os
import timeit

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


def human_readable_size(num, precision=1, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0 or unit == "Zi":
            break
        num /= 1024.0
    return f"{num:.{precision}f}{unit}{suffix}"


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
        filename, num_reducers, seed, round_index, num_rounds):
    # Load file.
    rows = pd.read_parquet(filename)

    # Select rows based on our map index and the random seed.
    rows = rows.sample(frac=1, random_state=seed)
    rows = np.array_split(rows, num_rounds)[round_index]

    # Return a list of chunks, one for each reducer.
    split = np.array_split(rows, num_reducers)
    if len(split) == 1:
        split = split[0]
    return split


@ray.remote
def shuffle_reduce(reduce_index, batches_per_round, *chunks):
    # Concatenate and shuffle all rows in the chunks.
    batch = pd.concat(chunks)
    batch = batch.sample(frac=1)
    if batches_per_round > 1:
        return np.array_split(batch, batches_per_round)
    else:
        return batch


@ray.remote
def consume(chunk, round_index):
    print("Round:", round_index)
    return timeit.default_timer()


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

    print(f"Doing {num_rounds} shuffle rounds.")

    start = timeit.default_timer()

    seed = 0
    consumer_start_times = []
    consumer_end_times = []
    # TODO(Clark): Move to streaming implementation.
    for round_index in range(num_rounds):
        start_round = timeit.default_timer()
        chunks = []
        for filename in filenames:
            chunk = shuffle_select_from_disk.options(
                num_returns=num_trainers).remote(
                filename, num_trainers, seed, round_index, num_rounds)
            if not isinstance(chunk, list):
                chunk = [chunk]
            chunks.append(chunk)

        shuffled = [
            shuffle_reduce.remote(
                j,
                batches_per_round,
                *[chunks[i][j] for i in range(len(filenames))])
            for j in range(num_trainers)]
        # TODO(Clark): Add pipelining of shuffle rounds.
        consumer_start_times.append(start_round)
        consumer_end_times.extend([
            consume.remote(batch, round_index) for batch in shuffled])
    consumer_times = [end_round - start_round
                      for start_round, end_round in zip(
                          consumer_start_times,
                          ray.get(consumer_end_times))]

    end = timeit.default_timer()

    # ray.get(v.check.remote(batches_per_round, *final_shuffled))

    return end - start, consumer_times


#
# In-memory shuffling, loads data from disk once per epoch.
#


@ray.remote
def shuffle_select_from_memory(rows, num_reducers):
    # Return a list of chunks, one for each reducer.
    split = np.array_split(rows, num_reducers)
    if len(split) == 1:
        split = split[0]
    return split


@ray.remote
def cache_round_partitions(filename, num_rounds):
    # Load file.
    rows = pd.read_parquet(filename)

    # Shuffle rows.
    rows = rows.sample(frac=1)
    # Partition the rows into a partition per round.
    split = np.array_split(rows, num_rounds)
    if len(split) == 1:
        split = split[0]
    return split


def cache_in_memory(filenames, num_rounds):
    # TODO(Clark): In each epoch, we're currently loading the full dataset
    # from disk, shuffling each file, and partitioning these files into rounds.
    # We should optimize this so we aren't having to do a disk load at the
    # beginning of each epoch.
    # One option would be to read the files into the object store at the
    # beginning of training, chunked by round partitions, and only select from
    # the desired round partition in each round, shuffle within each round
    # partition, and split to reducers. At the beginning of each epoch, the
    # round partitions could be globally shuffled.
    round_partitions = []
    for filename in filenames:
        rounds = cache_round_partitions.options(
            num_returns=num_rounds).remote(filename, num_rounds)
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

    print(f"Doing {num_rounds} shuffle rounds.")

    rounds_of_partitions = cache_in_memory(filenames, num_rounds)

    start = timeit.default_timer()

    consumer_start_times = []
    consumer_end_times = []
    # TODO(Clark): Move to streaming implementation.
    for round_index, round_partitions in enumerate(rounds_of_partitions):
        start_round = timeit.default_timer()
        chunks = []
        for round_partition in round_partitions:
            chunk = shuffle_select_from_memory.options(
                num_returns=num_trainers).remote(
                    round_partition, num_trainers)
            if not isinstance(chunk, list):
                chunk = [chunk]
            chunks.append(chunk)
        shuffled = [
            shuffle_reduce.remote(
                j,
                batches_per_round,
                *[chunks[i][j] for i in range(len(round_partitions))])
            for j in range(num_trainers)]
        # TODO(Clark): Add pipelining of shuffle rounds.
        consumer_start_times.append(start_round)
        consumer_end_times.extend([
            consume.remote(batch, round_index) for batch in shuffled])
    consumer_times = [end - start
                      for start, end in zip(
                          consumer_start_times,
                          ray.get(consumer_end_times))]

    # ray.get(v.check.remote(batches_per_round, *final_shuffled))

    end = timeit.default_timer()
    return end - start, consumer_times


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
            "Using from-disk shuffler that loads data from disk each rounds.")
        shuffle = shuffle_from_disk
    else:
        print(
            "Using from-memory shuffler that caches data in memory between "
            "rounds.")
        shuffle = shuffle_from_memory
    times = []
    all_consumer_times = []
    if num_trials is not None:
        print(f"Running {num_trials} shuffle trials with {num_trainers} "
              f"trainers and a batch size of {batch_size} over {num_rows} "
              f"rows, with {batches_per_round} batches per round.")
        for trial in range(num_trials):
            print(f"Starting trial {trial}.")
            shuffle_time, consumer_times = shuffle(
                filenames,
                num_trainers,
                batch_size,
                batches_per_round,
                num_rows)
            print(f"Trial {trial} done after {shuffle_time} seconds.")
            times.append(shuffle_time)
            all_consumer_times.append(consumer_times)
    elif trials_timeout is not None:
        print(f"Running {trials_timeout} seconds of shuffle trials with "
              f"{num_trainers} trainers and a {batch_size} batch_size over "
              f"{num_rows} rows.")
        start = timeit.default_timer()
        trial = 0
        while timeit.default_timer() - start < trials_timeout:
            print(f"Starting trial {trial}.")
            shuffle_time, consumer_times = shuffle(
                filenames,
                num_trainers,
                batch_size,
                batches_per_round,
                num_rows)
            print(f"Trial {trial} done after {shuffle_time} seconds.")
            times.append(shuffle_time)
            all_consumer_times.append(consumer_times)
            trial += 1
    else:
        raise ValueError(
            "One of num_trials and trials_timeout must be specified")
    return times, all_consumer_times


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
    parser.add_argument("--clear-old-data", action="store_true")
    parser.add_argument("--use-old-data", action="store_true")
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
    times, all_consumer_times = run_trials(
        filenames,
        num_trainers,
        batch_size,
        batches_per_round,
        num_rows,
        args.use_from_disk_shuffler,
        num_trials,
        trials_timeout)

    flat_all_consumer_times = [
        time
        for consumer_times in all_consumer_times
        for time in consumer_times]
    mean = np.mean(flat_all_consumer_times)
    std = np.std(flat_all_consumer_times)
    throughput_std = np.std([
        num_rows / time
        for time in flat_all_consumer_times])
    batch_throughput_std = np.std([
        (num_rows / batch_size) / time
        for time in flat_all_consumer_times])
    print(f"\nMean over {len(flat_all_consumer_times)} "
          f"consumptions and {len(times)} trials: {mean:.3f}s +- {std}")
    print(f"Mean throughput over {len(flat_all_consumer_times)} consumptions "
          f"and {len(times)} trials: {num_rows / mean:.2f} rows/s +- "
          f"{throughput_std:.2f}")
    print(f"Mean batch throughput over {len(flat_all_consumer_times)} "
          f"consumptions and {len(times)} trials: "
          f"{(num_rows / batch_size) / mean:.2f} batches/s +- "
          f"{batch_throughput_std:.2f}")
    shuffle_type = (
        "from_disk" if args.use_from_disk_shuffler else "from_memory")
    for trial, consumer_times in enumerate(all_consumer_times):
        with open(
                f"output_{num_trainers}_{batches_per_round}_"
                f"{shuffle_type}_{trial}.txt", "w+") as f:
            for consumer_time in consumer_times:
                f.write(f"{consumer_time}\n")

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
