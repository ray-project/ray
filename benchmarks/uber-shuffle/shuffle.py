import argparse
import glob
import os
import timeit

import pandas as pd
import numpy as np
import ray


# TODOs:
# - Plot the results.
# - Run on a large machine with the full dataset size
# - Get some basic metrics: disk read time, shuffle time between map and reduce
# tasks, average map/reduce task duration.
# - Compute number of rounds based on batch size.
# - Scale past memory capacity of the cluster (long-term)

# To confirm:
# - batch size
# - dataset size/schema
#   - do we need to have the batches as part of the schema?
#   - how large is each row-group file expected to be?

# Dataset info:
#
# 100 row groups
# 4M rows/group
# ~52GB total
# 256K rows/batch
# 4M rows/group, 256 rows/batch -> 170MB/file


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


@ray.remote
def select(filename, num_reducers, seed, round_index, num_rounds):
    # Load file.
    rows = pd.read_parquet(filename)

    # Select rows based on our map index and the random seed.
    # TODO(Clark): In each round, we're currently loading the full row group
    # from disk, shuffling the row group (same, deterministic shuffle in each
    # round), and discarding all row groups that don't belong in the current
    # round. We should optimize this.
    rows = rows.sample(frac=1, random_state=seed)
    rows = np.array_split(rows, num_rounds)[round_index]

    # Return a list of chunks, one for each reducer.
    return np.array_split(rows, num_reducers)


@ray.remote
def shuffle(reduce_index, batches_per_round, *all_chunks):
    # Select rows for this reducer.
    chunks = [chunks[reduce_index] for chunks in all_chunks]

    # Concatenate and shuffle all rows in the chunks.
    batch = pd.concat(chunks)
    batch = batch.sample(frac=1)
    if batches_per_round > 1:
        return np.array_split(batch, batches_per_round)
    else:
        return batch


@ray.remote
def consume(chunk):
    return timeit.default_timer()


def shuffle_all(
        filenames, num_trainers, batch_size, batches_per_round, num_rows):
    v = Validator.remote(filenames)
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

    final_shuffled = []
    seed = 0
    # TODO(Clark): Move to streaming implementation.
    for round_index in range(num_rounds):
        # TODO(Clark): Set num returns = num trainers. So that we"re not
        # sending all data to all reducers. Should only matter for
        # distributed version.
        chunks = [
            select.remote(
                filename, num_trainers, seed, round_index, num_rounds)
            for filename in filenames]
        shuffled = [
            shuffle.remote(i, batches_per_round, *chunks)
            for i in range(num_trainers)]
        # TODO(Clark): Add pipelining of shuffle rounds.
        ray.get([consume.remote(batch) for batch in shuffled])
        # finished = ray.get([consume.remote(batch) for batch in shuffled])
        final_shuffled += shuffled

        # for t in finished:
        #     print(t - start)
    end = timeit.default_timer()

    ray.get(v.check.remote(batches_per_round, *final_shuffled))

    return end - start


def run_trials(
        filenames,
        num_trainers,
        batch_size,
        batches_per_round,
        num_rows,
        num_trials=None,
        trials_timeout=None):
    times = []
    if num_trials is not None:
        print(f"Running {num_trials} shuffle trials with {num_trainers} "
              f"trainers and a batch size of {batch_size} over {num_rows} "
              "rows, with {batches_per_round} batches per round.")
        for trial in range(num_trials):
            print(f"Starting trial {trial}.")
            shuffle_time = shuffle_all(
                filenames,
                num_trainers,
                batch_size,
                batches_per_round,
                num_rows)
            print(f"Trial {trial} done after {shuffle_time} seconds.")
            times.append(shuffle_time)
    elif trials_timeout is not None:
        print(f"Running {trials_timeout} seconds of shuffle trials with "
              f"{num_trainers} trainers and a {batch_size} batch_size over "
              f"{num_rows} rows.")
        start = timeit.default_timer()
        trial = 0
        while timeit.default_timer() - start < trials_timeout:
            print(f"Starting trial {trial}.")
            shuffle_time = shuffle_all(
                filenames,
                num_trainers,
                batch_size,
                batches_per_round,
                num_rows)
            print(f"Trial {trial} done after {shuffle_time} seconds.")
            times.append(shuffle_time)
            trial += 1
    else:
        raise ValueError(
            "One of num_trials and trials_timeout must be specified")
    return times


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
    times = run_trials(
        filenames,
        num_trainers,
        batch_size,
        batches_per_round,
        num_rows,
        num_trials,
        trials_timeout)

    mean = np.mean(times)
    std = np.std(times)
    print(f"\nMean over {len(times)} trials: {mean} +- {std}")
