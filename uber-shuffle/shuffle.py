import ray
import time
import pandas as pd
import numpy as np
import argparse


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


@ray.remote
class Validator:
    def __init__(self, filenames):
        self.num_expected_rows = 0
        # Load file.
        for filename in filenames:
            rows = pd.read_parquet(filename)
            self.num_expected_rows += len(rows)

    def get_num_expected_rows(self):
        return self.num_expected_rows

    def check(self, *chunks):
        shuffled = pd.concat(chunks)
        if self.num_expected_rows != len(shuffled):
            return False

        return set(shuffled['key']) == set(range(self.num_expected_rows))


@ray.remote
def select(filename, num_reducers, seed, r, num_rounds):
    # Load file.
    rows = pd.read_parquet(filename)

    # Select rows based on our map index and the random seed.
    rows = rows.sample(frac=1, random_state=seed)
    rows = np.array_split(rows, num_rounds)[r]

    # Return a list of chunks, one for each reducer.
    return np.array_split(rows, num_reducers)


@ray.remote
def shuffle(reduce_index, *all_chunks):
    # Select rows for this reducer.
    chunks = [chunks[reduce_index] for chunks in all_chunks]

    # Concatenate and shuffle all rows in the chunks.
    batch = pd.concat(chunks)
    batch = batch.sample(frac=1)
    # TODO: Return multiple batches per round?
    return batch


@ray.remote
def consume(chunk):
    return time.time()


def shuffle_all(filenames, num_trainers, num_rounds):
    v = Validator.remote(filenames)
    num_expected_rows = ray.get(v.get_num_expected_rows.remote())
    print("Expecting", num_expected_rows, "rows")

    start = time.time()

    final_shuffled = []
    seed = 0
    for r in range(num_rounds):
        # TODO: Set num returns = num trainers. So that we're not sending all
        # data to all reducers. Probably only matters for distributed version.
        chunks = [select.remote(filename, num_trainers, seed, r, num_rounds)
                for filename in filenames]
        shuffled = [shuffle.remote(i, *chunks) for i in range(num_trainers)]
        finished = ray.get([consume.remote(batch) for batch in shuffled])
        final_shuffled += shuffled

        for t in finished:
            print(t - start)

    assert ray.get(v.check.remote(*final_shuffled))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PyTorch Test with Dense and Sparse Adam Optimizers')
    parser.add_argument('--num-trainers', type=int)
    parser.add_argument('--num-rounds', type=int)
    args = parser.parse_args()

    ray.init()
    filenames = [f'input{i}' for i in range(10)]
    shuffle_all(filenames, args.num_trainers, args.num_rounds)
