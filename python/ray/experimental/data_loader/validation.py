import pandas as pd

import ray


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
        assert (list(shuffled["key"]) != list(range(num_expected_rows))
                and set(shuffled["key"]) == set(range(num_expected_rows)))
