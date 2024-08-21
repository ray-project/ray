"""Minimal example for `map_batches` to test performance.

The `map_batches` as used in `RLlib`'s OFfline RL API is very slow.
This example is one of three to test `map_batches` in different
scenarios.

This aprticular example should show how `map_batches` can be tuned
by `override_num_blocks` and should show that more blocks should
in general increase performance by an almost constant rate. This
relationship gets, however, lost when turning to more real-world
use cases.

Note, `RLlib` uses `map_batches` for preprocessing data while its
(remote/local) learner(s) is/are learning on already processed
batches. The `MapBatches`  class should emulate this preprocessing
by using the `time.sleep` method.
Note further, the 3 seconds are large, normally preprocessing steps
on batches of 2000 take RLlib around 0.02 seconds (see for this also
the `test_offline_data.py` that tests performance on each step.)
"""

import time

import numpy as np
import pandas as pd

import ray
import ray.data


# This class should emulate `RLlib`s `OfflinePreLearner`'s `__call__`
# method. Note, 3 seconds is a lot, we usually need around 0.02s (for
# a batch of 2,000).
class MapBatches:
    def __call__(self, batch):
        time.sleep(3)

        return batch


# Initialize Ray
ray.init(ignore_reinit_error=True)

# Create a larger sample dataset
np.random.seed(42)  # For reproducibility
data = {
    "col1": np.random.randint(1, 100, size=10000),
    "col2": np.random.choice(["a", "b", "c", "d", "e"], size=10000),
    "col3": np.random.random(size=10000),
}

# Convert the data dictionary to a Ray dataset.
df = pd.DataFrame(data)
# Define also the number of blocks.
ds = ray.data.from_pandas(df, override_num_blocks=60)

# ---------------------------------------------------
# 1. Test `take_batch`. This should be quite fast
# Expected value: 0.03s.
start = time.perf_counter()
batch = ds.take_batch(200)
stop = time.perf_counter()
print(f"Time for simple take_batch: {stop - start}")

# ---------------------------------------------------
# 2. Test ´map_batches` with the emulated PreLearner.
#   This is slow.
# Expected value: 75s!
start = time.perf_counter()
batch = ds.map_batches(
    MapBatches,
    batch_size=200,
    concurrency=4,
    num_gpus=0,
    num_cpus=4,
).take_batch(200)
print(batch)
stop = time.perf_counter()
print(f"Time for map_batches: {stop - start}")

ray.shutdown()
