# flake8: noqa
# isort: skip_file

# __basic__
import ray
from ray.air import session
from ray.air.config import ScalingConfig
from ray.train.torch import TorchTrainer

import numpy as np
from typing import Dict

# Load the data.
train_ds = ray.data.read_parquet("example://iris.parquet")

# Define a preprocessing function.
def normalize_length(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    new_col = batch["sepal.length"] / np.max(batch["sepal.length"])
    batch["normalized.sepal.length"] = new_col
    del batch["sepal.length"]
    return batch

# Preprocess your data any way you want.
# You can use Ray Data preprocessors here as well,
# e.g., preprocessor.fit_transform(train_ds)
train_ds = train_ds.map_batches(normalize_length)

def train_loop_per_worker():
    # Get an iterator to the dataset we passed in below.
    it = session.get_dataset_shard("train")

    # Train for 10 epochs over the data. We'll use a shuffle buffer size
    # of 10k elements, and prefetch up to 10 batches of size 128 each.
    for _ in range(10):
        for batch in it.iter_batches(
                 local_shuffle_buffer_size=10000,
                 batch_size=128,
                 prefetch_batches=10):
            print("Do some training on batch", batch)

my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": train_ds},
)
my_trainer.fit()
# __basic_end__
