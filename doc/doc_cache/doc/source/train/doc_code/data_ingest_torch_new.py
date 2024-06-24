# flake8: noqa
# isort: skip_file

# __basic__
import ray
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

import numpy as np
from typing import Dict

# Load the data.
train_ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
## Uncomment to randomize the block order each epoch.
# train_ds = train_ds.randomize_block_order()


# Define a preprocessing function.
def normalize_length(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    new_col = batch["sepal.length"] / np.max(batch["sepal.length"])
    batch["normalized.sepal.length"] = new_col
    del batch["sepal.length"]
    return batch


# Preprocess your data any way you want. This will be re-run each epoch.
# You can use Ray Data preprocessors here as well,
# e.g., preprocessor.fit_transform(train_ds)
train_ds = train_ds.map_batches(normalize_length)


def train_loop_per_worker():
    # Get an iterator to the dataset we passed in below.
    it = train.get_dataset_shard("train")

    # Train for 10 epochs over the data. We'll use a shuffle buffer size
    # of 10k elements, and prefetch up to 10 batches of size 128 each.
    for _ in range(10):
        for batch in it.iter_batches(
            local_shuffle_buffer_size=10000, batch_size=128, prefetch_batches=10
        ):
            print("Do some training on batch", batch)


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": train_ds},
)
my_trainer.fit()
# __basic_end__

# __custom_split__
dataset_a = ray.data.read_text(
    "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
)
dataset_b = ray.data.read_csv("s3://anonymous@ray-example-data/dow_jones.csv")

my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"a": dataset_a, "b": dataset_b},
    dataset_config=ray.train.DataConfig(
        datasets_to_split=["a"],
    ),
)
# __custom_split_end__


def augment_data(batch):
    return batch


# __materialized__
# Load the data.
train_ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

# Preprocess the data. Transformations that are made to the materialize call below
# will only be run once.
train_ds = train_ds.map_batches(normalize_length)

# Materialize the dataset in object store memory.
train_ds = train_ds.materialize()

# Add per-epoch preprocessing. Transformations that you want to run per-epoch, such
# as data augmentation, should go after the materialize call.
train_ds = train_ds.map_batches(augment_data)
# __materialized_end__

# __options__
from ray.train import DataConfig

options = DataConfig.default_ingest_options()
options.resource_limits.object_store_memory = 10e9


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=2),
    dataset_config=ray.train.DataConfig(
        execution_options=options,
    ),
)
# __options_end__

# __custom__
# Note that this example class is doing the same thing as the basic DataConfig
# impl included with Ray Train.
from typing import Optional, Dict, List

from ray.data import Dataset, DataIterator, NodeIdStr
from ray.actor import ActorHandle


class MyCustomDataConfig(DataConfig):
    def configure(
        self,
        datasets: Dict[str, Dataset],
        world_size: int,
        worker_handles: Optional[List[ActorHandle]],
        worker_node_ids: Optional[List[NodeIdStr]],
        **kwargs,
    ) -> List[Dict[str, DataIterator]]:
        assert len(datasets) == 1, "This example only handles the simple case"

        # Configure Ray Data for ingest.
        ctx = ray.data.DataContext.get_current()
        ctx.execution_options = DataConfig.default_ingest_options()

        # Split the stream into shards.
        iterator_shards = datasets["train"].streaming_split(
            world_size, equal=True, locality_hints=worker_node_ids
        )

        # Return the assigned iterators for each worker.
        return [{"train": it} for it in iterator_shards]


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": train_ds},
    dataset_config=MyCustomDataConfig(),
)
my_trainer.fit()
# __custom_end__
