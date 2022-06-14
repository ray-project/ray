# flake8: noqa

# __gen_csv_dataset_start__
import numpy as np
from numpy import random
import pandas as pd


# Generate a linear 180MiB dataset.
def generate_csv(filename: str):
    size = 10000000
    a = 2
    b = 5
    x = np.arange(0, 10, 10 / size, dtype=np.float32)
    random.shuffle(x)
    y = a * x + b
    df = pd.DataFrame({"x": x, "y": y})
    ds = ray.data.from_pandas(df)
    ds.write_csv(filename)
# __gen_csv_dataset_end__


# __tuning_dataset_start__
import ray
from ray.train.torch import TorchTrainer
from ray import tune
from ray.tune.tuner import Tuner, RunConfig

ray.init(num_cpus=5)

# Generate two random linear datasets, 180MiB each.
generate_csv("my_data1")
generate_csv("my_data2")


def train_func(config):
    assert train.get_dataset_shard("train").to_torch(label_column="y")


config = {"batch_size": 4}
scaling_config = {"num_workers": 1, "use_gpu": False}
trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    train_loop_config=config,
    scaling_config=scaling_config,
)

# Run the Trainer in four trials. Like shown in Figure(b).
param_space = {
    "train_loop_config": {
        "batch_size": tune.grid_search([4, 8]),
    },
    "datasets": tune.grid_search({"train": ray.data.read_csv("my_data1")}, {"train": ray.data.read_csv("my_data2")}),
}

tuner = Tuner(
    trainable=trainer,
    run_config=RunConfig(name="test_tuner"),
    param_space=param_space,
)
tuner.fit()
ray.shutdown()
# __tuning_dataset_start__

# __tuning_non_dataset_start__
import ray
from ray.train.torch import TorchTrainer
from ray import tune
from ray.tune.tuner import Tuner, RunConfig

ray.init(num_cpus=5)

# Generate two random linear datasets, 180MiB each.
generate_csv("my_data")


def train_func(config):
    assert train.get_dataset_shard("train").to_torch(label_column="y")


config = {"batch_size": 4}
scaling_config = {"num_workers": 1, "use_gpu": False}
trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    train_loop_config=config,
    scaling_config=scaling_config,
    datasets={"train": ray.data.read_csv("my_data")}
)

# Run the Trainer in four trials. Like shown in Figure(b).
param_space = {
    "train_loop_config": {
        "batch_size": tune.grid_search([4, 8]),
    }
}

tuner = Tuner(
    trainable=trainer,
    run_config=RunConfig(name="test_tuner"),
    param_space=param_space,
)
tuner.fit()
ray.shutdown()
# __tuning_non_dataset_start__

# __check_ingest_1__
import ray
from ray.air.preprocessors import Chain, BatchMapper
from ray.air.util.check_ingest import DummyTrainer

# Generate a synthetic dataset of ~10GiB of float64 data. The dataset is sharded
# into 100 blocks (parallelism=100).
dataset = ray.data.range_tensor(50000, shape=(80, 80, 4), parallelism=100)

# An example preprocessor chain that just scales all values by 4.0 in two stages.
preprocessor = Chain(
    BatchMapper(lambda df: df * 2),
    BatchMapper(lambda df: df * 2),
)
# __check_ingest_1_end__

# __check_ingest_2__
# Setup the dummy trainer that prints ingest stats.
# Run and print ingest stats.
trainer = DummyTrainer(
    scaling_config={"num_workers": 1, "use_gpu": False},
    datasets={"train": dataset},
    preprocessor=preprocessor,
    runtime_seconds=1,  # Stop after this amount or time or 1 epoch is read.
    prefetch_blocks=1,  # Number of blocks to prefetch when reading data.
    batch_size=None,  # Use whole blocks as batches.
)
trainer.fit()
# __check_ingest_2_end__

# __config_1__
import ray
from ray.train.torch import TorchTrainer
from ray.air.config import DatasetConfig

train_ds = ray.data.range_tensor(1000)
valid_ds = ray.data.range_tensor(100)
test_ds = ray.data.range_tensor(100)

my_trainer = TorchTrainer(
    lambda: None,  # No-op training loop.
    scaling_config={"num_workers": 2},
    datasets={
        "train": train_ds,
        "valid": valid_ds,
        "test": test_ds,
    },
    dataset_config={
        "valid": DatasetConfig(split=True),
        "test": DatasetConfig(split=True),
    },
)
print(my_trainer.get_dataset_config())
# -> {'train': DatasetConfig(fit=True, split=True, ...),
#     'valid': DatasetConfig(fit=False, split=True, ...),
#     'test': DatasetConfig(fit=False, split=True, ...), ...}
# __config_1_end__

# __config_2__
import ray
from ray.train.torch import TorchTrainer
from ray.air.config import DatasetConfig

train_ds = ray.data.range_tensor(1000)
side_ds = ray.data.range_tensor(10)

my_trainer = TorchTrainer(
    lambda: None,  # No-op training loop.
    scaling_config={"num_workers": 2},
    datasets={
        "train": train_ds,
        "side": side_ds,
    },
    dataset_config={
        "side": DatasetConfig(transform=False),
    },
)
print(my_trainer.get_dataset_config())
# -> {'train': DatasetConfig(fit=True, split=True, ...),
#     'side': DatasetConfig(fit=False, split=False, transform=False, ...), ...}
# __config_2_end__

# __config_4__
import ray
from ray import train
from ray.data import Dataset
from ray.train.torch import TorchTrainer
from ray.air.config import DatasetConfig


def train_loop_per_worker():
    # By default, bulk loading is used and returns a Dataset object.
    data_shard: Dataset = train.get_dataset_shard("train")

    # Manually iterate over the data 10 times (10 epochs).
    for _ in range(10):
        for batch in data_shard.iter_batches():
            print("Do some training on batch", batch)

    # View the stats for performance debugging.
    print(data_shard.stats())


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config={"num_workers": 1},
    datasets={
        "train": ray.data.range_tensor(1000),
    },
)
my_trainer.fit()
# __config_4_end__

# __config_5__
import ray
from ray import train
from ray.data import DatasetPipeline
from ray.train.torch import TorchTrainer
from ray.air.config import DatasetConfig


def train_loop_per_worker():
    # A DatasetPipeline object is returned when `use_stream_api` is set.
    data_shard: DatasetPipeline = train.get_dataset_shard("train")

    # Use iter_epochs(10) to iterate over 10 epochs of data.
    for epoch in data_shard.iter_epochs(10):
        for batch in epoch.iter_batches():
            print("Do some training on batch", batch)

    # View the stats for performance debugging.
    print(data_shard.stats())


# Set N = 200 bytes for this toy example. Typically, you'd set N >= 1GiB.
N = 200

my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config={"num_workers": 1},
    datasets={
        "train": ray.data.range_tensor(1000),
    },
    dataset_config={
        "train": DatasetConfig(use_stream_api=True, stream_window_size=N),
    },
)
my_trainer.fit()
# __config_5_end__
