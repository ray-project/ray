# flake8: noqa

# __shared_dataset_start__
import ray
from ray.air.util.check_ingest import DummyTrainer
from ray.tune.tuner import Tuner, TuneConfig

ray.init(num_cpus=5)

# Generate a synthetic 100MiB tensor dataset.
dataset = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=10)

# Create an example trainer that simply loops over the data a few times.
trainer = DummyTrainer(datasets={"train": dataset}, runtime_seconds=1)

# Run the Trainer 4x in parallel with Tune.
tuner = Tuner(
    trainer,
    tune_config=TuneConfig(num_samples=4),
)
tuner.fit()
# __shared_dataset_end__

ray.shutdown()

# __indep_dataset_start__
import ray
from ray import tune
from ray.air.util.check_ingest import DummyTrainer
from ray.tune.tuner import Tuner, TuneConfig

ray.init(num_cpus=5)


def make_ds_1():
    """Dataset creator function 1."""
    return ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=10)


def make_ds_2():
    """Dataset creator function 2."""
    return ray.data.range_tensor(50, shape=(80, 80, 4), parallelism=10)


# Create an example trainer that simply loops over the data a few times.
trainer = DummyTrainer(datasets={}, runtime_seconds=1)

# Run the Trainer 4x in parallel with Tune.
# Two trials will use the dataset created by `make_ds_1`, and two trials will
# use the dataset created by `make_ds_2`.
tuner = Tuner(
    trainer,
    # Instead of passing Dataset references directly, we pass functions that
    # generate the dataset when called.
    param_space={"datasets": {"train": tune.grid_search([make_ds_1, make_ds_2])}},
    tune_config=TuneConfig(num_samples=2),
)
tuner.fit()
# __indep_dataset_end__

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
from ray.air.train.integrations.torch import TorchTrainer
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
from ray.air.train.integrations.torch import TorchTrainer
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
from ray.air.train.integrations.torch import TorchTrainer
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
from ray.air.train.integrations.torch import TorchTrainer
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
