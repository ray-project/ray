# flake8: noqa
# isort: skip_file


# __check_ingest_1__
import ray
from ray.data.preprocessors import Chain, BatchMapper
from ray.air.util.check_ingest import DummyTrainer
from ray.air.config import ScalingConfig

# Generate a synthetic dataset of ~10GiB of float64 data. The dataset is sharded
# into 100 blocks (parallelism=100).
dataset = ray.data.range_tensor(50000, shape=(80, 80, 4), parallelism=100)

# An example preprocessor chain that just scales all values by 4.0 in two stages.
preprocessor = Chain(
    BatchMapper(lambda df: df * 2, batch_format="pandas"),
    BatchMapper(lambda df: df * 2, batch_format="pandas"),
)
# __check_ingest_1_end__

# __check_ingest_2__
# Setup the dummy trainer that prints ingest stats.
# Run and print ingest stats.
trainer = DummyTrainer(
    scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
    datasets={"train": dataset},
    preprocessor=preprocessor,
    num_epochs=1,  # Stop after this number of epochs is read.
    prefetch_blocks=1,  # Number of blocks to prefetch when reading data.
    batch_size=None,  # Use whole blocks as batches.
)
trainer.fit()
# __check_ingest_2_end__

# __config_1__
import ray
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig, DatasetConfig

train_ds = ray.data.range_tensor(1000)
valid_ds = ray.data.range_tensor(100)
test_ds = ray.data.range_tensor(100)

my_trainer = TorchTrainer(
    lambda: None,  # No-op training loop.
    scaling_config=ScalingConfig(num_workers=2),
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
from ray.air.config import ScalingConfig, DatasetConfig

train_ds = ray.data.range_tensor(1000)
side_ds = ray.data.range_tensor(10)

my_trainer = TorchTrainer(
    lambda: None,  # No-op training loop.
    scaling_config=ScalingConfig(num_workers=2),
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
from ray.air import session
from ray.data import DatasetIterator
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

# A simple preprocessor that just scales all values by 2.0.
preprocessor = BatchMapper(lambda df: df * 2, batch_format="pandas")


def train_loop_per_worker():
    # Get a handle to the worker's assigned DatasetIterator shard.
    data_shard: DatasetIterator = session.get_dataset_shard("train")

    # Manually iterate over the data 10 times (10 epochs).
    for _ in range(10):
        for batch in data_shard.iter_batches():
            print("Do some training on batch", batch)

    # Print the stats for performance debugging.
    print(data_shard.stats())


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=1),
    datasets={
        "train": ray.data.range_tensor(1000),
    },
    preprocessor=preprocessor,
)
my_trainer.fit()
# __config_4_end__

# __config_5__
import ray
from ray.air import session
from ray.data import DatasetIterator
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig, DatasetConfig

# A simple preprocessor that just scales all values by 2.0.
preprocessor = BatchMapper(lambda df: df * 2, batch_format="pandas")


def train_loop_per_worker():
    data_shard: DatasetIterator = session.get_dataset_shard("train")

    # Iterate over 10 epochs of data.
    for _ in range(10):
        for batch in data_shard.iter_batches():
            print("Do some training on batch", batch)

    # View the stats for performance debugging.
    print(data_shard.stats())


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=1),
    datasets={
        "train": ray.data.range_tensor(1000),
    },
    dataset_config={
        # Use 20% of object store memory.
        "train": DatasetConfig(max_object_store_memory_fraction=0.2),
    },
    preprocessor=preprocessor,
)
my_trainer.fit()
# __config_5_end__

# __global_shuffling_start__
import ray
from ray.air import session
from ray.data import DatasetIterator
from ray.train.torch import TorchTrainer
from ray.air.config import DatasetConfig, ScalingConfig


def train_loop_per_worker():
    data_shard: DatasetIterator = session.get_dataset_shard("train")

    # Iterate over 10 epochs of data.
    for epoch in range(10):
        for batch in data_shard.iter_batches():
            print("Do some training on batch", batch)

    # View the stats for performance debugging.
    print(data_shard.stats())


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": ray.data.range_tensor(1000)},
    dataset_config={
        "train": DatasetConfig(global_shuffle=True),
    },
)
print(my_trainer.get_dataset_config())
# -> {'train': DatasetConfig(fit=True, split=True, global_shuffle=True, ...)}
my_trainer.fit()
# __global_shuffling_end__

# __local_shuffling_start__
import ray
from ray.air import session
from ray.data import DatasetIterator
from ray.train.torch import TorchTrainer
from ray.air.config import DatasetConfig, ScalingConfig


def train_loop_per_worker():
    data_shard: DatasetIterator = session.get_dataset_shard("train")

    # Iterate over 10 epochs of data.
    for epoch in range(10):
        for batch in data_shard.iter_batches(
            batch_size=10_000,
            local_shuffle_buffer_size=100_000,
        ):
            print("Do some training on batch", batch)

    # View the stats for performance debugging.
    print(data_shard.stats())


my_trainer = TorchTrainer(
    train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": ray.data.range_tensor(1000)},
    dataset_config={
        # global_shuffle is disabled by default, but we're emphasizing here that you
        # would NOT want to use both global and local shuffling together.
        "train": DatasetConfig(global_shuffle=False),
    },
)
print(my_trainer.get_dataset_config())
# -> {'train': DatasetConfig(fit=True, split=True, global_shuffle=False, ...)}
my_trainer.fit()
# __local_shuffling_end__

ray.shutdown()

# __resource_allocation_1_begin__
import ray
from ray.air import session
from ray.data.preprocessors import BatchMapper
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

# Create a cluster with 4 CPU slots available.
ray.init(num_cpus=4)

# A simple example training loop.
def train_loop_per_worker():
    data_shard = session.get_dataset_shard("train")
    for _ in range(10):
        for batch in data_shard.iter_batches():
            print("Do some training on batch", batch)


# A simple preprocessor that just scales all values by 2.0.
preprocessor = BatchMapper(lambda df: df * 2, batch_format="pandas")

my_trainer = TorchTrainer(
    train_loop_per_worker,
    # This will hang if you set num_workers=4, since the
    # Trainer will reserve all 4 CPUs for workers, leaving
    # none left for Datasets execution.
    scaling_config=ScalingConfig(num_workers=2),
    datasets={
        "train": ray.data.range_tensor(1000),
    },
    preprocessor=preprocessor,
)
my_trainer.fit()
# __resource_allocation_1_end__

ray.shutdown()

# __resource_allocation_2_begin__
import ray
from ray.air import session
from ray.data.preprocessors import BatchMapper
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

# Create a cluster with 4 CPU slots available.
ray.init(num_cpus=4)

# A simple example training loop.
def train_loop_per_worker():
    data_shard = session.get_dataset_shard("train")
    for _ in range(10):
        for batch in data_shard.iter_batches():
            print("Do some training on batch", batch)


# A simple preprocessor that just scales all values by 2.0.
preprocessor = BatchMapper(lambda df: df * 2, batch_format="pandas")

my_trainer = TorchTrainer(
    train_loop_per_worker,
    # This will hang if you set num_workers=4, since the
    # Trainer will reserve all 4 CPUs for workers, leaving
    # none left for Datasets execution.
    scaling_config=ScalingConfig(num_workers=2),
    datasets={
        "train": ray.data.range_tensor(1000),
    },
    preprocessor=preprocessor,
)
my_trainer.fit()
# __resource_allocation_2_end__
