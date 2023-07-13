# flake8: noqa
# isort: skip_file

# __legacy_api__
import random
import ray

from ray.air.config import ScalingConfig, DatasetConfig
from ray.data.preprocessors.batch_mapper import BatchMapper
from ray.train.torch import TorchTrainer

train_ds = ray.data.range_tensor(1000)
test_ds = ray.data.range_tensor(10)

# A randomized preprocessor that adds a random float to all values.
add_noise = BatchMapper(lambda df: df + random.random(), batch_format="pandas")

my_trainer = TorchTrainer(
    lambda: None,
    scaling_config=ScalingConfig(num_workers=1),
    datasets={
        "train": train_ds,
        "test": test_ds,
    },
    dataset_config={
        "train": DatasetConfig(
            split=True,
            # Apply the preprocessor for each epoch.
            per_epoch_preprocessor=add_noise,
        ),
        "test": DatasetConfig(
            split=False,
        ),
    },
)
my_trainer.fit()
# __legacy_api_end__

# __new_api__
from ray.train import DataConfig

train_ds = ray.data.range_tensor(1000)
test_ds = ray.data.range_tensor(10)

# Apply the preprocessor before passing the Dataset to the Trainer.
# This operation is lazy. It will be re-executed for each epoch.
train_ds = add_noise.transform(train_ds)

my_trainer = TorchTrainer(
    lambda: None,
    scaling_config=ScalingConfig(num_workers=1),
    datasets={
        "train": train_ds,
        "test": test_ds,
    },
    # Specify which datasets to split.
    dataset_config=DataConfig(
        datasets_to_split=["train"],
    ),
)
my_trainer.fit()
# __new_api_end__
