# flake8: noqa

# __shared_dataset_start__
import ray
from ray.ml.utils.check_ingest import DummyTrainer
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
from ray.ml.utils.check_ingest import DummyTrainer
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
from ray.ml.preprocessors import Chain, BatchMapper
from ray.ml.utils.check_ingest import DummyTrainer

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
