# flake8: noqa
# isort: skip_file

# __basic_start__
import ray
from ray import tune
from ray.tune import Tuner
from ray.train.xgboost import XGBoostTrainer

dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

trainer = XGBoostTrainer(
    label_column="target",
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": 4,
    },
    datasets={"train": dataset},
)

# Create Tuner
tuner = Tuner(
    trainer,
    # Add some parameters to tune
    param_space={"params": {"max_depth": tune.choice([4, 5, 6])}},
    # Specify tuning behavior
    tune_config=tune.TuneConfig(metric="train-logloss", mode="min", num_samples=2),
)
# Run tuning job
tuner.fit()
# __basic_end__

# __xgboost_start__
import ray
from ray import tune
from ray.tune import Tuner
from ray.train.xgboost import XGBoostTrainer
from ray.air.config import ScalingConfig, RunConfig

dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

# Create an XGBoost trainer
trainer = XGBoostTrainer(
    label_column="target",
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": 4,
    },
    num_boost_round=10,
    datasets={"train": dataset},
)

param_space = {
    # Tune parameters directly passed into the XGBoostTrainer
    "num_boost_round": tune.randint(5, 20),
    # `params` will be merged with the `params` defined in the above XGBoostTrainer
    "params": {
        "min_child_weight": tune.uniform(0.8, 1.0),
        # Below will overwrite the XGBoostTrainer setting
        "max_depth": tune.randint(1, 5),
    },
    # Tune the number of distributed workers
    "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
}

tuner = Tuner(
    trainable=trainer,
    run_config=RunConfig(name="test_tuner"),
    param_space=param_space,
    tune_config=tune.TuneConfig(
        mode="min", metric="train-logloss", num_samples=2, max_concurrent_trials=2
    ),
)
result_grid = tuner.fit()
# __xgboost_end__

# __torch_start__
from ray import tune
from ray.tune import Tuner
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.torch import TorchTrainer

trainer = TorchTrainer(
    train_loop_per_worker=linear_train_func,
    train_loop_config={"lr": 1e-2, "batch_size": 4, "epochs": 10},
    scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
)

param_space = {
    # The params will be merged with the ones defined in the TorchTrainer
    "train_loop_config": {
        # This is a parameter that hasn't been set in the TorchTrainer
        "hidden_size": tune.randint(1, 4),
        # This will overwrite whatever was set when TorchTrainer was instantiated
        "batch_size": tune.choice([4, 8]),
    },
    # Tune the number of distributed workers
    "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
}

tuner = Tuner(
    trainable=trainer,
    run_config=RunConfig(name="test_tuner", local_dir="~/ray_results"),
    param_space=param_space,
    tune_config=tune.TuneConfig(
        mode="min", metric="loss", num_samples=2, max_concurrent_trials=2
    ),
)
result_grid = tuner.fit()
# __torch_end__


# __tune_preprocess_start__
from ray.data.preprocessors import StandardScaler
from ray.tune import Tuner

prep_v1 = StandardScaler(["worst radius", "worst area"])
prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
tuner = Tuner(
    trainer,
    param_space={
        "preprocessor": tune.grid_search([prep_v1, prep_v2]),
        # Your other parameters go here
    },
)
# __tune_preprocess_end__

# __tune_dataset_start__
def get_dataset():
    return ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")


def get_another_dataset():
    # imagine this is a different dataset
    return ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")


dataset_1 = get_dataset()
dataset_2 = get_another_dataset()

tuner = tune.Tuner(
    trainer,
    param_space={
        "datasets": {
            "train": tune.grid_search([dataset_1, dataset_2]),
        }
        # Your other parameters go here
    },
)
# __tune_dataset_end__

# __tune_parallelism_start__
from ray.tune import TuneConfig

config = TuneConfig(
    # ...
    num_samples=100,
    max_concurrent_trials=10,
)
# __tune_parallelism_end__

# __tune_optimization_start__
from ray.tune.search.bayesopt import BayesOptSearch
from ray.tune.schedulers import HyperBandScheduler
from ray.tune import TuneConfig

config = TuneConfig(
    # ...
    search_alg=BayesOptSearch(),
    scheduler=HyperBandScheduler(),
)
# __tune_optimization_end__

# __result_grid_inspection_start__
from ray.tune import Tuner, TuneConfig

tuner = Tuner(
    trainable=trainer,
    param_space=param_space,
    tune_config=TuneConfig(mode="min", metric="loss", num_samples=5),
)
result_grid = tuner.fit()

num_results = len(result_grid)

# Check if there have been errors
if result_grid.errors:
    print("At least one trial failed.")

# Get the best result
best_result = result_grid.get_best_result()

# And the best checkpoint
best_checkpoint = best_result.checkpoint

# And the best metrics
best_metric = best_result.metrics

# Or a dataframe for further analysis
results_df = result_grid.get_dataframe()
print("Shortest training time:", results_df["time_total_s"].min())

# Iterate over results
for result in result_grid:
    if result.error:
        print("The trial had an error:", result.error)
        continue

    print("The trial finished successfully with the metrics:", result.metrics["loss"])
# __result_grid_inspection_end__

# __run_config_start__
from ray import air, tune
from ray.air.config import RunConfig

run_config = RunConfig(
    name="MyExperiment",
    local_dir="./your_log_directory/",
    verbose=2,
    sync_config=tune.SyncConfig(upload_dir="s3://..."),
    checkpoint_config=air.CheckpointConfig(checkpoint_frequency=2),
)
# __run_config_end__

# __tune_config_start__
from ray.tune import TuneConfig
from ray.tune.search.bayesopt import BayesOptSearch

tune_config = TuneConfig(
    metric="loss",
    mode="min",
    max_concurrent_trials=10,
    num_samples=100,
    search_alg=BayesOptSearch(),
)
# __tune_config_end__

# __tune_restore_start__
tuner = Tuner.restore("~/ray_results/test_tuner", restart_errored=True)
tuner.fit()
# __tune_restore_end__
