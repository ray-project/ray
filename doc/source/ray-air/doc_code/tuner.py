# flake8: noqa
# isort: skip_file

from ray.air import session


def my_trainer(config):
    session.report({"loss": config["parameter"]})


# __basic_start__
from ray import tune

# Create Tuner
tuner = tune.Tuner(
    my_trainer,
    # Add some parameters to tune
    param_space={"parameter": tune.uniform(0.0, 1.0)},
    # Specify tuning behavior
    tune_config=tune.TuneConfig(num_samples=10),
)
# Run tuning job
tuner.fit()
# __basic_end__

# __xgboost_start__


# Function that returns the training dataset
import ray


def get_dataset():
    return ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")


from ray.train.xgboost import XGBoostTrainer

# Create an XGBoost trainer
trainer = XGBoostTrainer(
    label_column="target",
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": 4,
    },
    datasets={"train": get_dataset()},
)

from ray import tune
from ray.air.config import ScalingConfig

param_space = {
    # You can tune arguments that are passed directly into the XGBoostTrainer
    "num_boost_round": tune.randint(20, 30),
    # The params will be merged with the ones defined in the XGBoostTrainer
    "params": {
        # This is a parameter that hasn't been set in the XGBoostTrainer
        "min_child_weight": tune.uniform(0.8, 1.0),
        # This will overwrite whatever was set when XGBoostTrainer was instantiated
        "max_depth": tune.randint(1, 9),
    },
    # We can also tune the number of distributed workers
    "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
}

# __xgboost_end__

# __torch_start__
from ray import tune
from ray.air.examples.pytorch.torch_linear_example import (
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
    # We can also tune the number of distributed workers
    "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
}
# __torch_end__


# __tune_preprocess_start__
from ray.data.preprocessors import StandardScaler

prep_v1 = StandardScaler(["worst radius", "worst area"])
prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
tuner = tune.Tuner(
    trainer,
    param_space={
        # ...
        "preprocessor": tune.grid_search([prep_v1, prep_v2]),
    },
)
# __tune_preprocess_end__

get_another_dataset = get_dataset

# __tune_dataset_start__
dataset_1 = get_dataset()
dataset_2 = get_another_dataset()

tuner = tune.Tuner(
    trainer,
    param_space={
        # ...
        "datasets": {
            "train": tune.grid_search([dataset_1, dataset_2]),
        }
    },
)
# __tune_dataset_end__


# __tuner_start__
from ray.air.config import RunConfig
from ray.tune.tuner import Tuner, TuneConfig

tuner = Tuner(
    trainable=trainer,
    run_config=RunConfig(name="test_tuner"),
    param_space=param_space,
    tune_config=TuneConfig(mode="min", metric="loss", num_samples=2),
)
result_grid = tuner.fit()
# __tuner_end__


# __result_grid_inspection_start__

# Number of results
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

# Inspect all results
for result in result_grid:
    if result.error:
        print("The trial had an error:", result.error)
        continue

    print("The trial finished successfully with the metrics:", result.metrics)
# __result_grid_inspection_end__

# __run_config_start__
from ray import air, tune
from ray.air.config import RunConfig
from ray.tune import Callback


class MyCallback(Callback):  # Tuner expose callbacks for customer logics.
    def on_trial_result(self, iteration, trials, trial, result, **info):
        print(f"Got result: {result['metric']}")


run_config = RunConfig(
    name="MyExperiment",
    callbacks=[MyCallback()],
    sync_config=tune.SyncConfig(upload_dir="s3://..."),
    checkpoint_config=air.CheckpointConfig(checkpoint_frequency=2),
)

# __run_config_end__

# __tune_config_start__
from ray.tune import TuneConfig
from ray.tune.search.bayesopt import BayesOptSearch

algo = BayesOptSearch(random_search_steps=4)

tune_config = TuneConfig(
    metric="score",
    mode="min",
    search_alg=algo,
)

# __tune_config_end__
