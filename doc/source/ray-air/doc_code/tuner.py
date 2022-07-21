# flake8: noqa
# isort: skip_file

# __xgboost_start__

from ray.data import from_pandas
from sklearn.datasets import load_breast_cancer


def get_dataset():
    data_raw = load_breast_cancer(as_frame=True)
    dataset_df = data_raw["data"]
    dataset_df["target"] = data_raw["target"]
    dataset = from_pandas(dataset_df)
    return dataset


from ray.train.xgboost import XGBoostTrainer

trainer = XGBoostTrainer(
    label_column="target",
    datasets={"train": get_dataset()},
)

from ray import tune
from ray.air.config import ScalingConfig

param_space = {
    "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
    "params": {
        "objective": "binary:logistic",
        "tree_method": "approx",  # This will overwrite whatever is set when Trainer is instantiated.
        "max_depth": tune.randint(1, 9),
    },
}
# This will generate a search space that randomly samples max_depth.
# And it also does a grid search on the number of xgboost_ray workers.
# __xgboost_end__

# __tune_preprocess_start__
from ray.data.preprocessors import StandardScaler

prep_v1 = StandardScaler(["worst radius", "worst area"])
prep_v2 = StandardScaler(["worst concavity", "worst smoothness"])
param_space_tune_preprocessors = {
    "preprocessor": tune.grid_search([prep_v1, prep_v2]),
}
# __tune_preprocess_end__


# __tuner_start__
from ray.air.config import RunConfig
from ray.tune.tuner import Tuner, TuneConfig

tuner = Tuner(
    trainable=trainer,
    run_config=RunConfig(name="test_tuner"),
    param_space=param_space,
    tune_config=TuneConfig(mode="min", metric="train-error", num_samples=2),
)
result_grid = tuner.fit()
# __tuner_stop__


# __torch_start__
from ray.air.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.torch import TorchTrainer

config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 10}
scaling_config = ScalingConfig(num_workers=1, use_gpu=False)
trainer = TorchTrainer(
    train_loop_per_worker=linear_train_func,
    train_loop_config=config,
    scaling_config=scaling_config,
)
param_space = {
    "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
    "train_loop_config": {
        "batch_size": tune.grid_search([4, 8]),
    },
}
# __torch_stop__

# __result_grid_inspection_start__
for i in range(len(result_grid)):
    result = result_grid[i]
    if not result.error:
        print(f"Trial finishes successfully with metric {result.metric}.")
    else:
        print(f"Trial errors out with {result.error}.")
best_result = result_grid.get_best_result()
best_checkpoint = best_result.checkpoint
best_metric = best_result.metric
# __result_grid_inspection_stop__
