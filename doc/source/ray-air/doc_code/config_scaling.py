# flake8: noqa
# isort: skip_file


# __config_scaling_1__
from ray.train.xgboost import XGBoostTrainer
from ray.air.config import ScalingConfig
import ray.data


trainer = XGBoostTrainer(
    params={},
    scaling_config=ScalingConfig(num_workers=1),
    datasets={"train": ray.data.range_tensor(1000)},
)
trainer.fit()
# __config_scaling_1_end__

# __config_scaling_2__
from ray.train.xgboost import XGBoostTrainer
import ray.data
from ray.air.config import ScalingConfig
from ray import tune
from ray.tune.tuner import Tuner
from ray.tune.tune_config import TuneConfig


trainer = XGBoostTrainer(
    params={},
    datasets={"train": ray.data.range_tensor(1000)},
)
param_space = {
    "scaling_config": ScalingConfig(num_workers=tune.grid_search([1, 2])),
    "params": {
        "objective": "binary:logistic",
        "tree_method": "approx",
        "eval_metric": ["logloss", "error"],
        "eta": tune.loguniform(1e-4, 1e-1),
        "subsample": tune.uniform(0.5, 1.0),
        "max_depth": tune.randint(1, 9),
    },
}
tuner = Tuner(
    trainable=trainer,
    param_space=param_space,
    tune_config=TuneConfig(mode="min", metric="train-error"),
)
results = tuner.fit()
# __config_scaling_2_end__
