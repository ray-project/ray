import pandas as pd
import ray
from ray import tune
from ray.ml import RunConfig
from ray.ml.train.integrations.xgboost import XGBoostTrainer
from ray.ml.preprocessors import StandardScaler
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from sklearn.datasets import fetch_covtype


def get_training_data() -> ray.data.Dataset:
    data_raw = fetch_covtype()
    df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    df["target"] = data_raw["target"]
    return ray.data.from_pandas(df)


train_dataset = get_training_data()


# XGBoost specific params
params = {
    "tree_method": "approx",
    "objective": "multi:softmax",
    "eval_metric": ["mlogloss", "merror"],
    "num_class": 8,
}

# Let's tune the preprocessors
prep_v1 = StandardScaler(columns=[f"Wilderness_Area_{i}" for i in range(4)])
prep_v2 = StandardScaler(columns=[f"Soil_Type_{i}" for i in range(40)])

trainer = XGBoostTrainer(
    scaling_config={
        "num_workers": 2,
    },
    label_column="target",
    params=params,
    datasets={"train": train_dataset},
    num_boost_round=10,
)

tuner = Tuner(
    trainer,
    run_config=RunConfig(verbose=3),
    param_space={
        "params": {"max_depth": tune.randint(2, 8)},
        "preprocessor": tune.grid_search([prep_v1, prep_v2]),
    },
    tune_config=TuneConfig(num_samples=4, metric="train-mlogloss", mode="min"),
)

results = tuner.fit()

# This will fetch the best result according to the `metric` and `mode` specified
# in the `TuneConfig` above:

best_result = results.get_best_result()

print("Best result error rate", best_result.metrics["train-merror"])
