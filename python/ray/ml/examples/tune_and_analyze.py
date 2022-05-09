import pandas as pd
import ray
from ray import tune
from ray.ml import RunConfig
from ray.ml.train.integrations.xgboost import XGBoostTrainer
from ray.ml.preprocessors import StandardScaler
from ray.tune.tuner import Tuner
from sklearn.datasets import fetch_lfw_pairs


def get_training_data() -> ray.data.Dataset:
    data_raw = fetch_lfw_pairs()
    df = pd.DataFrame(
        data_raw["data"], columns=[f"col{i}" for i in range(len(data_raw["data"][0]))]
    )
    df["target"] = data_raw["target"]
    return ray.data.from_pandas(df)


train_dataset = get_training_data()


# XGBoost specific params
params = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
}

# Let's tune the preprocessors
prep_v1 = StandardScaler(columns=[f"col{i}" for i in range(100)])
prep_v2 = StandardScaler(columns=[f"col{i}" for i in range(100, 200)])

trainer = XGBoostTrainer(
    scaling_config={
        "num_workers": 2,
    },
    label_column="target",
    params=params,
    datasets={"train": train_dataset},
    num_boost_round=20,
)

tuner = Tuner(
    trainer,
    run_config=RunConfig(verbose=3),
    param_space={
        # "scaling_config": {
        #     "num_workers": tune.choice([2,]),
        # },
        "params": {"max_depth": tune.randint(4, 10)},
        "preprocessor": tune.grid_search([prep_v1, prep_v2]),
    },
)

results = tuner.fit()
print(results)
