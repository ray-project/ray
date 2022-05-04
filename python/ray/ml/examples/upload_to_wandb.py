# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.6
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Experiment tracking and model upload with Weights & Biases
import ray

from ray.ml import RunConfig
from ray.ml.result import Result
from ray.ml.train.integrations.xgboost import XGBoostTrainer
from ray.tune.integration.wandb import WandbLoggerCallback
from sklearn.datasets import load_breast_cancer


def get_train_dataset() -> ray.data.Dataset:
    data_raw = load_breast_cancer(as_frame=True)
    df = data_raw["data"]
    df["target"] = data_raw["target"]
    return ray.data.from_pandas(df)


def train_model(train_dataset: ray.data.Dataset, wandb_project: str) -> Result:
    trainer = XGBoostTrainer(
        scaling_config={"num_workers": 2},
        params={"tree_method": "auto"},
        label_column="target",
        datasets={"train": train_dataset},
        num_boost_round=10,
        run_config=RunConfig(
            callbacks=[
                WandbLoggerCallback(
                    project=wandb_project,
                    save_checkpoints=True,
                )
            ]
        ),
    )
    result = trainer.fit()
    return result


wandb_project = "ray_air_example"

train_dataset = get_train_dataset()
result = train_model(train_dataset=train_dataset, wandb_project=wandb_project)
