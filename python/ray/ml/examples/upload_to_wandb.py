"""

```
pip install -U wandb wandbfs
```


"""
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


def train_model(train_dataset: ray.data.Dataset) -> Result:
    trainer = XGBoostTrainer(
        scaling_config={
            "num_workers": 2,
        },
        params={"tree_method": "auto"},
        label_column="target",
        datasets={"train": train_dataset},
        num_boost_round=10,
        run_config=RunConfig(
            callbacks=[
                WandbLoggerCallback(project="Wandb_example", save_checkpoints=True)
            ]
        ),
    )
    result = trainer.fit()
    return result


train_dataset = get_train_dataset()
result = train_model(train_dataset=train_dataset)
