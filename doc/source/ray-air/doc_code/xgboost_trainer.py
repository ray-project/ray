import ray

from ray.train.xgboost import XGBoostTrainer
from ray.air.config import ScalingConfig

train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
trainer = XGBoostTrainer(
    label_column="y",
    params={"objective": "reg:squarederror"},
    scaling_config=ScalingConfig(num_workers=3),
    datasets={"train": train_dataset},
)
result = trainer.fit()
