import ray

from ray.train.sklearn import SklearnTrainer
from sklearn.ensemble import RandomForestRegressor

train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
trainer = SklearnTrainer(
    estimator=RandomForestRegressor(),
    label_column="y",
    scaling_config=ray.air.config.ScalingConfig(trainer_resources={"CPU": 4}),
    datasets={"train": train_dataset},
)
result = trainer.fit()
