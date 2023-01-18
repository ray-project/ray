# flake8: noqa
# isort: skip_file

# __train_predict_start__
import numpy as np
import ray

from ray.train.xgboost import XGBoostTrainer, XGBoostPredictor
from ray.air.config import ScalingConfig

train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
trainer = XGBoostTrainer(
    label_column="y",
    params={"objective": "reg:squarederror"},
    scaling_config=ScalingConfig(num_workers=3),
    datasets={"train": train_dataset},
)
result = trainer.fit()

predictor = XGBoostPredictor.from_checkpoint(result.checkpoint)
predictions = predictor.predict(np.expand_dims(np.arange(32, 64), 1))
# __train_predict_end__

# __batch_predict_start__
import pandas as pd
from ray.train.batch_predictor import BatchPredictor

batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, XGBoostPredictor)
predict_dataset = ray.data.from_pandas(pd.DataFrame({"x": np.arange(32)}))
predictions = batch_predictor.predict(
    data=predict_dataset,
    batch_size=8,
    min_scoring_workers=2,
)
# __batch_predict_end__
