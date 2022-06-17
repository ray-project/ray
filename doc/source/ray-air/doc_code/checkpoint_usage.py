# __checkpoint_quick_start__
from ray.train.tensorflow import to_air_checkpoint


# This can be a trained model.
def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(1),
        ]
    )
    return model

model = build_model()

checkpoint = to_air_checkpoint(model)
# __checkpoint_quick_end__


import ray
import pandas as pd
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

from ray.data.preprocessors import *

data_raw = load_breast_cancer()
dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)
train_dataset = ray.data.from_pandas(train_df)
valid_dataset = ray.data.from_pandas(test_df)


# __use_trainer_checkpoint_start__
from ray.train.xgboost import XGBoostTrainer

num_workers = 2
use_gpu = False
# XGBoost specific params
params = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
    "max_depth": 2,
}

trainer = XGBoostTrainer(
    scaling_config={
        "num_workers": num_workers,
        "use_gpu": use_gpu,
    },
    label_column="target",
    params=params,
    datasets={"train": train_dataset, "valid": valid_dataset},
    num_boost_round=5,
)

result = trainer.fit()
print(result.metrics)
checkpoint = result.checkpoint
print(checkpoint)
# __use_trainer_checkpoint_end__

# __batch_pred_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.xgboost import XGBoostPredictor

batch_predictor = BatchPredictor.from_checkpoint(checkpoint, XGBoostPredictor)

# Bulk batch prediction.
predicted_labels = (
    batch_predictor.predict(test_dataset)
    .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
    .to_pandas(limit=float("inf"))
)
# __batch_pred_end__


# __online_inference_start__
from ray import serve
from fastapi import Request
from ray.serve.model_wrappers import ModelWrapperDeployment
from ray.serve.http_adapters import json_request


async def adapter(request: Request):
    content = await request.json()
    print(content)
    return pd.DataFrame.from_dict(content)


serve.start(detached=True)
deployment = ModelWrapperDeployment.options(name="XGBoostService")

deployment.deploy(
    XGBoostPredictor, checkpoint, batching_params=False, http_adapter=adapter
)

print(deployment.url)
# __online_inference_end__

# __basic_checkpoint_start__
from ray.air.checkpoint import Checkpoint

# Create checkpoint data dict
checkpoint_data = {"data": 123}

# Create checkpoint object from data
checkpoint = Checkpoint.from_dict(checkpoint_data)

# Save checkpoint to temporary location
path = checkpoint.to_directory()

# This path can then be passed around, e.g. to a different function

# At some other location, recover Checkpoint object from path
checkpoint = Checkpoint.from_directory(path)

# Convert into dictionary again
recovered_data = checkpoint.to_dict()

# It is guaranteed that the original data has been recovered
assert recovered_data == checkpoint_data
# __basic_checkpoint_end__

