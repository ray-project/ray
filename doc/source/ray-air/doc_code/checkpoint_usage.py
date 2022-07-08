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
from ray.air import train_test_split

from ray.data.preprocessors import *

dataset = ray.data.read_csv("s3://air-example-data/breast_cancer.csv")
# Split data into train and validation.
train_dataset, valid_dataset = train_test_split(dataset, test_size=0.3)

# Create a test dataset by dropping the target column.
test_dataset = valid_dataset.map_batches(
    lambda df: df.drop("target", axis=1), batch_format="pandas"
)

# __use_trainer_checkpoint_start__
from ray.train.xgboost import XGBoostTrainer

trainer = XGBoostTrainer(
    scaling_config={"num_workers": 2},
    label_column="target",
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    },
    datasets={"train": train_dataset},
    num_boost_round=5,
)

result = trainer.fit()
checkpoint = result.checkpoint
# __use_trainer_checkpoint_end__

# __batch_pred_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.xgboost import XGBoostPredictor

batch_predictor = BatchPredictor.from_checkpoint(checkpoint, XGBoostPredictor)

# Bulk batch prediction.
batch_predictor.predict(test_dataset)
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

# Save checkpoint to a directory on the file system.
path = checkpoint.to_directory()

# This path can then be passed around, e.g. to a different function or a different script.

# At another function or script, recover Checkpoint object from path
checkpoint = Checkpoint.from_directory(path)

# Convert into dictionary again
recovered_data = checkpoint.to_dict()

# It is guaranteed that the original data has been recovered
assert recovered_data == checkpoint_data
# __basic_checkpoint_end__

