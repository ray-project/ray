# flake8: noqa
# isort: skip_file

# __air_preprocessors_start__
import ray
import pandas as pd
from sklearn.datasets import load_breast_cancer

from ray.data.preprocessors import *

# Split data into train and validation.
dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)
test_dataset = valid_dataset.drop_columns(["target"])

columns_to_scale = ["mean radius", "mean texture"]
preprocessor = StandardScaler(columns=columns_to_scale)
# __air_preprocessors_end__

# __air_trainer_start__
from ray.train.xgboost import XGBoostTrainer
from ray.air.config import ScalingConfig

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
    scaling_config=ScalingConfig(
        num_workers=num_workers,
        use_gpu=use_gpu,
    ),
    label_column="target",
    params=params,
    datasets={"train": train_dataset, "valid": valid_dataset},
    preprocessor=preprocessor,
    num_boost_round=5,
)

result = trainer.fit()
# __air_trainer_end__

# __air_trainer_output_start__
print(result.metrics)
print(result.checkpoint)
# __air_trainer_output_end__

# __air_tuner_start__
from ray import tune
from ray.tune.tuner import Tuner, TuneConfig

tuner = Tuner(
    trainer,
    param_space={"params": {"max_depth": tune.randint(1, 9)}},
    tune_config=TuneConfig(num_samples=5, metric="train-logloss", mode="min"),
)
result_grid = tuner.fit()
best_result = result_grid.get_best_result()
print(best_result)
# __air_tuner_end__

# __air_checkpoints_start__
checkpoint = result.checkpoint
print(checkpoint)
# Checkpoint(local_path=..../checkpoint_000005)

tuned_checkpoint = result_grid.get_best_result().checkpoint
print(tuned_checkpoint)
# Checkpoint(local_path=..../checkpoint_000005)
# __air_checkpoints_end__

# __checkpoint_adhoc_start__
from ray.train.tensorflow import TensorflowCheckpoint
import tensorflow as tf

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

checkpoint = TensorflowCheckpoint.from_model(model)
# __checkpoint_adhoc_end__


# __air_batch_predictor_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.xgboost import XGBoostPredictor

batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, XGBoostPredictor)

# Bulk batch prediction.
predicted_probabilities = batch_predictor.predict(test_dataset)

# Pipelined batch prediction: instead of processing the data in bulk, process it
# incrementally in windows of the given size.
pipeline = batch_predictor.predict_pipelined(test_dataset, bytes_per_window=1048576)
for batch in pipeline.iter_batches():
    print("Pipeline result", batch)

# __air_batch_predictor_end__

# __air_deploy_start__
from ray import serve
from fastapi import Request
from ray.serve import PredictorDeployment
from ray.serve.http_adapters import json_request


async def adapter(request: Request):
    content = await request.json()
    print(content)
    return pd.DataFrame.from_dict(content)


serve.run(
    PredictorDeployment.options(name="XGBoostService").bind(
        XGBoostPredictor, result.checkpoint, batching_params=False, http_adapter=adapter
    )
)
# __air_deploy_end__

# __air_inference_start__
import requests

sample_input = test_dataset.take(1)
sample_input = dict(sample_input[0])

output = requests.post("http://localhost:8000/", json=[sample_input]).json()
print(output)
# __air_inference_end__
