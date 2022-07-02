# flake8: noqa
# isort: skip_file

# __air_preprocessors_start__
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
test_dataset = ray.data.from_pandas(test_df.drop("target", axis=1))

columns_to_scale = ["mean radius", "mean texture"]
preprocessor = StandardScaler(columns=columns_to_scale)
# __air_preprocessors_end__

# __air_trainer_start__
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
from ray.serve.model_wrappers import ModelWrapperDeployment
from ray.serve.http_adapters import json_request


async def adapter(request: Request):
    content = await request.json()
    print(content)
    return pd.DataFrame.from_dict(content)


serve.start(detached=True)
deployment = ModelWrapperDeployment.options(name="XGBoostService")

deployment.deploy(
    XGBoostPredictor, result.checkpoint, batching_params=False, http_adapter=adapter
)

print(deployment.url)
# __air_deploy_end__

# __air_inference_start__
import requests

sample_input = test_dataset.take(1)
sample_input = dict(sample_input[0])

output = requests.post(deployment.url, json=[sample_input]).json()
print(output)
# __air_inference_end__
