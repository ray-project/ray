# __air_preprocessors_start__
import ray
import pandas as pd
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

from ray.ml.preprocessors import *

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
from ray.ml.train.integrations.xgboost import XGBoostTrainer

num_workers = 4
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
    num_boost_round=20,
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
    param_space={
        "params": {
            "max_depth": tune.randint(1, 9)
        }
    },
    tune_config=TuneConfig(num_samples=20, metric="train-logloss", mode="min"),
)
result_grid = tuner.fit()
best_result = result_grid.get_best_result()
print(best_result)
# __air_tuner_end__

# __air_batch_predictor_start__

batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint, XGBoostPredictor
)

predicted_labels = (
    batch_predictor.predict(test_dataset)
    .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
    .to_pandas(limit=float("inf"))
)

# __air_batch_predictor_end__

# __air_deploy_start__
from ray import serve
from ray.serve import ModelWrapperDeployment

serve.start(detached=True)
deployment = ModelWrapperDeployment.options(name=name)
deployment.deploy(RLPredictor, checkpoint)
print(deployment.url)
# __air_deploy_end__

# __air_inference_start__

# __air_inference_end__
