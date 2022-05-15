# flake8: noqa

# __air_xgb_preprocess_start__
import ray
from ray.ml.preprocessors import StandardScaler

import pandas as pd

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

data_raw = load_breast_cancer()
dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)
train_dataset = ray.data.from_pandas(train_df)
valid_dataset = ray.data.from_pandas(test_df)
test_dataset = ray.data.from_pandas(test_df.drop("target", axis=1))


# Create a preprocessor to scale some columns
columns_to_scale = ["mean radius", "mean texture"]
preprocessor = StandardScaler(columns=columns_to_scale)
# __air_xgb_preprocess_end__


# __air_xgb_train_start__
from ray.ml.train.integrations.xgboost import XGBoostTrainer

# XGBoost specific params
params = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
}

num_workers = 2
use_gpu = False  # use GPUs if detected.

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
print(result.metrics)
# __air_xgb_train_end__

# __air_xgb_batchpred_start__
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.xgboost import XGBoostPredictor

batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, XGBoostPredictor)

predicted_labels = (
    batch_predictor.predict(test_dataset)
    .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
    .to_pandas(limit=float("inf"))
)
print("PREDICTED LABELS")
print(f"{predicted_labels}")

shap_values = batch_predictor.predict(test_dataset, pred_contribs=True).to_pandas(
    limit=float("inf")
)
print("SHAP VALUES")
print(f"{shap_values}")
# __air_xgb_batchpred_end__
