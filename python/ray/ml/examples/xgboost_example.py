import argparse
from typing import Tuple

import pandas as pd

import ray
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictors.integrations.xgboost import XGBoostPredictor
from ray.ml.train.integrations.xgboost import XGBoostTrainer
from ray.data.dataset import Dataset
from ray.ml.result import Result
from ray.ml.preprocessors import StandardScaler
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split


def prepare_data() -> Tuple[Dataset, Dataset, Dataset]:
    data_raw = load_breast_cancer()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    train_df, test_df = train_test_split(dataset_df, test_size=0.3)
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    test_dataset = ray.data.from_pandas(test_df.drop("target", axis=1))
    return train_dataset, valid_dataset, test_dataset


def train_xgboost(num_workers: int, use_gpu: bool = False) -> Result:
    train_dataset, valid_dataset, _ = prepare_data()

    # Scale some random columns
    columns_to_scale = ["mean radius", "mean texture"]
    preprocessor = StandardScaler(columns=columns_to_scale)

    # XGBoost specific params
    params = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
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
        num_boost_round=100,
    )
    result = trainer.fit()
    print(result.metrics)

    return result


def predict_xgboost(result: Result):
    _, _, test_dataset = prepare_data()
    checkpoint_object_ref = result.checkpoint.to_object_ref()

    class XGBoostScorer:
        def __init__(self):
            self.predictor = XGBoostPredictor.from_checkpoint(
                Checkpoint.from_object_ref(checkpoint_object_ref)
            )

        def __call__(self, batch) -> pd.DataFrame:
            return self.predictor.predict(batch)

    predicted_labels = (
        test_dataset.map_batches(XGBoostScorer, compute="actors", batch_format="pandas")
        .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
        .to_pandas(limit=float("inf"))
    )
    print(f"PREDICTED LABELS\n{predicted_labels}")

    class XGBoostScorerSHAP(XGBoostScorer):
        def __call__(self, batch) -> pd.DataFrame:
            return self.predictor.predict(batch, pred_contribs=True)

    shap_values = test_dataset.map_batches(
        XGBoostScorerSHAP, compute="actors", batch_format="pandas"
    ).to_pandas(limit=float("inf"))
    print(f"SHAP VALUES\n{shap_values}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    args, _ = parser.parse_known_args()

    ray.init(address=args.address)
    result = train_xgboost(num_workers=args.num_workers, use_gpu=args.use_gpu)
    predict_xgboost(result)
