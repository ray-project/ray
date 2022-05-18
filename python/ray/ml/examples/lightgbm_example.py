import argparse
import math
from typing import Tuple

import pandas as pd

import ray
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.lightgbm import LightGBMPredictor
from ray.ml.preprocessors.chain import Chain
from ray.ml.preprocessors.encoder import Categorizer
from ray.ml.train.integrations.lightgbm import LightGBMTrainer
from ray.data.dataset import Dataset
from ray.ml.result import Result
from ray.ml.preprocessors import StandardScaler
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split


def prepare_data() -> Tuple[Dataset, Dataset, Dataset]:
    data_raw = load_breast_cancer()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    # add a random categorical column
    num_samples = len(dataset_df)
    dataset_df["categorical_column"] = pd.Series(
        (["A", "B"] * math.ceil(num_samples / 2))[:num_samples]
    )
    train_df, test_df = train_test_split(dataset_df, test_size=0.3)
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    test_dataset = ray.data.from_pandas(test_df.drop("target", axis=1))
    return train_dataset, valid_dataset, test_dataset


def train_lightgbm(num_workers: int, use_gpu: bool = False) -> Result:
    train_dataset, valid_dataset, _ = prepare_data()

    # Scale some random columns, and categorify the categorical_column,
    # allowing LightGBM to use its built-in categorical feature support
    columns_to_scale = ["mean radius", "mean texture"]
    preprocessor = Chain(
        Categorizer(["categorical_column"]), StandardScaler(columns=columns_to_scale)
    )

    # LightGBM specific params
    params = {
        "objective": "binary",
        "metric": ["binary_logloss", "binary_error"],
    }

    trainer = LightGBMTrainer(
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


def predict_lightgbm(result: Result):
    _, _, test_dataset = prepare_data()
    batch_predictor = BatchPredictor.from_checkpoint(
        result.checkpoint, LightGBMPredictor
    )

    predicted_labels = (
        batch_predictor.predict(test_dataset)
        .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
        .to_pandas(limit=float("inf"))
    )
    print(f"PREDICTED LABELS\n{predicted_labels}")

    shap_values = batch_predictor.predict(test_dataset, pred_contrib=True).to_pandas(
        limit=float("inf")
    )
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
    result = train_lightgbm(num_workers=args.num_workers, use_gpu=args.use_gpu)
    predict_lightgbm(result)
