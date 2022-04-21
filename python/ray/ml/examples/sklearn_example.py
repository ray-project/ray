import argparse
import math
from typing import Tuple

import pandas as pd

import ray
from ray.data.dataset import Dataset
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.sklearn import SklearnPredictor
from ray.ml.preprocessors import Chain, OrdinalEncoder, StandardScaler
from ray.ml.result import Result
from ray.ml.train.integrations.sklearn import SklearnTrainer

from sklearn.datasets import load_breast_cancer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split


try:
    from cuml.ensemble import RandomForestClassifier as cuMLRandomForestClassifier
except ImportError:
    cuMLRandomForestClassifier = None


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


def train_sklearn(num_cpus: int, use_gpu: bool = False) -> Result:
    if use_gpu and not cuMLRandomForestClassifier:
        raise RuntimeError("cuML must be installed for GPU enabled sklearn estimators.")

    train_dataset, valid_dataset, _ = prepare_data()

    # Scale some random columns
    columns_to_scale = ["mean radius", "mean texture"]
    preprocessor = Chain(
        OrdinalEncoder(["categorical_column"]), StandardScaler(columns=columns_to_scale)
    )

    if use_gpu:
        trainer_resources = {"CPU": 1, "GPU": 1}
        estimator = cuMLRandomForestClassifier()
    else:
        trainer_resources = {"CPU": num_cpus}
        estimator = RandomForestClassifier()

    trainer = SklearnTrainer(
        estimator=estimator,
        label_column="target",
        datasets={"train": train_dataset, "valid": valid_dataset},
        preprocessor=preprocessor,
        cv=5,
        scaling_config={
            "trainer_resources": trainer_resources,
        },
    )
    result = trainer.fit()
    print(result.metrics)

    return result


def predict_sklearn(result: Result, use_gpu: bool = False):
    _, _, test_dataset = prepare_data()

    batch_predictor = BatchPredictor.from_checkpoint(
        result.checkpoint, SklearnPredictor
    )

    predicted_labels = (
        batch_predictor.predict(
            test_dataset,
            num_gpus_per_worker=int(use_gpu),
        )
        .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
        .to_pandas(limit=float("inf"))
    )
    print(f"PREDICTED LABELS\n{predicted_labels}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-cpus",
        "-n",
        type=int,
        default=2,
        help="Sets number of CPUs used for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    args, _ = parser.parse_known_args()

    ray.init(address=args.address)
    result = train_sklearn(num_cpus=args.num_cpus, use_gpu=args.use_gpu)
    predict_sklearn(result, use_gpu=args.use_gpu)
