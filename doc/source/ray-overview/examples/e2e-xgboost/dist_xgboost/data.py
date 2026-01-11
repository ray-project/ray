import os
import pickle
import shutil
from tempfile import TemporaryDirectory
from typing import Tuple
from urllib.parse import urlparse

import mlflow
import ray
from ray.data import Dataset
from ray.train import Checkpoint
from ray.train.xgboost import RayTrainReportCallback

from dist_xgboost.constants import (
    experiment_name,
    model_fname,
    model_registry,
    preprocessor_fname,
)


def prepare_data() -> Tuple[Dataset, Dataset, Dataset]:
    """Load and split the dataset into train, validation, and test sets."""
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
    seed = 42
    train_dataset, rest = dataset.train_test_split(
        test_size=0.3, shuffle=True, seed=seed
    )
    # 15% for validation, 15% for testing
    valid_dataset, test_dataset = rest.train_test_split(
        test_size=0.5, shuffle=True, seed=seed
    )
    return train_dataset, valid_dataset, test_dataset


def get_best_model_from_registry():
    mlflow.set_tracking_uri(f"file:{model_registry}")
    sorted_runs = mlflow.search_runs(
        experiment_names=[experiment_name], order_by=["metrics.validation_error ASC"]
    )
    best_run = sorted_runs.iloc[0]
    best_artifacts_dir = urlparse(best_run.artifact_uri).path
    return best_run, best_artifacts_dir


def load_model_and_preprocessor():
    best_run, best_artifacts_dir = get_best_model_from_registry()

    # load the preprocessor
    with open(os.path.join(best_artifacts_dir, preprocessor_fname), "rb") as f:
        preprocessor = pickle.load(f)

    # load the model
    checkpoint = Checkpoint.from_directory(best_artifacts_dir)
    model = RayTrainReportCallback.get_model(checkpoint)

    return preprocessor, model


def clean_up_old_runs():
    # clean up old MLFlow runs
    os.path.isdir(model_registry) and shutil.rmtree(model_registry)
    mlflow.delete_experiment(experiment_name)
    os.makedirs(model_registry, exist_ok=True)


def log_run_to_mlflow(model_config, result, preprocessor_path):
    # create a model registry in our user storage
    mlflow.set_tracking_uri(f"file:{model_registry}")

    # create a new experiment and log metrics and artifacts
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(
        description="xgboost breast cancer classifier on all features"
    ):
        mlflow.log_params(model_config)
        mlflow.log_metrics(result.metrics)

        # Selectively log just the preprocessor and model weights
        with TemporaryDirectory() as tmp_dir:
            shutil.copy(
                os.path.join(result.checkpoint.path, model_fname),
                os.path.join(tmp_dir, model_fname),
            )
            shutil.copy(
                preprocessor_path,
                os.path.join(tmp_dir, preprocessor_fname),
            )

            mlflow.log_artifacts(tmp_dir)
