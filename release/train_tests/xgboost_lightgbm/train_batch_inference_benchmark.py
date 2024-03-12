import json
import numpy as np
import os
import pandas as pd
import time
from typing import Dict

import xgboost as xgb

import ray
from ray import data
from ray.train.lightgbm import LightGBMTrainer
from ray.train.xgboost import XGBoostTrainer
from ray.train import RunConfig, ScalingConfig

_TRAINING_TIME_THRESHOLD = 600
_PREDICTION_TIME_THRESHOLD = 450

_EXPERIMENT_PARAMS = {
    "smoke_test": {
        "data": (
            "https://air-example-data-2.s3.us-west-2.amazonaws.com/"
            "10G-xgboost-data.parquet/8034b2644a1d426d9be3bbfa78673dfa_000000.parquet"
        ),
        "num_workers": 1,
        "cpus_per_worker": 1,
    },
    "10G": {
        "data": "s3://air-example-data-2/10G-xgboost-data.parquet/",
        "num_workers": 1,
        "cpus_per_worker": 12,
    },
    "100G": {
        "data": "s3://air-example-data-2/100G-xgboost-data.parquet/",
        "num_workers": 10,
        "cpus_per_worker": 12,
    },
}


class BasePredictor:
    def __init__(self, trainer_cls, result: ray.train.Result):
        self.model = trainer_cls.get_model(result.checkpoint)

    def __call__(self, data):
        raise NotImplementedError


class XGBoostPredictor(BasePredictor):
    def __call__(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        dmatrix = xgb.DMatrix(data)
        return {"predictions": self.model.predict(dmatrix)}


class LightGBMPredictor(BasePredictor):
    def __call__(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        return {"predictions": self.model.predict(data)}


_FRAMEWORK_PARAMS = {
    "xgboost": {
        "trainer_cls": XGBoostTrainer,
        "predictor_cls": XGBoostPredictor,
        "params": {
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error"],
        },
    },
    "lightgbm": {
        "trainer_cls": LightGBMTrainer,
        "predictor_cls": LightGBMPredictor,
        "params": {
            "objective": "binary",
            "metric": ["binary_logloss", "binary_error"],
        },
    },
}


def train(
    framework: str, data_path: str, num_workers: int, cpus_per_worker: int
) -> ray.train.Result:
    ds = data.read_parquet(data_path)
    framework_params = _FRAMEWORK_PARAMS[framework]

    trainer_cls = framework_params["trainer_cls"]

    trainer = trainer_cls(
        params=framework_params["params"],
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            resources_per_worker={"CPU": cpus_per_worker},
            trainer_resources={"CPU": 0},
        ),
        label_column="labels",
        datasets={"train": ds},
        run_config=RunConfig(
            storage_path="/mnt/cluster_storage", name=f"{framework}_benchmark"
        ),
    )
    result = trainer.fit()
    return result


def predict(framework: str, result: ray.train.Result, data_path: str):
    framework_params = _FRAMEWORK_PARAMS[framework]

    predictor_cls = framework_params["predictor_cls"]

    ds = data.read_parquet(data_path)
    ds = ds.drop_columns(["labels"])

    concurrency = int(ray.cluster_resources()["CPU"] // 2)
    result = ds.map_batches(
        predictor_cls,
        # Improve prediction throughput with larger batch size than default 4096
        batch_size=8192,
        concurrency=concurrency,
        fn_constructor_kwargs={
            "trainer_cls": framework_params["trainer_cls"],
            "result": result,
        },
        batch_format="pandas",
    )

    for _ in result.iter_batches():
        pass


def main(args):
    framework = args.framework

    experiment = args.size if not args.smoke_test else "smoke_test"
    experiment_params = _EXPERIMENT_PARAMS[experiment]

    data_path, num_workers, cpus_per_worker = (
        experiment_params["data"],
        experiment_params["num_workers"],
        experiment_params["cpus_per_worker"],
    )

    print(f"Running {framework} training benchmark...")
    training_start = time.perf_counter()
    result = train(framework, data_path, num_workers, cpus_per_worker)
    training_time = time.perf_counter() - training_start

    print(f"Running {framework} prediction benchmark...")
    prediction_start = time.perf_counter()
    predict(framework, result, data_path)
    prediction_time = time.perf_counter() - prediction_start

    times = {"training_time": training_time, "prediction_time": prediction_time}
    print("Training result:\n", result)
    print("Training/prediction times:", times)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(times, f)

    if not args.disable_check:
        if training_time > _TRAINING_TIME_THRESHOLD:
            raise RuntimeError(
                f"Training is taking {training_time} seconds, "
                f"which is longer than expected ({_TRAINING_TIME_THRESHOLD} seconds)."
            )

        if prediction_time > _PREDICTION_TIME_THRESHOLD:
            raise RuntimeError(
                f"Batch prediction is taking {prediction_time} seconds, "
                f"which is longer than expected ({_PREDICTION_TIME_THRESHOLD} seconds)."
            )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "framework", type=str, choices=["xgboost", "lightgbm"], default="xgboost"
    )
    parser.add_argument("--size", type=str, choices=["10G", "100G"], default="100G")
    # Add a flag for disabling the timeout error.
    # Use case: running the benchmark as a documented example, in infra settings
    # different from the formal benchmark's EC2 setup.
    parser.add_argument(
        "--disable-check",
        action="store_true",
        help="disable runtime error on benchmark timeout",
    )
    parser.add_argument("--smoke-test", action="store_true")
    args = parser.parse_args()
    main(args)
