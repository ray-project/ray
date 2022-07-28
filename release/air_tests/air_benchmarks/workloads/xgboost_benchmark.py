from functools import wraps
import json
import multiprocessing
from multiprocessing import Process
import os
import time
import traceback
import xgboost as xgb

import ray
from ray import data
from ray.train.xgboost import (
    XGBoostTrainer,
    XGBoostCheckpoint,
    XGBoostPredictor,
)
from ray.train.batch_predictor import BatchPredictor
from ray.air.config import ScalingConfig

_XGB_MODEL_PATH = "model.json"
_TRAINING_TIME_THRESHOLD = 1000
_PREDICTION_TIME_THRESHOLD = 450

_EXPERIMENT_PARAMS = {
    "10G": {
        "data": "s3://air-example-data-2/10G-xgboost-data.parquet/",
        "num_workers": 1,
    },
    "100G": {
        "data": "s3://air-example-data-2/100G-xgboost-data.parquet/",
        "num_workers": 10,
    },
}


def run_and_time_it(f):
    """Runs f in a separate process and times it."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        class MyProcess(Process):
            def __init__(self, *args, **kwargs):
                super(MyProcess, self).__init__(*args, **kwargs)
                self._pconn, self._cconn = multiprocessing.Pipe()
                self._exception = None

            def run(self):
                try:
                    super(MyProcess, self).run()
                except Exception as e:
                    tb = traceback.format_exc()
                    print(tb)
                    self._cconn.send(e)

            @property
            def exception(self):
                if self._pconn.poll():
                    self._exception = self._pconn.recv()
                return self._exception

        p = MyProcess(target=f, args=args, kwargs=kwargs)
        start = time.monotonic()
        p.start()
        p.join()
        if p.exception:
            raise p.exception
        time_taken = time.monotonic() - start
        print(f"{f.__name__} takes {time_taken} seconds.")
        return time_taken

    return wrapper


@run_and_time_it
def run_xgboost_training(data_path: str, num_workers: int):
    ds = data.read_parquet(data_path)
    params = {
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }

    trainer = XGBoostTrainer(
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            resources_per_worker={"CPU": 12},
        ),
        label_column="labels",
        params=params,
        datasets={"train": ds},
    )
    result = trainer.fit()
    checkpoint = XGBoostCheckpoint.from_checkpoint(result.checkpoint)
    xgboost_model = checkpoint.get_model()
    xgboost_model.save_model(_XGB_MODEL_PATH)
    ray.shutdown()


@run_and_time_it
def run_xgboost_prediction(model_path: str, data_path: str):
    model = xgb.Booster()
    model.load_model(model_path)
    ds = data.read_parquet(data_path)
    ckpt = XGBoostCheckpoint.from_model(booster=model)
    batch_predictor = BatchPredictor.from_checkpoint(ckpt, XGBoostPredictor)
    result = batch_predictor.predict(ds.drop_columns(["labels"]))
    return result


def main(args):
    experiment_params = _EXPERIMENT_PARAMS[args.size]
    data_path, num_workers = experiment_params["data"], experiment_params["num_workers"]
    print("Running xgboost training benchmark...")
    training_time = run_xgboost_training(data_path, num_workers)
    print("Running xgboost prediction benchmark...")
    prediction_time = run_xgboost_prediction(_XGB_MODEL_PATH, data_path)
    result = {
        "training_time": training_time,
        "prediction_time": prediction_time,
    }
    print("Results:", result)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    if training_time > _TRAINING_TIME_THRESHOLD:
        raise RuntimeError(
            f"Training on XGBoost is taking {training_time} seconds, "
            f"which is longer than expected ({_TRAINING_TIME_THRESHOLD} seconds)."
        )

    if prediction_time > _PREDICTION_TIME_THRESHOLD:
        raise RuntimeError(
            f"Batch prediction on XGBoost is taking {prediction_time} seconds, "
            f"which is longer than expected ({_PREDICTION_TIME_THRESHOLD} seconds)."
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=str, choices=["10G", "100G"], default="100G")
    args = parser.parse_args()
    main(args)
