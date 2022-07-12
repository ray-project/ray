from functools import wraps
import json
from multiprocessing import Process, Queue
import os
import time
import xgboost as xgb

import ray
from ray import data
from ray.train.xgboost import (
    XGBoostTrainer,
    load_checkpoint,
    to_air_checkpoint,
    XGBoostPredictor,
)
from ray.train.batch_predictor import BatchPredictor

_XGB_MODEL_PATH = "model.json"


def run_in_separate_process(f):
    """Runs f in a separate process.

    Note: f should take "queue" as a kwarg and communicates
    its result through the queue.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        q = Queue()
        p = Process(target=f, args=args, kwargs={"queue": q})
        p.start()
        p.join()
        return q.get()

    return wrapper


def time_it_in_separate_process(f):
    """Times f in a separate process and
    sends its result and the time take through a queue."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        q = kwargs.pop("queue")
        assert q
        start = time.monotonic()
        result = f(*args, **kwargs)
        time_taken = time.monotonic() - start
        print(f"{f.__name__} takes {time_taken} seconds.")
        q.put((result, time_taken))

    return wrapper


@run_in_separate_process
@time_it_in_separate_process
def run_xgboost_training():
    ds = data.read_parquet(
        "s3://air-example-data-2/100G-xgboost-data.parquet/"
    )  # silver tier
    params = {
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }

    trainer = XGBoostTrainer(
        scaling_config={"num_workers": 10, "resources_per_worker": {"CPU": 12}},
        label_column="labels",
        params=params,
        datasets={"train": ds},
    )
    result = trainer.fit()
    return result


@run_in_separate_process
@time_it_in_separate_process
def run_xgboost_prediction(model_path: str):
    model = xgb.Booster()
    model.load_model(model_path)
    ds = data.read_parquet(
        "s3://air-example-data-2/100G-xgboost-data.parquet/"
    )  # silver tier
    ckpt = to_air_checkpoint(".", model)
    batch_predictor = BatchPredictor.from_checkpoint(ckpt, XGBoostPredictor)
    result = batch_predictor.predict(ds.drop_columns(["labels"]))
    return result


def main():
    print("Running xgboost training benchmark...")
    result, training_time = run_xgboost_training()
    xgboost_model = load_checkpoint(result.checkpoint)[0]
    xgboost_model.save_model(_XGB_MODEL_PATH)
    ray.shutdown()
    print("Running xgboost prediction benchmark...")
    _, prediction_time = run_xgboost_prediction(_XGB_MODEL_PATH)
    result = {
        "training_time": training_time,
        "prediction_time": prediction_time,
    }
    print("Results:", result)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()
