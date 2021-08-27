import argparse
import json
import os
import time

import dask
import dask.dataframe as dd
import ray
from ray import tune

from ray.util.dask import ray_dask_get

from xgboost_ray import RayDMatrix, RayParams, train, predict

from utils.utils import is_anyscale_connect

FILE_URL = "https://ray-ci-higgs.s3.us-west-2.amazonaws.com/" \
                      "simpleHIGGS.csv"


def train_xgboost(config, train_df, test_df, target_column, ray_params):
    # distributed loading of a parquet dataset
    train_set = RayDMatrix(train_df, target_column)
    test_set = RayDMatrix(test_df, target_column)

    evals_result = {}

    start_time = time.time()
    # Train the classifier
    bst = train(
        params=config,
        dtrain=train_set,
        evals=[(test_set, "eval")],
        evals_result=evals_result,
        verbose_eval=False,
        num_boost_round=100,
        ray_params=ray_params)
    print(f"Total time taken: {time.time()-start_time}")

    model_path = "model.xgb"
    bst.save_model(model_path)
    print("Final validation error: {:.4f}".format(
        evals_result["eval"]["error"][-1]))

    return bst


def tune_xgboost(train_df, test_df, target_column):
    # Set XGBoost config.
    config = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "eta": tune.loguniform(1e-4, 1e-1),
        "subsample": tune.uniform(0.5, 1.0),
        "max_depth": tune.randint(1, 9)
    }

    ray_params = RayParams(
        max_actor_restarts=1, gpus_per_actor=0, cpus_per_actor=8, num_actors=4)

    analysis = tune.run(
        tune.with_parameters(
            train_xgboost,
            train_df=train_df,
            test_df=test_df,
            target_column=target_column,
            ray_params=ray_params),
        # Use the `get_tune_resources` helper function to set the resources.
        resources_per_trial=ray_params.get_tune_resources(),
        config=config,
        num_samples=1,
        metric="eval-error",
        mode="min",
        verbose=1)

    accuracy = 1. - analysis.best_result["eval-error"]
    print(f"Best model parameters: {analysis.best_config}")
    print(f"Best model total accuracy: {accuracy:.4f}")

    return analysis.best_config


def main():
    print("Loading HIGGS data.")

    dask.config.set(scheduler=ray_dask_get)
    colnames = ["label"] + ["feature-%02d" % i for i in range(1, 29)]
    data = dd.read_csv(FILE_URL, names=colnames)

    print("Loaded HIGGS data.")

    # partition on a column
    df_train = data[(data["feature-01"] < 0.4)]
    df_validation = data[(data["feature-01"] >= 0.4)
                         & (data["feature-01"] < 0.8)]

    config = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }

    bst = train_xgboost(
        config, df_train, df_validation, "label",
        RayParams(max_actor_restarts=1, cpus_per_actor=8, num_actors=4))
    tune_xgboost(df_train, df_validation, "label")
    inference_df = RayDMatrix(
        df_train[sorted(df_train.columns)], ignore=["label", "partition"])
    predict(
        bst,
        inference_df,
        ray_params=RayParams(cpus_per_actor=2, num_actors=16))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Finish quickly for testing.")
    args = parser.parse_args()

    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "dask_xgboost_test")
    if is_anyscale_connect(addr):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    main()

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/dask_xgboost_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
