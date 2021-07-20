import argparse

import os
import json
import os
import time

import dask
import dask.dataframe as dd
import ray
from ray.util.dask import ray_dask_get
from xgboost_ray import RayDMatrix, RayParams, train

FILE_S3_URI = "s3://ray-ci-higgs/simpleHIGGS.csv"

def is_anyscale_connect():
    """Returns whether or not the Ray Address points to an Anyscale cluster."""
    address = os.environ.get("RAY_ADDRESS")
    is_anyscale_connect = address is not None and address.startswith(
        "anyscale://")
    return is_anyscale_connect


def main():
    print("Loading HIGGS data.")

    dask.config.set(scheduler=ray_dask_get)
    colnames = ["label"] + ["feature-%02d" % i for i in range(1, 29)]
    data = dd.read_csv(FILE_S3_URI, names=colnames)
    if args.smoke_test:
        data = data.head(n=1000)

    print("Loaded HIGGS data.")

    # partition on a column
    df_train = data[(data["feature-01"] < 0.4)]
    # df_train = df_train.persist()
    df_validation = data[(data["feature-01"] >= 0.4)
                         & (data["feature-01"] < 0.8)]
    # df_validation = df_validation.persist()

    dtrain = RayDMatrix(df_train, label="label", columns=colnames)
    dvalidation = RayDMatrix(df_validation, label="label")

    evallist = [(dvalidation, "eval")]
    evals_result = {}
    config = {"tree_method": "hist", "eval_metric": ["logloss", "error"]}
    train(
        params=config,
        dtrain=dtrain,
        evals_result=evals_result,
        ray_params=RayParams(
            max_actor_restarts=1, num_actors=4, cpus_per_actor=2),
        num_boost_round=100,
        evals=evallist)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Finish quickly for testing.")
    args = parser.parse_args()

    start = time.time()

    client_builder = ray.client()
    if is_anyscale_connect():
        job_name = os.environ.get("RAY_JOB_NAME", "dask_xgboost_test")
        client_builder.job_name(job_name)
    client_builder.connect()

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
