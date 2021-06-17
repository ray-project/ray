import argparse
import os

import modin.pandas as pd
import ray
from xgboost_ray import RayDMatrix, RayParams, train

FILE_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/" \
           "00280/HIGGS.csv.gz"


@ray.remote
def test():
    print("Loading HIGGS data.")

    colnames = ["label"] + ["feature-%02d" % i for i in range(1, 29)]

    if args.smoke_test:
        data = pd.read_csv(FILE_URL, names=colnames, nrows=1000)
    else:
        data = pd.read_csv(FILE_URL, names=colnames)

    print("Loaded HIGGS data.")

    # partition on a column
    df_train = data[(data["feature-01"] < 0.4)]
    df_validation = data[(data["feature-01"] >= 0.4)
                         & (data["feature-01"] < 0.8)]

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

    address = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "modin_xgboost_test")
    if address.startswith("anyscale://"):
        ray.client(address=address).job_name(job_name).connect()
    else:
        ray.init(address="auto")

    ray.get(test.remote())
