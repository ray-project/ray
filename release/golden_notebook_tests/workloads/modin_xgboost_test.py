import argparse
import json
import os
import time

import modin.pandas as pd
import ray
from xgboost_ray import RayDMatrix, RayParams, train

from utils.utils import is_anyscale_connect

FILENAME_CSV = "HIGGS.csv.gz"


def download_higgs(target_file):
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/" \
          "00280/HIGGS.csv.gz"

    try:
        import urllib.request
    except ImportError as e:
        raise ValueError(
            f"Automatic downloading of the HIGGS dataset requires `urllib`."
            f"\nFIX THIS by running `pip install urllib` or manually "
            f"downloading the dataset from {url}.") from e

    print(f"Downloading HIGGS dataset to {target_file}")
    urllib.request.urlretrieve(url, target_file)
    return os.path.exists(target_file)


def main():
    print("Loading HIGGS data.")

    if not os.path.exists(FILENAME_CSV):
        assert download_higgs(FILENAME_CSV), \
            "Downloading of HIGGS dataset failed."
        print("HIGGS dataset downloaded.")
    else:
        print("HIGGS dataset found locally.")

    colnames = ["label"] + ["feature-%02d" % i for i in range(1, 29)]

    data = pd.read_csv(os.path.abspath(FILENAME_CSV), names=colnames)
    if args.smoke_test:
        data = data.sample(frac=0.2, random_state=1)

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

    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "modin_xgboost_test")
    if is_anyscale_connect(addr):
        ray.client(address=addr).job_name(job_name).connect()
    else:
        ray.init(address="auto")

    main()

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/modin_xgboost_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
