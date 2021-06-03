import os

import anyscale
from xgboost_ray import RayDMatrix, RayParams, train

FILENAME_CSV = "HIGGS.csv.gz"


def download_higgs(target_file):
    import urllib.request
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/" \
          "00280/HIGGS.csv.gz"
    print(f"Downloading HIGGS dataset to {target_file}")
    urllib.request.urlretrieve(url, target_file)
    return os.path.exists(target_file)


def main():
    # import ray
    # import ray.util.client.server.server as ray_client_server
    # server = ray_client_server.serve("localhost:25555")
    # client = ray.client("localhost:25555")
    # client.connect()
    anyscale.connect()

    print("Loading HIGGS data.")

    if not os.path.exists(FILENAME_CSV):
        download_higgs(FILENAME_CSV)

    print("Reading HIGGS data.")

    colnames = ["label"] + ["feature-%02d" % i for i in range(1, 29)]
    import dask.dataframe as dd
    data = dd.read_csv(FILENAME_CSV, names=colnames)

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
    # server.stop(0)


if __name__ == "__main__":
    main()
