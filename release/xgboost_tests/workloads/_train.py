import glob
import os

import time

from xgboost_ray import train, RayDMatrix, RayFileType, \
    RayDeviceQuantileDMatrix, RayParams

if "OMP_NUM_THREADS" in os.environ:
    del os.environ["OMP_NUM_THREADS"]


def train_ray(path,
              num_workers,
              num_boost_rounds,
              num_files=0,
              regression=False,
              use_gpu=False,
              ray_params=None,
              xgboost_params=None,
              **kwargs):
    if not os.path.exists(path):
        raise ValueError(f"Path does not exist: {path}")

    if num_files:
        files = sorted(glob.glob(f"{path}/**/*.parquet"))
        while num_files > len(files):
            files = files + files
        path = files[0:num_files]

    use_device_matrix = False
    if use_gpu:
        try:
            import cupy  # noqa: F401
            use_device_matrix = True
        except ImportError:
            use_device_matrix = False

    if use_device_matrix:
        dtrain = RayDeviceQuantileDMatrix(
            path,
            num_actors=num_workers,
            label="labels",
            ignore=["partition"],
            filetype=RayFileType.PARQUET)
    else:
        dtrain = RayDMatrix(
            path,
            num_actors=num_workers,
            label="labels",
            ignore=["partition"],
            filetype=RayFileType.PARQUET)

    config = {"tree_method": "hist" if not use_gpu else "gpu_hist"}

    if not regression:
        # Classification
        config.update({
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error"],
        })
    else:
        # Regression
        config.update({
            "objective": "reg:squarederror",
            "eval_metric": ["logloss", "rmse"],
        })

    if xgboost_params:
        config.update(xgboost_params)

    start = time.time()
    evals_result = {}
    additional_results = {}
    bst = train(
        config,
        dtrain,
        evals_result=evals_result,
        additional_results=additional_results,
        num_boost_round=num_boost_rounds,
        ray_params=ray_params or RayParams(
            max_actor_restarts=2,
            num_actors=num_workers,
            cpus_per_actor=1,
            gpus_per_actor=1 if not use_gpu else 1),
        evals=[(dtrain, "train")],
        **kwargs)
    taken = time.time() - start
    print(f"TRAIN TIME TAKEN: {taken:.2f} seconds")

    bst.save_model("benchmark_{}.xgb".format("cpu" if not use_gpu else "gpu"))
    print("Final training error: {:.4f}".format(
        evals_result["train"]["error"][-1]))
    return bst, additional_results, taken
