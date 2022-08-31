import glob
import os
import time

import ray

from xgboost_ray import (
    train,
    RayDMatrix,
    RayFileType,
    RayDeviceQuantileDMatrix,
    RayParams,
)
from xgboost_ray.session import get_actor_rank, put_queue
from xgboost.callback import TrainingCallback
from xgboost.rabit import get_world_size

if "OMP_NUM_THREADS" in os.environ:
    del os.environ["OMP_NUM_THREADS"]


@ray.remote
class FailureState:
    def __init__(self):
        self._failed_ids = set()

    def set_failed(self, id):
        if id in self._failed_ids:
            return False
        self._failed_ids.add(id)
        return True

    def has_failed(self, id):
        return id in self._failed_ids


class FailureInjection(TrainingCallback):
    def __init__(self, id, state, ranks, iteration):
        self._id = id
        self._state = state
        self._ranks = ranks or []
        self._iteration = iteration
        super(FailureInjection).__init__()

    def after_iteration(self, model, epoch, evals_log):
        if epoch == self._iteration:
            rank = get_actor_rank()
            if rank in self._ranks:
                if not ray.get(self._state.has_failed.remote(self._id)):
                    success = ray.get(self._state.set_failed.remote(self._id))
                    if not success:
                        # Another rank is already about to fail
                        return

                    pid = os.getpid()
                    print(f"Killing process: {pid} for actor rank {rank}")
                    time.sleep(1)
                    os.kill(pid, 9)


class TrackingCallback(TrainingCallback):
    def before_iteration(self, model, epoch, evals_log):
        if get_actor_rank() == 3:
            print(f"[Rank {get_actor_rank()}] I am at iteration {epoch}")
        put_queue(get_world_size())


def get_parquet_files(path, num_files=0):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        raise ValueError(f"Path does not exist: {path}")

    files = sorted(glob.glob(f"{path}/**/*.parquet"))
    while num_files > len(files):
        files = files + files
    return files[0:num_files]


def train_ray(
    path,
    num_workers,
    num_boost_rounds,
    num_files=0,
    regression=False,
    use_gpu=False,
    ray_params=None,
    xgboost_params=None,
    **kwargs,
):
    if not isinstance(path, list):
        path = get_parquet_files(path, num_files=num_files)

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
            filetype=RayFileType.PARQUET,
        )
    else:
        dtrain = RayDMatrix(
            path,
            num_actors=num_workers,
            label="labels",
            ignore=["partition"],
            filetype=RayFileType.PARQUET,
        )

    config = {"tree_method": "hist" if not use_gpu else "gpu_hist"}

    if not regression:
        # Classification
        config.update(
            {
                "objective": "binary:logistic",
                "eval_metric": ["logloss", "error"],
            }
        )
    else:
        # Regression
        config.update(
            {
                "objective": "reg:squarederror",
                "eval_metric": ["logloss", "rmse"],
            }
        )

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
        ray_params=ray_params
        or RayParams(
            max_actor_restarts=2,
            num_actors=num_workers,
            cpus_per_actor=1,
            gpus_per_actor=1 if not use_gpu else 1,
        ),
        evals=[(dtrain, "train")],
        **kwargs,
    )
    taken = time.time() - start
    print(f"TRAIN TIME TAKEN: {taken:.2f} seconds")

    out_file = os.path.expanduser(
        "~/benchmark_{}.xgb".format("cpu" if not use_gpu else "gpu")
    )
    bst.save_model(out_file)

    print("Final training error: {:.4f}".format(evals_result["train"]["error"][-1]))
    return bst, additional_results, taken
