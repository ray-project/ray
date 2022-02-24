import glob
import os
import time

import ray

from lightgbm_ray import (
    train,
    RayDMatrix,
    RayFileType,
    RayParams,
    RayDeviceQuantileDMatrix,
)
from lightgbm_ray.tune import _TuneLGBMRank0Mixin
from lightgbm.callback import CallbackEnv

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


class FailureInjection(_TuneLGBMRank0Mixin):
    def __init__(self, id, state, ranks, iteration):
        self._id = id
        self._state = state
        self._ranks = ranks or []
        self._iteration = iteration

    def __call__(self, env: CallbackEnv):
        if env.iteration == self._iteration:
            rank = 0 if self.is_rank_0 else 1
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

    order = 2


class TrackingCallback(_TuneLGBMRank0Mixin):
    def __call__(self, env: CallbackEnv):
        if self.is_rank_0:
            print(f"[Rank 0] I am at iteration {env.iteration}")

    order = 1


def train_ray(
    path,
    num_workers,
    num_boost_rounds,
    num_files=0,
    regression=False,
    use_gpu=False,
    ray_params=None,
    lightgbm_params=None,
    **kwargs,
):
    path = os.path.expanduser(path)
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

    config = {"device": "cpu" if not use_gpu else "gpu"}

    if not regression:
        # Classification
        config.update(
            {
                "objective": "binary",
                "metric": ["binary_logloss", "binary_error"],
            }
        )
    else:
        # Regression
        config.update(
            {
                "objective": "regression",
                "metric": ["l2", "rmse"],
            }
        )

    if lightgbm_params:
        config.update(lightgbm_params)

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
            cpus_per_actor=2,
            gpus_per_actor=0 if not use_gpu else 1,
        ),
        evals=[(dtrain, "train")],
        **kwargs,
    )
    taken = time.time() - start
    print(f"TRAIN TIME TAKEN: {taken:.2f} seconds")

    out_file = os.path.expanduser(
        "~/benchmark_{}.lgbm".format("cpu" if not use_gpu else "gpu")
    )
    bst.booster_.save_model(out_file)

    print(
        "Final training error: {:.4f}".format(
            evals_result["train"]["binary_error" if not regression else "rmse"][-1]
        )
    )
    return bst, additional_results, taken
