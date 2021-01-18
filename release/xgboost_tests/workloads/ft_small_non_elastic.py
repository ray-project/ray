import os
import time

import ray
from xgboost_ray import RayParams
from xgboost_ray.session import get_actor_rank

from xgboost.callback import TrainingCallback

from _train import train_ray


class FailureInjection(TrainingCallback):
    def __init__(self, ranks, iteration, path="/tmp/failure_actor_"):
        self._ranks = ranks or []
        self._iteration = iteration
        self._path = path
        super(FailureInjection).__init__()

    def after_iteration(self, model, epoch, evals_log):
        if epoch == self._iteration:
            rank = get_actor_rank()
            if rank in self._ranks:
                actor_fail_file = f"{self._path}{rank}.lock"

                if not os.path.exists(actor_fail_file):
                    with open(actor_fail_file, "wt") as fp:
                        fp.write("1")

                    pid = os.getpid()
                    print(f"Killing process: {pid} for actor rank {rank}")
                    time.sleep(1)
                    os.kill(pid, 9)


if __name__ == "__main__":
    ray.init(address="auto")

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=0)

    train_ray(
        path="/data/classification.parquet",
        num_workers=4,
        num_boost_rounds=100,
        num_files=25,
        regression=False,
        use_gpu=False,
        ray_params=ray_params,
        xgboost_params=None,
        callbacks=[
            FailureInjection(ranks=[3], iteration=14),
            FailureInjection(ranks=[0], iteration=34),
        ])
