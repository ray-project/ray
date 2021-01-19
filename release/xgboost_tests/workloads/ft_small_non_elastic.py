"""Fault tolerance test (small cluster, non-elastic training)

In this run, two training actors will die after some time. It is expected that
in both cases xgboost_ray stops training, restarts the dead actors, and
continues training with all four actors.

Test owner: krfricke

Acceptance criteria: Should run through and report final results. Intermediate
output should show that training halts wenn an actor dies and continues only
when all four actors are available again.

Notes: This test seems to be somewhat flaky. This might be due to
race conditions in handling dead actors. This is likely a problem of
the xgboost_ray implementation and not of this test.
"""
import os
import time

import ray
from xgboost_ray import RayParams
from xgboost_ray.session import get_actor_rank

from xgboost.callback import TrainingCallback

from _train import train_ray


@ray.remote
class FailureState:
    def __init__(self):
        self._failed_ranks = set()

    def set_failed(self, rank):
        self._failed_ranks.add(rank)

    def has_failed(self, rank):
        return rank in self._failed_ranks


class FailureInjection(TrainingCallback):
    def __init__(self, state, ranks, iteration):
        self._state = state
        self._ranks = ranks or []
        self._iteration = iteration
        super(FailureInjection).__init__()

    def after_iteration(self, model, epoch, evals_log):
        if epoch == self._iteration:
            rank = get_actor_rank()
            if rank in self._ranks:
                if not ray.get(self._state.has_failed.remote(rank)):
                    ray.get(self._state.set_failed.remote(rank))

                    pid = os.getpid()
                    print(f"Killing process: {pid} for actor rank {rank}")
                    time.sleep(1)
                    os.kill(pid, 9)


if __name__ == "__main__":
    ray.init(address="auto")

    failure_state = FailureState.remote()

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
            FailureInjection(state=failure_state, ranks=[3], iteration=14),
            FailureInjection(state=failure_state, ranks=[0], iteration=34),
        ])
