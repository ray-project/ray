"""Fault tolerance test (small cluster, elastic training)

In this run, two training actors will die after some time. It is expected that
in both cases xgboost_ray stops training, but continues right away with the
remaining three actors. Shortly after, the actors will be restarted and
re-integrated in the training loop. Training should finish with all four
actors.

Test owner: krfricke

Acceptance criteria: Should run through and report final results. Intermediate
output should show that training continues with fewer actors when an
actor died.

Notes: This test seems to be somewhat flaky. This might be due to
race conditions in handling dead actors. This is likely a problem of
the xgboost_ray implementation and not of this test.
"""
import ray
from xgboost_ray import RayParams

from _train import train_ray
from ft_small_non_elastic import FailureState, FailureInjection

if __name__ == "__main__":
    ray.init(address="auto")

    failure_state = FailureState.remote()

    ray_params = RayParams(
        elastic_training=True,
        max_failed_actors=2,
        max_actor_restarts=3,
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
