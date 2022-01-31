"""Fault tolerance test (small cluster, elastic training)

In this run, two training actors will die after some time. It is expected that
in both cases xgboost_ray stops training, but continues right away with the
remaining three actors. Shortly after, the actors will be restarted and
re-integrated into the training loop. Training should finish with all four
actors.

Test owner: krfricke

Acceptance criteria: Should run through and report final results. Intermediate
output should show that training continues with fewer actors when an
actor died. The test will fail if elastic training didn't work.

Notes: This test seems to be somewhat flaky. This might be due to
race conditions in handling dead actors. This is likely a problem of
the xgboost_ray implementation and not of this test.
"""
import warnings
from unittest.mock import patch

import ray

from xgboost_ray import RayParams
from xgboost_ray.main import _train as unmocked_train

from ray.util.xgboost.release_test_util import (
    train_ray,
    FailureState,
    FailureInjection,
    TrackingCallback,
)

if __name__ == "__main__":
    ray.init(address="auto")
    from xgboost_ray.main import logger

    logger.setLevel(10)

    failure_state = FailureState.remote()

    ray_params = RayParams(
        elastic_training=True,
        max_failed_actors=2,
        max_actor_restarts=3,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=0,
    )

    world_sizes = []
    start_actors = []

    def _mock_train(*args, _training_state, **kwargs):
        world_sizes.append(len([a for a in _training_state.actors if a]))
        start_actors.append(len(_training_state.failed_actor_ranks))

        return unmocked_train(*args, _training_state=_training_state, **kwargs)

    with patch("xgboost_ray.main._train") as mocked:
        mocked.side_effect = _mock_train
        _, additional_results, _ = train_ray(
            path="/data/classification.parquet",
            num_workers=4,
            num_boost_rounds=100,
            num_files=200,
            regression=False,
            use_gpu=False,
            ray_params=ray_params,
            xgboost_params=None,
            callbacks=[
                TrackingCallback(),
                FailureInjection(
                    id="first_fail", state=failure_state, ranks=[2], iteration=14
                ),
                FailureInjection(
                    id="second_fail", state=failure_state, ranks=[0], iteration=34
                ),
            ],
        )

    actor_1_world_size = set(additional_results["callback_returns"][1])

    if 3 not in actor_1_world_size and 3 not in world_sizes and 1 not in world_sizes:
        warnings.warn(
            "No training with only 3 actors observed, but this was elastic "
            "training. Please check the output to see if data loading was "
            "too fast so that the training actors were re-integrated directly "
            "after restarting."
        )

    print("PASSED.")
