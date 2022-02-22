"""Fault tolerance test (small cluster, non-elastic training)

In this run, two training actors will die after some time. It is expected that
in both cases xgboost_ray stops training, restarts the dead actors, and
continues training with all four actors.

Test owner: krfricke

Acceptance criteria: Should run through and report final results. Intermediate
output should show that training halts wenn an actor dies and continues only
when all four actors are available again. The test will fail if fault
tolerance did not work correctly.

Notes: This test seems to be somewhat flaky. This might be due to
race conditions in handling dead actors. This is likely a problem of
the xgboost_ray implementation and not of this test.
"""
import ray

from xgboost_ray import RayParams


from ray.util.xgboost.release_test_util import (
    train_ray,
    FailureState,
    FailureInjection,
    TrackingCallback,
)

if __name__ == "__main__":
    ray.init(address="auto")

    failure_state = FailureState.remote()

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=0,
    )

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
    assert len(actor_1_world_size) == 1 and 4 in actor_1_world_size, (
        "Training with fewer than 4 actors observed, but this was "
        "non-elastic training. Please report to test owner."
    )

    print("PASSED.")
