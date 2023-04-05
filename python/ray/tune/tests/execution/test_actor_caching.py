import pytest
import sys
from ray.tune.tests.execution.utils import create_execution_test_objects, TestingTrial


def test_actor_cached(tmpdir):
    tune_controller, actor_manger, resource_manager = create_execution_test_objects(
        tmpdir, max_pending_trials=8
    )

    assert not actor_manger.added_actors

    tune_controller.add_trial(
        TestingTrial(
            "trainable1", stub=True, trial_id="trial1", experiment_path=str(tmpdir)
        )
    )
    tune_controller.step()

    tracked_actor, cls_name, kwargs = actor_manger.added_actors[0]
    assert cls_name == "trainable1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
