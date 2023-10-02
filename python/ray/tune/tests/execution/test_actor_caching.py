import pytest
import sys

from ray.tune import PlacementGroupFactory

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


def test_actor_reuse_unstaged(tmpdir):
    """A trial that hasn't been staged can re-use an actor.

    In specific circumstances, this can lead to errors. Notably, when an
    external source (e.g. a scheduler) directly calls TuneController APIs,
    we can be in a situation where a trial has not been staged, but there is
    still an actor available for it to use (because it hasn't been evicted from
    the cache, yet).

    This test constructs such a situation an asserts that actor re-use does not
    lead to errors in those cases.
    """
    tune_controller, actor_manger, resource_manager = create_execution_test_objects(
        tmpdir, max_pending_trials=1
    )
    tune_controller._reuse_actors = True

    assert not actor_manger.added_actors

    trialA1 = TestingTrial(
        "trainable1",
        stub=True,
        trial_id="trialA1",
        experiment_path=str(tmpdir),
        placement_group_factory=PlacementGroupFactory([{"CPU": 1}]),
    )
    tune_controller.add_trial(trialA1)
    trialB1 = TestingTrial(
        "trainable1",
        stub=True,
        trial_id="trialB1",
        experiment_path=str(tmpdir),
        placement_group_factory=PlacementGroupFactory([{"CPU": 5}]),
    )
    tune_controller.add_trial(trialB1)
    trialA2 = TestingTrial(
        "trainable1",
        stub=True,
        trial_id="trialA2",
        experiment_path=str(tmpdir),
        placement_group_factory=PlacementGroupFactory([{"CPU": 1}]),
    )
    tune_controller.add_trial(trialA2)
    tune_controller.step()

    # Prevent trial A3 from being staged by setting the number
    # of pending actors to the maximum allowed
    actor_manger.set_num_pending(2)

    trialA3 = TestingTrial(
        "trainable1",
        stub=True,
        trial_id="trialA3",
        experiment_path=str(tmpdir),
        placement_group_factory=PlacementGroupFactory([{"CPU": 1}]),
    )
    tune_controller.add_trial(trialA3)
    tune_controller.step()

    tracked_actorA1, _, _ = actor_manger.added_actors[0]
    tracked_actorB1, _, _ = actor_manger.added_actors[1]
    tracked_actorA2, _, _ = actor_manger.added_actors[2]

    # Start trial A1, report that it's done training.
    # This will cache the actor for A1 as A2 is already scheduled.
    tune_controller._actor_started(tracked_actorA1)
    tune_controller._on_training_result(trialA1, {"done": True})

    # Trial A2 should be in the staged trials. A3 should still not be staged.
    assert trialA2 in tune_controller._staged_trials
    assert trialA3 not in tune_controller._staged_trials

    # The actor of A1 should be cached for re-use now.
    assert tune_controller._actor_cache.num_cached_objects == 1

    # In the meantime, actor A2 started. This will unstage it.
    tune_controller._actor_started(tracked_actorA2)

    # Now, an external source (e.g. the BOHB scheduler) wants to prematurely
    # stop trial A2. This will leave the cached actor intact, but trial A3
    # is still not scheduled.
    tune_controller._schedule_trial_stop(trialA2)
    assert tune_controller._actor_cache.num_cached_objects == 1

    # Process events. This will invoke "path 3" in TuneController._maybe_add_actors
    # and re-use the cached actor
    tune_controller.step()

    # Reset future scheduled
    assert actor_manger.scheduled_futures[-1][2] == "reset"

    # Prior to https://github.com/ray-project/ray/pull/36951, there was a bug here:
    # Because trial A3 was never staged, the unstage ran into an error.
    # This fails without the line: self._staged_trials.add(start_trial)
    tune_controller._on_trial_reset(trialA3, True)

    # When the actor finally stops, the cache size is adjusted and the actor is
    # evicted. This test failed without the line:
    # self._actor_cache.increase_max(start_trial.placement_group_factory)
    tune_controller._actor_stopped(tracked_actorA1)
    tune_controller.step()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
