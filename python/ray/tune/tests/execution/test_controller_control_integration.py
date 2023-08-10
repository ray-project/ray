from collections import Counter

import pytest
import sys

import ray
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.tune import PlacementGroupFactory, register_trainable
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_stop_trial(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Stopping a trial while RUNNING or PENDING should work.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testStopTrial
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 10},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 2, "GPU": 1}]),
        "config": {"sleep": 1},
    }
    trials = [
        Trial("__fake", **kwargs),
        Trial("__fake", **kwargs),
        Trial("__fake", **kwargs),
        Trial("__fake", **kwargs),
    ]
    for t in trials:
        runner.add_trial(t)

    counter = Counter(t.status for t in trials)

    # Wait until 2 trials started
    while counter.get("RUNNING", 0) != 2:
        runner.step()
        counter = Counter(t.status for t in trials)

    assert counter.get("RUNNING", 0) == 2
    assert counter.get("PENDING", 0) == 2

    # Stop trial that is running
    for trial in trials:
        if trial.status == Trial.RUNNING:
            runner._schedule_trial_stop(trial)
            break

    counter = Counter(t.status for t in trials)

    # Wait until the next trial started
    while counter.get("RUNNING", 0) < 2:
        runner.step()
        counter = Counter(t.status for t in trials)

    assert counter.get("RUNNING", 0) == 2
    assert counter.get("TERMINATED", 0) == 1
    assert counter.get("PENDING", 0) == 1

    # Stop trial that is pending
    for trial in trials:
        if trial.status == Trial.PENDING:
            runner._schedule_trial_stop(trial)
            break

    counter = Counter(t.status for t in trials)

    # Wait until 2 trials are running again
    while counter.get("RUNNING", 0) < 2:
        runner.step()
        counter = Counter(t.status for t in trials)

    assert counter.get("RUNNING", 0) == 2
    assert counter.get("TERMINATED", 0) == 2
    assert counter.get("PENDING", 0) == 0


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_remove_actor_tracking(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """When we reuse actors, actors that have been requested but not started
    should not be tracked in ``_stopping_actors``.

    When actors are re-used, we cancel original actor requests for the trial.
    If these actors haven't been alive, there won't be a stop future to be resolved,
    and thus they would remain in ``TuneController._stopping_actors`` until they
    get cleaned up after 600 seconds.

    This test asserts that these actors are not tracked in
    ``TuneController._stopping_actors`` at all.

    We start 4 actors, and one can run at a time. Actors are re-used across trials.
    When the experiment ends, we expect that only one actor is left to track
    in ``self._stopping_trials``.
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(), reuse_actors=True
    )

    def train(config):
        return 1

    register_trainable("test_remove_actor_tracking", train)

    kwargs = {
        "placement_group_factory": PlacementGroupFactory([{"CPU": 4, "GPU": 2}]),
    }
    trials = [Trial("test_remove_actor_tracking", **kwargs) for i in range(4)]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    # Only one actor should be left to stop
    assert len(runner._stopping_actors) == 1

    runner.cleanup()

    assert len(runner._stopping_actors) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "--reruns", "3", __file__]))
