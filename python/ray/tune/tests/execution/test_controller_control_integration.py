import sys
from collections import Counter

import pytest

import ray
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.train.tests.util import mock_storage_context
from ray.tune import PlacementGroupFactory, register_trainable
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.utils.mock_trainable import MOCK_TRAINABLE_NAME, register_mock_trainable

STORAGE = mock_storage_context()


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

    register_mock_trainable()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(), storage=STORAGE
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 10},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 2, "GPU": 1}]),
        "config": {"sleep": 1},
        "storage": STORAGE,
    }
    trials = [
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
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
        resource_manager_factory=lambda: resource_manager_cls(),
        reuse_actors=True,
        storage=STORAGE,
    )

    def train_fn(config):
        return 1

    register_trainable("test_remove_actor_tracking", train_fn)

    kwargs = {
        "placement_group_factory": PlacementGroupFactory([{"CPU": 4, "GPU": 2}]),
        "storage": STORAGE,
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


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_pause_trial_during_buffered_results(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls
):
    """A scheduler pausing a trial while a batch of buffered results is processed
    should not lead to a queued CONTINUE being executed on the removed actor.

    This is what PBT does when it exploits a trial. Previously, the CONTINUE
    decision queued for the earlier result was executed after the pause and
    raised a ``KeyError`` in ``_schedule_trial_task``, and the remaining
    buffered results were still passed to the scheduler.

    Regression test for https://github.com/ray-project/ray/issues/58483
    """

    class PauseOnSecondResult(FIFOScheduler):
        def __init__(self):
            super().__init__()
            self.seen_iterations = []

        def on_trial_result(self, tune_controller, trial, result):
            self.seen_iterations.append(result["training_iteration"])
            if result["training_iteration"] == 2:
                # Same as PBT's exploit step.
                tune_controller.pause_trial(trial, should_checkpoint=False)
                return TrialScheduler.NOOP
            return TrialScheduler.CONTINUE

    register_mock_trainable()
    scheduler = PauseOnSecondResult()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        scheduler=scheduler,
        storage=STORAGE,
    )
    trial = Trial(
        MOCK_TRAINABLE_NAME,
        stopping_criterion={"training_iteration": 10},
        storage=STORAGE,
    )
    runner.add_trial(trial)

    while trial.status != Trial.RUNNING:
        runner.step()

    # A batch of results, as returned by ``Trainable.train_buffered``.
    runner._on_training_result(trial, [{"training_iteration": i} for i in range(1, 4)])

    assert trial.status == Trial.PAUSED
    assert trial not in runner._trial_to_actor
    assert trial.trial_id not in runner._queued_trial_decisions
    # Results after the pause are ignored.
    assert scheduler.seen_iterations == [1, 2]

    # The paused trial can be resumed.
    while trial.status != Trial.RUNNING:
        runner.step()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "--reruns", "3", __file__]))
