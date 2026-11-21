import os
import sys
from collections import Counter

import pytest

import ray
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal.actor_manager import RayActorManager
from ray.exceptions import OutOfMemoryError
from ray.train.tests.util import mock_storage_context
from ray.tune import CheckpointConfig, PlacementGroupFactory, TuneError
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.registry import TRAINABLE_CLASS, _global_registry
from ray.tune.schedulers import FIFOScheduler
from ray.tune.search import BasicVariantGenerator
from ray.tune.tests.execution.utils import BudgetResourceManager
from ray.tune.utils.mock_trainable import MOCK_TRAINABLE_NAME, register_mock_trainable

STORAGE = mock_storage_context()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.fixture(autouse=True)
def register_test_trainable():
    register_mock_trainable()


def create_mock_components():
    class _MockScheduler(FIFOScheduler):
        errored_trials = []

        def on_trial_error(self, tune_controller, trial):
            self.errored_trials += [trial]

    class _MockSearchAlg(BasicVariantGenerator):
        errored_trials = []

        def on_trial_complete(self, trial_id, error=False, **kwargs):
            if error:
                self.errored_trials += [trial_id]

    searchalg = _MockSearchAlg()
    scheduler = _MockScheduler()
    return searchalg, scheduler


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_invalid_trainable(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """An invalid trainable should make the trial fail on startup.

    The controller itself should continue. Other trials should run.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testErrorHandling
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(), storage=STORAGE
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 1},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "storage": STORAGE,
        "config": {"sleep": 0.5},
    }
    _global_registry.register(TRAINABLE_CLASS, "asdf", None)
    trials = [Trial("asdf", **kwargs), Trial(MOCK_TRAINABLE_NAME, **kwargs)]
    for t in trials:
        runner.add_trial(t)

    while not trials[1].status == Trial.RUNNING:
        runner.step()
    assert trials[0].status == Trial.ERROR
    assert trials[1].status == Trial.RUNNING


def test_overstep(ray_start_4_cpus_2_gpus_extra):
    """Stepping when trials are finished should raise a TuneError.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testThrowOnOverstep
    """
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"
    runner = TuneController(
        resource_manager_factory=lambda: BudgetResourceManager({"CPU": 4}),
        storage=STORAGE,
    )
    runner.step()
    with pytest.raises(TuneError):
        runner.step()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("max_failures_persistent", [(0, False), (1, False), (2, True)])
def test_failure_recovery(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, max_failures_persistent
):
    """Test failure recover with `max_failures`.

    Trials should be retried up to `max_failures` times.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testFailureRecoveryDisabled
    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testFailureRecoveryEnabled
    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testFailureRecoveryMaxFailures
    """
    max_failures, persistent_error = max_failures_persistent
    searchalg, scheduler = create_mock_components()

    runner = TuneController(
        search_alg=searchalg,
        scheduler=scheduler,
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
    )
    kwargs = {
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "stopping_criterion": {"training_iteration": 2},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        "max_failures": max_failures,
        "config": {"mock_error": True, "persistent_error": persistent_error},
        "storage": STORAGE,
    }
    runner.add_trial(Trial(MOCK_TRAINABLE_NAME, **kwargs))
    trials = runner.get_trials()

    while not runner.is_finished():
        runner.step()

    if persistent_error or not max_failures:
        assert trials[0].status == Trial.ERROR

        num_failures = max_failures + 1
        assert trials[0].num_failures == num_failures
        # search alg receives on_complete, so only after the max failures
        # have been exhausted. Thus, it only has errored_trials if the
        # trial fails even in the last try.
        assert len(searchalg.errored_trials) == 1
        # search alg receives on_error, so every failure is registered.
        assert len(scheduler.errored_trials) == num_failures
    else:
        assert trials[0].status == Trial.TERMINATED
        assert trials[0].num_failures == 1
        assert len(searchalg.errored_trials) == 0
        assert len(scheduler.errored_trials) == 1


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("max_failures", [0, 1])
def test_oom_kill_is_a_trial_failure(
    ray_start_4_cpus_2_gpus_extra, monkeypatch, resource_manager_cls, max_failures
):
    """An out-of-memory kill of a trial's worker is a trial failure.

    The trial is retried up to `max_failures` times. The controller keeps running
    instead of raising, and the trial ends in ERROR once it runs out of retries.
    """
    searchalg, scheduler = create_mock_components()

    # Fail the first train future with the error the memory monitor leads to.
    oom_futures = []
    scheduled_train = []
    real_schedule = RayActorManager.schedule_actor_task
    real_get = ray.get

    def schedule_actor_task(self, *args, **kwargs):
        if kwargs.get("method_name") in ("train", "train_buffered") and not (
            scheduled_train
        ):
            scheduled_train.append(True)
            requested = kwargs.get("_return_future", False)
            kwargs["_return_future"] = True
            future = real_schedule(self, *args, **kwargs)
            oom_futures.append(future)
            return future if requested else None
        return real_schedule(self, *args, **kwargs)

    def get(refs, *args, **kwargs):
        if isinstance(refs, ray.ObjectRef) and refs in oom_futures:
            oom_futures.remove(refs)
            raise OutOfMemoryError("Worker killed by the memory monitor")
        return real_get(refs, *args, **kwargs)

    monkeypatch.setattr(RayActorManager, "schedule_actor_task", schedule_actor_task)
    monkeypatch.setattr(ray, "get", get)

    runner = TuneController(
        search_alg=searchalg,
        scheduler=scheduler,
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
    )
    runner.add_trial(
        Trial(
            MOCK_TRAINABLE_NAME,
            placement_group_factory=PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
            stopping_criterion={"training_iteration": 2},
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            max_failures=max_failures,
            storage=STORAGE,
        )
    )
    trial = runner.get_trials()[0]

    while not runner.is_finished():
        runner.step()

    assert trial.num_failures == 1
    assert len(scheduler.errored_trials) == 1
    if max_failures:
        assert trial.status == Trial.TERMINATED
        assert len(searchalg.errored_trials) == 0
    else:
        assert trial.status == Trial.ERROR
        assert len(searchalg.errored_trials) == 1


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("fail_fast", [True, TuneController.RAISE])
def test_fail_fast(ray_start_4_cpus_2_gpus_extra, resource_manager_cls, fail_fast):
    """Test fail_fast feature.

    If fail_fast=True, after the first failure, all other trials should be terminated
    (because we end the experiment).

    If fail_fast=RAISE, after the first failure, we should raise an error.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testFailFast
    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testFailFastRaise
    """

    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        fail_fast=fail_fast,
        storage=STORAGE,
    )
    kwargs = {
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        "max_failures": 0,
        "config": {
            "mock_error": True,
            "persistent_error": True,
        },
        "storage": STORAGE,
    }
    runner.add_trial(Trial(MOCK_TRAINABLE_NAME, **kwargs))
    runner.add_trial(Trial(MOCK_TRAINABLE_NAME, **kwargs))
    trials = runner.get_trials()

    if fail_fast == TuneController.RAISE:
        with pytest.raises(Exception):
            while not runner.is_finished():
                runner.step()
        runner.cleanup()
        return
    else:
        while not runner.is_finished():
            runner.step()

    status_count = Counter(t.status for t in trials)

    # One trial failed
    assert status_count.get(Trial.ERROR) == 1
    # The other one was pre-empted
    assert status_count.get(Trial.TERMINATED) == 1

    # Controller finished
    with pytest.raises(TuneError):
        runner.step()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
