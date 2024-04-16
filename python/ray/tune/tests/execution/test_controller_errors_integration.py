import os
import sys
from collections import Counter

import pytest

import ray
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.train import CheckpointConfig
from ray.train.tests.util import mock_storage_context
from ray.tune import PlacementGroupFactory, TuneError
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.registry import TRAINABLE_CLASS, _global_registry
from ray.tune.schedulers import FIFOScheduler
from ray.tune.search import BasicVariantGenerator
from ray.tune.tests.execution.utils import BudgetResourceManager

STORAGE = mock_storage_context()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


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
    }
    _global_registry.register(TRAINABLE_CLASS, "asdf", None)
    trials = [Trial("asdf", **kwargs), Trial("__fake", **kwargs)]
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
    runner.add_trial(Trial("__fake", **kwargs))
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
    runner.add_trial(Trial("__fake", **kwargs))
    runner.add_trial(Trial("__fake", **kwargs))
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
