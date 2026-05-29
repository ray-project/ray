import os
import sys
import time
from collections import Counter

import pytest

import ray
from ray import tune
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.train.tests.util import mock_storage_context
from ray.tune import PlacementGroupFactory, TuneError
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import BasicVariantGenerator
from ray.tune.utils.mock import TrialStatusSnapshot, TrialStatusSnapshotTaker
from ray.tune.utils.mock_trainable import MOCK_TRAINABLE_NAME, register_mock_trainable

STORAGE = mock_storage_context()


@pytest.fixture(autouse=True)
def register_test_trainable():
    register_mock_trainable()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize(
    "bundles",
    [
        [{"CPU": 1}, {"CPU": 3, "GPU": 1}],
        [{"CPU": 1, "a": 2}],
        [{"CPU": 1}, {"a": 2}],
        [{"CPU": 1, "GPU": 1}, {"GPU": 1}],
    ],
)
def test_resource_parallelism_single(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, bundles
):
    """Test that extra and custom resources are respected for parallelism.

    We schedule two trials with resources according to the bundle. If only
    the head bundle or only CPU/GPU resources were considered, both trials
    could run in parallel.

    However, we assert that the resources in child bundles and extra resources
    are respected and only one trial runs in parallel.

    Legacy test: test_trial_runner.py::TrialRunnerTest::testExtraResources
    Legacy test: test_trial_runner.py::TrialRunnerTest::testCustomResources
    Legacy test: test_trial_runner.py::TrialRunnerTest::testExtraCustomResources
    Legacy test: test_trial_runner.py::TrialRunnerTest::testResourceScheduler
    """
    snapshot = TrialStatusSnapshot()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        callbacks=[TrialStatusSnapshotTaker(snapshot)],
        storage=STORAGE,
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 1},
        "placement_group_factory": PlacementGroupFactory(bundles),
        "storage": STORAGE,
    }
    trials = [
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
        Trial(MOCK_TRAINABLE_NAME, **kwargs),
    ]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    assert snapshot.max_running_trials() == 1
    assert snapshot.all_trials_are_terminated()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_fractional_gpus(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Test that fractional GPUs lead to more parallelism.

    We schedule four trials with 0.75 GPUs each. Since our cluster has 2 GPUs,
    we should be able to run 2 trials in parallel.

    Legacy test: test_trial_runner.py::TrialRunnerTest::testFractionalGpus
    """
    snapshot = TrialStatusSnapshot()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        callbacks=[TrialStatusSnapshotTaker(snapshot)],
        storage=STORAGE,
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 1},
        "placement_group_factory": PlacementGroupFactory([{"GPU": 0.75}]),
        "config": {
            "sleep": 1,
        },
        "storage": STORAGE,
    }
    trials = [Trial(MOCK_TRAINABLE_NAME, **kwargs) for i in range(4)]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    assert snapshot.max_running_trials() == 2
    assert snapshot.all_trials_are_terminated()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_multi_step(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Test that trials can run for more than one iteration.

    Todo (krfricke): This is not a resource test, so it should be moved.

    Legacy test: test_trial_runner.py::TrialRunnerTest::testMultiStepRun
    Legacy test: test_trial_runner.py::TrialRunnerTest::testMultiStepRun2
    """
    snapshot = TrialStatusSnapshot()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        callbacks=[TrialStatusSnapshotTaker(snapshot)],
        storage=STORAGE,
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 5},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "storage": STORAGE,
    }
    trials = [Trial(MOCK_TRAINABLE_NAME, **kwargs) for i in range(2)]
    for t in trials:
        runner.add_trial(t)

    while not runner.is_finished():
        runner.step()

    # Overstepping should throw error
    # test_trial_runner.py::TrialRunnerTest::testMultiStepRun2
    with pytest.raises(TuneError):
        runner.step()

    assert snapshot.all_trials_are_terminated()
    assert all(t.last_result["training_iteration"] == 5 for t in runner.get_trials())


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_resources_changing(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Checks that resource requirements can be changed on fly.

    Legacy test: test_trial_runner.py::TrialRunnerTest::testChangeResources
    """

    class ChangingScheduler(FIFOScheduler):
        def on_trial_result(self, tune_controller, trial, result):
            if result["training_iteration"] == 1:
                # NOTE: This is a hack to get around the new pausing logic,
                # which doesn't set the trial status to PAUSED immediately.
                orig_status = trial.status
                trial.set_status(Trial.PAUSED)
                trial.update_resources(dict(cpu=4, gpu=0))
                trial.set_status(orig_status)
                return TrialScheduler.PAUSE
            return TrialScheduler.NOOP

    scheduler = ChangingScheduler()
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        scheduler=scheduler,
        storage=STORAGE,
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 2},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 2, "GPU": 0}]),
        "storage": STORAGE,
    }
    trials = [Trial(MOCK_TRAINABLE_NAME, **kwargs)]
    for t in trials:
        runner.add_trial(t)

    while not trials[0].status == Trial.RUNNING:
        runner.step()

    assert trials[0].status == Trial.RUNNING
    assert runner._actor_manager.get_live_actors_resources().get("CPU") == 2

    with pytest.raises(ValueError):
        trials[0].update_resources(dict(cpu=4, gpu=0))

    while trials[0].status == Trial.RUNNING:
        runner.step()

    assert trials[0].status == Trial.PAUSED

    while not trials[0].status == Trial.RUNNING:
        runner.step()

    assert runner._actor_manager.get_live_actors_resources().get("CPU") == 4

    runner.step()


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_queue_filling(ray_start_4_cpus_2_gpus_extra, resource_manager_cls):
    """Checks that the trial queue is filled even if only 1 pending trial is allowed.

    Legacy test: test_trial_runner.py::TrialRunnerTest::testQueueFilling
    """
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

    def f1(config):
        for i in range(10):
            yield i
            time.sleep(1)

    tune.register_trainable("f1", f1)

    search_alg = BasicVariantGenerator()
    search_alg.add_configurations(
        {
            "foo": {
                "run": "f1",
                "num_samples": 100,
                "config": {
                    "a": tune.sample_from(lambda spec: 5.0 / 7),
                    "b": tune.sample_from(lambda spec: "long" * 40),
                },
                "resources_per_trial": {"cpu": 2},
            }
        }
    )

    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        search_alg=search_alg,
        storage=STORAGE,
    )

    while len(runner.get_trials()) < 3:
        runner.step()

    # All trials are enqueued
    assert len(runner.get_trials()) == 3

    status_count = Counter(t.status for t in runner.get_trials())
    while status_count.get(Trial.RUNNING, 0) < 2 and not runner.is_finished():
        runner.step()
        status_count = Counter(t.status for t in runner.get_trials())

    assert len(runner.get_trials()) == 3

    status_count = Counter(t.status for t in runner.get_trials())
    assert status_count.get(Trial.RUNNING) == 2
    assert status_count.get(Trial.PENDING) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "--reruns", "3", __file__]))
