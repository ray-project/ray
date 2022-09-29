import pytest
import ray
from ray.air.execution.impl.tune.tests.common import (
    SimpleSearchAlgorithm,
    TrialStateCallback,
)
from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.tune import PlacementGroupFactory, register_trainable
from ray.tune.experiment import Trial
from ray.tune.trainable import wrap_function


@pytest.fixture
def ray_start_local():
    address_info = ray.init(local_mode=True, num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def tune_setup():
    resource_manager = FixedResourceManager(total_resources={"CPU": 4, "GPU": 2})
    search_alg = SimpleSearchAlgorithm()
    trial_states = TrialStateCallback()
    controller = TuneController(
        search_alg=search_alg,
        resource_manager=resource_manager,
        callbacks=[trial_states],
    )

    yield resource_manager, search_alg, trial_states, controller


def _empty_train_fn(config):
    return 1


_empty_train_class = wrap_function(_empty_train_fn)


def test_parallel_trials_extra_resources(ray_start_local, tune_setup):
    """Tests that extra resources are accounted for in trial parallelism.

    Legacy test: test_trial_runner::TrialRunnerTest::testExtraResources

    - Cluster has 4 CPUs, 2 GPUs
    - Start a number of trials each requesting 2 CPUs and 1 GPU, in different bundles
    - Run them
    - Assert that maximum of parallel running trials is 2 (limited by GPU in extra)
    """
    resource_manager, search_alg, trial_states, controller = tune_setup
    register_trainable("_empty_train_class", _empty_train_class)

    for i in range(10):
        search_alg.add_trial(
            Trial(
                "_empty_train_class",
                placement_group_factory=PlacementGroupFactory(
                    [{"CPU": 1}, {"CPU": 1, "GPU": 1}]
                ),
            )
        )

    controller.step_until_finished()

    assert trial_states.max_running_trials() == 2
    assert trial_states.max_pending_trials() >= 8
    assert trial_states.all_trials_terminated()


def test_parallel_trials_custom_resources(ray_start_local, tune_setup):
    """Tests that custom resources are accounted for in trial parallelism.

    Legacy test: test_trial_runner::TrialRunnerTest::testCustomResources
    Legacy test: test_trial_runner::TrialRunnerTest::testExtraCustomResources

    - Cluster has 4 CPUs, 2 GPUs, 4 'a'
    - Start a number of trials each requesting 1 CPUs and 2 'a'
    - Run them
    - Assert that maximum of parallel running trials is 2 (limited by 'a')
    """
    resource_manager, search_alg, trial_states, controller = tune_setup

    resource_manager._total_resources["a"] = 4

    register_trainable("_empty_train_class", _empty_train_class)

    for i in range(10):
        search_alg.add_trial(
            Trial(
                "_empty_train_class",
                placement_group_factory=PlacementGroupFactory([{"CPU": 1}, {"a": 2}]),
            )
        )

    controller.step_until_finished()

    assert trial_states.max_running_trials() == 2
    assert trial_states.max_pending_trials() >= 8
    assert trial_states.all_trials_terminated()


def test_parallel_trials_fractional_gpus(ray_start_local, tune_setup):
    """Tests that fractional GPUs are working.

    Legacy test: test_trial_runner::TrialRunnerTest::testFractionalGpus

    - Cluster has 4 CPUs, 2 GPUs
    - Start a number of trials each requesting 1 CPUs and 0.5 GPUs
    - Run them
    - Assert that maximum of parallel running trials is 4
    """
    resource_manager, search_alg, trial_states, controller = tune_setup

    register_trainable("_empty_train_class", _empty_train_class)

    for i in range(10):
        search_alg.add_trial(
            Trial(
                "_empty_train_class",
                placement_group_factory=PlacementGroupFactory([{"CPU": 1, "GPU": 0.5}]),
            )
        )

    controller.step_until_finished()

    assert trial_states.max_running_trials() == 4
    assert trial_states.max_pending_trials() >= 6
    assert trial_states.all_trials_terminated()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
