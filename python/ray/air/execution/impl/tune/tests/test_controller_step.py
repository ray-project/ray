import pytest
import ray
from ray.air.execution.impl.tune.tests.common import (
    SimpleSearchAlgorithm,
    TrialStateCallback,
)
from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.rllib import _register_all
from ray.tune.experiment import Trial
from ray.tune.trainable import wrap_function


@pytest.fixture
def ray_start_local():
    address_info = ray.init(
        local_mode=True, num_cpus=4, num_gpus=2, include_dashboard=False
    )
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


def test_stopping_criterion(ray_start_local, tune_setup):
    """Tests that we step multiple times

    Legacy test: test_trial_runner::TrialRunnerTest::testMultiStepRun
    Legacy test: test_trial_runner::TrialRunnerTest::testMultiStepRun2

    - Cluster has 4 CPUs, 2 GPUs
    - Start a number of trials, with stopping criterion 5 iterations
    - Run them
    - Assert that they all ran for five iterations
    """
    resource_manager, search_alg, trial_states, controller = tune_setup
    _register_all()

    for i in range(4):
        search_alg.add_trial(
            Trial(
                "__fake",
                stopping_criterion={"training_iteration": 5},
            )
        )

    controller.step_until_finished()

    assert trial_states.max_running_trials() == 4
    assert trial_states.all_trials_terminated()

    assert [t.last_result["training_iteration"] for t in controller._all_trials] == [
        5,
        5,
        5,
        5,
    ]

    # test_trial_runner::TrialRunnerTest::testMultiStepRun2: Overstepping raises error
    with pytest.raises(RuntimeError):
        controller.step()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
