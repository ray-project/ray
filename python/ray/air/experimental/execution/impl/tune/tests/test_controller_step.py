import pytest
from ray.air.experimental.execution.impl.tune.tests.common import tune_setup
from ray.rllib import _register_all
from ray.tune.experiment import Trial


def test_stopping_criterion(ray_start_local):
    """Tests that we step multiple times.

    Legacy test: test_trial_runner::TrialRunnerTest::testMultiStepRun
    Legacy test: test_trial_runner::TrialRunnerTest::testMultiStepRun2

    - Cluster has 4 CPUs, 2 GPUs
    - Start a number of trials, with stopping criterion 5 iterations
    - Run them
    - Assert that they all ran for five iterations
    """
    resource_manager, search_alg, scheduler, trial_states, controller = tune_setup()
    _register_all()

    for i in range(4):
        search_alg.add_trial(
            Trial(
                "_inf_iter",
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


def test_no_step(ray_start_local):
    """If we have no trials we should raise an error on step().

    Legacy test: test_trial_runner_2::TrialRunnerTest2::testThrowOnOverstep
    """
    resource_manager, search_alg, scheduler, trial_states, controller = tune_setup()
    with pytest.raises(RuntimeError):
        controller.step()


def test_continue_experiment_on_trial_error(ray_start_local):
    """If one trial fails, we should continue the rest of the trials still.

    Legacy test: test_trial_runner_2::TrialRunnerTest2::testErrorHandling
    """
    resource_manager, search_alg, scheduler, trial_states, controller = tune_setup()
    search_alg.add_trial(
        Trial(
            "_failing",
        )
    )
    search_alg.add_trial(
        Trial(
            "_one_iter",
        )
    )

    controller.step_until_finished()

    assert controller.trials[0].status == Trial.ERROR
    assert controller.trials[1].status == Trial.TERMINATED


def test_max_failures(ray_start_local):
    """If max failures are reached, we stop retries.

    - Start a failing trial
    - Set max_failures to `max_failures`
    - Assert that we only get `max_failures` retries and then stop.

    Legacy test: test_trial_runner_2::TrialRunnerTest2::testFailureRecoveryDisabled
    """
    resource_manager, search_alg, scheduler, trial_states, controller = tune_setup()
    max_failures = 2
    search_alg.add_trial(Trial("_failing", max_failures=max_failures))

    controller.step_until_finished()

    assert controller.trials[0].status == Trial.ERROR
    # trial.max_failures is the number of failures, so the number of total tries
    # is max_failures + 1
    assert controller.trials[0].num_failures == max_failures + 1

    assert len(search_alg.errored_trials) == 1
    assert len(scheduler.errored_trials) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
