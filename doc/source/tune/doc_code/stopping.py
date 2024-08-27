# flake8: noqa

# fmt: off
# __stopping_example_trainable_start__
from ray import train


def my_trainable(config):
    i = 1
    while True:
        # Do some training...
        time.sleep(1)

        # Report some metrics for demonstration...
        train.report({"mean_accuracy": min(i / 10, 1.0)})
        i += 1
# __stopping_example_trainable_end__
# fmt: on


def my_trainable(config):
    # NOTE: This re-defines the training loop with the sleep removed for faster testing.
    i = 1
    # Training won't finish unless one of the stopping criteria is met!
    while True:
        # Do some training, and report some metrics for demonstration...
        train.report({"mean_accuracy": min(i / 10, 1.0)})
        i += 1


# __stopping_dict_start__
from ray import train, tune

tuner = tune.Tuner(
    my_trainable,
    run_config=train.RunConfig(stop={"training_iteration": 10, "mean_accuracy": 0.8}),
)
result_grid = tuner.fit()
# __stopping_dict_end__

final_iter = result_grid[0].metrics["training_iteration"]
assert final_iter == 8, final_iter

# __stopping_fn_start__
from ray import train, tune


def stop_fn(trial_id: str, result: dict) -> bool:
    return result["mean_accuracy"] >= 0.8 or result["training_iteration"] >= 10


tuner = tune.Tuner(my_trainable, run_config=train.RunConfig(stop=stop_fn))
result_grid = tuner.fit()
# __stopping_fn_end__

final_iter = result_grid[0].metrics["training_iteration"]
assert final_iter == 8, final_iter

# __stopping_cls_start__
from ray import train, tune
from ray.tune import Stopper


class CustomStopper(Stopper):
    def __init__(self):
        self.should_stop = False

    def __call__(self, trial_id: str, result: dict) -> bool:
        if not self.should_stop and result["mean_accuracy"] >= 0.8:
            self.should_stop = True
        return self.should_stop

    def stop_all(self) -> bool:
        """Returns whether to stop trials and prevent new ones from starting."""
        return self.should_stop


stopper = CustomStopper()
tuner = tune.Tuner(
    my_trainable,
    run_config=train.RunConfig(stop=stopper),
    tune_config=tune.TuneConfig(num_samples=2),
)
result_grid = tuner.fit()
# __stopping_cls_end__

for result in result_grid:
    final_iter = result.metrics.get("training_iteration", 0)
    assert final_iter <= 8, final_iter

# __stopping_on_trial_error_start__
from ray import train, tune
import time


def my_failing_trainable(config):
    if config["should_fail"]:
        raise RuntimeError("Failing (on purpose)!")
    # Do some training...
    time.sleep(10)
    train.report({"mean_accuracy": 0.9})


tuner = tune.Tuner(
    my_failing_trainable,
    param_space={"should_fail": tune.grid_search([True, False])},
    run_config=train.RunConfig(failure_config=train.FailureConfig(fail_fast=True)),
)
result_grid = tuner.fit()
# __stopping_on_trial_error_end__

for result in result_grid:
    # Should never get to report
    final_iter = result.metrics.get("training_iteration")
    assert not final_iter, final_iter

# __early_stopping_start__
from ray import train, tune
from ray.tune.schedulers import AsyncHyperBandScheduler


scheduler = AsyncHyperBandScheduler(time_attr="training_iteration")

tuner = tune.Tuner(
    my_trainable,
    run_config=train.RunConfig(stop={"training_iteration": 10}),
    tune_config=tune.TuneConfig(
        scheduler=scheduler, num_samples=2, metric="mean_accuracy", mode="max"
    ),
)
result_grid = tuner.fit()
# __early_stopping_end__


def my_trainable(config):
    # NOTE: Introduce the sleep again for the time-based unit-tests.
    i = 1
    while True:
        time.sleep(1)
        # Do some training, and report some metrics for demonstration...
        train.report({"mean_accuracy": min(i / 10, 1.0)})
        i += 1


# __stopping_trials_by_time_start__
from ray import train, tune

tuner = tune.Tuner(
    my_trainable,
    # Stop a trial after it's run for more than 5 seconds.
    run_config=train.RunConfig(stop={"time_total_s": 5}),
)
result_grid = tuner.fit()
# __stopping_trials_by_time_end__

# Should only get ~5 reports
assert result_grid[0].metrics["training_iteration"] < 8


# __stopping_experiment_by_time_start__
from ray import tune

# Stop the entire experiment after ANY trial has run for more than 5 seconds.
tuner = tune.Tuner(my_trainable, tune_config=tune.TuneConfig(time_budget_s=5.0))
result_grid = tuner.fit()
# __stopping_experiment_by_time_end__

# Should only get ~5 reports
assert result_grid[0].metrics["training_iteration"] < 8
