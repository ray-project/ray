# flake8: noqa

# fmt: off
# __stopping_example_trainable_start__
from ray.air import session


def my_trainable(config):
    i = 1
    # Training won't finish unless one of the stopping criteria is met!
    while True:
        # Do some training, and report some metrics for demonstration...
        session.report({"mean_accuracy": min(i / 10, 1.0)})
        i += 1
# __stopping_example_trainable_end__
# fmt: on


# __stopping_dict_start__
from ray import air, tune

tuner = tune.Tuner(
    my_trainable,
    run_config=air.RunConfig(stop={"training_iteration": 10, "mean_accuracy": 0.8}),
)
result_grid = tuner.fit()
# __stopping_dict_end__

final_iter = result_grid[0].metrics["training_iteration"]
assert final_iter == 8, final_iter

# __stopping_fn_start__
from ray import air, tune


def stop_fn(trial_id: str, result: dict) -> bool:
    return result["mean_accuracy"] >= 0.8 or result["training_iteration"] >= 10


tuner = tune.Tuner(my_trainable, run_config=air.RunConfig(stop=stop_fn))
result_grid = tuner.fit()
# __stopping_fn_end__

final_iter = result_grid[0].metrics["training_iteration"]
assert final_iter == 8, final_iter

# __stopping_cls_start__
from ray import air, tune
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
    run_config=air.RunConfig(stop=stopper),
    tune_config=tune.TuneConfig(num_samples=2),
)
result_grid = tuner.fit()
# __stopping_cls_end__

for result in result_grid:
    final_iter = result.metrics.get("training_iteration", 0)
    assert final_iter <= 8, final_iter

# __stopping_on_trial_error_start__
from ray import air, tune
from ray.air import session
import time


def my_failing_trainable(config):
    if config["should_fail"]:
        raise RuntimeError("Failing (on purpose)!")
    # Do some training...
    time.sleep(10)
    session.report({"mean_accuracy": 0.9})


tuner = tune.Tuner(
    my_failing_trainable,
    param_space={"should_fail": tune.grid_search([True, False])},
    run_config=air.RunConfig(failure_config=air.FailureConfig(fail_fast=True)),
)
result_grid = tuner.fit()
# __stopping_on_trial_error_end__

for result in result_grid:
    # Should never get to report
    final_iter = result.metrics.get("training_iteration")
    assert not final_iter, final_iter

# __early_stopping_start__
from ray import air, tune
from ray.tune.schedulers import AsyncHyperBandScheduler


scheduler = AsyncHyperBandScheduler(time_attr="training_iteration")

tuner = tune.Tuner(
    my_trainable,
    run_config=air.RunConfig(stop={"training_iteration": 10}),
    tune_config=tune.TuneConfig(
        scheduler=scheduler, num_samples=2, metric="mean_accuracy", mode="max"
    ),
)
result_grid = tuner.fit()
# __early_stopping_end__
