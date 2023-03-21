# flake8: noqa

# __ft_initial_run_start__
from ray import air, tune
from ray.air import Checkpoint, session


def trainable(config):
    # Checkpoint loading
    checkpoint = session.get_checkpoint()
    start = 1
    if checkpoint:
        start = checkpoint.to_dict()["epoch"] + 1

    for epoch in range(start, config["num_epochs"]):
        # Do some training...

        # Checkpoint saving
        session.report(
            {"epoch": epoch}, checkpoint=Checkpoint.from_dict({"epoch": epoch})
        )


tuner = tune.Tuner(
    trainable,
    param_space={"num_epochs": 10},
    run_config=air.RunConfig(
        local_dir="~/ray_results", name="experiment_fault_tolerance"
    ),
)
tuner.fit()
# __ft_initial_run_end__

# __ft_restored_run_start__
tuner = tune.Tuner.restore(
    path="~/ray_results/tune_fault_tolerance_guide", trainable=trainable
)
tuner.fit()
# __ft_restored_run_end__

# __ft_restore_multiplexing_start__
import os
from ray import air, tune

local_dir = "~/ray_results"
exp_name = "tune_fault_tolerance_guide"
path = os.path.join(local_dir, exp_name)

if tune.Tuner.can_restore(path):
    tuner = tune.Tuner.restore(path, trainable=trainable)
else:
    tuner = tune.Tuner(
        trainable,
        param_space={"num_epochs": 10},
        run_config=air.RunConfig(
            local_dir="~/ray_results", name="single_script_fit_or_restore"
        ),
    )
tuner.fit()
# __ft_restore_multiplexing_start__

# __ft_trial_failure_start__
from ray import air, tune

tuner = tune.Tuner(
    trainable,
    param_space={"num_epochs": 10},
    run_config=air.RunConfig(
        local_dir="~/ray_results",
        name="trial_fault_tolerance",
        failure_config=air.FailureConfig(max_failures=3),
    ),
)
tuner.fit()
# __ft_trial_failure_end__
