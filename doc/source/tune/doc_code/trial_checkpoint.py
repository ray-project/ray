# flake8: noqa

# __class_api_checkpointing_start__
import os
import torch
from torch import nn

from ray import tune


class MyTrainableClass(tune.Trainable):
    def setup(self, config):
        self.model = nn.Sequential(
            nn.Linear(config.get("input_size", 32), 32), nn.ReLU(), nn.Linear(32, 10)
        )

    def step(self):
        return {}

    def save_checkpoint(self, tmp_checkpoint_dir):
        checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return tmp_checkpoint_dir

    def load_checkpoint(self, tmp_checkpoint_dir):
        checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
        self.model.load_state_dict(torch.load(checkpoint_path))


tuner = tune.Tuner(
    MyTrainableClass,
    param_space={"input_size": 64},
    run_config=tune.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=tune.CheckpointConfig(checkpoint_frequency=2),
    ),
)
tuner.fit()
# __class_api_checkpointing_end__

# __class_api_manual_checkpointing_start__
import random


# to be implemented by user.
def detect_instance_preemption():
    choice = random.randint(1, 100)
    # simulating a 1% chance of preemption.
    return choice <= 1


def train_func(self):
    # training code
    result = {"mean_accuracy": "my_accuracy"}
    if detect_instance_preemption():
        result.update(should_checkpoint=True)
    return result


# __class_api_manual_checkpointing_end__

# __class_api_periodic_checkpointing_start__

tuner = tune.Tuner(
    MyTrainableClass,
    run_config=tune.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=tune.CheckpointConfig(checkpoint_frequency=10),
    ),
)
tuner.fit()

# __class_api_periodic_checkpointing_end__


# __class_api_end_checkpointing_start__
tuner = tune.Tuner(
    MyTrainableClass,
    run_config=tune.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=tune.CheckpointConfig(
            checkpoint_frequency=10, checkpoint_at_end=True
        ),
    ),
)
tuner.fit()

# __class_api_end_checkpointing_end__


class MyModel:
    def state_dict(self) -> dict:
        return {}

    def load_state_dict(self, state_dict):
        pass


# __function_api_checkpointing_from_dir_start__
import os
import tempfile

from ray import tune
from ray.tune import Checkpoint


def train_func(config):
    start = 1
    my_model = MyModel()

    checkpoint = tune.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            checkpoint_dict = torch.load(os.path.join(checkpoint_dir, "checkpoint.pt"))
            start = checkpoint_dict["epoch"] + 1
            my_model.load_state_dict(checkpoint_dict["model_state"])

    for epoch in range(start, config["epochs"] + 1):
        # Model training here
        # ...

        metrics = {"metric": 1}
        with tempfile.TemporaryDirectory() as tempdir:
            torch.save(
                {"epoch": epoch, "model_state": my_model.state_dict()},
                os.path.join(tempdir, "checkpoint.pt"),
            )
            tune.report(metrics=metrics, checkpoint=Checkpoint.from_directory(tempdir))


tuner = tune.Tuner(train_func, param_space={"epochs": 5})
result_grid = tuner.fit()
# __function_api_checkpointing_from_dir_end__

assert not result_grid.errors

# __function_api_checkpointing_periodic_start__
NUM_EPOCHS = 12
# checkpoint every three epochs.
CHECKPOINT_FREQ = 3


def train_func(config):
    for epoch in range(1, config["epochs"] + 1):
        # Model training here
        # ...

        # Report metrics and save a checkpoint
        metrics = {"metric": "my_metric"}
        if epoch % CHECKPOINT_FREQ == 0:
            with tempfile.TemporaryDirectory() as tempdir:
                # Save a checkpoint in tempdir.
                tune.report(metrics, checkpoint=Checkpoint.from_directory(tempdir))
        else:
            tune.report(metrics)


tuner = tune.Tuner(train_func, param_space={"epochs": NUM_EPOCHS})
result_grid = tuner.fit()
# __function_api_checkpointing_periodic_end__

assert not result_grid.errors
assert len(result_grid[0].best_checkpoints) == NUM_EPOCHS // CHECKPOINT_FREQ

# __callback_api_checkpointing_start__
from ray import tune
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME
from ray.tune.experiment import Trial


class CheckpointByStepsTaken(tune.Callback):
    def __init__(self, steps_per_checkpoint: int):
        self.steps_per_checkpoint = steps_per_checkpoint
        self._trials_last_checkpoint = {}

    def on_trial_result(
        self, iteration: int, trials: list[Trial], trial: Trial, result: dict, **info
    ):
        current_step = result[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME]
        if (
            current_step - self._trials_last_checkpoint.get(trial, -1)
            >= self.steps_per_checkpoint
        ):
            trial.checkpoint_now()
            self._trials_last_checkpoint[trial] = current_step


# __callback_api_checkpointing_end__
