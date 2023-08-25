# flake8: noqa

# __class_api_checkpointing_start__
import os
import torch
from torch import nn

from ray import train, tune


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
    run_config=train.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=train.CheckpointConfig(checkpoint_frequency=2),
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
    run_config=train.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=train.CheckpointConfig(checkpoint_frequency=10),
    ),
)
tuner.fit()

# __class_api_periodic_checkpointing_end__


# __class_api_end_checkpointing_start__
tuner = tune.Tuner(
    MyTrainableClass,
    run_config=train.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=train.CheckpointConfig(
            checkpoint_frequency=10, checkpoint_at_end=True
        ),
    ),
)
tuner.fit()

# __class_api_end_checkpointing_end__


class MyModel:
    pass


# __function_api_checkpointing_from_dir_start__
from ray import train, tune
from ray.train import Checkpoint


# like Keras, or pytorch save methods.
def write_model_to_dir(model, dir_path):
    pass


def write_epoch_to_dir(epoch: int, dir_path: str):
    with open(os.path.join(dir_path, "epoch"), "w") as f:
        f.write(str(epoch))


def get_epoch_from_dir(dir_path: str) -> int:
    with open(os.path.join(dir_path, "epoch"), "r") as f:
        return int(f.read())


def train_func(config):
    start = 1
    if train.get_checkpoint() is not None:
        loaded_checkpoint = train.get_checkpoint()
        with loaded_checkpoint.as_directory() as loaded_checkpoint_path:
            start = get_epoch_from_dir(loaded_checkpoint_path) + 1

    for epoch in range(start, config["epochs"] + 1):
        # Model training here
        # ...

        my_model = MyModel()
        metrics = {"metric": "my_metric"}
        # some function to write model to directory
        write_model_to_dir(my_model, "my_model")
        write_epoch_to_dir(epoch, "my_model")
        train.report(metrics=metrics, checkpoint=Checkpoint.from_directory("my_model"))


# __function_api_checkpointing_from_dir_end__


# __function_api_checkpointing_periodic_start__
def train_func(config):
    # checkpoint every three epochs.
    checkpoint_freq = 3
    for epoch in range(1, config["epochs"] + 1):
        # Model training here
        # ...

        # Report metrics and save a checkpoint
        metrics = {"metric": "my_metric"}
        if epoch % checkpoint_freq == 0:
            checkpoint = Checkpoint.from_dict({"epoch": epoch})
            train.report(metrics, checkpoint=checkpoint)
        else:
            train.report(metrics)


tuner = tune.Tuner(train_func, param_space={"epochs": 12})

# __function_api_checkpointing_periodic_end__
