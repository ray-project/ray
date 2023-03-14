# flake8: noqa

# __class_api_checkpointing_start__
import os
import torch
from torch import nn

from ray import air, tune


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
    run_config=air.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=air.CheckpointConfig(checkpoint_frequency=2),
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
    run_config=air.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=air.CheckpointConfig(checkpoint_frequency=10),
    ),
)
tuner.fit()

# __class_api_periodic_checkpointing_end__


# __class_api_end_checkpointing_start__
tuner = tune.Tuner(
    MyTrainableClass,
    run_config=air.RunConfig(
        stop={"training_iteration": 2},
        checkpoint_config=air.CheckpointConfig(
            checkpoint_frequency=10, checkpoint_at_end=True
        ),
    ),
)
tuner.fit()

# __class_api_end_checkpointing_end__


# __function_api_checkpointing_start__
from ray import tune
from ray.air import session
from ray.air.checkpoint import Checkpoint


def train_func(config):
    epochs = config.get("epochs", 2)
    start = 0
    loaded_checkpoint = session.get_checkpoint()
    if loaded_checkpoint:
        last_step = loaded_checkpoint.to_dict()["step"]
        start = last_step + 1

    for step in range(start, epochs):
        # Model training here
        # ...

        # Report metrics and save a checkpoint
        metrics = {"metric": "my_metric"}
        checkpoint = Checkpoint.from_dict({"step": step})
        session.report(metrics, checkpoint=checkpoint)


tuner = tune.Tuner(train_func)
results = tuner.fit()
# __function_api_checkpointing_end__


# __function_api_checkpointing_periodic_start__


def train_func(config):
    # checkpoint occurs every three epochs.
    checkpoint_freq = 3
    for epoch in range(config["epochs"]):
        # Model training here
        # ...

        # Report metrics and save a checkpoint
        metrics = {"metric": "my_metric"}
        if epoch % checkpoint_freq == (checkpoint_freq - 1):
            checkpoint = Checkpoint.from_dict({"epoch": epoch})
            session.report(metrics, checkpoint=checkpoint)
        else:
            session.report(metrics)


# __function_api_checkpointing_periodic_end__


class MyModel:
    pass


# __function_api_checkpointing_from_dir_start__
from ray import tune
from ray.air import session
from ray.air.checkpoint import Checkpoint


# like Keras, or pytorch save methods.
def write_model_to_dir(model, dir_path):
    pass


def train_func(config):
    for epoch in range(config["epochs"]):
        # Model training here
        # ...

        my_model = MyModel()
        metrics = {"metric": "my_metric"}
        # some function to write model to directory
        write_model_to_dir(my_model, "my_model")
        session.report(
            metrics=metrics, checkpoint=Checkpoint.from_directory("my_model")
        )


# __function_api_checkpointing_from_dir_end__
