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

# fmt: off
# __example_objective_start__
def objective(x, a, b):
    return a * (x ** 0.5) + b
# __example_objective_end__
# fmt: on

# __function_api_report_intermediate_metrics_start__
from ray import tune
from ray.air import session


def trainable(config: dict):
    intermediate_score = 0
    for x in range(20):
        intermediate_score = objective(x, config["a"], config["b"])
        session.report({"score": intermediate_score})  # This sends the score to Tune.


tuner = tune.Tuner(trainable, param_space={"a": 2, "b": 4})
results = tuner.fit()
# __function_api_report_intermediate_metrics_end__

# __function_api_report_final_metrics_start__
from ray import tune
from ray.air import session


def trainable(config: dict):
    final_score = 0
    for x in range(20):
        final_score = objective(x, config["a"], config["b"])

    session.report({"score": final_score})  # This sends the score to Tune.


tuner = tune.Tuner(trainable, param_space={"a": 2, "b": 4})
results = tuner.fit()
# __function_api_report_final_metrics_end__

# fmt: off
# __function_api_return_final_metrics_start__
def trainable(config: dict):
    final_score = 0
    for x in range(20):
        final_score = objective(x, config["a"], config["b"])

    return {"score": final_score}  # This sends the score to Tune.
# __function_api_return_final_metrics_end__
# fmt: on

# __class_api_example_start__
from ray import air, tune


class Trainable(tune.Trainable):
    def setup(self, config: dict):
        # config (dict): A dict of hyperparameters
        self.x = 0
        self.a = config["a"]
        self.b = config["b"]

    def step(self):  # This is called iteratively.
        score = objective(self.x, self.a, self.b)
        self.x += 1
        return {"score": score}


tuner = tune.Tuner(
    Trainable,
    run_config=air.RunConfig(
        # Train for 20 steps
        stop={"training_iteration": 20},
        checkpoint_config=air.CheckpointConfig(
            # We haven't implemented checkpointing yet. See below!
            checkpoint_at_end=False
        ),
    ),
    param_space={"a": 2, "b": 4},
)
results = tuner.fit()
# __class_api_example_end__
