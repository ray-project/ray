import torch
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import subprocess

import ray
from ray import train
from ray.air import session, Checkpoint
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from ray.air.config import RunConfig
from ray.air.config import CheckpointConfig
from torch.distributed.launcher.api import LaunchConfig

# If using GPUs, set this to True.
use_gpu = False

# Define NN layers archicture, epochs, and number of workers
num_workers = 2

# Define your train worker loop
def train_loop_per_worker():
    subprocess.run(["python", "train.py"], check=True, capture_output=True)
    # subprocess.run(["python", "-c", "'print(1)'"], check=True, capture_output=True)
    session.report({"msg:": "Finished training!"})

# Define scaling and run configs
scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
run_config = RunConfig(checkpoint_config=CheckpointConfig(num_to_keep=1))

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=scaling_config
)

result = trainer.fit()

print(result.metrics)


class elastic_launch:
    """
    Launches an torchelastic agent on the container that invoked the entrypoint.

        1. Pass the ``entrypoint`` arguments as non ``kwargs`` (e.g. no named parameters)/
           ``entrypoint`` can be a function or a command.
        2. The return value is a map of each worker's output mapped
           by their respective global rank.

    Usage

    ::

    def worker_fn(foo):
        # ...

    def main():
        # entrypoint is a function.
        outputs = elastic_launch(LaunchConfig, worker_fn)(foo)
        # return rank 0's output
        return outputs[0]

        # entrypoint is a command and ``script.py`` is the python module.
        outputs = elastic_launch(LaunchConfig, "script.py")(args)
        outputs = elastic_launch(LaunchConfig, "python")("script.py")
    """

    def __init__(
        self,
        config: LaunchConfig,
        entrypoint: Union[Callable, str, None],
    ):
        self._config = config
        self._entrypoint = entrypoint

    def __call__(self, *args):
        #return launch_agent(self._config, self._entrypoint, list(args))
        # Define your train worker loop
        def train_loop_per_worker():
            subprocess.run(["python", "train.py"], check=True, capture_output=True)
            # subprocess.run(["python", "-c", "'print(1)'"], check=True, capture_output=True)
            session.report({"msg:": "Finished training!"})

        # Define scaling and run configs
        scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)

        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=scaling_config
        )

        result = trainer.fit()
        print(result.metrics)
