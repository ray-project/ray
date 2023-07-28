import torch
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import subprocess

from ray.air import session
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from torch.distributed.launcher.api import LaunchConfig


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

    def get_scaling_config(self) -> ScalingConfig:
        return ScalingConfig(num_workers=self._config.min_nodes * self._config.nproc_per_node, use_gpu=False)

    def __call__(self, *args):
        def train_loop_per_worker():
            if isinstance(self._entrypoint, Callable):
                self._entrypoint(*args)
            else:
                commands = [self._entrypoint] + list(args)
                subprocess.run(commands, check=True, capture_output=True)
            session.report({"msg:": "Finished training!"})

        # Define scaling and run configs
        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=self.get_scaling_config()
        )

        result = trainer.fit()
        print(result.metrics)
