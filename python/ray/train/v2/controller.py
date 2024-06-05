from typing import Callable

from ray.train.v2.api.config import ScalingConfig


class TrainController:
    def __init__(self, scaling_config: ScalingConfig):
        self._worker_group = None
        self._scaling_config = scaling_config

    def relaunch_training(self, train_fn: Callable, num_workers: int):
        pass

    def _handle_failures(self):
        pass

    def _handle_scaling(self):
        pass

    def _finished(self) -> bool:
        pass

    def run(self, train_fn: Callable):

        while not self._finished():
            pass
