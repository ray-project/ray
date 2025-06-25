from typing import Any, Callable, Dict, Optional

from ray.tune.logger import Logger
from ray.tune.trainable import Trainable
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class RLAlgorithm(Trainable):
    def __init__(
        self,
        config: AlgorithmConfig,
        logger_creator: Optional[Callable[[], Logger]] = None,
        **kwargs
    ):
        # Initialize the super class.
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)
        self._setup(config=config)

    def _setup(self, config: AlgorithmConfig):
        """Sets up all components of this `RLAlgorithm`."""
        print("Setup RLAlgorithm ... ")
        super()._setup(config=config)

    def training_step(self) -> None:
        """Implements the training logic."""
        pass

    def sync(self, state: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """Syncs components."""
        if state is None:
            state = {}
        return super().sync(state, **kwargs)

    # ...
