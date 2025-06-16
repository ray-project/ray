from typing import Callable, Optional

from ray.tune.logger import Logger
from ray.tune.trainable import Trainable
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.annotations import override


class RLAlgorithm(Trainable):
    def __init__(
        self,
        config: AlgorithmConfig,
        logger_creator: Optional[Callable[[], Logger]] = None,
        **kwargs
    ):
        # Initialize the super class.
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)

    def _setup(self, config: AlgorithmConfig):
        """Sets up all components of this `RLAlgorithm`."""
        print("Setup RLAlgorithm ... ")
        super()._setup(config=config)

    def training_step(self) -> None:
        """Implements the training logic."""
        pass

    # ...
