import logging
from typing import Any, Callable, Dict, Optional

from ray.tune.logger import Logger
from ray.tune.trainable import Trainable
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class RLAlgorithm(Trainable):
    """Base class for RL algorithms.

    This class defines the basic methods and attributes for any RL
    Algorithm. An RL Algorithm needs to inherit from this class
    and implement a couple of APIs defined under `rl_algorithm_apis`.

    Some major inheritance rules apply when inheriting from these
    classes:
        1. `RLAlgorithm` is always last in inheritance order.
        2. Any API is included to the left.
        3. Any overriden method must call `super()`.
    """

    # If the `_setup` method was already run and the algorithm is set up.
    is_setup: bool = False
    # A logger instance.
    logger: logging.Logger = None

    def __init__(
        self,
        config: AlgorithmConfig,
        logger_creator: Optional[Callable[[], Logger]] = None,
        **kwargs,
    ):
        # Setup the logger.
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(config.log_level)
        # Initialize the super class.
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)
        self._setup(config=config)

    def _setup(self, config: AlgorithmConfig):
        """Sets up all components of this `RLAlgorithm`."""
        if not self.is_setup:
            self.logger.info(f"Setup RLAlgorithm ... ")
            super()._setup(config=config)
            self.is_setup = True

    def training_step(self) -> None:
        """Implements the training logic."""
        pass

    def sync(self, state: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """Syncs components."""
        if state is None:
            state = {}
        return super().sync(state, **kwargs)

    # ...
