from typing import Any, Callable, Dict, Optional

from ray.tune.logger import Logger
from ray.tune.trainable import Trainable
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class RLAlgorithm(Trainable):
    """Base class for RL algorithms.

    This class defines the basic methods and attributes for any RL
    Algorithm. An RL Algorithm needs to inherit from this class
    and implement a couple of mixins defined under rl_algorithm_mixins.

    Some major inheritance rules apply when inheriting from these
    classes:
        1. `RLAlgorithm` is always last in inheritance order.
        2. Any mixin is included to the left.
        3. Any overriden method must call `super`.
    """

    def __init__(
        self,
        config: AlgorithmConfig,
        logger_creator: Optional[Callable[[], Logger]] = None,
        **kwargs,
    ):
        # Initialize the super class.
        super().__init__(config=config, logger_creator=logger_creator, **kwargs)
        self._setup(config=config)

    def _setup(self, config: AlgorithmConfig):
        """Sets up all components of this `RLAlgorithm`."""
        print(f"Setup RLAlgorithm ... ")
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
