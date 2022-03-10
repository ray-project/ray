import abc
from typing import Dict, Any, Optional

from ray.ml.checkpoint import Checkpoint
from ray.ml.train import Trainer
from ray.tune import PlacementGroupFactory
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class FunctionTrainer(Trainer):
    """A Trainer that provides a function interface.

    When subclassing ``FunctionTrainer``, you only need to override the
    ``training_function`` and ``resource_function`` methods. There is no
    need to directly work with Trainables.

    Args:
        Same as ray.ml.Trainer.

    """

    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def training_function(
        self, config: Dict[str, Any], checkpoint: Optional[Checkpoint] = None
    ):
        """Function to define the training logic.

        Implement this the same way you would implement a training function
        for Tune.

        Dataset attributes have already been preprocessed.

        Args:
            config: Configurations for training logic specific implementation.
            checkpoint: A checkpoint to resume from, if applicable.
        """
        raise NotImplementedError

    def resource_func(
        self,
        scaling_config: Dict,
    ) -> PlacementGroupFactory:
        """Converts ``scaling_config`` to ``PlacementGroupFactory``.

        If this method is not overridden, then the Trainable produced by
        ``self.as_trainable()`` will just use the default resource request.

        Args:
            scaling_config: The scaling config to convert.
        """
        raise NotImplementedError
