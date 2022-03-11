import abc
import logging
from typing import Dict, Union, Callable, Optional, TYPE_CHECKING, Type

from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.result import Result
from ray.ml.config import ScalingConfig
from ray.ml.run_config import RunConfig
from ray.tune import Trainable
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data import Dataset

# A type representing either a ray.data.Dataset or a function that returns a
# ray.data.Dataset and accepts no arguments.
GenDataset = Union["Dataset", Callable[[], "Dataset"]]


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class TrainingFailedError(RuntimeError):
    """An error indicating that training has failed."""

    pass


@PublicAPI(stability="alpha")
class Trainer(abc.ABC):
    """Defines interface for distributed training on Ray.

    Args:
        scaling_config: Configuration for how to scale training.
        run_config: Configuration for the execution of the training run.
        train_dataset: Either a distributed Ray :ref:`Dataset <dataset-api>`
            or a Callable that returns a Dataset to use for training.
        extra_datasets: Any extra Datasets (such as validation or test
            datasets) to use for training.
        preprocessor: A preprocessor to preprocess the provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional[GenDataset] = None,
        extra_datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        raise NotImplementedError

    def fit(self) -> Result:
        """Runs training.

        Returns:
            A Result object containing the training result.

        Raises:
            TrainingFailedError: If any failures during the execution of
            ``self.as_trainable()``.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def as_trainable(self) -> Type[Trainable]:
        """Convert self to a ``tune.Trainable`` class."""
        raise NotImplementedError
