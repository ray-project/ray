import abc
import logging
from typing import Dict, Union, Callable, Optional, TYPE_CHECKING

from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.result import Result
from ray.ml.config import ScalingConfig, RunConfig
from ray.tune.trainable import ConvertibleToTrainable
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data import Dataset

GenDataset = Union["Dataset", Callable[[], "Dataset"]]


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class Trainer(ConvertibleToTrainable, abc.ABC):
    """Defines interface for distributed training on Ray.
    Args:
        scaling_config: Configuration for how to
            scale training.
        run_config: Configuration for the execution of
            the training run.
        train_dataset: Either a distributed Ray
            :ref:`Dataset <dataset-api>` or a Callable that returns a Dataset,
            to use for training. If a ``preprocessor`` is also provided,
            it will be fit on this dataset and this dataset will be
            transformed.
        extra_datasets: Any extra Datasets (such as validation or test
            datasets) to use for training. If a ``preprocessor`` is
            provided, the datasets specified here will only be transformed,
            and not fit on.
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
        """
        raise NotImplementedError

    def _override_attributes_with_config(self, config: Dict) -> Dict:
        """Overrides attributes of the Trainer with values in ``config``.

        This is needed to allow attributes of Trainer to be tuned as
        hyperparameters.
        This method does a deep update of class attributes that are dicts.

        Args:
            config: A dictionary to update attributes with.
        Returns:
            A dict containing any remaining key-value pairs from ``config``
            that don't override any attributes. This leftover config can be
            used for any downstream tasks (such as as passing to a training
            function).
        """
        raise NotImplementedError
