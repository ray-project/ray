import logging
from typing import Dict, Callable, Optional, TYPE_CHECKING

from ray.ml.trainer import Trainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.train.trainer import GenDataset
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.train import BackendConfig
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data import Dataset

logger = logging.getLogger(__name__)


@DeveloperAPI
class DataParallelTrainer(Trainer):
    """A Trainer for data parallel training.

    This Trainer runs the provided function ``train_loop_for_rank`` on multiple Ray Actors. The
    provided datasets are automatically sharded to each Ray actor running
    the training function.

    The provided ``train_func`` is expected to take on the following form with anywhere from 0
    to 4 arguments:

    .. code-block:: python

        def train_loop_for_rank(
            config: Optional[Dict] = None,
            train_dataset_shard: Optional[Dataset] = None,
            extra_datasets:  Optional[str, Dataset] = None,
            rank: Optional[int] = None
            )

    If ``train_loop_config``
    If a ``train_dataset`` argument is provided to the Trainer, it will be preprocessed and
    sharded, and the shard will be passed in as the second argument.

    If ``extra_datasets`` or ``train_loop_config`` are provided to the Trainer, they will be
    directly passed in as the third and fourth arguments respectively. ``extra_datasets`` will
    be preprocessed prior to being passed into the ``train_loop_for_rank``.


    Example:

        .. code-block:: python

        # Training loop for each worker.
        def train_loop_for_rank(rank, train_dataset_shard):



    Args:
        train_loop_for_rank: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_func`` if it accepts an argument.
        backend_config: Used to specify which backend to setup on the
            workers enable distributed communication, for example torch or
            horovod. If no Backend should be set up, then set this to None.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        train_dataset: Either a distributed Ray :ref:`Dataset <dataset-api>`
            or a Callable that returns a Dataset, to use for training. If a
            ``preprocessor`` is also provided, it will be fit on this
            dataset and this dataset will be transformed.
        extra_datasets: Any extra Datasets (such as validation or test
            datasets) to use for training. If a ``preprocessor`` is
            provided, the datasets specified here will only be transformed,
            and not fit on.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_for_rank: Callable[
            [int, Optional[Dataset], Optional[Dict[str, Dataset]], Optional[Dict]], None
        ],
        train_func_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional[GenDataset] = None,
        extra_datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        raise NotImplementedError
