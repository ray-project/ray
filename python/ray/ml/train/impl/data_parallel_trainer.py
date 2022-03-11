import logging
from typing import Dict, Callable, Optional

from ray.ml.trainer import Trainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.train.trainer import GenDataset
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.train import BackendConfig
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class DataParallelTrainer(Trainer):
    """A Trainer for data parallel training.

    This Trainer runs the provided function on multiple Ray Actors. The
    provided datasets are automatically sharded to each Ray actor running
    the training function.

    Args:
        train_func: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_func_config: Configurations to pass into
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
        preprocessor: A preprocessor to preprocess the provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_func: Callable,
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
