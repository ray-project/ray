import logging
from dataclasses import dataclass
from typing import Dict, Callable, Optional

from ray.ml.trainer import Trainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.train.trainer import GenDataset
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.train import BackendConfig
from ray.tune import PlacementGroupFactory
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class DataParallelFunctionTrainer(Trainer):
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


@dataclass
class _DataParallelScalingConfig:
    """Configuration for scaling data parallel training.

    num_workers (int): The number of workers (Ray actors) to launch.
        Each worker will reserve 1 CPU by default. The number of CPUs
        reserved by each worker can be overridden with the
        ``resources_per_worker`` argument.
    use_gpu (bool): If True, training will be done on GPUs (1 per
        worker). Defaults to False. The number of GPUs reserved by each
        worker can be overridden with the ``resources_per_worker``
        argument.
    resources_per_worker (Optional[Dict]): If specified, the resources
        defined in this Dict will be reserved for each worker. The
        ``CPU`` and ``GPU`` keys (case-sensitive) can be defined to
        override the number of CPU/GPUs used by each worker.
    max_retries (int): Number of retries when Ray actors fail.
        Defaults to 3. Set to -1 for unlimited retries.
    placement_strategy (str): The placement strategy to use for the
        placement group of the Ray actors.
    """

    num_workers: int
    use_gpu: bool = False
    resources_per_worker: Optional[Dict] = None
    max_retries: int = 3
    placement_strategy: str = "PACK"

    def __post_init__(self):
        raise NotImplementedError

    @property
    def num_cpus_per_worker(self):
        """The number of CPUs to set per worker."""
        raise NotImplementedError

    @property
    def num_gpus_per_worker(self):
        """The number of GPUs to set per worker."""
        raise NotImplementedError

    @property
    def additional_resources_per_worker(self):
        """Resources per worker, not including CPU or GPU resources."""
        raise NotImplementedError

    def get_placement_group_factory(self) -> PlacementGroupFactory:
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        raise NotImplementedError
