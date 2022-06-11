import inspect
import os
from typing import Optional, Dict, Tuple, Type, Union, Callable, Any, TYPE_CHECKING

import ray.cloudpickle as cpickle
from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfig, RunConfig
from ray.train.trainer import BaseTrainer, GenDataset
from ray.air._internal.checkpointing import (
    load_preprocessor_from_dir,
    save_preprocessor_to_dir,
)
from ray.tune import Trainable, PlacementGroupFactory
from ray.tune.logger import Logger
from ray.tune.registry import get_trainable_cls
from ray.tune.resources import Resources
from ray.util.annotations import PublicAPI
from ray.util.ml_utils.dict import merge_dicts

if TYPE_CHECKING:
    from ray.air.preprocessor import Preprocessor


import inspect
import logging
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
    Type,
    TYPE_CHECKING,
)

import ray
from ray import tune
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.constants import (
    TRAIN_DATASET_KEY,
    WILDCARD_KEY,
)
from ray.train.trainer import BaseTrainer
from ray.air.config import ScalingConfig, RunConfig, DatasetConfig
from ray.train.trainer import GenDataset
from ray.air.checkpoint import Checkpoint
from ray.train._internal.dataset_spec import DataParallelIngestSpec
from ray.train import BackendConfig, TrainingIterator
from ray.train._internal.backend_executor import BackendExecutor
from ray.train._internal.checkpoint import TuneCheckpointManager
from ray.train._internal.utils import construct_train_func
from ray.util.annotations import DeveloperAPI
from ray.util.ml_utils.checkpoint_manager import CheckpointStrategy, _TrackedCheckpoint

try:
    import alpa
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "alpa isn't installed. To install alpa, run 'pip install " "alpa'."
    )

if TYPE_CHECKING:
    from ray.air.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class AlpaTrainer(BaseTrainer):
    """Alpa: Automating Parallelism trainer.

    References:
        Alpa: Automating Inter- and Intra-Operator
            Parallelism for Distributed Deep Learning
        https://arxiv.org/pdf/2201.12023.pdf
    """

    _dataset_config = {
        TRAIN_DATASET_KEY: DatasetConfig(fit=True, split=False),
        WILDCARD_KEY: DatasetConfig(split=False),
    }
    
    
    def __init__(
        self,
        train_loop: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        if not ray.is_initialized():
            ray.init()

        # connect to the ray cluster
        if not alpa.api.is_initialized:
            alpa.init("ray")

        cluster = alpa.get_global_cluster()
        logger.info(
             "Distributed Training with Alpa using "
            f"{cluster.num_cpus} cpus and {cluster.num_devices} gpus."
        )

        self._train_loop = train_loop
        self._train_loop_config = train_loop_config

        self._dataset_config = DatasetConfig.validated(
            DatasetConfig.merge(self._dataset_config, dataset_config), datasets
        )
        
        self._ingest_spec = DataParallelIngestSpec(
            dataset_config=self._dataset_config,
        )
        
        self._datasets = datasets
        
        super(AlpaTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def training_loop(self) -> None:
        self._train_loop(self._datasets, self._train_loop_config)
        