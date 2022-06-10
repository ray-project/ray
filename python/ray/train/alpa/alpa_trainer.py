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

import alpa

if TYPE_CHECKING:
    from ray.air.preprocessor import Preprocessor

logger = logging.getLogger(__name__)



@PublicAPI(stability="alpha")
class AlpaTrainer(BaseTrainer):
    """Alpa: Automating Parallelism trainer.

    References:
        Alpa: Automating Inter- and Intra-Operator
            Parallelism for Distributed Deep Learning
        https://arxiv.org/pdf/2201.12023.pdf
    """
    
    def __init__(
        self,
        train_loop: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        if not ray.is_initialized():
            ray.init()
        
        if not alpa.is_initialized:
            alpa.init('ray')

        self._train_loop = train_loop
        self._train_loop_config = train_loop_config
        
        super(AlpaTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )


    def _validate_attributes(self):
        pass

    def _get_alpa_config(self, process_datasets: bool = False) -> Dict:
        pass
    
    def training_loop(self) -> None:
        self._train_loop(self._train_loop_config)
        
    def as_trainable(self) -> Type[Trainable]:
        pass
