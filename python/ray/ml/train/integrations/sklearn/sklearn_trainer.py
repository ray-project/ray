from typing import TYPE_CHECKING, Dict, Type, Any, Optional
import warnings
import os

import ray.cloudpickle as cpickle
from ray import tune
from ray.ml.trainer import GenDataset
from ray.ml.config import ScalingConfig, RunConfig, ScalingConfigDataClass
from ray.ml.preprocessor import Preprocessor
from ray.util import PublicAPI
from ray.ml.trainer import Trainer
from ray.ml.checkpoint import Checkpoint
from ray.tune import Trainable
from ray.ml.constants import MODEL_KEY, PREPROCESSOR_KEY, TRAIN_DATASET_KEY

from sklearn.base import BaseEstimator

# some nice properties of Sklearn models:
# - they are always pickleable
# - no logic is present in __init__


@PublicAPI(stability="alpha")
class SklearnTrainer(Trainer):
    def __init__(
        self,
        *,
        sklearn_model: BaseEstimator,
        label_column: Optional[str] = None,
        fit_params: Optional[Dict[str, Any]] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None
    ):
        self.sklearn_model = sklearn_model
        self.label_column = label_column
        self.fit_params = fit_params
        super().__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def training_loop(self) -> None:
        return super().training_loop()
