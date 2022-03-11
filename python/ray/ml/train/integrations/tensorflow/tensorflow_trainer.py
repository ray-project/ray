from typing import Callable, Optional, Dict

from ray.train.tensorflow import TensorflowConfig
from ray.ml.trainer import GenDataset
from ray.ml.train.impl.data_parallel_trainer import DataParallelTrainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.util import PublicAPI


@PublicAPI(stability="alpha")
class TensorflowTrainer(DataParallelTrainer):
    """A Trainer for data parallel TensorFlow training.

    This Trainer runs the provided function on multiple Ray Actors. The
    provided datasets are automatically sharded to each Ray actor running
    the training function.

    Args:
        train_func (Callable): The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_func_config (Optional[Dict]): Configurations to pass into
            ``train_func`` if it accepts an argument.
        tensorflow_config (Optional[TensorflowConfig]): Configuration
            for setting up the TensorFlow backend. If set to None, use the
            default configuration.
        scaling_config: Configuration for how to scale training.
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
        tensorflow_config: Optional[TensorflowConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional[GenDataset] = None,
        additional_datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        raise NotImplementedError
