from typing import Callable, Optional, Dict

from ray.train.torch import TorchConfig
from ray.train.v2.trainer import DataParallelFunctionTrainer, GenDataset
from ray.ml.config import DataParallelScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint


class TorchTrainer(DataParallelFunctionTrainer):
    """
    A Trainer for data parallel PyTorch training.

    This Trainer runs the provided function on multiple Ray Actors. The
    provided datasets are automatically sharded to each Ray actor running
    the training function.

    Args:
        train_func (Callable): The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_func_config (Optional[Dict]): Configurations to pass into
            ``train_func`` if it accepts an argument.
        torch_config (Optional[TorchConfig]): Configuration
            for setting up the PyTorch backend. If set to None, use the
            default configuration.
        scaling_config (Optional[DataParallelScalingConfig]): Configuration
            for how to scale data parallel training.
        run_config (Optional[RunConfig]): Configuration for the execution of
            the training run.
        train_dataset (Optional[GenDataset]): Either a distributed Ray
            :ref:`Dataset <dataset-api>` or :ref:`DatasetPipeline
            <dataset-pipeline-api>`, or a Callbable that returns a Dataset, to
            use for training. If a ``preprocessor`` is also provided,
            it will be fit on this dataset.
        additional_datasets (Optional[GenDataset]): Any additional Ray
            Datasets (such as validation or test datasets) to use for
            training. If a ``preprocessor`` is provided, it will only
            transform these datasets.
        preprocessor (Optional[Preprocessor]): A preprocessor to preprocess
            the provided datasets.
        resume_from_checkpoint (Optional[Checkpoint]): A checkpoint to
            resume training from.

        """

    def __init__(
        self,
        train_func: Callable,
        train_func_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[DataParallelScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional[GenDataset] = None,
        additional_datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not torch_config:
            torch_config = TorchConfig()

        super(TorchTrainer, self).__init__(
            train_func=train_func,
            train_func_config=train_func_config,
            backend_config=torch_config,
            scaling_config=scaling_config,
            run_config=run_config,
            train_dataset=train_dataset,
            additional_datasets=additional_datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )