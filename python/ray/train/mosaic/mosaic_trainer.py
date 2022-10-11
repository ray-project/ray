import inspect
import os

from ray.air import session
from ray.train.constants import (
    EVALUATION_DATASET_KEY,
    TRAIN_DATASET_KEY,
)

from typing import TYPE_CHECKING, Callable, Dict, Optional
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig

from ray.train.torch import TorchConfig, TorchTrainer
from ray.train.trainer import GenDataset
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

import composer.trainer


@PublicAPI(stability="alpha")
class MosaicTrainer(TorchTrainer):
    """A Trainer for data parallel Mosaic Composers on PyTorch training.

    This Trainer runs the ``composer.trainer.Trainer.fit()`` method on multiple
    Ray Actors. The training is carried out in a distributed fashion through PyTorch
    DDP. These actors already have the necessary torch process group already
    configured for distributed PyTorch training.

    The training function ran on every Actor will first run the
    specified ``trainer_init_per_worker`` function to obtain an instantiated
    ``composer.Trainer`` object. The ``trainer_init_per_worker`` function
    will have access to preprocessed train and evaluation datasets.

    Args:
        trainer_init_per_worker: The function that returns an instantiated
            ``composer.Trainer`` object and takes in an optional configuration
            dictionary as an argument.
        datasets: Any Ray Datasets to use for training.
        trainer_init_config: Configurations to pass into ``trainer_init_per_worker`` as
            kwargs.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A MosiacCheckpoint to resume training from.
    """

    def __init__(
        self,
        trainer_init_per_worker: Callable[[Optional[Dict]], composer.trainer.Trainer],
        *,
        datasets: Optional[Dict[str, GenDataset]] = None,
        trainer_init_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        self._validate_trainer_init_per_worker(
            trainer_init_per_worker, "trainer_init_per_worker"
        )

        trainer_init_config = trainer_init_config.copy() if trainer_init_config else {}
        if "_trainer_init_per_worker" in trainer_init_config:
            raise ValueError(
                "'_trainer_init_per_worker' is a reserved key in `trainer_init_config`."
            )
        trainer_init_config["_trainer_init_per_worker"] = trainer_init_per_worker

        super().__init__(
            train_loop_per_worker=_mosaic_train_loop_per_worker,
            train_loop_config=trainer_init_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _validate_trainer_init_per_worker(
        self, trainer_init_per_worker: Callable, fn_name: str
    ) -> None:
        num_params = len(inspect.signature(trainer_init_per_worker).parameters)
        if num_params > 1:
            raise ValueError(
                f"{fn_name} should take in at most 1 argument, "
                f"but it accepts {num_params} arguments instead."
            )


def _mosaic_train_loop_per_worker(config):
    """Per-worker training loop for Mosaic Composers."""
    trainer_init_per_worker = config.pop("_trainer_init_per_worker")

    os.environ["RANK"] = str(session.get_world_rank())
    os.environ["WORLD_SIZE"] = str(session.get_world_size())
    os.environ["LOCAL_RANK"] = str(session.get_local_rank())

    # Arbitrary values set for these as they are needed for some composer functions
    os.environ["LOCAL_WORLD_SIZE"] = os.environ["WORLD_SIZE"]
    os.environ["NODE_RANK"] = "0"

    # get dataset shard -- For now we don't support using ray dataset shards.
    # the dataloader should be prepared inside the ``trainer_init_per_worker`` function
    train_dataset = session.get_dataset_shard(TRAIN_DATASET_KEY)
    eval_dataset = session.get_dataset_shard(EVALUATION_DATASET_KEY)

    # initialize Composer trainer
    trainer: composer.trainer.Trainer = trainer_init_per_worker(**config)

    # call the trainer
    trainer.fit()
