from typing import Type, Optional, Dict
from distutils.version import LooseVersion
from inspect import isclass
import os

import pytorch_lightning

from ray.util import PublicAPI
from ray.train.torch import TorchTrainer, TorchConfig
from ray.air.config import ScalingConfig, DatasetConfig, RunConfig
from ray.train.trainer import GenDataset
from ray.data.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint
from ray.train._internal.utils import get_address_and_port
from ray.air import session
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.lightning._lightning_utils import process_datasets, TrainReportLogger


@PublicAPI(stability="alpha")
class LightningTrainer(TorchTrainer):
    """A Trainer for data parallel PyTorch Lightning training.

    This Trainer runs the ``pytorch_lightning.Trainer.fit()`` method on multiple
    Ray Actors. The training is carried out in a distributed fashion through PyTorch
    DDP. These actors already have the necessary torch process group already
    configured for distributed PyTorch training.

    The training function ran on every Actor will first initialize an instance
    of the provided ``lightning_module`` class object, which is a subclass of
    ``pytorch_lightning.LightningModule`` using the arguments provided in
    ``lightning_module_init_config``. The training function will then convert the
    Ray Dataset shards to a ``pytorch_lightning.LightningDataModule``. Then, the
    training function will initialize an instance of ``pytorch_lightning.Trainer``
    using the arguments provided in ``trainer_init_config`` and then run
    ``pytorch_lightning.Trainer.fit``.

    If the ``datasets`` dict contains a training dataset (denoted by the "train"
    key), then it will be split into multiple dataset shards, with each Actor
    training on a single shard. All the other datasets will not be split. To specify
    a batch size for your datasets, you should set the ``batch_size`` key of
    ``lightning_module_init_config``. Note that in your ``LightningModule``'s
    ``__init__``, you should not accept a ``batch_size`` argument.

    This Trainer requires ``pytorch-lightning>=1.7.0`` package.

    Args:
        lightning_module: A class object (not a class instance) that is a subclass
            of ``pytorch_lightning.LightningModule``. This class should define your
            model logic.
        lightning_module_init_config: Configurations to pass into
            ``lightning_module.__init__`` as kwargs.
        trainer_init_config: Configurations to pass into
            ``pytorch_lightning.Trainer.__init__`` as kwargs.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset and (optionally) key "evaluation" to denote the evaluation
            dataset. Can only contain a training dataset
            and up to one extra dataset to be used for evaluation.
            If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        lightning_module: Type[pytorch_lightning.LightningModule],
        *,
        lightning_module_init_config: Optional[Dict] = None,
        trainer_init_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        # Functionality required for LightningTrainer only added in this
        # version
        if LooseVersion(pytorch_lightning.__version__) < LooseVersion("1.7.0"):
            raise RuntimeError(
                "HuggingFaceTrainer requires pytorch-lightning>=1.7.0, but you "
                f"have {pytorch_lightning.__version__} which is incompatible. "
                "Update on all nodes with `pip install -U 'pytorch-lightning>=4.19.0'`."
            )

        if not isclass(lightning_module):
            raise ValueError(
                "'lightning_module' must be a class, not a class instance."
            )
        if not issubclass(lightning_module, pytorch_lightning.LightningModule):
            raise ValueError(
                "'lightning_module' must be a subclass of "
                "'pytorch_lightning.LightningModule'"
            )
        if any(
            dataloader_func in lightning_module.__dict__
            for dataloader_func in {
                "train_dataloader",
                "val_dataloader",
                "test_dataloader",
                "predict_dataloader",
            }
        ):
            raise ValueError(
                "Do not implement the 'train_dataloader', 'val_dataloader', "
                "'test_dataloader', or 'predict_dataloader' functions in your "
                "LightningModule"
            )

        trainer_init_config = trainer_init_config.copy() if trainer_init_config else {}
        self._validate_trainer_init_config(trainer_init_config)
        trainer_init_config["_lightning_module"] = lightning_module
        trainer_init_config[
            "_lightning_module_init_config"
        ] = lightning_module_init_config

        head_node_addr, head_node_port = get_address_and_port()
        super().__init__(
            train_loop_per_worker=_create_train_loop(head_node_addr, head_node_port),
            train_loop_config=trainer_init_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _validate_trainer_init_config(self, trainer_init_config: Dict) -> None:
        if "_lightning_module" in trainer_init_config:
            raise ValueError(
                "'_lightning_module' is a reserved key in `trainer_init_config`."
            )
        if "_lightning_module_init_config" in trainer_init_config:
            raise ValueError(
                "'_lightning_module_init_config' is a reserved key in "
                "`trainer_init_config`."
            )
        if (
            "strategy" in trainer_init_config
            and trainer_init_config["strategy"] != "ddp"
        ):
            raise ValueError(
                "The 'strategy' key in 'trainer_init_config' can only be " "'ddp'."
            )
        if "num_nodes" in trainer_init_config:
            raise ValueError(
                "Do not set the 'num_nodes' key in " "'trainer_init_config'."
            )
        if any(
            isinstance(logger, TrainReportLogger)
            for logger in trainer_init_config.get("logger", [])
        ):
            raise ValueError(
                "Do not pass in a Ray Train reporting logger to "
                "'trainer_init_config'."
            )


def _create_train_loop(head_node_addr, head_node_port):
    def _lightning_train_loop_per_worker(config):
        os.environ["MASTER_ADDR"] = head_node_addr
        os.environ["MASTER_PORT"] = str(head_node_port)
        os.environ["NODE_RANK"] = str(session.get_world_rank())
        os.environ["LOCAL_RANK"] = str(session.get_local_rank())
        os.environ["WORLD_SIZE"] = str(session.get_world_size())

        LightningModule = config.pop("_lightning_module")
        lightning_module_init_config = config.pop("_lightning_module_init_config")
        lightning_module_instance = LightningModule(**lightning_module_init_config)

        datamodule = process_datasets(
            session.get_dataset_shard(TRAIN_DATASET_KEY),
            session.get_dataset_shard("val"),
            session.get_dataset_shard("test"),
            session.get_dataset_shard("predict"),
            batch_size=lightning_module_init_config.pop("batch_size", None),
        )

        # TODO: do we need to do anything for Train checkpointing?
        config["strategy"] = "ddp"
        config["logger"] = [*config.get("logger", []), TrainReportLogger()]
        trainer = pytorch_lightning.Trainer(**config)
        # TODO: disable PTL checkpointing because we checkpoint in Train?
        trainer.fit(lightning_module_instance, datamodule=datamodule)

    return _lightning_train_loop_per_worker
