import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
import logging

from typing import TYPE_CHECKING, Callable, Dict, Optional, Union, Type, Any
from inspect import isclass
from pytorch_lightning.plugins.environments import ClusterEnvironment

import ray
from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import CheckpointConfig, DatasetConfig, RunConfig, ScalingConfig
from ray.data.preprocessor import Preprocessor
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.torch.config import TorchConfig
from ray.train.trainer import GenDataset
from ray.train.constants import (
    EVALUATION_DATASET_KEY,
    TRAIN_DATASET_KEY,
)
from torch.utils.data import DataLoader
from ray.train.lightning.lightning_utils import (
    RayDDPStrategy, RayEnvironment, RayModelCheckpoint, RayDataModule
)
from ray.util import PublicAPI
import ray.train as train

logger = logging.getLogger(__name__)

LIGHTNING_MODULE_KEY = "_lightning_module"
LIGHTNING_MODULE_CONFIG_KEY = "_lightning_module_config"
LIGHTNING_TRAINER_CONFIG_KEY = "_lightning_trainer_config"
LIGHTNING_TRAINER_FIT_PARAMS_KEY = "_lightning_trainer_fit_params"
MODEL_CHECKPOINT_CONFIG_KEY = "_model_checkpoint_config"
DDP_STRATEGY_CONFIG_KEY = "_ddp_strategy_config"
RAY_DATASET_ITER_CONFIG_KEY = "_ray_dataset_iter_config"


@PublicAPI(stability="alpha")
class LightningTrainer(DataParallelTrainer):
    """A Trainer for data parallel PyTorch Lightning training.

    This Trainer runs the ``pytorch_lightning.Trainer.fit()`` method on multiple
    Ray Actors. The training is carried out in a distributed fashion through PyTorch
    DDP. These actors already have the necessary Torch process group configured for 
    distributed data parallel training.

    The training function ran on every Actor will first initialize an instance
    of the user-provided ``lightning_module`` class, which is a subclass of
    ``pytorch_lightning.LightningModule`` using the arguments provided in
    ``lightning_module_init_config``.

    For data ingestion, the LightningTrainer will then either convert the Ray Dataset 
    shards to a ``pytorch_lightning.LightningDataModule``, or directly use the datamodule 
    if provided by users. 

    The trainer will also create a ModelCheckpoint callback based on the configuration 
    provided in ``model_checkpoint_config``. Notice that all the other ModelCheckpoint 
    callbacks specified in ``lightning_trainer_config`` will be ignored.

    Then, the training function will initialize an instance of ``pytorch_lightning.Trainer``
    using the arguments provided in ``trainer_init_config`` and then run
    ``pytorch_lightning.Trainer.fit``.

    Args:
        lightning_module: A subclass of ``pytorch_lightning.LightningModule``
            that defines your model and training logic.
        lightning_module_config: Configurations to pass into
            ``lightning_module.__init__`` as kwargs.
        lightning_trainer_config: Configurations to pass into
            ``pytorch_lightning.Trainer.__init__`` as kwargs. For valid arguments to
            pass, see
            https://pytorch-lightning.readthedocs.io/en/stable/common/trainer.html#init.
        lightning_trainer_fit_params: Paramters to pass into
            ``pytorch_lightning.Trainer.fit``. Specifically, LightningTrainer extracts and 
            feeds `train_dataloaders`, `val_dataloaders`, and `datamodule` into `Trainer.fit()`. 
        ddp_strategy_config: Configurations to pass into
            ``pytorch_lightning.strategies.DDPStrategy.__init__`` as kwargs. Most users
            should only set this to ``{"find_unused_parameters": False}`` or leave this
            as-is. For valid arguments to pass, see
            https://pytorch-lightning.readthedocs.io/en/stable/api/pytorch_lightning.strategies.DDPStrategy.html
            and
            https://pytorch.org/docs/stable/generated/torch.nn.parallel.DistributedDataParallel.html
        model_checkpoint_config: Configurations used to build a `ModelCheckpoint` callback.
            The valid arguments list is the same as ``pytorch_lightning.callbacks.ModelCheckpoint``. Please check:
            https://pytorch-lightning.readthedocs.io/en/stable/api/pytorch_lightning.callbacks.ModelCheckpoint.html
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        datasets: A dictionary of Ray Datasets to use for training. 
            Use the key "train" to denote which dataset is the training
            dataset and (optionally) key "val" to denote the validation
            dataset. If a ``preprocessor`` is provided and has not already 
            been fit, it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided.
        datasets_iter_config: Configuration to iterate the provided ray datasets.
            Please check the valid arguments list here: 
            :meth:`ray.data.Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        lightning_module: Type[pl.LightningModule],
        *,
        lightning_module_config: Optional[Dict] = None,
        lightning_trainer_config: Optional[Dict] = None,
        lightning_trainer_fit_params: Optional[Dict] = None,
        ddp_strategy_config: Optional[Dict] = None,
        model_checkpoint_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        datasets_iter_config: Optional[Dict] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not torch_config:
            torch_config = TorchConfig()

        train_loop_config = self._create_trainer_loop_config(
            lightning_module,
            lightning_module_config,
            lightning_trainer_config,
            lightning_trainer_fit_params,
            ddp_strategy_config,
            model_checkpoint_config,
            datasets_iter_config,
        )

        run_config.checkpoint_config = self._create_air_checkpoint_config(
            model_checkpoint_config=model_checkpoint_config
        )

        super(LightningTrainer, self).__init__(
            train_loop_per_worker=_lightning_train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    @classmethod
    def _create_trainer_loop_config(
        cls,
        lightning_module: pl.LightningModule,
        lightning_module_config: Optional[Dict] = None,
        lightning_trainer_config: Optional[Dict] = None,
        lightning_trainer_fit_params: Optional[Dict] = None,
        ddp_strategy_config: Optional[Dict] = None,
        model_checkpoint_config: Optional[Dict] = None,
        datasets_iter_config: Optional[Dict] = None,
    ) -> Dict[str, Any]:

        trainer_loop_config = {}
        if not isclass(lightning_module):
            raise ValueError(
                "'lightning_module' must be a class, not a class instance."
            )
        if not issubclass(lightning_module, pl.LightningModule):
            raise ValueError(
                "'lightning_module' must be a subclass of "
                "'pytorch_lightning.LightningModule'"
            )
        trainer_loop_config[LIGHTNING_MODULE_KEY] = lightning_module
        trainer_loop_config[LIGHTNING_MODULE_CONFIG_KEY] = lightning_module_config
        trainer_loop_config[LIGHTNING_TRAINER_CONFIG_KEY] = lightning_trainer_config
        trainer_loop_config[LIGHTNING_TRAINER_FIT_PARAMS_KEY] = lightning_trainer_fit_params
        trainer_loop_config[DDP_STRATEGY_CONFIG_KEY] = ddp_strategy_config
        trainer_loop_config[MODEL_CHECKPOINT_CONFIG_KEY] = model_checkpoint_config
        trainer_loop_config[RAY_DATASET_ITER_CONFIG_KEY] = datasets_iter_config

        for key, config in trainer_loop_config.items():
            if not config:
                trainer_loop_config[key] = {}

        return trainer_loop_config

    def _create_air_checkpoint_config(self, model_checkpoint_config: Optional[Dict] = None) -> CheckpointConfig:
        """
        Generate AIR CheckpointConfig based on provided Lightning checkpoint config.

        PTL checkpointing logic:
            no monitor + no save_top_k:  default = 1, only save last one
            no monitor +    save_top_k:  if save_top_k != 1, save all
               monitor + no save_top_k:  default = 1, save the best checkpoint
               monitor +    save_top_k:  n/a
        """

        if not model_checkpoint_config:
            model_checkpoint_config = {}

        mode = model_checkpoint_config.get("mode", "min")
        monitor = model_checkpoint_config.get("monitor", None)
        num_to_keep = model_checkpoint_config.get("save_top_k", 1)
        if not monitor and num_to_keep != 1:
            num_to_keep = None

        air_checkpoint_config = CheckpointConfig(
            num_to_keep=num_to_keep,
            checkpoint_score_attribute=monitor,
            checkpoint_score_order=mode,
        )
        return air_checkpoint_config


def _prepare_dataloaders(dataloaders: Optional[DataLoader]) -> Optional[DataLoader]:
    if dataloaders:
        if isinstance(dataloaders, list):
            for i, dataloader in enumerate(dataloaders):
                dataloaders[i] = train.torch.prepare_data_loader(dataloader)
        else:
            dataloaders = train.torch.prepare_data_loader(dataloaders)
    return dataloaders


def _lightning_train_loop_per_worker(config):
    """Per-worker training loop for a Lightning Trainer."""
    trainer_fit_config = config.pop(LIGHTNING_TRAINER_FIT_PARAMS_KEY)

    # Prepare data
    datamodule = trainer_fit_config.get("datamodule", None)
    train_dataloaders = _prepare_dataloaders(
        trainer_fit_config.get("train_dataloaders", None))
    val_dataloaders = _prepare_dataloaders(
        trainer_fit_config.get("val_dataloaders", None))

    train_ray_dataset = session.get_dataset_shard("train")
    val_ray_dataset = session.get_dataset_shard("val")
    dataset_iter_config = config.pop(RAY_DATASET_ITER_CONFIG_KEY)
    if train_ray_dataset:
        if datamodule or train_dataloaders:
            logger.warning(
                "Using Ray Dataset as primary datasource, datamodule and dataloaders "
                "specified in lightning_trainer_fit_params will be ignored."
            )
        datamodule = RayDataModule(
            train_dataset=train_ray_dataset,
            val_dataset=val_ray_dataset,
            dataset_iter_config=dataset_iter_config
        )

    # Prepare Lightning Module
    LightningModuleCls = config.pop(LIGHTNING_MODULE_KEY)
    lightning_module_init_config = config.pop(LIGHTNING_MODULE_CONFIG_KEY)
    lightning_module = LightningModuleCls(**lightning_module_init_config)

    # Prepare Lightning Trainer
    trainer_config = config.pop(LIGHTNING_TRAINER_CONFIG_KEY)
    trainer_config["enable_progress_bar"] = False

    # Setup trainer's parallel devices
    if trainer_config.get("accelerator", None) == "gpu":
        current_device = ray.train.torch.get_device()
        trainer_config["devices"] = [current_device.index]

    # Setup ray cluster environment info
    trainer_config["plugins"] = [plugin for plugin in trainer_config.get(
        "plugins", []) if not isinstance(plugin, ClusterEnvironment)]
    trainer_config["plugins"].append(RayEnvironment())

    # Setup ddp strategy for ray orchestration
    ddp_strategy_config = config.pop(DDP_STRATEGY_CONFIG_KEY)
    trainer_config["strategy"] = RayDDPStrategy(**ddp_strategy_config)

    # Prepare RayModelCheckpoint Callback
    callbacks = trainer_config.get("callbacks", [])
    trainer_config["callbacks"] = list(
        filter(lambda callback: not isinstance(callback, ModelCheckpoint), callbacks))
    if len(trainer_config["callbacks"]) < len(callbacks):
        logger.warning(
            "LightningTrainer only initialized one ModelCheckpoint callback based on"
            " `model_checkpoint_config`. All other checkpoint callbacks are ignored."
        )

    # We always need a checkpoint callback to report metrics
    trainer_config["enable_checkpointing"] = True
    # TODO(yunxuanx): Align with AIR Checkpoint Config
    model_checkpoint_config = config.get(MODEL_CHECKPOINT_CONFIG_KEY)
    ray_model_checkpoint = RayModelCheckpoint(**model_checkpoint_config)
    trainer_config["callbacks"].append(ray_model_checkpoint)

    trainer = pl.Trainer(**trainer_config)

    # Restore the training from a previously interrupted/failed run.
    checkpoint_path = None
    checkpoint = session.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as ckpt_dir:
            checkpoint_path = f"{ckpt_dir}/checkpoint.ckpt"

    trainer.fit(
        lightning_module,
        train_dataloaders=train_dataloaders,
        val_dataloaders=val_dataloaders,
        datamodule=datamodule,
        ckpt_path=checkpoint_path
    )
