import os
import ray
from inspect import isclass
from typing import Any, Dict, Optional, Type
import shutil

from ray.air import session
from ray.air.constants import MODEL_KEY
from ray.air.config import CheckpointConfig, DatasetConfig, RunConfig, ScalingConfig
from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.trainer import GenDataset
from ray.train.torch import TorchTrainer
from ray.train.torch.config import TorchConfig
from ray.util import PublicAPI
from ray.train.lightning._lightning_utils import (
    RayDDPStrategy,
    RayEnvironment,
    RayDataModule,
    RayModelCheckpoint,
)

import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, model_checkpoint
from pytorch_lightning.plugins.environments import ClusterEnvironment

import logging

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class LightningConfig:
    """Configuration Class to pass into LightningTrainer."""

    def __init__(self) -> None:
        """Initialize the configurations with default values."""
        self.module_class = None
        self.module_init_config = {}
        self.trainer_init_config = {}
        self.trainer_fit_params = {}
        self.ddp_strategy_config = {}
        self.model_checkpoint_config = {}
        self.ray_dataset_iter_config = {}

    def set_module_class(
        self, module_class: Type[pl.LightningModule]
    ) -> "LightningConfig":
        """Set up the Pytorch Lightning module class.

        Args:
            module_class: A subclass of ``pytorch_lightning.LightningModule``
            that defines your model and training logic. Note that this is a
            class definition instead of a class instance.
        """
        if not isclass(module_class):
            raise ValueError("'module_class' must be a class, not a class instance.")
        if not issubclass(module_class, pl.LightningModule):
            raise ValueError(
                "'module_class' must be a subclass of 'pytorch_lightning.LightningModule'"
            )
        self.module_class = module_class
        return self

    def set_module_init_config(self, **kwargs) -> "LightningConfig":
        """Set up the initialization argument list of your lightning module."""
        self.module_init_config.update(**kwargs)
        return self

    def set_trainer_init_config(self, **kwargs) -> "LightningConfig":
        """Set up the configurations of ``pytorch_lightning.Trainer``.

        For valid arguments to pass, please refer to:
        https://pytorch-lightning.readthedocs.io/en/stable/common/trainer.html#init.
        """
        self.trainer_init_config.update(**kwargs)
        return self

    def set_trainer_fit_params(self, **kwargs) -> "LightningConfig":
        """Setup the parameter lists for ``pytorch_lightning.Trainer.fit()``.

        For valid arguments to pass, please refer to:
        https://pytorch-lightning.readthedocs.io/en/stable/common/trainer.html#fit.
        """
        self.trainer_fit_params.update(**kwargs)
        return self

    def set_ddp_strategy_config(self, **kwargs) -> "LightningConfig":
        """Set up the configurations of ``pytorch_lightning.Trainer``.

        For valid arguments to pass, please refer to:
        https://pytorch-lightning.readthedocs.io/en/stable/api/pytorch_lightning.strategies.DDPStrategy.html
        """
        self.ddp_strategy_config.update(**kwargs)
        return self

    def set_model_checkpoint_config(self, **kwargs) -> "LightningConfig":
        """Set up the configurations of ``pytorch_lightning.callbacks.ModelCheckpoint``.

        LightningTrainer creates a `ModelCheckpoint` callback based on this config.
        The AIR checkpointing and logging methods are triggered in that callback.
        For valid arguments to pass, please refer to:
        https://pytorch-lightning.readthedocs.io/en/stable/api/pytorch_lightning.callbacks.ModelCheckpoint.html
        """
        self.model_checkpoint_config.update(**kwargs)
        return self


@PublicAPI(stability="alpha")
class LightningTrainer(TorchTrainer):
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

    Example:
        .. code-block:: python

            import torch
            import torch.nn.functional as F
            from torchmetrics import Accuracy
            from torch.utils.data import DataLoader
            from torchvision.datasets import MNIST
            from torchvision import transforms
            import pytorch_lightning as pl
            from ray.air.config import ScalingConfig
            from ray.train.lightning import LightningTrainer, LightningConfig

            class MNISTClassifier(pl.LightningModule):
                def __init__(self, lr, feature_dim):
                    super(MNISTClassifier, self).__init__()
                    self.fc1 = torch.nn.Linear(28 * 28, feature_dim)
                    self.fc2 = torch.nn.Linear(feature_dim, 10)
                    self.lr = lr
                    self.accuracy = Accuracy()

                def forward(self, x):
                    x = x.view(-1, 28 * 28)
                    x = torch.relu(self.fc1(x))
                    x = self.fc2(x)
                    return x

                def training_step(self, batch, batch_idx):
                    x, y = batch
                    y_hat = self(x)
                    loss = torch.nn.functional.cross_entropy(y_hat, y)
                    self.log('train_loss', loss)
                    return loss

                def validation_step(self, val_batch, batch_idx):
                    x, y = val_batch
                    logits = self.forward(x)
                    loss = F.nll_loss(logits, y)
                    acc = self.accuracy(logits, y)
                    return {"val_loss": loss, "val_accuracy": acc}

                def validation_epoch_end(self, outputs):
                    avg_loss = torch.stack([x["val_loss"] for x in outputs]).mean()
                    avg_acc = torch.stack([x["val_accuracy"] for x in outputs]).mean()
                    self.log("ptl/val_loss", avg_loss)
                    self.log("ptl/val_accuracy", avg_acc)

                def configure_optimizers(self):
                    optimizer = torch.optim.Adam(self.parameters(), lr=self.lr)
                    return optimizer

            # Prepare MNIST Datasets
            transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
            mnist_train = MNIST('/tmp/data', train=True, download=True, transform=transform)
            mnist_val = MNIST('/tmp/data', train=False, download=True, transform=transform)
            train_loader = DataLoader(mnist_train, batch_size=32, shuffle=True)
            val_loader = DataLoader(mnist_val, batch_size=32, shuffle=False)

            lightning_config = LightningConfig()
            lightning_config.set_module_class(MNISTClassifier)
            lightning_config.set_module_init_config(lr=1e-3, feature_dim=128)
            lightning_config.set_trainer_init_config(max_epochs=5, accelerator="gpu")
            lightning_config.set_trainer_fit_params(train_dataloaders=train_loader, val_dataloaders=val_loader)

            scaling_config = ScalingConfig(num_workers=2, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1})
            trainer = LightningTrainer(
                lightning_config=lightning_config,
                scaling_config=scaling_config,
            )
            results = trainer.fit()

    Args:
        lightning_config: Configuration for setting up the Pytorch Lightning Trainer.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
            You don't need to provide an AIR CheckpointConfig will be overrided by
        datasets: A dictionary of Ray Datasets to use for training.
            Use the key "train" to denote which dataset is the training
            dataset and (optionally) key "val" to denote the validation
            dataset. If a ``preprocessor`` is provided and has not already
            been fit, it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided.
        datasets_iter_config: Configurations for iterating over input Ray datasets.
            This configuration is only valid when `datasets` argument is provided to
            the LightningTrainer. Otherwise, LightningTrainer will use datamodule
            or dataloaders specified in ``LightningConfig.trainer_init_config``.
            For valid arguments to pass, please refer to:
            :py:meth:`Dataset.iter_torch_batches <ray.data.Dataset.iter_torch_batches>`
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        lightning_config: LightningConfig,
        *,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_iter_config: Optional[Dict[str, Any]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        train_loop_config = {
            "lightning_config": lightning_config,
            "dataset_iter_config": dataset_iter_config,
        }

        if not run_config:
            run_config = RunConfig()

        run_config.checkpoint_config = self._create_air_checkpoint_config(
            lightning_config.model_checkpoint_config
        )

        super(LightningTrainer, self).__init__(
            train_loop_per_worker=_lightning_train_loop_per_worker,
            train_loop_config=train_loop_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _create_air_checkpoint_config(
        self, model_checkpoint_config: Optional[Dict] = None
    ) -> CheckpointConfig:
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


def _lightning_train_loop_per_worker(config):
    """Per-worker training loop for a Lightning Trainer."""
    ptl_config = config["lightning_config"]
    dataset_iter_config = config["dataset_iter_config"]

    trainer_fit_params = ptl_config.trainer_fit_params

    # Prepare data
    datamodule = trainer_fit_params.get("datamodule", None)
    train_dataloaders = trainer_fit_params.get("train_dataloaders", None)
    val_dataloaders = trainer_fit_params.get("val_dataloaders", None)

    train_ray_dataset = session.get_dataset_shard("train")
    val_ray_dataset = session.get_dataset_shard("val")
    if train_ray_dataset:
        if not dataset_iter_config:
            raise RuntimeError(
                "'dataset_iter_config' cannot be None if you want to use Ray Dataset for training."
            )

        if datamodule:
            logger.warning(
                "Using Ray datasets as primary input. The 'datamodule' defined in "
                "'LightningConfig.trainer_fit_params' will be ignored!"
            )

        datamodule = RayDataModule(
            dataset_iter_config=dataset_iter_config,
            train_dataset=train_ray_dataset,
            val_dataset=val_ray_dataset,
        )

    # Prepare Lightning Module
    lightning_module = ptl_config.module_class(**ptl_config.module_init_config)

    # Prepare Lightning Trainer
    trainer_config = ptl_config.trainer_init_config
    trainer_config["enable_progress_bar"] = False

    # Setup trainer's parallel devices
    if trainer_config.get("accelerator", None) == "gpu":
        current_device = ray.train.torch.get_device()
        trainer_config["devices"] = [current_device.index]

    # Setup ray cluster environment info
    trainer_config["plugins"] = [
        plugin
        for plugin in trainer_config.get("plugins", [])
        if not isinstance(plugin, ClusterEnvironment)
    ]
    trainer_config["plugins"].append(RayEnvironment())

    # Setup ddp strategy for ray orchestration
    if "strategy" in trainer_config:
        logger.warning(
            "`strategy` specified in `LightningConfig.trainer_init_config` will be ignored."
            "LightningTrainer will create a RayDDPStrategy object based on `LightningConfig.ddp_strategy_config`."
        )
    trainer_config["strategy"] = RayDDPStrategy(ptl_config.ddp_strategy_config)

    # Filter out existing ModelCheckpoint Callbacks
    callbacks = []
    for callback in trainer_config.get("callbacks", []):
        if isinstance(callback, ModelCheckpoint):
            logger.warning(
                "LightningTrainer only initialized one ModelCheckpoint callback based on"
                " `LightningConfig.model_checkpoint_config`. All other checkpoint callbacks are ignored."
            )
        else:
            callbacks.append(callback)

    # AIR needs a RayModelCheckpoint for metircs logging anyway.
    trainer_config["enable_checkpointing"] = True
    callbacks.append(RayModelCheckpoint(**ptl_config.model_checkpoint_config))

    trainer_config["callbacks"] = callbacks

    trainer = pl.Trainer(**trainer_config)

    # Restore the training from a previously interrupted/failed run.
    ckpt_path = None
    checkpoint = session.get_checkpoint()
    if checkpoint:
        ckpt_dir = checkpoint.to_directory()
        ckpt_path = f"{ckpt_dir}/{MODEL_KEY}"

    trainer.fit(
        lightning_module,
        datamodule=datamodule,
        train_dataloaders=train_dataloaders,
        val_dataloaders=val_dataloaders,
        ckpt_path=ckpt_path,
    )

    if ckpt_path:
        shutil.rmtree(os.path.dirname(ckpt_path))
