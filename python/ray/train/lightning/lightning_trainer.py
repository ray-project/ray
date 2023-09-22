import os
import pytorch_lightning as pl

from copy import copy
from inspect import isclass
from typing import Any, Dict, Optional, Type

from ray.air import session
from ray.air.config import CheckpointConfig, RunConfig, ScalingConfig
from ray.air.constants import MODEL_KEY
from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train import DataConfig
from ray.train.trainer import GenDataset
from ray.train.torch import TorchTrainer
from ray.train.torch.config import TorchConfig
from ray.util.annotations import Deprecated
from ray.train.lightning._lightning_utils import (
    RayDDPStrategy,
    RayFSDPStrategy,
    RayDeepSpeedStrategy,
    RayLightningEnvironment,
    RayDataModule,
    RayModelCheckpoint,
    prepare_trainer,
)


import logging

logger = logging.getLogger(__name__)


LIGHTNING_CONFIG_BUILDER_DEPRECATION_MESSAGE = (
    "The LightningConfigBuilder will be hard deprecated in Ray 2.8. "
    "Use TorchTrainer instead. "
    "See https://docs.ray.io/en/releases-2.7.0/train/getting-started-pytorch-lightning.html#lightningtrainer-migration-guide "  # noqa: E501
    "for more details."
)


@Deprecated(message=LIGHTNING_CONFIG_BUILDER_DEPRECATION_MESSAGE, warning=True)
class LightningConfigBuilder:
    """Configuration Class to pass into LightningTrainer.

    Example:
        .. testcode::

            import torch
            import torch.nn as nn
            import pytorch_lightning as pl
            from ray.train.lightning import LightningConfigBuilder

            class LinearModule(pl.LightningModule):
                def __init__(self, input_dim, output_dim) -> None:
                    super().__init__()
                    self.linear = nn.Linear(input_dim, output_dim)

                def forward(self, input):
                    return self.linear(input)

                def training_step(self, batch):
                    output = self.forward(batch)
                    loss = torch.sum(output)
                    self.log("loss", loss)
                    return loss

                def predict_step(self, batch, batch_idx):
                    return self.forward(batch)

                def configure_optimizers(self):
                    return torch.optim.SGD(self.parameters(), lr=0.1)

            class MyDataModule(pl.LightningDataModule):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    # ...

            lightning_config = (
                LightningConfigBuilder()
                .module(
                    cls=LinearModule,
                    input_dim=32,
                    output_dim=4,
                )
                .trainer(max_epochs=5, accelerator="gpu")
                .fit_params(datamodule=MyDataModule())
                .strategy(name="ddp")
                .checkpointing(monitor="loss", save_top_k=2, mode="min")
                .build()
            )

    """

    def __init__(self) -> None:
        """Initialize the configurations with default values."""
        self._module_class = None
        self._module_init_config = {}
        self._trainer_init_config = {}
        self._trainer_fit_params = {}
        self._strategy_config = {}
        self._model_checkpoint_config = {}

    def module(
        self, cls: Optional[Type[pl.LightningModule]] = None, **kwargs
    ) -> "LightningConfigBuilder":
        """Set up the Pytorch Lightning module class.

        Args:
            cls: A subclass of ``pytorch_lightning.LightningModule``
                that defines your model and training logic. Note that this is a
                class definition instead of a class instance.
            **kwargs: The initialization argument list of your lightning module.
        """
        self._module_class = cls
        self._module_init_config.update(**kwargs)
        return self

    def trainer(self, **kwargs) -> "LightningConfigBuilder":
        """Set up the configurations of ``pytorch_lightning.Trainer``.

        Note that you don't have to specify the ``strategy``, ``device`` and
        ``num_nodes`` arguments here, since the ``LightningTrainer`` creates
        a PyTorch Lightning Strategy object with the configurations specified
        in the `.strategy()` method. The ``device`` and ``num_nodes`` are also
        configured automatically by the LightningTrainer. If no configuration
        is specified, it creates a ``DDPStrategy`` by default.

        For ``accelerator``, currently only ``"cpu"`` and ``"gpu"`` are supported.

        Args:
            kwargs: The initialization arguments for ``pytorch_lightning.Trainer``
                For valid arguments to pass, please refer to:
                https://lightning.ai/docs/pytorch/stable/common/trainer.html#init.
        """
        self._trainer_init_config.update(**kwargs)
        return self

    def fit_params(self, **kwargs) -> "LightningConfigBuilder":
        """The parameter lists for ``pytorch_lightning.Trainer.fit()``

        ``LightningTrainer`` creates a model instance with the parameters provided
        in `.module()` and feeds it into the ``pl.Trainer.fit()`` method.
        Therefore, you do not need to provide a model instance here.

        Args:
            kwargs: The parameter lists for ``pytorch_lightning.Trainer.fit()``
                For valid arguments to pass, please refer to:
                https://lightning.ai/docs/pytorch/stable/common/trainer.html#fit.
        """

        if "model" in kwargs:
            logger.warning(
                "You don't have to provide `model` argument in "
                "`LightningConfigBuilder.fit_params()`. LightningTrainer will create "
                "a model instance based on the parameters you provide in "
                "`LightningConfigBuilder..module()`."
            )

        self._trainer_fit_params.update(**kwargs)
        return self

    def strategy(self, name: str = "ddp", **kwargs) -> "LightningConfigBuilder":
        """Set up the configurations of ``pytorch_lightning.strategies.Strategy``.

        Args:
            name: The name of your distributed strategy. You can choose
                from "ddp", "fsdp", and "deepspeed". Default: "ddp".
            kwargs: For valid arguments to pass, please refer to:
                https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.strategies.DDPStrategy.html
                ,
                https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.strategies.FSDPStrategy.html
                and
                https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.strategies.DeepSpeedStrategy.html
        """
        if name not in ["ddp", "fsdp", "deepspeed"]:
            raise ValueError(
                "LightningTrainer currently supports 'ddp', 'fsdp', and 'deepspeed'"
                " strategy. Please choose one of them."
            )

        self._strategy_config["_strategy_name"] = name
        self._strategy_config.update(**kwargs)
        return self

    def checkpointing(self, **kwargs) -> "LightningConfigBuilder":
        """Set up the configurations of ``pytorch_lightning.callbacks.ModelCheckpoint``.

        LightningTrainer creates a subclass instance of the `ModelCheckpoint` callback
        with the kwargs. It handles checkpointing and metrics logging logics.

        Specifically, the callback periodically reports the latest metrics
        and checkpoint via
        :meth:`ray.train.report() <ray.train.report>`.
        The report frequency matches the checkpointing frequency here.
        You have to make sure that the target metrics (e.g. metrics defined in
        :class:`TuneConfig <ray.tune.TuneConfig>` or
        :class:`CheckpointConfig <ray.train.CheckpointConfig>`)
        are ready when a new checkpoint is being saved.

        Note that this method is not a replacement for the
        ``ray.train.CheckpointConfig``. You still need to specify your
        checkpointing strategy in ``CheckpointConfig``. Otherwise, Ray stores
        all the reported checkpoints by default.

        Args:
            kwargs: For valid arguments to pass, please refer to:
                https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.callbacks.ModelCheckpoint.html
        """
        self._model_checkpoint_config.update(**kwargs)
        return self

    def build(self) -> Dict["str", Any]:
        """Build and return a config dictionary to pass into LightningTrainer."""
        config_dict = self.__dict__.copy()

        if self._module_class:
            if not isclass(self._module_class):
                raise ValueError(
                    "'module_class' must be a class, not a class instance."
                )
            if not issubclass(self._module_class, pl.LightningModule):
                raise ValueError(
                    "'module_class' must be a subclass of 'pl.LightningModule'!"
                )
        else:
            # Avoid default key-value pair to adapt with Ray Tune scheduler.
            config_dict.pop("_module_class")
        return config_dict


LIGHTNING_TRAINER_DEPRECATION_MESSAGE = (
    "The LightningTrainer will be hard deprecated in Ray 2.8. "
    "Use TorchTrainer instead. "
    "See https://docs.ray.io/en/releases-2.7.0/train/getting-started-pytorch-lightning.html#lightningtrainer-migration-guide "  # noqa: E501
    "for more details."
)


@Deprecated(message=LIGHTNING_TRAINER_DEPRECATION_MESSAGE, warning=True)
class LightningTrainer(TorchTrainer):
    """A Trainer for data parallel PyTorch Lightning training.

    This Trainer runs the ``pytorch_lightning.Trainer.fit()`` method on multiple
    Ray Actors. The training is carried out in a distributed fashion through PyTorch
    DDP. These actors already have the necessary Torch process group configured for
    distributed data parallel training. We will support more distributed training
    strategies in the future.

    The training function ran on every Actor will first initialize an instance
    of the user-provided ``lightning_module`` class, which is a subclass of
    ``pytorch_lightning.LightningModule`` using the arguments provided in
    ``LightningConfigBuilder.module()``.

    For data ingestion, the LightningTrainer will then either convert the Ray Dataset
    shards to a ``pytorch_lightning.LightningDataModule``, or directly use the
    datamodule or dataloaders if provided by users.

    The trainer also creates a ModelCheckpoint callback based on the configuration
    provided in ``LightningConfigBuilder.checkpointing()``. In addition to
    checkpointing, this callback also calls ``train.report()`` to report the
    latest metrics along with the checkpoint.

    For logging, users can continue to use Lightning's native loggers, such as
    WandbLogger, TensorboardLogger, etc. LightningTrainer will also log the latest
    metrics to the training results directory whenever a new checkpoint is saved.

    Then, the training function will initialize an instance of ``pl.Trainer``
    using the arguments provided in ``LightningConfigBuilder.fit_params()`` and then
    run ``pytorch_lightning.Trainer.fit``.

    Example:
        .. testcode::

            import torch
            import torch.nn.functional as F
            from torchmetrics import Accuracy
            from torch.utils.data import DataLoader, Subset
            from torchvision.datasets import MNIST
            from torchvision import transforms
            import pytorch_lightning as pl
            from ray.air.config import ScalingConfig
            from ray.train.lightning import LightningTrainer, LightningConfigBuilder


            class MNISTClassifier(pl.LightningModule):
                def __init__(self, lr, feature_dim):
                    super(MNISTClassifier, self).__init__()
                    self.fc1 = torch.nn.Linear(28 * 28, feature_dim)
                    self.fc2 = torch.nn.Linear(feature_dim, 10)
                    self.lr = lr
                    self.accuracy = Accuracy(task="multiclass", num_classes=10, top_k=1)
                    self.val_loss = []
                    self.val_acc = []

                def forward(self, x):
                    x = x.view(-1, 28 * 28)
                    x = torch.relu(self.fc1(x))
                    x = self.fc2(x)
                    return x

                def training_step(self, batch, batch_idx):
                    x, y = batch
                    y_hat = self(x)
                    loss = torch.nn.functional.cross_entropy(y_hat, y)
                    self.log("train_loss", loss)
                    return loss

                def validation_step(self, val_batch, batch_idx):
                    x, y = val_batch
                    logits = self.forward(x)
                    loss = F.nll_loss(logits, y)
                    acc = self.accuracy(logits, y)
                    self.val_loss.append(loss)
                    self.val_acc.append(acc)
                    return {"val_loss": loss, "val_accuracy": acc}

                def on_validation_epoch_end(self):
                    avg_loss = torch.stack(self.val_loss).mean()
                    avg_acc = torch.stack(self.val_acc).mean()
                    self.log("ptl/val_loss", avg_loss)
                    self.log("ptl/val_accuracy", avg_acc)
                    self.val_acc.clear()
                    self.val_loss.clear()

                def configure_optimizers(self):
                    optimizer = torch.optim.Adam(self.parameters(), lr=self.lr)
                    return optimizer

            # Prepare MNIST Datasets
            transform = transforms.Compose(
                [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
            )
            mnist_train = MNIST(
                './data', train=True, download=True, transform=transform
            )
            mnist_val = MNIST(
                './data', train=False, download=True, transform=transform
            )

            # Take small subsets for smoke test
            # Please remove these two lines if you want to train the full dataset
            mnist_train = Subset(mnist_train, range(1000))
            mnist_train = Subset(mnist_train, range(500))

            train_loader = DataLoader(mnist_train, batch_size=128, shuffle=True)
            val_loader = DataLoader(mnist_val, batch_size=128, shuffle=False)

            lightning_config = (
                LightningConfigBuilder()
                .module(cls=MNISTClassifier, lr=1e-3, feature_dim=128)
                .trainer(max_epochs=3, accelerator="cpu")
                .fit_params(train_dataloaders=train_loader, val_dataloaders=val_loader)
                .build()
            )

            scaling_config = ScalingConfig(
                num_workers=4, use_gpu=False, resources_per_worker={"CPU": 1}
            )
            trainer = LightningTrainer(
                lightning_config=lightning_config,
                scaling_config=scaling_config,
            )
            result = trainer.fit()
            result

        .. testoutput::
            :hide:

            ...

    Args:
        lightning_config: Configuration for setting up the Pytorch Lightning Trainer.
            You can setup the configurations with ``LightningConfigBuilder``, and
            generate this config dictionary through ``LightningBuilder.build()``.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        datasets: A dictionary of Ray Datasets to use for training.
            Use the key "train" to denote which dataset is the training
            dataset and (optionally) key "val" to denote the validation
            dataset. Internally, LightningTrainer shards the training dataset
            across all workers, and creates a PyTorch Dataloader for each shard.

            If ``datasets`` is not specified, ``LightningTrainer`` will use datamodule
            or dataloaders specified in ``LightningConfigBuilder.fit_params`` instead.
        datasets_iter_config: Configuration for iterating over the input ray datasets.
            You can configure the per-device batch size, prefetch batch size, collate
            function, and more. For valid arguments to pass, please refer to:
            :py:meth:`Dataset.iter_torch_batches
            <ray.data.Dataset.iter_torch_batches>`

            Note that if you provide a ``datasets`` parameter, you must always specify
            ``datasets_iter_config`` for it.

        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
    """

    def __init__(
        self,
        lightning_config: Optional[Dict[str, Any]] = None,
        *,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[DataConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        datasets_iter_config: Optional[Dict[str, Any]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):

        run_config = copy(run_config) or RunConfig()
        lightning_config = lightning_config or LightningConfigBuilder().build()

        if datasets and not datasets_iter_config:
            raise RuntimeError(
                "No `datasets_iter_config` provided for the input `datasets`!"
                "Please refer to the API of `ray.data.DataIterator.iter_torch_batches`"
                "for all valid arguments."
            )

        run_config.checkpoint_config = self._unify_checkpoint_configs(
            ptl_ckpt_config=lightning_config["_model_checkpoint_config"],
            air_ckpt_config=run_config.checkpoint_config,
        )

        # Disable strict checking to prevent validation errors against metrics that
        # are reported at different frequencies. This works here because the Trainer
        # is always constructed on the same host as the Tuner.
        # TODO(yunxuanxiao): find a long term solution that doesn't involve setting a
        # environment variable globally.
        os.environ["TUNE_DISABLE_STRICT_METRIC_CHECKING"] = "1"

        train_loop_config = {
            "lightning_config": lightning_config,
            "datasets_iter_config": datasets_iter_config,
        }

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
            metadata=metadata,
        )

    def _unify_checkpoint_configs(
        self, ptl_ckpt_config: Dict, air_ckpt_config: CheckpointConfig
    ) -> CheckpointConfig:
        """Unify the Lightning checkpointing config and the Ray CheckpointConfig."""

        ptl_ckpt_metric = ptl_ckpt_config.get("monitor", None)
        air_ckpt_metric = air_ckpt_config.checkpoint_score_attribute

        if ptl_ckpt_metric and air_ckpt_metric and ptl_ckpt_metric != air_ckpt_metric:
            logger.warning(
                "You have specified different metrics to track in "
                "`CheckpointConfig` and Lightning ModelCheckpoint. "
                "Make sure that you have logged both metrics before "
                "a checkpoint is created."
            )

        if (
            air_ckpt_config.checkpoint_frequency != 0
            or air_ckpt_config.checkpoint_at_end
        ):
            logger.warning(
                "Attrributes `checkpoint_frequency` and `checkpoint_at_end` will not "
                "be used in `LightningTrainer`! Please set up checkpoint frequency "
                "through `LightningConfigBuilder.checkpointing()`."
            )

        # Auto-fill the AIR CheckpointConfig if the user didn't specify it.
        if air_ckpt_config == CheckpointConfig():
            # save_tok_k = 1 -> num_to_keep = 1     : Lightning saves 1 ckpt by default
            # save_top_k = 0 -> num_to_keep = 1     : AIR saves at least 1 ckpt
            # save_top_k = -1 -> num_to_keep = None : Save all ckpts

            save_top_k = ptl_ckpt_config.get("save_top_k", 1)
            if save_top_k == -1:
                num_to_keep = None
            else:
                num_to_keep = max(save_top_k, 1)

            return CheckpointConfig(
                num_to_keep=num_to_keep,
                checkpoint_score_attribute=ptl_ckpt_config.get("monitor", None),
                checkpoint_score_order=ptl_ckpt_config.get("mode", "min"),
            )
        else:
            return air_ckpt_config


def _lightning_train_loop_per_worker(config):
    """Per-worker training loop for a Lightning Trainer."""
    from ray.train._internal.storage import _use_storage_context

    # TODO(justinvyu)/NOTE: This is no longer needed, because we do not switch to
    # a rank-specific working directory in the new persistence mode.
    # Lightning requires each worker to be in the same working directory.
    if not _use_storage_context():
        # Change the working directory for all workers to the same directory.
        # This aligns with Lightning's settings and avoids inconsistency. Otherwise,
        # each worker will have a different log and checkpoint directory if they are
        # using relative paths.
        working_dir = os.path.join(session.get_trial_dir(), "rank_all")
        os.makedirs(working_dir, exist_ok=True)
        os.chdir(working_dir)

    if not config["lightning_config"]:
        raise RuntimeError("'lightning_config' not specified in LightningTrainer!")

    # Unpack all configs
    ptl_config = config["lightning_config"]
    datasets_iter_config = config["datasets_iter_config"]
    trainer_config = ptl_config["_trainer_init_config"]
    trainer_fit_params = ptl_config["_trainer_fit_params"]
    module_class = ptl_config["_module_class"]
    module_init_config = ptl_config["_module_init_config"]
    strategy_config = ptl_config["_strategy_config"]
    strategy_name = strategy_config.pop("_strategy_name", "ddp")
    model_checkpoint_config = ptl_config["_model_checkpoint_config"]

    # Prepare data
    datamodule = trainer_fit_params.get("datamodule", None)
    train_dataloaders = trainer_fit_params.get("train_dataloaders", None)

    train_ray_dataset = session.get_dataset_shard("train")
    val_ray_dataset = session.get_dataset_shard("val")

    if not (train_dataloaders or datamodule or train_ray_dataset):
        raise RuntimeError(
            "Please provide at least one of the following data inputs: "
            "train_dataloaders, datamodule, or Datasets with key 'train'."
        )

    if train_ray_dataset:
        if datamodule:
            logger.warning(
                "Using Datasets as primary input. The 'datamodule' defined in "
                "'LightningConfig.trainer_fit_params' is ignored!"
            )

        trainer_fit_params["datamodule"] = RayDataModule(
            dataset_iter_config=datasets_iter_config,
            train_dataset=train_ray_dataset,
            val_dataset=val_ray_dataset,
        )

    # Prepare Lightning Module
    lightning_module = module_class(**module_init_config)

    # Prepare Lightning Trainer
    # Setup trainer's parallel devices
    trainer_config["devices"] = "auto"

    # Setup ray cluster environment info
    if "plugins" not in trainer_config:
        trainer_config["plugins"] = []
    trainer_config["plugins"].append(RayLightningEnvironment())

    # Setup ddp strategy for ray orchestration
    if "strategy" in trainer_config:
        logger.warning(
            "`strategy` specified in `LightningConfig.trainer_init_config` "
            "will be ignored. LightningTrainer will create a strategy based on "
            "the settings passed into `LightningConfigBuilder.strategy()`."
        )

    if strategy_name == "ddp":
        trainer_config["strategy"] = RayDDPStrategy(**strategy_config)
    if strategy_name == "fsdp":
        trainer_config["strategy"] = RayFSDPStrategy(**strategy_config)
    if strategy_name == "deepspeed":
        trainer_config["strategy"] = RayDeepSpeedStrategy(**strategy_config)

    # LightningTrainer always requires checkpointing
    trainer_config["enable_checkpointing"] = True
    model_checkpoint_config["save_last"] = True

    trainer_config["callbacks"] = trainer_config.get("callbacks", []) + [
        RayModelCheckpoint(**model_checkpoint_config)
    ]

    trainer = pl.Trainer(**trainer_config)

    trainer = prepare_trainer(trainer)

    checkpoint = session.get_checkpoint()
    if checkpoint:
        checkpoint_log_message = "Resuming training from a checkpoint."
        if "ckpt_path" in trainer_fit_params:
            checkpoint_log_message += " `ckpt_path` will be ignored."
        logger.info(checkpoint_log_message)

        with checkpoint.as_directory() as ckpt_dir:
            trainer_fit_params["ckpt_path"] = f"{ckpt_dir}/{MODEL_KEY}"
            trainer.fit(lightning_module, **trainer_fit_params)
    else:
        trainer.fit(lightning_module, **trainer_fit_params)
