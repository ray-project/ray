from typing import TYPE_CHECKING, Type, Optional, Dict

import pytorch_lightning

from ray.util import PublicAPI
from ray.train.torch import TorchTrainer

if TYPE_CHECKING:
    from ray.train.torch import TorchConfig
    from ray.air.config import ScalingConfig, DatasetConfig, RunConfig
    from ray.train.trainer import GenDataset
    from ray.data.preprocessor import Preprocessor
    from ray.air.checkpoint import Checkpoint


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
            ``pytorch_lightning.Trainer.__init__`` as kwargs. For valid arguments to
            pass, see
            https://pytorch-lightning.readthedocs.io/en/stable/common/trainer.html#init.
        ddp_strategy_init_config: Configurations to pass into
            ``pytorch_lightning.strategies.DDPStrategy.__init__`` as kwargs. Most users
            should only set this to ``{"find_unused_parameters": False}`` or leave this
            as-is. For valid arguments to pass, see
            https://pytorch-lightning.readthedocs.io/en/stable/api/pytorch_lightning.strategies.DDPStrategy.html#pytorch_lightning.strategies.DDPStrategy
            and
            https://pytorch.org/docs/stable/generated/torch.nn.parallel.DistributedDataParallel.html#torch.nn.parallel.DistributedDataParallel.
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
        lightning_module_init_config: Optional[Dict[str, Any]] = None,
        trainer_init_config: Optional[Dict[str, Any]] = None,
        ddp_strategy_init_config: Optional[Dict[str, Any]] = None,
        torch_config: Optional["TorchConfig"] = None,
        scaling_config: Optional["ScalingConfig"] = None,
        dataset_config: Optional[Dict[str, "DatasetConfig"]] = None,
        run_config: Optional["RunConfig"] = None,
        datasets: Optional[Dict[str, "GenDataset"]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional["Checkpoint"] = None,
    ):
        super().__init__(
            train_loop_per_worker=_lightning_train_loop_per_worker,
            train_loop_config=trainer_init_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )


def _lightning_train_loop_per_worker(config):
    pass
