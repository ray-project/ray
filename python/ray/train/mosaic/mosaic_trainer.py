import inspect
import os
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type
import warnings

from composer.trainer import Trainer
from composer.loggers.logger_destination import LoggerDestination

from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.mosaic._mosaic_utils import RayLogger
from ray.train.torch import TorchConfig, TorchTrainer
from ray.train.trainer import GenDataset
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


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

    Example:
        >>> import torch.utils.data  # doctest: +SKIP
        >>> import torchvision  # doctest: +SKIP
        >>> from torchvision import transforms, datasets  # doctest: +SKIP
        >>>
        >>> from composer.models.tasks import ComposerClassifier # doctest: +SKIP
        >>> import composer.optim # doctest: +SKIP
        >>> from composer.algorithms import LabelSmoothing # doctest: +SKIP
        >>>
        >>> import ray
        >>> from ray.air.config import ScalingConfig
        >>> import ray.train as train
        >>> from ray.air import session
        >>> from ray.train.mosaic import MosaicTrainer # doctest: +SKIP
        >>>
        >>> def trainer_init_per_worker(config):
        ...     # prepare the model for distributed training and wrap with
        ...     # ComposerClassifier for Composer Trainer compatibility
        ...     model = torchvision.models.resnet18(num_classes=10)
        ...     model = ComposerClassifier(ray.train.torch.prepare_model(model))
        ...
        ...     # prepare train/test dataset
        ...     mean = (0.507, 0.487, 0.441)
        ...     std = (0.267, 0.256, 0.276)
        ...     cifar10_transforms = transforms.Compose(
        ...         [transforms.ToTensor(), transforms.Normalize(mean, std)]
        ...     )
        ...     data_directory = "~/data"
        ...     train_dataset = datasets.CIFAR10(
        ...         data_directory,
        ...         train=True,
        ...         download=True,
        ...         transform=cifar10_transforms
        ...     )
        ...
        ...     # prepare train dataloader
        ...     batch_size_per_worker = BATCH_SIZE // session.get_world_size()
        ...     train_dataloader = torch.utils.data.DataLoader(
        ...         train_dataset,
        ...         batch_size=batch_size_per_worker
        ...     )
        ...     train_dataloader = ray.train.torch.prepare_data_loader(train_dataloader)
        ...
        ...     # prepare optimizer
        ...     optimizer = composer.optim.DecoupledSGDW(
        ...         model.parameters(),
        ...         lr=0.05,
        ...         momentum=0.9,
        ...         weight_decay=2.0e-3,
        ...     )
        ...
        ...     return composer.trainer.Trainer(
        ...         model=model,
        ...         train_dataloader=train_dataloader,
        ...         optimizers=optimizer,
        ...         **config
        ...     )
        ...
        >>> scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
        >>> trainer_init_config = {
        ...     "max_duration": "1ba",
        ...     "algorithms": [LabelSmoothing()],
        ... } # doctest: +SKIP
        ...
        >>> trainer = MosaicTrainer(
        ...     trainer_init_per_worker=trainer_init_per_worker,
        ...     trainer_init_config=trainer_init_config,
        ...     scaling_config=scaling_config,
        ... ) # doctest: +SKIP
        ...
        >>> trainer.fit() # doctest: +SKIP


    Args:
        trainer_init_per_worker: The function that returns an instantiated
            ``composer.Trainer`` object and takes in configuration
            dictionary (``config``) as an argument. This dictionary is based on
            ``trainer_init_config`` and is modified for Ray - Composer integration.
        datasets: Any Ray Datasets to use for training. At the moment, we do not support
            passing datasets to the trainer and using the dataset shards in the trainer
            loop. Instead, configure and load the datasets inside
            ``trainer_init_per_worker`` function
        trainer_init_config: Configurations to pass into ``trainer_init_per_worker`` as
            kwargs. Although the kwargs can be hard-coded in the
            ``trainer_init_per_worker``, using the config allows the flexibility of
            reusing the same worker init function while changing the trainer arguments.
            For example, when hyperparameter tuning you can reuse the
            same ``trainer_init_per_worker`` function with different hyperparameter
            values rather than having multiple ``trainer_init_per_worker`` functions
            with different hard-coded hyperparameter values.
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
        trainer_init_per_worker: Callable[[Optional[Dict]], Trainer],
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

        self._validate_datasets(datasets)
        self._validate_trainer_init_config(trainer_init_config)

        super().__init__(
            train_loop_per_worker=_mosaic_train_loop_per_worker,
            train_loop_config=self._create_trainer_init_config(
                trainer_init_per_worker, trainer_init_config
            ),
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    @classmethod
    def _create_trainer_init_config(
        cls,
        trainer_init_per_worker: Callable[[Optional[Dict]], Trainer],
        trainer_init_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        trainer_init_config = trainer_init_config.copy() if trainer_init_config else {}
        if "_trainer_init_per_worker" in trainer_init_config:
            raise ValueError(
                "'_trainer_init_per_worker' is a reserved key in `trainer_init_config`."
            )
        trainer_init_config["_trainer_init_per_worker"] = trainer_init_per_worker
        return trainer_init_config

    @classmethod
    def restore(
        cls: Type["MosaicTrainer"],
        path: str,
        trainer_init_per_worker: Optional[Callable[[Optional[Dict]], Trainer]] = None,
        trainer_init_config: Optional[Dict[str, Any]] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        scaling_config: Optional[ScalingConfig] = None,
    ) -> "MosaicTrainer":
        train_loop_config = (
            cls._create_trainer_init_config(
                trainer_init_per_worker, trainer_init_config
            )
            if train_loop_config
            else None
        )

        return super(MosaicTrainer, cls).restore(
            train_loop_per_worker=_mosaic_train_loop_per_worker,
            train_loop_config=train_loop_config,
            path=path,
            datasets=datasets,
            preprocessor=preprocessor,
            scaling_config=scaling_config,
        )

    def _validate_trainer_init_per_worker(
        self, trainer_init_per_worker: Callable, fn_name: str
    ) -> None:
        num_params = len(inspect.signature(trainer_init_per_worker).parameters)
        if num_params != 1:
            raise ValueError(
                f"{fn_name} should take in at most 1 argument (`config`), "
                f"but it accepts {num_params} arguments instead."
            )

    def _validate_datasets(self, datasets) -> None:
        if not (datasets is None or len(datasets) == 0):
            raise ValueError(
                "MosaicTrainer does not support providing dataset shards \
                to `trainer_init_per_worker`. Instead of passing in the dataset into \
                    MosaicTrainer, define a dataloader and use `prepare_dataloader` \
                    inside the `trainer_init_per_worker`."
            )

    def _validate_trainer_init_config(self, config) -> None:
        if config is not None and "loggers" in config:
            warnings.warn(
                "Composer's Loggers (any subclass of LoggerDestination) are \
                not supported for MosaicComposer. Use Ray AIR provided loggers instead"
            )


def _mosaic_train_loop_per_worker(config):
    """Per-worker training loop for Mosaic Composers."""
    trainer_init_per_worker = config.pop("_trainer_init_per_worker")

    os.environ["RANK"] = str(session.get_world_rank())
    os.environ["WORLD_SIZE"] = str(session.get_world_size())
    os.environ["LOCAL_RANK"] = str(session.get_local_rank())
    os.environ["LOCAL_WORLD_SIZE"] = str(session.get_local_world_size())
    os.environ["NODE_RANK"] = str(session.get_node_rank())

    # Replace Composer's Loggers with RayLogger
    ray_logger = RayLogger(keys=config.pop("log_keys", []))

    # initialize Composer trainer
    trainer: Trainer = trainer_init_per_worker(config)

    # Remove Composer's Loggers if there are any added in the trainer_init_per_worker
    # this removes the logging part of the loggers
    filtered_callbacks = list()
    for callback in trainer.state.callbacks:
        if not isinstance(callback, LoggerDestination):
            filtered_callbacks.append(callback)
    filtered_callbacks.append(ray_logger)
    trainer.state.callbacks = filtered_callbacks

    # this prevents data to be routed to all the Composer Loggers
    trainer.logger.destinations = (ray_logger,)

    # call the trainer
    trainer.fit()
