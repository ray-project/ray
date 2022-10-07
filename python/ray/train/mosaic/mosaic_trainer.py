import inspect
import os

from ray.air import session
from ray.train.constants import (
    EVALUATION_DATASET_KEY,
    TRAIN_DATASET_KEY,
)

from typing import TYPE_CHECKING, Callable, Dict, List, Optional
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.mosaic._mosaic_utils import (
    process_datasets,
    RayLogger,
    RayTrainReportCallback,
    get_load_path_if_exists,
)
from ray.train.torch import TorchConfig, TorchTrainer
from ray.train.trainer import GenDataset
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

import composer.trainer
from composer.callbacks.checkpoint_saver import CheckpointSaver
from composer.loggers import InMemoryLogger


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

    To use ``MosaicTrainer``, the ray dataset should be mapped to pandas DataFrame,
    since the labels for the columns are used to create a batched iterable to be
    passed to Composer trainer. The column labels that would be used for training
    should be provided in the ``trainer_init_config``

    With ``MosaicTrainer``, there will always be checkpointing, with a default
    ``CheckpointSaver`` that saves checkpoints to `ray_tmp` folder. For more details on
    checkpointing and loading, sese ``trainer_init_config`` argument details.

    All logged information, whether there is a composer logger provided or not, is
    reported as metrics via ``RayLogger``. ``RayLogger`` is added to the logger
    configuration for Composer trainer initialization. (For more detail check out
    ``RayLogger``.) As the information logged on ``InMemoryLogger`` is not automatically
    relayed to the object if passed in as a config, all ``InMemoryLoggers`` will be
    reported in the final MosaicCheckpoint object with ``"in_memory_logger"`` key
    returned by ``MosaicTrainer.fit`` function call.

    Example:
            .. code-block:: python
            >>>
            >>> BATCH_SIZE = 1024
            >>> TRAIN_DURATION = "3ep"
            >>> def trainer_init_per_worker(**config):
            ...     model=torchvision.models.resnet18(num_classes=10)
            ...
            ...     # prepare the model for distributed training and wrap with
            ...     # ComposerClassifier for Composer Trainer compatibility
            ...     model = ComposerClassifier(ray.train.torch.prepare_model(model))
            ...
            ...     # torchvision dataset
            ...     train_dataset = datasets.CIFAR10( ... )
            ...     test_dataset = datasets.CIFAR10( ... )
            ...
            ...     batch_size_per_worker = BATCH_SIZE // session.get_world_size()
            ...     train_dataloader = torch.utils.data.DataLoader(
            ...         train_dataset,
            ...         batch_size=batch_size_per_worker,
            ...     )
            ...     test_dataloader = torch.utils.data.DataLoader(
            ...         test_dataset,
            ...         batch_size=batch_size_per_worker,
            ...     )
            ...
            ...     # prepare dataloader with ray.train
            ...     train_dataloader = train.torch.prepare_data_loader(train_dataloader)
            ...     test_dataloader = train.torch.prepare_data_loader(test_dataloader)
            ...
            ...     evaluator = Evaluator(
            ...          dataloader = test_dataloader,
            ...          label = 'my_evaluator',
            ...          metrics = Accuracy()
            ...     )
            ...
            ...     optimizer = composer.optim.DecoupledSGDW(
            ...         model.parameters(),
            ...         lr=0.05,
            ...         momentum=0.9,
            ...         weight_decay=2.0e-3
            ...     )
            ...
            ...     return composer.trainer.Trainer(
            ...         model=model,
            ...         train_dataloader=train_dataloader,
            ...         eval_dataloader=evaluator,
            ...         optimizers=optimizer,
            ...         **config
            ...     )
            >>>
            >>> trainer_init_config = {
            ...     "max_duration" : TRAIN_DURATION,
            ...     "loggers" : [InMemoryLogger()],
            ...     "log_keys":["metrics/my_evaluator/Accuracy"], # evaluation log key
            ...     "algorithms": [LabelSmoothing(0.1)], # Composer algorithm
            ... }
            >>> scaling_config = ScalingConfig(num_workers=2, use_gpu=(DEVICE == "gpu"))
            >>> trainer = MosaicTrainer(
            ...     trainer_init_per_worker=trainer_init_per_worker,
            ...     trainer_init_config=trainer_init_config,
            ...     scaling_config=scaling_config,
            ... )
            >>>
            >>> ### Run Trainer ###
            >>> result=trainer.fit()


    Args:
        trainer_init_per_worker: The function that returns an instantiated
            ``composer.Trainer`` object and takes in an optional configuration
            dictionary as an argument.
        datasets: Any Ray Datasets to use for training. The datasets must be mapped to
            pandas DataFrame and the labels for each column should be provided. Use
            the key "train" to denote which dataset is the training
            dataset and (optionally) key "evaluation" to denote the evaluation
            dataset. Can only contain a training dataset
            and up to one extra dataset to be used for evaluation.
            If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided. If the datasets are
            provided, they can be accessed via ``train_dataset_shard`` or
            ``eval_dataset_shard`` in the configuration dictionary passed in as an
            argument for the trainer init function. Additionally, the
            ``trainer_init_config`` should contain ``labels`` and ``batch_size``
            information, as these are needed when converting Ray Datasets into
            Iterables.
        trainer_init_config: Configurations to pass into
            ``trainer_init_per_worker`` as kwargs. There are no required items but the
            following is a list of keys that are used by the
            ``trainer_init_per_worker`` :
                ``log_keys`` : in the result metrics_dataframe returned by the trainer's
                    ``fit`` function, the metrics that are not logged in the very first
                    report will not be included. As such, to have the desired keys that
                    are not reported on the very first ``session.report`` call should be
                    provided here to be present in the final result metrics_dataframe.
                ``load_path`` : a path to the composer checkpoint that will be loaded to
                    the trainer. If you are resuming from checkpoint, by default, this
                    is set to the latest checkpoint path. The path must be an absolute
                    path, unless the checkpoint is loaded from a remote storage, in
                    which case ``remote_dir`` and ``load_from_remote`` should also be
                    provided.
                ``remote_dir`` : URI to a remote storage. If this key is defined, then
                    the logging directory will be synced with the provided remote
                    directory after the ``fit`` call
                ``load_from_remote`` : a boolean value, which defaults to False. If this
                    is set to true, then the logging directory is synced with the
                    provided remote directory and the checkpoint file is loaded from
                    ``remote_dir/load_path``. If you are resuming from a
                    MosaicCheckpoint and the checkpoint has composer checkpoint objects
                    stored in the cloud, then this must be set to True; otherwise, the
                    trainer will look for a file path saved in the checkpoint from the
                    local machine.
                ``fit_config`` : arguments that will be passed in to the ``fit`` call
                    for the composer trainer
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
    fit_config = config.pop("fit_config", {})
    remote_dir = config.pop("remote_dir", None)

    os.environ["RANK"] = str(session.get_world_rank())
    os.environ["WORLD_SIZE"] = str(session.get_world_size())
    os.environ["LOCAL_RANK"] = str(session.get_local_rank())

    # Arbitrary values set for these as they are needed for some composer functions
    os.environ["LOCAL_WORLD_SIZE"] = os.environ["WORLD_SIZE"]
    os.environ["NODE_RANK"] = "0"

    # get dataset shard
    train_dataset = session.get_dataset_shard(TRAIN_DATASET_KEY)
    eval_dataset = session.get_dataset_shard(EVALUATION_DATASET_KEY)

    # convert ray dataset into iterable
    train_torch_iterable, eval_torch_iterable = process_datasets(
        train_dataset,
        eval_dataset,
        config.pop("batch_size", None),
        config.pop("labels", None),
    )

    # resume from checkpoint if checkpoint exists
    checkpoint = session.get_checkpoint()
    config["load_path"] = get_load_path_if_exists(
        checkpoint,
        config.pop("load_path", None),
        remote_dir,
        config.pop("load_from_remote", False),
    )

    # add RayLogger to Composer trainer loggers
    ray_logger = RayLogger(keys=config.pop("log_keys", []))
    if "loggers" in config:
        if not isinstance(config["loggers"], List):
            config["loggers"] = [config["loggers"]]
        config["loggers"].append(ray_logger)
    else:
        config["loggers"] = [ray_logger]

    in_memory_logger = []
    for logger in config["loggers"]:
        if isinstance(logger, InMemoryLogger):
            in_memory_logger.append(logger)

    # initialize Composer trainer
    if train_torch_iterable:
        config["train_dataset_shard"] = train_dataset
    if eval_torch_iterable:
        config["eval_dataset_shard"] = eval_torch_iterable
    trainer: composer.trainer.Trainer = trainer_init_per_worker(**config)

    # We wrap all existing CheckpointSavers with RayTrainReportCallback
    # If none exists, then we create a default one with "ray_tmp" folder
    checkpoint_savers = []
    for callback in trainer.state.callbacks:
        if isinstance(callback, CheckpointSaver):
            # trainer.state.callbacks.remove(callback)
            checkpoint_savers.append(callback)
    if len(checkpoint_savers) == 0:
        trainer.state.callbacks.append(
            CheckpointSaver(folder="ray_tmp", overwrite=True)
        )
        checkpoint_savers.append(trainer.state.callbacks[-1])
    trainer.state.callbacks.append(
        RayTrainReportCallback(
            in_memory_logger=in_memory_logger,
            ray_logger=ray_logger,
            checkpoint_savers=checkpoint_savers,
            remote_dir=remote_dir,
        )
    )

    # call the trainer
    trainer.fit(**fit_config)
