import inspect
import os

from ray.air import session
from ray.train.constants import (
    EVALUATION_DATASET_KEY,
    TRAIN_DATASET_KEY,
)

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Iterable
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.mosaic._mosaic_utils import (
    process_datasets,
    RayLogger,
    RayTrainReportCallback,
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
    ``CheckpointSaver`` that saves checkpoints to `ray_tmp` folder. All
    ``CheckpointSaver`` objects are wrapped with ``RayTrainReportCallback`` to
    allow reporting the paths of saved checkpoints. (For more detail check out
    ``RayTrainReportCallback``). When resuming training from checkpoint, if
    ``load_path`` is provided in ``trainer_init_config``, it will be used to load
    saved checkpoints. Otherwise, if ``resume_from_checkpoint`` is provided, the
    last reported path will be passed in as ``load_path`` to the Composer trainer.

    All logged information, whether there is a composer logger provided or not, is
    reported as metrics via ``RayLogger``. ``RayLogger`` is added to the logger
    configuration for Composer trainer initialization. (For more detail check out
    ``RayLogger``.)

    Example:

            .. code-block:: python
            # Based on https://docs.mosaicml.com/en/v0.9.0/examples/getting_started.html

            from typing import Tuple
            import pandas as pd

            import ray
            from ray.data.datasource import SimpleTorchDatasource
            from ray.train.mosaic import MosaicTrainer
            from ray.air.config import ScalingConfig
            from ray.data.extensions import TensorArray

            import torch
            import torch.utils.data
            from torchmetrics.classification.accuracy import Accuracy

            import torchvision.datasets
            import torchvision.transforms as transforms

            import composer

            # create ray datasets
            transform = transforms.Compose(
                [transforms.ToTensor(),
                    transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
            )

            def train_dataset_factory():
                return torchvision.datasets.CIFAR10(root="./data",
                    download=True, train=True, transform=transform)

            def test_dataset_factory():
                return torchvision.datasets.CIFAR10(root="./data",
                    download=True, train=False, transform=transform)

            train_dataset: ray.data.Dataset = \
                ray.data.read_datasource(SimpleTorchDatasource(),
                    dataset_factory=train_dataset_factory)
            test_dataset: ray.data.Dataset = \
                ray.data.read_datasource(SimpleTorchDatasource(),
                    dataset_factory=test_dataset_factory)

            # map the dataset to pandas DataFrame
            def convert_batch_pair(batch: Tuple[torch.Tensor, int]):
                images = TensorArray([image.numpy() for image, _ in batch])
                labels = [label for _, label in batch]
                df = pd.DataFrame({"image" : images, "label": labels})
                return df.head(32)

            train_dataset_mapped = train_dataset.map_batches(convert_batch_pair)
            test_dataset_mapped = test_dataset.map_batches(convert_batch_pair)

            # create trainer_init_per_worker
            def trainer_init_per_worker(train_dataset, eval_dataset, **config):
                model = config.pop("model")
                optimizer = config.pop("optimizer")
                scheduler = config.pop("scheduler")

                evaluator = composer.core.evaluator.Evaluator(
                    dataloader = eval_dataset,
                    label = 'my_evaluator',
                    metrics = Accuracy()
                )

                return composer.trainer.Trainer(
                    model=model,
                    run_name="logger_test",
                    train_dataloader=train_dataset,
                    eval_dataloader=evaluator,
                    optimizers=optimizer,
                    schedulers=scheduler,
                    **config
                )


            # setup trainer_init_config
            model = composer.models.composer_resnet_cifar(model_name='resnet_20',
                num_classes=10)

            optimizer = composer.optim.DecoupledSGDW(
                model.parameters(),
                lr=0.05,
                momentum=0.9,
                weight_decay=2.0e-3
            )

            lr_scheduler = composer.optim.LinearWithWarmupScheduler(
                t_warmup="1ep",
                alpha_i=1.0,
                alpha_f=1.0
            )

            scaling_config = ScalingConfig(num_workers=1, use_gpu=False)

            loggers = [composer.loggers.InMemoryLogger()]

            trainer_init_config = {
                "model" : model,
                "optimizer" : optimizer,
                "scheduler" : lr_scheduler,
                "batch_size" : 32,
                "max_duration" : "1ep",
                "labels" : ["image","label"],
                "loggers" : loggers,
                "save_folder" : "my_folder",
            }

            # create MosaicTrainer
            datasets = {"train": train_dataset_mapped,
                "evaluation": test_dataset_mapped}
            trainer = MosaicTrainer(
                trainer_init_per_worker=trainer_init_per_worker,
                datasets=datasets,
                trainer_init_config=trainer_init_config,
                scaling_config=scaling_config
            )

            # run trainer
            result=trainer.fit()

            # retrieve checkpoint information
            chkpt_dict = result.checkpoint.to_dict()
            loggers = chkpt_dict["loggers"]
            last_checkpoint = chkpt_dict["last_checkpoint"]
            all_checkpoints = chkpt_dict["all_checkpoints"]

            # retrieve metrics
            metrics = result.metrics

            # get epoch timeseries from in memory logger
            in_memory_logger = loggers[0]
            epoch_time_series = in_memory_logger.get_timeseries('epoch')

            # create a new MosaicTrainer to resume from the last checkpoint
            trainer_init_config["max_duration"] = "2ep"
            trainer = MosaicTrainer(
                trainer_init_per_worker=trainer_init_per_worker,
                datasets=datasets,
                trainer_init_config=trainer_init_config,
                scaling_config=scaling_config,
                resume_from_checkpoint=result.checkpoint
            )

            # train
            trainer.fit()


    Args:
        trainer_init_per_worker: The function that returns an instantiated
            ``composer.Trainer`` object and takes in the following arguments:
            train ``Iterable``, optional evaluation
            ``Iterable`` and config as kwargs. The Composer Trainer should take in
            these arguments as ``train_dataloader`` and ``eval_dataloader`` without
            creating a new dataloader for each dataset. The Iterable yields batches
            of size defined in the ``trainer_init_config``. The Iterables are
            automatically created by converting the Ray Datasets internally before
            they are passed into the function.
        datasets: Any Ray Datasets to use for training. The datasets must be mapped to
            pandas DataFrame and the labels for each column should be provided. Use
            the key "train" to denote which dataset is the training
            dataset and (optionally) key "evaluation" to denote the evaluation
            dataset. Can only contain a training dataset
            and up to one extra dataset to be used for evaluation.
            If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided.
        trainer_init_config: Configurations to pass into
            ``trainer_init_per_worker`` as kwargs. It must contain
            ``labels`` and ``batch_size`` information, as these are needed when
            converting Ray Datasets into Iterables.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        trainer_init_per_worker: Callable[
            [Iterable, Optional[Iterable], Any], composer.trainer.Trainer
        ],
        *,
        datasets: Dict[str, GenDataset],
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
        if num_params < 3:
            raise ValueError(
                f"{fn_name} should take in at least 3 arguments, "
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
    os.environ["NODE_RANK"] = str(0)

    # get dataset shard
    train_dataset = session.get_dataset_shard(TRAIN_DATASET_KEY)
    eval_dataset = session.get_dataset_shard(EVALUATION_DATASET_KEY)

    # convert ray dataset into iterable
    train_torch_iterable, eval_torch_iterable = process_datasets(
        train_dataset, eval_dataset, config.pop("batch_size"), config.pop("labels")
    )

    # resume from checkpoint if checkpoint exists
    checkpoint = session.get_checkpoint()
    # use the last checkpoint as the load_path if load_path is not already defined
    if checkpoint and "load_path" not in config:
        config["load_path"] = str(checkpoint.to_dict()["last_checkpoint"][-1])

    # add RayLogger to Composer trainer loggers
    if "loggers" in config:
        if not isinstance(config["loggers"], List):
            config["loggers"] = [config["loggers"]]
        config["loggers"].append(RayLogger())
    else:
        config["loggers"] = [RayLogger()]

    in_memory_logger = []
    for logger in config["loggers"]:
        if isinstance(logger, InMemoryLogger):
            in_memory_logger.append(logger)

    # initialize Composer trainer
    trainer: composer.trainer.Trainer = trainer_init_per_worker(
        train_torch_iterable, eval_torch_iterable, **config
    )

    # We wrap all existing CheckpointSavers with RayTrainReportCallback
    # If none exists, then we create a default one with "ray_tmp" folder
    is_report_call_added = False
    for callback in trainer.state.callbacks:
        if isinstance(callback, CheckpointSaver):
            trainer.state.callbacks.remove(callback)
            trainer.state.callbacks.append(
                RayTrainReportCallback(
                    in_memory_logger=in_memory_logger, checkpoint_saver=callback
                )
            )
            is_report_call_added = True
    if not is_report_call_added:
        trainer.state.callbacks.append(
            RayTrainReportCallback(
                in_memory_logger=in_memory_logger, folder="ray_tmp", overwrite=True
            )
        )

    # call the trainer
    trainer.fit()
