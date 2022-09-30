import math
from typing import Any, Dict, Tuple, Union, Iterable
import torch
from pathlib import Path
from ray.air import session
from ray.data.dataset import Dataset
from ray.train.mosaic.mosaic_checkpoint import MosaicCheckpoint

from composer.loggers import Logger
from composer.loggers.logger import LogLevel
from composer.loggers.logger_destination import LoggerDestination
from composer.core.state import State
from composer.callbacks.checkpoint_saver import CheckpointSaver
from composer.core.callback import Callback


class _mosaic_iterator:
    """An iterator that provides batches of given size from Ray Dataset.

    Each item returned by the iterator is a list of pandas DataFrame column.
    The labels for the columns to be included should be provided by the user
    as part of `trainer_init_config`, and the columns will be in the same
    order as the list of labels.
    """

    def __init__(self, dataset, batch_size, labels):
        self.dataset = dataset
        self.labels = labels
        self.batch_iter = self.dataset.iter_torch_batches(batch_size=batch_size)

    def __next__(self):
        next_data = next(self.batch_iter)
        return [next_data[label] for label in self.labels]


class _ray_dataset_mosaic_iterable:
    """A wrapper that provides an iterator over Ray Dataset for training Composer models.

    Composer trainer can take an Iterable as a dataloader, so we provide an Iterable
    wrappervover Ray Dataset for Composer models' data consumption. Each item provided
    by the iterator should be the next batch to be trained on. The `__iter__` function
    returns `_mosaic_iterator`, which iterates through batches of size provided by the
    user as part of `trainer_init_config`. There is no default batch_size, and it must
    be provided for MosaicTrainer to run. The length of the Iterable is the number of
    batches contained in the given dataset.

    The dataset should be of pandas DataFrame type, and the labels for the columns to be
    included in the batch should be provided as part of the `trainer_init_config`.

    Args:
        dataset: Ray Dataset that will be iteratred over
        batch_size: the size of each batch that will be returned by the iterator
        labels: the labels of the dataset columns to be included in each batch
    """

    def __init__(self, dataset, batch_size, labels):
        self.dataset = dataset
        self.batch_size = batch_size
        self.labels = labels
        self.total_samples = dataset.count()

    def __len__(self):
        return math.ceil(self.total_samples / self.batch_size)

    def __iter__(self):
        return _mosaic_iterator(self.dataset, self.batch_size, self.labels)


def process_datasets(
    train_dataset: Dataset, eval_dataset: Dataset, batch_size, labels
) -> Tuple["Iterable", "Iterable"]:
    """Convert Ray train and validation to Iterables."""
    if train_dataset:
        train_torch_iterable = _ray_dataset_mosaic_iterable(
            train_dataset, batch_size, labels
        )
    else:
        train_torch_iterable = None

    if eval_dataset:
        eval_torch_iterable = _ray_dataset_mosaic_iterable(
            eval_dataset, batch_size, labels
        )
    else:
        eval_torch_iterable = None

    return train_torch_iterable, eval_torch_iterable


class RayLogger(LoggerDestination):
    """A logger to relay information logged by composer models to ray.

    This logger allows utilizing all necessary logging and logged data handling provided
    by the Composer library. All the logged information is saved in the data dictionary
    every time a new information is logged, but to reduce unnecessary reporting, the
    most up-to-date logged information is reported as metrics every batch checkpoint and
    epoch checkpoint (see Composer's Event module for more details).

    Because ray's metric dataframe will not include new keys that is reported after the
    very first report call, any logged information with the keys not included in the
    first batch checkpoint would not be retrievable after training. In other words, if
    the log level is greater than `LogLevel.BATCH` for some data, they would not be
    present in `Result.metrics_dataframe`. To allow preserving those information, the
    user can provide keys to be always included in the reported data by using `keys`
    argument in the constructor. For `MosaicTrainer`, use
    `trainer_init_config['log_keys']` to populate these keys.

    Note that in the Event callback functions, we remove unused variables, as this is
    practiced in Mosaic's composer library.

    Args:
        log_level: the granuality to log data. The default value is ``LogLevel.BATCH``
    """

    def __init__(
        self, log_level: Union[str, int, LogLevel] = LogLevel.BATCH, keys=list()
    ) -> None:
        self.log_level = LogLevel(log_level)
        self.data = {}
        for key in keys:
            self.data[key] = None

    def log_data(self, state: State, log_level: LogLevel, data: Dict[str, Any]):
        if log_level > self.log_level:
            # the logged metric is more verbose than what we want to record.
            return
        self.data.update(data.items())
        for key, val in self.data.items():
            if isinstance(val, torch.Tensor):
                self.data[key] = val.item()

    def batch_checkpoint(self, state: State, logger: Logger) -> None:
        del logger  # unused
        session.report(self.data)

    def epoch_checkpoint(self, state: State, logger: Logger) -> None:
        del logger  # unused
        session.report(self.data)


class RayTrainReportCallback(Callback):
    """A callback that wraps Composer's ``CheckpointSaver``.

    This class is used to wrap each Composer ``CheckpointSaver`` to be used in Composer
    trainer. The main role of this callback is to report the paths of the checkpoints
    saved by the Composer ``CheckpointSaver`` it wraps. In addition, when the training
    ends, (either with or without exception) the last checkpoint and the list of all
    the checkpoints that have been saved and the list of Composer InMemoryLogger are
    reported.

    Example:
        .. code-block:: python
            # create a MosaicTrainer
            mosaic_trainer =  MosaicTrainer(
                    trainer_init_per_worker=trainer_init_per_worker,
                    datasets={"train": train_dataset},
                    trainer_init_config=trainer_init_config,
                    scaling_config=scaling_config
                )
            result = mosaic_trainer.fit()

            chkpt_dict = result.checkpoint.to_dict()

            in_memory_logger = chkpt_dict["in_memory_logger"]
            last_checkpoint = chkpt_dict["last_checkpoint"]
            all_checkpoints = chkpt_dict["all_checkpoints"]

    Args:
        in_memory_logger: The list of Composer InMemoryLogger that would be used in
            Composer trainer initialization.
        checkpoint_saver: A Composer ``CheckpointSaver`` that the callback will wrap.
            If this argument is provided, then the parent class is initialized with the
            passed in ``CheckpointSaver`` object's attributes. Otherwise, the parent
            class is initialized with args provided.
        args: Arguments for initializing a Composer ``CheckpointSaver`` object.
    """

    def __init__(
        self,
        in_memory_logger,
        ray_logger,
        checkpoint_savers: CheckpointSaver,
    ):
        self.in_memory_logger = in_memory_logger
        self.ray_logger = ray_logger
        self.checkpoint_savers = checkpoint_savers
        print("checkpoint saver : ", self.checkpoint_savers)

    def close(self, state: State, logger: Logger) -> None:
        del logger  # unused
        all_checkpoints = []
        for checkpoint_saver in self.checkpoint_savers:
            all_checkpoints.extend(
                [Path(p).absolute() for p in checkpoint_saver.saved_checkpoints]
            )

        checkpoint = MosaicCheckpoint.from_dict(
            {
                # "in_memory_logger": self.in_memory_logger,
                "all_checkpoints": all_checkpoints,
            }
        )

        session.report(metrics=self.ray_logger.data, checkpoint=checkpoint)
