import math
from typing import Any, Dict, Tuple, Union, Iterable

from ray.air.checkpoint import Checkpoint
from ray.air import session
from ray.data.dataset import Dataset

from composer.loggers import Logger
from composer.loggers.logger import LogLevel
from composer.loggers.logger_destination import LoggerDestination
from composer.core.state import State
from composer.callbacks.checkpoint_saver import CheckpointSaver


class _mosaic_iterator:
    def __init__(self, dataset, batch_size, labels):
        self.dataset = dataset
        self.labels = labels
        self.total_samples = dataset.count()
        self.batch_iter = self.dataset.iter_torch_batches(batch_size=batch_size)

    def __next__(self):
        next_data = next(self.batch_iter)
        return [next_data[label] for label in self.labels]


class RayDatasetMosaicIterable:
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
    train_torch_iterable = RayDatasetMosaicIterable(train_dataset, batch_size, labels)

    if eval_dataset:
        eval_torch_iterable = RayDatasetMosaicIterable(eval_dataset, batch_size, labels)
    else:
        eval_torch_iterable = None

    return train_torch_iterable, eval_torch_iterable


class RayLogger(LoggerDestination):
    """A logger to relay all logged information to ray.

    This logger allows utilizing all necessary logging and logged data handling provided
    by ray. When some information is logged, then the logged information is reported as
    metrics.

    Args:
        log_level: the granuality to log data. The default value is ``LogLevel.BATCH``
    """

    def __init__(self, log_level: Union[str, int, LogLevel] = LogLevel.BATCH) -> None:
        self.log_level = LogLevel(log_level)
        self.data = {}

    def log_data(self, state: State, log_level: LogLevel, data: Dict[str, Any]):
        if log_level > self.log_level:
            # the logged metric is more verbose than what we want to record.
            return
        self.data.update(data.items())

    def batch_checkpoint(self, state: State, logger: Logger) -> None:
        del logger  # unused
        session.report(self.data)

    def epoch_checkpoint(self, state: State, logger: Logger) -> None:
        del logger  # unused
        session.report(self.data)


class RayTrainReportCallback(CheckpointSaver):
    """A callback that wraps Composer's ``CheckpointSaver``.

    This class is used to wrap each Composer ``CheckpointSaver`` to be used in Composer
    trainer. The main role of this callback is to report the paths of the checkpoints
    saved by the Composer ``CheckpointSaver`` it wraps. In addition, when the training
    ends, (either with or without exception) the last checkpoint and the list of all
    the checkpoints that have been saved and the list of Composer loggers are reported.

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

            loggers = chkpt_dict["loggers"]
            last_checkpoint = chkpt_dict["last_checkpoint"]
            all_checkpoints = chkpt_dict["all_checkpoints"]

    Args:
        loggers: The list of Composer loggers that would be used in Composer
            trainer initialization.
        checkpoint_saver: A Composer ``CheckpointSaver`` that the callback will wrap.
            If this argument is provided, then the parent class is initialized with the
            passed in ``CheckpointSaver`` object's attributes. Otherwise, the parent
            class is initialized with args provided.
        args: Arguments for initializing a Composer ``CheckpointSaver`` object.
    """

    def __init__(
        self, in_memory_logger=None, checkpoint_saver: CheckpointSaver = None, **args
    ):
        self.in_memory_logger = in_memory_logger
        self.last_checkpoint = None
        self.checkpoint_count = 0

        if checkpoint_saver:
            super(RayTrainReportCallback, self).__init__(
                checkpoint_saver.folder,
                checkpoint_saver.filename,
                checkpoint_saver.artifact_name,
                checkpoint_saver.latest_filename,
                checkpoint_saver.latest_artifact_name,
                checkpoint_saver.save_interval,
                overwrite=checkpoint_saver.overwrite,
                num_checkpoints_to_keep=checkpoint_saver.num_checkpoints_to_keep,
                weights_only=checkpoint_saver.weights_only,
                **args
            )
        else:
            super(RayTrainReportCallback, self).__init__(**args)

    def close(self, state: State, logger: Logger) -> None:
        del logger  # unused
        checkpoint = Checkpoint.from_dict(
            {
                "last_checkpoint": self.last_checkpoint,
                "in_memory_logger": self.in_memory_logger,
                "all_checkpoints": self.saved_checkpoints,
            }
        )
        session.report(metrics={}, checkpoint=checkpoint)

    def epoch_checkpoint(self, state: State, logger: Logger) -> None:
        super().epoch_checkpoint(state, logger)
        self._update_checkpoint(state)

    def batch_checkpoint(self, state: State, logger: Logger) -> None:
        super().batch_checkpoint(state, logger)
        self._update_checkpoint(state)

    def _update_checkpoint(self, state: State):
        # check that the saved checkpoint is not redundant
        if len(self.saved_checkpoints) > self.checkpoint_count:
            self.last_checkpoint = [
                chkpt_path.absolute() for chkpt_path in self.saved_checkpoints[-1][1]
            ]
            self.checkpoint_count = len(self.saved_checkpoints)
            session.report(
                metrics={},
                checkpoint=Checkpoint.from_dict(
                    {"last_checkpoint": self.last_checkpoint}
                ),
            )
