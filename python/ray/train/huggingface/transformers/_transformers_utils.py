import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional, Tuple, Type

import datasets.iterable_dataset
import transformers.trainer
from transformers.trainer_callback import TrainerCallback
from transformers.trainer_utils import IntervalStrategy

from ray.air import session
from ray.data import DataIterator
from ray.data.dataset import MaterializedDataset
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.train.huggingface.transformers.transformers_checkpoint import (
    TransformersCheckpoint,
)

if TYPE_CHECKING:
    from torch.utils.data import IterableDataset

logger = logging.getLogger(__name__)


def maybe_add_length(obj: Any, length: Optional[int]) -> Any:
    """Change the class of obj to a subclass with predefined __len__ if needed."""
    # By adding length to the dataset we let HF calculate steps per epoch
    # and other such values. Without length, it's not possible to use
    # epochs as the evaluation strategy, which makes for poor UX.

    if not length or hasattr(obj, "__len__"):
        return obj

    def __len__(self):
        return length

    new_class = type(
        f"{obj.__class__.__name__}WithLength", (obj.__class__,), {"__len__": __len__}
    )
    obj.__class__ = new_class
    return obj


def wrap_transformers_trainer(
    trainer: transformers.trainer.Trainer,
) -> transformers.trainer.Trainer:
    """Change the class of trainer to a subclass implementing Ray-specific logic."""
    base_trainer_class: Type[transformers.trainer.Trainer] = trainer.__class__

    class RayTrainer(base_trainer_class):
        def get_train_dataloader(self):
            data_loader = super().get_train_dataloader()
            if isinstance(
                data_loader.dataset, transformers.trainer.IterableDatasetShard
            ) and getattr(data_loader.dataset.dataset, "_do_not_split", False):
                # Default Trainer.get_train_dataloader will wrap the dataset in
                # IterableDatasetShard, which will perform additional sharding on top
                # of the already sharded dataset. By setting those two attributes,
                # we ensure that the additional sharding doesn't occur, but
                # we can still take advantage of IterableDatasetShard ensuring
                # consistent batch sizes.
                data_loader.dataset.num_processes = 1
                data_loader.dataset.process_index = 0
            return data_loader

    trainer.__class__ = RayTrainer
    return trainer


# TODO(ml-team): Replace with a Datasets-HuggingFace integration when available.
class RayDatasetHFIterable(datasets.iterable_dataset._BaseExamplesIterable):
    """HF ``_BaseExamplesIterable`` backed by a ``ray.data.DataIterator``.

    The other abstract methods of shuffling and sharding the data are not implemented,
    since those operations should be done by Ray Data. For example, the dataset
    is already sharded to each data parallel worker and is disabled
    (see ``wrap_transformers_trainer`` above).
    """

    def __init__(self, dataset: DataIterator) -> None:
        super().__init__()
        self.dataset = dataset

    def __iter__(self) -> Iterator[Tuple[int, dict]]:
        for idx, row in enumerate(self.dataset.iter_rows()):
            yield (idx, {k: v for k, v in row.items()})


def process_dataset_for_hf(
    dataset: DataIterator, disable_transformers_splitting: bool = False
) -> "IterableDataset":
    """Converts a Ray Dataset into a HF IterableDataset."""
    hf_iterable = RayDatasetHFIterable(dataset)

    iterable_dataset = datasets.iterable_dataset.IterableDataset(
        hf_iterable, format_type="torch"
    ).with_format("torch")

    if isinstance(dataset, StreamSplitDataIterator):
        if isinstance(dataset._base_dataset, MaterializedDataset):
            # In the materialized case, we can count efficiently. TODO(ekl) avoid
            # using the internal API here by passing in the base dataset from Train.
            dataset_length = dataset._base_dataset.count() // dataset.world_size()
        else:
            # Otherwise don't count to avoid breaking streaming.
            dataset_length = None
            logger.warning(
                f"The length for {dataset._base_dataset} cannot be determined "
                "since it is a streaming dataset. HF transformers requires "
                "`max_steps` to be passed in this case, or you can materialize the "
                "dataset with `ds.materialize()`."
            )
    else:
        # Legacy + non-split case.
        try:
            dataset_length = dataset._base_dataset.count()
        except (ValueError, AttributeError):
            # pipeline case
            dataset_length = None

    iterable_dataset = maybe_add_length(iterable_dataset, dataset_length)
    # Trigger logic in `wrap_transformers_trainer` to disable built-in
    # HuggingFace splitting, as we have already split the dataset ourselves.
    iterable_dataset._do_not_split = disable_transformers_splitting
    return iterable_dataset


def process_datasets(
    train_dataset: DataIterator,
    eval_dataset: DataIterator,
) -> Tuple["IterableDataset", "IterableDataset"]:
    """Convert Ray train and validation to HF IterableDatasets."""
    if train_dataset:
        train_torch_dataset = process_dataset_for_hf(
            train_dataset, disable_transformers_splitting=True
        )
    else:
        train_torch_dataset = None

    if eval_dataset:
        eval_torch_dataset = process_dataset_for_hf(eval_dataset)
    else:
        eval_torch_dataset = None

    return train_torch_dataset, eval_torch_dataset


class TrainReportCallback(TrainerCallback):
    """HF TrainerCallback for Ray Train metric reporting & checkpointing."""

    def __init__(self) -> None:
        # HF first logs metrics, and then checkpoints. With Ray AIR, we need the
        # opposite. Furthermore, some metrics are logged just at the end.
        # Therefore, if we detect that a checkpoint will be created,
        # we delay the session.report call after the checkpoint is reported
        # to Ray Train.
        self.delayed_report = {"metrics": {}, "checkpoint": None}
        self.last_metrics = {}
        self.last_step = 0
        super().__init__()

    def on_epoch_end(self, args, state, control, **kwargs):
        if control.should_training_stop:
            # If the last step was the final step of the entire
            # loop, do not log again (would cause a divide by zero error)
            if state.global_step != self.last_step:
                if args.evaluation_strategy not in ("no", IntervalStrategy.NO):
                    control.should_evaluate = True
                control.should_log = True

            # Always save at the end.
            control.should_save = True
        return control

    def on_log(self, args, state, control, model=None, logs=None, **kwargs):
        # Log is called in multiple places (evaluation, train metrics).
        report = {**logs, "step": state.global_step, "epoch": state.epoch}
        self.delayed_report["metrics"].update(report)
        self.last_step = state.global_step

    def on_save(self, args, state, control, **kwargs):
        # Save is called after evaluation.
        checkpoint_path = Path(
            transformers.trainer.get_last_checkpoint(args.output_dir)
        ).absolute()
        if checkpoint_path:
            # Use TransformersCheckpoint here to avoid a warning in _TrainSession
            self.delayed_report["checkpoint"] = TransformersCheckpoint.from_directory(
                str(checkpoint_path)
            )

    def _report(self):
        if self.delayed_report["metrics"]:
            session.report(**self.delayed_report)
            self.last_metrics = self.delayed_report["metrics"]
            self.delayed_report = {"metrics": {}, "checkpoint": None}

    def on_epoch_begin(self, args, state, control, **kwargs):
        # Report previous step/epoch - this way we ensure everything
        # is recorded.
        self._report()

    def on_step_begin(self, args, state, control, **kwargs):
        # Report previous step/epoch - this way we ensure everything
        # is recorded.
        self._report()

    def on_train_end(self, args, state, control, **kwargs):
        # Final callback. Train metrics are logged right before this.

        # Use last eval metrics
        self.delayed_report["metrics"] = {
            **self.last_metrics,
            **self.delayed_report["metrics"],
        }
        self._report()
