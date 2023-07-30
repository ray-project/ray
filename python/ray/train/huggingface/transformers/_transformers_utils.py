import logging
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional, Tuple, Type
from torch.utils.data import DataLoader, Dataset, IterableDataset
from ray.train.torch import create_dataloader

from transformers import Trainer
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
from ray.util import PublicAPI


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

# TODO(yunxuanx) Remove this placeholder class
class RayDataIterableDataset:
    def __init__(self) -> None:
        pass

def _wrap_transformers_trainer(
    trainer: transformers.trainer.Trainer,
) -> transformers.trainer.Trainer:
    """Change the class of trainer to a subclass implementing Ray-specific logic."""
    base_trainer_class: Type[transformers.trainer.Trainer] = trainer.__class__

    class RayTrainer(base_trainer_class):
        def get_train_dataloader(self):
            # TODO(yunxuanx): replace RayDataIterableDataset to the class returned by iter_torch_batches
            if isinstance(self.train_dataset, RayDataIterableDataset):
                return create_dataloader(self.train_dataset)
            else:
                return super().get_train_dataloader()

        def get_eval_dataloader(
            self, eval_dataset: Optional[Dataset] = None
        ) -> DataLoader:
            # TODO(yunxuanx): replace RayDataIterableDataset to the class returned by iter_torch_batches
            if isinstance(eval_dataset, RayDataIterableDataset):
                return create_dataloader(eval_dataset)
            else:
                return super().get_eval_dataloader(eval_dataset)

    trainer.__class__ = RayTrainer
    return trainer


@PublicAPI(stability="alpha")
def prepare_trainer(trainer: Trainer) -> Trainer:
    """Prepare your HuggingFace Transformer Trainer for Ray Train."""

    # Check callback strategies compatibility
    save_strategy = trainer.args.save_strategy
    logging_strategy = trainer.args.logging_strategy
    evaluation_strategy = trainer.args.evaluation_strategy

    for strategy in [save_strategy, evaluation_strategy]:
        if strategy in ("no", IntervalStrategy.NO):
            continue
        if strategy != logging_strategy:
            raise ValueError(
                "When using Ray Train, `evaluation_strategy` and `save_strategy` "
                " must be 'no' or the same as `logging_strategy`. Got: \n"
                f"`logging_strategy`={trainer.args.logging_strategy}\n"
                f"`save_strategy`={trainer.args.save_strategy}\n"
                f"`evaluation_strategy`={trainer.args.evaluation_strategy}"
            )

    # Check callback steps compatibility
    save_steps = trainer.args.save_steps
    eval_steps = trainer.args.eval_steps
    logging_steps = trainer.args.logging_steps

    if save_strategy in ("steps", IntervalStrategy.STEPS):
        if save_steps < logging_steps or save_steps % logging_steps != 0:
            raise ValueError(
                "When `save_strategy == 'steps'`, `save_steps` must be a multiple of "
                "`logging_steps`, so that saving and logging happens at the same time."
                f"Got `save_steps`={trainer.args.save_steps}, "
                f"`logging_steps`={trainer.args.logging_steps}."
            )

    if evaluation_strategy in ("steps", IntervalStrategy.STEPS):
        if logging_steps != eval_steps:
            raise ValueError(
                "`logging_steps` must be equal to `eval_steps`. "
                f"Got `logging_steps`={trainer.args.logging_steps}, "
                f"`eval_steps`={trainer.args.eval_steps}"
            )

    if trainer.args.load_best_model_at_end:
        raise ValueError(
            "As Ray Train replaces Transformers checkpointing, "
            "`load_best_model_at_end` must be set to False.\n"
            "You can obtain the Ray Train Checkpoint with "
            "`Result.checkpoint` returned by the `fit()` method "
            "of this Trainer, and the model itself by calling "
            "`Checkpoint.get_model()`.\n"
            "You can configure the checkpointing by setting "
            "`run_config.checkpoint_config`."
        )

    if trainer.args.push_to_hub and not trainer.args.hub_token:
        warnings.warn(
            "You have set `push_to_hub=True` but didn't specify `hub_token`. "
            "Pushing to hub will most likely fail, as the credentials will not "
            "be automatically propagated from the local enviroment to the Ray Actors. "
            "If that happens, specify `hub_token` in `TrainingArguments`.",
            stacklevel=2,
        )

    trainer = _wrap_transformers_trainer(trainer)

    # TODO(ml-team): How to enable experiment tracking integration?
    # ensure no HF logging callbacks are added
    # aside from doubling functionality with our callbacks,
    # the Wandb callbacks causes training to freeze
    integration_callbacks = transformers.trainer.get_reporting_integration_callbacks(
        trainer.args.report_to
    )
    for callback in integration_callbacks:
        trainer.pop_callback(callback)

    trainer.add_callback(TrainReportCallback)

    return trainer
