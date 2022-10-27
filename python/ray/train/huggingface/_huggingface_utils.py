from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Tuple, Type

import datasets.iterable_dataset
import transformers.trainer
from transformers.trainer_callback import TrainerCallback
from transformers.trainer_utils import IntervalStrategy

from ray.air import session
from ray.util import get_node_ip_address
from ray.data.dataset import Dataset
from ray.train.huggingface.huggingface_checkpoint import HuggingFaceCheckpoint

if TYPE_CHECKING:
    from torch.utils.data import IterableDataset

# Constants for the sync checkpoint dict. See huggingface_trainer.py
CHECKPOINT_PATH_ON_NODE_KEY = "checkpoint_path_on_node"
NODE_IP_KEY = "node_ip"


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
            ):
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


# TODO(ml-team): Replace with a Ray Datasets-HuggingFace integration when available.
class RayDatasetHFIterable(datasets.iterable_dataset.ExamplesIterable):
    """HF ExamplesIterable backed by a Ray Dataset."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.generate_examples_fn = self.dataset.iter_rows

        # Required for the superclass
        self.kwargs = {}

    def __iter__(self):
        for row in self.generate_examples_fn(**self.kwargs):
            yield (0, {k: v for k, v in row.as_pydict().items()})


def process_dataset_for_hf(dataset: Dataset) -> "IterableDataset":
    """Converts a Ray Dataset into a HF IterableDataset."""
    hf_iterable = RayDatasetHFIterable(dataset)

    iterable_dataset = datasets.iterable_dataset.IterableDataset(
        hf_iterable, format_type="torch"
    ).with_format("torch")

    try:
        dataset_length = dataset.count()
    except ValueError:
        # pipeline case
        dataset_length = None

    iterable_dataset = maybe_add_length(iterable_dataset, dataset_length)
    return iterable_dataset


def process_datasets(
    train_dataset: Dataset,
    eval_dataset: Dataset,
) -> Tuple["IterableDataset", "IterableDataset"]:
    """Convert Ray train and validation to HF IterableDatasets."""
    train_torch_dataset = process_dataset_for_hf(train_dataset)

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
            # Use HuggingFaceCheckpoint here to avoid a warning in _TrainSession
            self.delayed_report["checkpoint"] = HuggingFaceCheckpoint.from_dict(
                {
                    NODE_IP_KEY: get_node_ip_address(),
                    CHECKPOINT_PATH_ON_NODE_KEY: str(checkpoint_path),
                }
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
