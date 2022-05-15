from pathlib import Path
from typing import Any, Optional, Tuple, Type

import datasets.iterable_dataset
import transformers.trainer
from torch.utils.data import IterableDataset, DataLoader
from transformers.trainer_callback import TrainerCallback

from ray import train
from ray.util import get_node_ip_address
from ray.data.dataset import Dataset

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
        # TODO(yard1): Upstream data collator removing unused columns to
        # transformers.
        # This is necessary to provide the same experience as with a
        # non-iterable HuggingFace Dataset, which can remove columns
        # not supported by the model.
        def _prepare_data_collator(self):
            """Wrap the data collator in a function removing superflous columns."""
            # Hack to set the self._signature_columns attribute.
            try:
                self._remove_unused_columns(None, description="nan")
            except AttributeError:
                pass

            if self._signature_columns and not hasattr(self, "_original_data_collator"):
                self._original_data_collator = self.data_collator

                def remove_columns_collator(features):
                    features = [
                        {
                            k: v
                            for k, v in feature.items()
                            if k in self._signature_columns
                        }
                        for feature in features
                    ]
                    return self._original_data_collator(features)

                collator = remove_columns_collator
            else:
                collator = self.data_collator

            self.data_collator = collator

        def get_train_dataloader(self):
            if self.train_dataset is None:
                raise ValueError("Trainer: training requires a train_dataset.")

            train_dataset = self.train_dataset

            # While we are not sharding the train dataset again, this
            # class ensures that the last batch has a consistent size.
            train_dataset = transformers.trainer.IterableDatasetShard(
                train_dataset,
                batch_size=self.args.train_batch_size,
                drop_last=self.args.dataloader_drop_last,
            )

            return DataLoader(
                train_dataset,
                batch_size=self.args.per_device_train_batch_size,
                collate_fn=self.data_collator,
                num_workers=self.args.dataloader_num_workers,
                pin_memory=self.args.dataloader_pin_memory,
            )

    trainer.__class__ = RayTrainer
    trainer._prepare_data_collator()
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


def process_dataset_for_hf(dataset: Dataset) -> IterableDataset:
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
) -> Tuple[IterableDataset, IterableDataset]:
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
        # we delay the train.report call after the checkpoint is reported
        # to Ray Train.
        self.delayed_report = {}
        super().__init__()

    def on_step_end(self, args, state, control, **kwargs):
        if control.should_training_stop:
            # Always save at the end.
            control.should_save = True
        return control

    def on_log(self, args, state, control, model=None, logs=None, **kwargs):
        # Log is called in multiple places (evaluation, train metrics).
        report = {**logs, "step": state.global_step, "epoch": state.epoch}
        self.delayed_report.update(report)

    def on_save(self, args, state, control, **kwargs):
        # Save is called after evaluation.
        checkpoint_path = Path(
            transformers.trainer.get_last_checkpoint(args.output_dir)
        ).absolute()
        if checkpoint_path:
            train.save_checkpoint(
                **{
                    NODE_IP_KEY: get_node_ip_address(),
                    CHECKPOINT_PATH_ON_NODE_KEY: str(checkpoint_path),
                }
            )

    def _report(self):
        if self.delayed_report:
            train.report(**self.delayed_report)
            self.delayed_report = {}

    def on_epoch_begin(self, args, state, control, **kwargs):
        # Report previous epoch - this way we ensure everything
        # is recorded.
        self._report()

    def on_train_end(self, args, state, control, **kwargs):
        # Final callback. Train metrics are logged right before this.
        self._report()
