from pathlib import Path
from typing import Dict, Generator, Iterator, List

import torch
import transformers.trainer
from torch.utils.data import IterableDataset
from transformers.trainer_callback import TrainerCallback

from ray import train
from ray.util import get_node_ip_address
from ray.data.dataset import Dataset

CHECKPOINT_PATH_ON_NODE_KEY = "checkpoint_path_on_node"
NODE_IP_KEY = "node_ip"


class HFIterableDatasetWithLen(IterableDataset):
    """Special Torch IterableDataset with preset length."""

    def __init__(self, generator: Generator, length: int):
        self.generator = generator
        self._len = length

    def __iter__(self) -> Iterator[Dict[str, torch.Tensor]]:
        it = self.generator
        for x in it:
            # HF-specific format
            yield {**x[0], "labels": x[1]}

    def __len__(self):
        return self._len


def process_dataset_for_hf(
    dataset: Dataset, feature_columns: Dict[str, List[str]], batch_size: int = 1
) -> IterableDataset:
    """Converts a Ray Dataset into a HF-compatible Torch Dataset."""
    torch_dataset = dataset.to_torch(
        batch_size=batch_size,
        feature_columns=feature_columns,
        label_column="labels",
        unsqueeze_label_tensor=False,
        unsqueeze_feature_tensors=False,
    )
    try:
        count = dataset.count()
    except ValueError:
        # pipeline case
        count = None
    if count:
        torch_dataset = HFIterableDatasetWithLen(torch_dataset, count)
    return torch_dataset


class TrainReportCallback(TrainerCallback):
    """HF TrainerCallback for Ray Train metric reporting & checkpointing."""

    def __init__(self) -> None:
        # HF first logs metrics, and then checkpoints. With Ray AIR, we need the
        # opposite. Therefore, if we detect that a checkpoint will be created,
        # we delay the train.report call after the checkpoint is reported
        # to Ray Train.
        self.delayed_report = None
        # Avoid double reporting at the end.
        # TODO(yard1): Train statistics are only reported at the end. Combine
        # the second to last report and the last report somehow. We want
        # steps/epochs to match the training iteration.
        self.last_step = None
        super().__init__()

    def on_log(self, args, state, control, model=None, logs=None, **kwargs):
        if state.global_step == self.last_step:
            return
        self.last_step = state.global_step
        report = {**logs, "step": state.global_step, "epoch": state.epoch}
        if control.should_save:
            self.delayed_report = report
        else:
            train.report(**report)

    def on_save(self, args, state, control, **kwargs):
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
        if self.delayed_report:
            train.report(**self.delayed_report)
            self.delayed_report = None
