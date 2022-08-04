from typing import TYPE_CHECKING, Optional

import pytorch_lightning
from torch.utils.data import IterableDataset

from ray.air import session

if TYPE_CHECKING:
    from ray.data.dataset import Dataset


def process_datasets(
    train_dataset: "Dataset",
    val_dataset: "Dataset",
    test_dataset: "Dataset",
    predict_dataset: "Dataset",
    batch_size: Optional[int] = None,
) -> pytorch_lightning.LightningDataModule:
    """Convert Ray dataset shards to a PTL DataModule."""

    def _iter_to_dataset(iterator):
        dataset = IterableDataset()
        # no need to split workload across workers in `__iter__` because data is
        # already sharded.
        dataset.__iter__ = lambda self: iterator
        return dataset

    torch_datasets = {
        "train_dataset": _iter_to_dataset(train_dataset.iter_torch_batches())
    }
    if val_dataset:
        torch_datasets["val_dataset"] = _iter_to_dataset(
            val_dataset.iter_torch_batches()
        )
    if test_dataset:
        torch_datasets["test_dataset"] = _iter_to_dataset(
            test_dataset.iter_torch_batches()
        )
    if predict_dataset:
        torch_datasets["predict_dataset"] = _iter_to_dataset(
            predict_dataset.iter_torch_batches()
        )
    return pytorch_lightning.LightningDataModule.from_datasets(
        **torch_datasets, batch_size=batch_size  # , num_workers=0
    )


class TrainReportLogger(pytorch_lightning.loggers.base.LightningLoggerBase):
    @pytorch_lightning.utilities.distributed.rank_zero_only
    def log_metrics(self, metrics, step):
        # TODO: do we want `rank_zero_only` here?

        # `metrics` is a dictionary of metric names and values
        # TODO: also report global step and epoch in `metrics` dict?
        session.report(**metrics)
