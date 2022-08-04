from typing import Iterable, Type

from pytorch_lightning import LightningDataModule

from ray.air import session

if TYPE_CHECKING:
    from ray.data.dataset import Dataset


def process_datasets(
    train_dataset: "Dataset",
    val_dataset: "Dataset",
    test_dataset: "Dataset",
    predict_dataset: "Dataset",
) -> LightningDataModule:
    """Convert Ray dataset shards to a PTL DataModule."""
    # TODO (s10a): batch_size
    torch_datasets = {"train_dataset": train_dataset.to_torch()}
    if eval_dataset:
        torch_datasets["val_dataset"] = eval_dataset.to_torch()
    if test_dataset:
        torch_datasets["test_dataset"] = test_dataset.to_torch()
    if predict_dataset:
        torch_datasets["predict_dataset"] = predict_dataset.to_torch()
    return LightningDataModule.from_datasets(**torch_datasets, batch_size=1, num_workers=0)


class TrainReportLogger(pytorch_lightning.loggers.base.LightningLoggerBase):
    @pytorch_lightning.utilities.distributed.rank_zero_only
    def log_metrics(self, metrics, step):
        # TODO: do we want `rank_zero_only` here?

        # `metrics` is a dictionary of metric names and values
        # TODO: also report global step and epoch in `metrics` dict?
        session.report(**metrics)
