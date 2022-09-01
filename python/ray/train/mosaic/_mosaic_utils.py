from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Iterable

from ray.air import session
from ray.data.dataset import Dataset

from torch.utils.data import IterableDataset
import torch

class RayDatasetMosaicIterable(Iterable):
    """Mosaic ExamplesIterable backed by a Ray Dataset."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.generate_examples_fn = self.dataset.iter_rows

        # Required for the superclass
        self.kwargs = {}

    def __iter__(self):
        for row in self.generate_examples_fn(**self.kwargs):
            yield (0, {k: v for k, v in row.as_pydict().items()})


# def process_dataset_for_mosaic(dataset: Dataset) -> "IterableDataset":
#     """Converts a Ray Dataset into a HF IterableDataset."""
#     mosaic_iterable = RayDatasetMosaicIterable(dataset)
    
#     return mosaic_iterable

#     # iterable_dataset = IterableDataset(
#     #     mosaic_iterable, format_type="torch"
#     # ).with_format("torch")

#     # return iterable_dataset

def process_dataset_for_mosaic(dataset: Dataset):
    return dataset.iter_torch_batches(dtypes=torch.float)


def process_datasets(
    train_dataset: Dataset,
    eval_dataset: Dataset,
) -> Tuple["IterableDataset", "IterableDataset"]:
    """Convert Ray train and validation to HF IterableDatasets."""
    train_torch_dataset = process_dataset_for_mosaic(train_dataset)

    if eval_dataset:
        eval_torch_dataset = process_dataset_for_mosaic(eval_dataset)
    else:
        eval_torch_dataset = None

    return train_torch_dataset, eval_torch_dataset