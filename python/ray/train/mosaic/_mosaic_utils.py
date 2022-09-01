from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional, Tuple, Type

from ray.air import session
from ray.data.dataset import Dataset

from torch.utils.data import IterableDataset

class RayDatasetMosaicIterable(IterableDataset):
    """Mosaic ExamplesIterable backed by a Ray Dataset."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.generate_examples_fn = self.dataset.iter_rows
        self.length = dataset.count()
        
        # Required for the superclass
        self.kwargs = {}

    def __iter__(self):
        for row in self.generate_examples_fn(**self.kwargs):
            yield (0, {k: v for k, v in row.as_pydict().items()})
    def __len__(self):
        return self.length

def process_dataset_for_mosaic(dataset: Dataset) -> "IterableDataset":
    """Converts a Ray Dataset into a IterableDataset."""
    return RayDatasetMosaicIterable(dataset)

def process_datasets(
    train_dataset: Dataset,
    eval_dataset: Dataset,
) -> Tuple["IterableDataset", "IterableDataset"]:
    """Convert Ray train and validation to IterableDatasets."""
    train_torch_dataset = process_dataset_for_mosaic(train_dataset)

    if eval_dataset:
        eval_torch_dataset = process_dataset_for_mosaic(eval_dataset)
    else:
        eval_torch_dataset = None

    return train_torch_dataset, eval_torch_dataset