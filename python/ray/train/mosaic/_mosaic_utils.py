from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Tuple, Type

import datasets.iterable_dataset
import composer.Trainer


from ray.air import session
from ray.data.dataset import Dataset

if TYPE_CHECKING:
    from torch.utils.data import IterableDataset

# TODO(ml-team): Replace with a Ray Datasets-HuggingFace integration when available.
class RayDatasetMosaicIterable(datasets.iterable_dataset.ExamplesIterable):
    """Mosaic ExamplesIterable backed by a Ray Dataset."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.generate_examples_fn = self.dataset.iter_rows

        # Required for the superclass
        self.kwargs = {}

    def __iter__(self):
        for row in self.generate_examples_fn(**self.kwargs):
            yield (0, {k: v for k, v in row.as_pydict().items()})


def process_dataset_for_mosaic(dataset: Dataset) -> "IterableDataset":
    """Converts a Ray Dataset into a HF IterableDataset."""
    mosaic_iterable = RayDatasetMosaicIterable(dataset)

    iterable_dataset = datasets.iterable_dataset.IterableDataset(
        mosaic_iterable, format_type="torch"
    ).with_format("torch")

    return iterable_dataset


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