from typing import Tuple, Iterable
from ray.data.dataset import Dataset
import math


class _mosaic_iterator:
    def __init__(self, dataset, batch_size, labels):
        self.dataset = dataset
        self.labels = labels
        self.total_samples = dataset.count()
        self.batch_iter = self.dataset.iter_torch_batches(batch_size=batch_size)

    def __next__(self):
        next_data = next(self.batch_iter)
        return [next_data[label] for label in self.labels]


class RayDatasetMosaicIterable:
    def __init__(self, dataset, batch_size, labels):
        self.dataset = dataset
        self.batch_size = batch_size
        self.labels = labels
        self.total_samples = dataset.count()

    def __len__(self):
        return math.ceil(self.dataset.count() / self.batch_size)

    def __iter__(self):
        return _mosaic_iterator(self.dataset, self.batch_size, self.labels)


def process_datasets(
    train_dataset: Dataset, eval_dataset: Dataset, batch_size, labels
) -> Tuple["Iterable", "Iterable"]:
    """Convert Ray train and validation to Iterables."""
    train_torch_iterable = RayDatasetMosaicIterable(train_dataset, batch_size, labels)

    if eval_dataset:
        eval_torch_iterable = RayDatasetMosaicIterable(eval_dataset, batch_size, labels)
    else:
        eval_torch_iterable = None

    return train_torch_iterable, eval_torch_iterable
