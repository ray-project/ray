from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Iterable

from ray.air import session
from ray.data.dataset import Dataset
import datasets.iterable_dataset

from torch.utils.data import IterableDataset
import torch
from composer.core import DataSpec
from ray.data.extensions import TensorArray


class RayDatasetMosaicIterable(torch.utils.data.IterableDataset):
    """Mosaic ExamplesIterable backed by a Ray Dataset."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.generate_examples_fn = self.dataset.iter_rows

        # Required for the superclass
        self.kwargs = {}

    def __iter__(self):
        for row in self.generate_examples_fn(**self.kwargs):
            yield (row[0],torch.tensor(row[1]))
    
    def __len__(self):
        return self.dataset.count()

class MosaicDataset(torch.utils.data.IterableDataset):
    def __init__(self, mosaic_iterable):
        super(MosaicDataset).__init__()
        self.total_length = len(mosaic_iterable)
        self.dataset = mosaic_iterable

    def __iter__(self):
        return iter(self.dataset)
    
    def __len__(self):
        return len(self.dataset)

def process_dataset_for_mosaic(dataset: Dataset) -> "IterableDataset":
#     """Converts a Ray Dataset into a HF IterableDataset."""
    mosaic_iterable = RayDatasetMosaicIterable(dataset)
    
    return mosaic_iterable

    # iterable_dataset = datasets.iterable_dataset.IterableDataset(
    #     mosaic_iterable, format_type="torch"
    # ).with_format("torch")

    # return iterable_dataset

# def process_dataset_for_mosaic(dataset: Dataset):
#     return DataSpec(RayDatasetMosaicIterable(dataset), get_num_samples_in_batch=lambda x:len(x[0]))
#     return DataSpec(dataset.iter_rows())
#     return MosaicDataset(RayDatasetMosaicIterable(dataset))
    
#     return IterableDataset(mosaic_iterable)
#     return IterableDataset(RayDatasetMosaicIterable(dataset))
#     def inverse_order(batch: Tuple[torch.Tensor, int]):
#         images = TensorArray([image.numpy() for image, _ in batch])
#         labels = [label for _, label in batch]
#         return [images, labels]

#     # return DataSpec(dataset.map_batches(inverse_order, batch_size=dataset.count()).iter_batches(), get_num_samples_in_batch=lambda x: len(x))
#     return DataSpec(RayDatasetMosaicIterable(dataset), get_num_samples_in_batch=lambda x: 1024)
#     # return dataset.iter_batches()


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