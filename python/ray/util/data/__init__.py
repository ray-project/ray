from typing import Callable, List

import ray.util.iter as parallel_iter
from ray.util.data.dataset import Dataset
from ray.util.data.distributed_dataset import (item_check, DistributedDataset,
                                               PandasDistributedDataset)
from ray.util.iter import T, ParallelIterator


def from_parallel_iter(para_it: ParallelIterator[T],
                       batch_size: int = 0,
                       add_type_check: bool = False,
                       item_check_fn: Callable[[T], bool] = item_check
                       ) -> DistributedDataset[T]:
    """Create a DistributedDataset from an existing ParallelIterator.

    The object of the ParallelIterator should be list like object or dataclass
    instance.

    Args:
        para_it (ParallelIterator[T]): An existing parallel iterator, and each
            should be a list like object or dataclass instance.
        batch_size (int): The batch size of the item.
        add_type_check (bool): Whether we need to wrap the transform function
            with type check.
        item_check_fn (Callable[[T], bool]): A check function for each item.
    Returns:
        a DistributedDataset
    """
    return DistributedDataset(para_it, batch_size, add_type_check,
                              item_check_fn)


def from_items(items: List[T], num_shards: int = 2,
               repeat: bool = False) -> "DistributedDataset[T]":
    """
    Create a DistributedDataset from an existing set of objects. The object
    should be list like object or dataclass instance.
    """
    it = parallel_iter.from_items(items, num_shards, repeat)
    return from_parallel_iter(it)


__all__ = [
    "from_items", "from_parallel_iter", "Dataset", "DistributedDataset",
    "PandasDistributedDataset"
]
