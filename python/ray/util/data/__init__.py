from collections import defaultdict
from typing import Iterable

import pandas as pd

from ray.util.data.dataset import MLDataset
from ray.util.data.parquet import read_parquet
from ray.util.iter import T, ParallelIterator

try:
    import dataclasses
except:  # noqa: E722
    pass
else:
    from dataclasses import is_dataclass


def to_pandas(
    it: ParallelIterator[T], batch_size: int = 32
) -> "ParallelIterator[pd.DataFrame]":
    """Convert the a ParallelIterator to ParallelIterator of pd.DataFrame.

    The record type should be list like object or dataclass instance. If
    the record is a iterable, we will convert to a list. If the record has
    __getitem__ attr, we will use __getitem__ to get the given column
    indexes data to create pandas DataFrame. If the record is dataclass
    instance we will use __getattr__ to get the given column.

    Args:
        it (ParallelIterator[T]): the ParallelIterator to converted
        batch_size (int): batch the given size to create a pandas DataFrame
    Returns:
        A ParallelIterator of pd.DataFrame
    """
    it = it.batch(batch_size)

    def convert_fn(input_it: Iterable[T]) -> Iterable[pd.DataFrame]:
        names = []
        for batch in input_it:
            assert isinstance(batch, list)
            if hasattr(batch[0], "__getitem__"):
                batch = pd.DataFrame(batch)
            elif hasattr(batch[0], "__iter__"):
                batch = [list(item) for item in batch]
                batch = pd.DataFrame(batch)
            elif is_dataclass(batch[0]):
                if not names:
                    names = [f.name for f in dataclasses.fields(batch[0])]
                values = defaultdict(lambda x: [])
                for item in batch:
                    for col in names:
                        values[col].append(getattr(item, col))
                batch = pd.DataFrame(values, columns=names)
            else:
                raise ValueError(
                    "MLDataset only support list like item or " "dataclass instance"
                )

            yield batch

    it = it._with_transform(
        lambda local_it: local_it.transform(convert_fn), ".to_pandas()"
    )
    return it


def from_parallel_iter(
    para_it: ParallelIterator[T],
    need_convert: bool = True,
    batch_size: int = 32,
    repeated: bool = False,
) -> MLDataset:
    """Create a MLDataset from an existing ParallelIterator.

    The object of the ParallelIterator should be list like object or dataclass
    instance.

    Args:
        para_it (ParallelIterator[T]): An existing parallel iterator, and each
            should be a list like object or dataclass instance.
        need_convert (bool): whether need to convert to pandas.DataFrame. This
            should be False if the record type is pandas.DataFrame.
        batch_size (int): if need_convert is True, we will batch the batch_size
            records to a pandas.DataFrame
        repeated (bool): whether the para_it is repeated.
    Returns:
        a MLDataset
    """

    if need_convert:
        para_it = to_pandas(para_it, batch_size)
    else:
        batch_size = 0

    return MLDataset.from_parallel_it(para_it, batch_size, repeated)


__all__ = ["from_parallel_iter", "read_parquet", "MLDataset"]
