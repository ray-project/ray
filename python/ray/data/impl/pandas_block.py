import collections
import random
import heapq
from typing import Dict, Iterator, List, Union, Tuple, Any, TypeVar, Optional, \
    TYPE_CHECKING

import numpy as np

try:
    import pandas
except ImportError:
    pandas = None

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.block_builder import BlockBuilder
from ray.data.impl.simple_block import SimpleBlockBuilder
from ray.data.impl.table_block import TableRow, TableBlockBuilder
from ray.data.aggregate import AggregateFn
from ray.data.impl.size_estimator import SizeEstimator

if TYPE_CHECKING:
    import pandas

T = TypeVar("T")

# TODO
# # An Arrow block can be sorted by a list of (column, asc/desc) pairs,
# # e.g. [("column1", "ascending"), ("column2", "descending")]
# SortKeyT = List[Tuple[str, str]]
# GroupKeyT = Union[None, str]


class PandasRow(TableRow):
    def as_pydict(self) -> dict:
        # TODO: If the type of column name is not str, e.g. int. The result keys will stay as int.
        # Should we enforce `str(k)`?
        return {k: v[0] for k, v in df.to_dict("list").items()}

    def __getitem__(self, key: str) -> Any:
        col = self._row[key]
        if len(col) == 0:
            return None
        item = col[0]
        try:
            # Try to interpret this as a numpy-type value.
            # See https://stackoverflow.com/questions/9452775/converting-numpy-dtypes-to-native-python-types.
            return item.item()
        except AttributeError:
            # Fallback to the original form.
            return item

    def __len__(self):
        return self._row.df.shape[1]


class PandasBlockBuilder(TableBlockBuilder[T]):
    def __init__(self):
        if pandas is None:
            raise ImportError("Run `pip install pandas` for pandas support")
        TableBlockBuilder.__init()

    def _table_from_pydict(self, columns: Dict[str, List[Any]]) -> Block:
        return pandas.DataFrame(columns)

    def _concat_tables(self, tables: List[Block]) -> Block:
        return pandas.DataFrame.concat(tables, ignore_index=True)

    def _empty_table(self):
        raise pandas.DataFrame()
