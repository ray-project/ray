from typing import Dict, List, Union, Tuple, Any, TypeVar, Optional, \
    TYPE_CHECKING

import numpy as np

try:
    import pandas
except ImportError:
    pandas = None

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.table_block import TableBlockAccessor, TableRow, \
    TableBlockBuilder
from ray.data.aggregate import AggregateFn

if TYPE_CHECKING:
    import pyarrow
    import pandas

T = TypeVar("T")

# TODO
# An Arrow block can be sorted by a list of (column, asc/desc) pairs,
# e.g. [("column1", "ascending"), ("column2", "descending")]
SortKeyT = List[Tuple[str, str]]
GroupKeyT = Union[None, str]


class PandasRow(TableRow):
    def as_pydict(self) -> dict:
        # TODO: If the type of column name is not str, e.g. int.
        # The result keys will stay as int. Should we enforce `str(k)`?
        return {k: v[0] for k, v in self._row.to_dict("list").items()}

    def __getitem__(self, key: str) -> Any:
        col = self._row[key]
        if len(col) == 0:
            return None
        item = col[0]
        try:
            # Try to interpret this as a numpy-type value.
            # See https://stackoverflow.com/questions/9452775/converting-numpy-dtypes-to-native-python-types.  # noqa: E501
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
        TableBlockBuilder.__init__(self, pandas.DataFrame)

    def _table_from_pydict(self, columns: Dict[str, List[Any]]) -> Block:
        return pandas.DataFrame(columns)

    def _concat_tables(self, tables: List[Block]) -> Block:
        return pandas.DataFrame.concat(tables, ignore_index=True)

    def _empty_table(self):
        raise pandas.DataFrame()


class PandasBlockSchema:
    # This is to be compatible with pyarrow.lib.schema
    def __init__(self, names, types):
        self.names = names
        self.types = types

class PandasBlockAccessor(TableBlockAccessor):
    def __init__(self, table: "pandas.DataFrame"):
        if pandas is None:
            raise ImportError("Run `pip install pandas` for pandas support")
        TableBlockAccessor.__init__(self, table)

    def _create_table_row(self, row: "pandas.DataFrame") -> PandasRow:
        return PandasRow(row)

    def slice(self, start: int, end: int, copy: bool) -> "pandas.DataFrame":
        view = self._table[start:end]
        if copy:
            view = view.copy(deep=True)
        return view

    def random_shuffle(self, random_seed: Optional[int]) -> List[T]:
        return self._table.sample(frac=1, random_state=random_seed)

    def schema(self) -> Any:
        # TODO: No native representation in pandas.
        dtypes = self._table.dtypes
        return PandasBlockSchema(names=dtypes.index.tolist(), types=dtypes.values.tolist())

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table

    def to_numpy(self, column: str = None) -> np.ndarray:
        if not column:
            raise ValueError(
                "`column` must be specified when calling .to_numpy() "
                "on pandas blocks.")
        if column not in self._table.columns:
            raise ValueError(
                "Cannot find column {}, available columns: {}".format(
                    column, self._table.columns.tolist()))
        return self._table[column].to_numpy()

    def to_arrow(self) -> "pyarrow.Table":
        return pyarrow.table(self._table)

    def num_rows(self) -> int:
        return self._table.shape[0]

    def size_bytes(self) -> int:
        # TODO: Should we count index?
        return self._table.memory_usage(index=True, deep=True).sum()

    def _zip(self, acc: BlockAccessor) -> "Block[T]":
        r = self.to_pandas().copy(deep=False)
        s = acc.to_pandas()
        for col_name in s.columns:
            col = s[col_name]
            # Ensure the column names are unique after zip.
            if col_name in r.column_names:
                i = 1
                new_name = col_name
                while new_name in r.column_names:
                    new_name = "{}_{}".format(col_name, i)
                    i += 1
                col_name = new_name
            r[col_name] = col
        return r

    @staticmethod
    def builder() -> PandasBlockBuilder[T]:
        return PandasBlockBuilder()

    def sample(self, n_samples: int, key: SortKeyT) -> "pandas.DataFrame":
        # TODO: to be implemented
        raise NotImplementedError

    def sort_and_partition(self, boundaries: List[T], key: SortKeyT,
                           descending: bool) -> List["Block[T]"]:
        # TODO: to be implemented
        raise NotImplementedError

    def combine(self, key: GroupKeyT,
                aggs: Tuple[AggregateFn]) -> Block[PandasRow]:
        # TODO: to be implemented
        raise NotImplementedError

    @staticmethod
    def merge_sorted_blocks(
            blocks: List[Block[T]], key: SortKeyT,
            _descending: bool) -> Tuple[Block[T], BlockMetadata]:
        # TODO: to be implemented
        raise NotImplementedError

    @staticmethod
    def aggregate_combined_blocks(blocks: List[Block[PandasRow]],
                                  key: GroupKeyT, aggs: Tuple[AggregateFn]
                                  ) -> Tuple[Block[PandasRow], BlockMetadata]:
        # TODO: to be implemented
        raise NotImplementedError
