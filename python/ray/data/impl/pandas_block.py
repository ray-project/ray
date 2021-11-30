from typing import Dict, List, Tuple, Any, TypeVar, Optional, TYPE_CHECKING

import collections
import numpy as np

try:
    import pandas
except ImportError:
    pandas = None

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.table_block import TableBlockAccessor, TableRow, \
    TableBlockBuilder, SortKeyT, GroupKeyT
from ray.data.impl.arrow_block import ArrowBlockAccessor
from ray.data.aggregate import AggregateFn

if TYPE_CHECKING:
    import pyarrow
    import pandas

T = TypeVar("T")


class PandasRow(TableRow):
    def as_pydict(self) -> dict:
        # TODO (kfsorm): If the type of column name is not str, e.g. int.
        # The result keys will stay as int. Should we enforce `str(k)`?
        # Maybe we don't even allow non-str column names?
        return {k: v[0] for k, v in self._row.to_dict("list").items()}

    def __getitem__(self, key: str) -> Any:
        assert isinstance(key, str)
        col = self._row[key]
        print(col)
        if len(col) == 0:
            return None
        item = col.iloc[0]
        try:
            # Try to interpret this as a numpy-type value.
            # See https://stackoverflow.com/questions/9452775/converting-numpy-dtypes-to-native-python-types.  # noqa: E501
            return item.item()
        except AttributeError:
            # Fallback to the original form.
            return item

    def __len__(self):
        return self._row.shape[1]


class PandasBlockBuilder(TableBlockBuilder[T]):
    def __init__(self):
        if pandas is None:
            raise ImportError("Run `pip install pandas` for Pandas support")
        TableBlockBuilder.__init__(self, pandas.DataFrame)

    def _table_from_pydict(self, columns: Dict[str, List[Any]]) -> Block:
        return pandas.DataFrame(columns)

    def _concat_tables(self, tables: List[Block]) -> Block:
        return pandas.concat(tables, ignore_index=True)

    @staticmethod
    def _empty_table() -> "pandas.DataFrame":
        return pandas.DataFrame()


# This is to be compatible with pyarrow.lib.schema
# TODO (kfstorm): We need a format-independent way to represent schema.
PandasBlockSchema = collections.namedtuple("PandasBlockSchema",
                                           ["names", "types"])


class PandasBlockAccessor(TableBlockAccessor):
    def __init__(self, table: "pandas.DataFrame"):
        if pandas is None:
            raise ImportError("Run `pip install pandas` for Pandas support")
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
        dtypes = self._table.dtypes
        return PandasBlockSchema(
            names=dtypes.index.tolist(), types=dtypes.values.tolist())

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table

    def to_numpy(self, column: str = None) -> np.ndarray:
        if not column:
            raise ValueError(
                "`column` must be specified when calling .to_numpy() "
                "on Pandas blocks.")
        if column not in self._table.columns:
            raise ValueError(
                "Cannot find column {}, available columns: {}".format(
                    column, self._table.columns.tolist()))
        return self._table[column].to_numpy()

    def to_arrow(self) -> "pyarrow.Table":
        import pyarrow
        return pyarrow.table(self._table)

    def num_rows(self) -> int:
        return self._table.shape[0]

    def size_bytes(self) -> int:
        # TODO (kfstorm): Should we count index?
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

    @staticmethod
    def _empty_table() -> "pandas.DataFrame":
        return PandasBlockBuilder._empty_table()

    def _sample(self, n_samples: int, key: SortKeyT) -> "pandas.DataFrame":
        return self._table[[k[0] for k in key]].sample(
            n_samples, ignore_index=True)

    def sort_and_partition(self, boundaries: List[T], key: SortKeyT,
                           descending: bool) -> List["Block[T]"]:
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        delegated_result = BlockAccessor.for_block(
            self.to_arrow()).sort_and_partition(boundaries, key, descending)
        return [
            BlockAccessor.for_block(_).to_pandas() for _ in delegated_result
        ]

    def combine(self, key: GroupKeyT,
                aggs: Tuple[AggregateFn]) -> Block[PandasRow]:
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        return BlockAccessor.for_block(self.to_arrow()).combine(
            key, aggs).to_pandas()

    @staticmethod
    def merge_sorted_blocks(
            blocks: List[Block[T]], key: SortKeyT,
            _descending: bool) -> Tuple[Block[T], BlockMetadata]:
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        block, metadata = ArrowBlockAccessor.merge_sorted_blocks(
            [BlockAccessor.for_block(block).to_arrow() for block in blocks],
            key, _descending)
        return BlockAccessor.for_block(block).to_pandas(), metadata

    @staticmethod
    def aggregate_combined_blocks(blocks: List[Block[PandasRow]],
                                  key: GroupKeyT, aggs: Tuple[AggregateFn]
                                  ) -> Tuple[Block[PandasRow], BlockMetadata]:
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        block, metadata = ArrowBlockAccessor.aggregate_combined_blocks(
            [BlockAccessor.for_block(block).to_arrow() for block in blocks],
            key, aggs)
        return BlockAccessor.for_block(block).to_pandas(), metadata
