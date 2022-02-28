from typing import Dict, List, Tuple, Iterator, Any, TypeVar, Optional, TYPE_CHECKING

import collections
import numpy as np

from ray.data.block import BlockAccessor, BlockMetadata, KeyFn
from ray.data.row import TableRow
from ray.data.impl.table_block import TableBlockAccessor, TableBlockBuilder
from ray.data.impl.arrow_block import ArrowBlockAccessor
from ray.data.aggregate import AggregateFn

if TYPE_CHECKING:
    import pyarrow
    import pandas
    from ray.data.impl.sort import SortKeyT

T = TypeVar("T")

_pandas = None


def lazy_import_pandas():
    global _pandas
    if _pandas is None:
        import pandas

        _pandas = pandas
    return _pandas


class PandasRow(TableRow):
    """
    Row of a tabular Dataset backed by a Pandas DataFrame block.
    """

    def __getitem__(self, key: str) -> Any:
        col = self._row[key]
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

    def __iter__(self) -> Iterator:
        for k in self._row.columns:
            yield k

    def __len__(self):
        return self._row.shape[1]


class PandasBlockBuilder(TableBlockBuilder[T]):
    def __init__(self):
        pandas = lazy_import_pandas()
        super().__init__(pandas.DataFrame)

    def _table_from_pydict(self, columns: Dict[str, List[Any]]) -> "pandas.DataFrame":
        pandas = lazy_import_pandas()
        return pandas.DataFrame(columns)

    def _concat_tables(self, tables: List["pandas.DataFrame"]) -> "pandas.DataFrame":
        pandas = lazy_import_pandas()
        return pandas.concat(tables, ignore_index=True)

    @staticmethod
    def _empty_table() -> "pandas.DataFrame":
        pandas = lazy_import_pandas()
        return pandas.DataFrame()


# This is to be compatible with pyarrow.lib.schema
# TODO (kfstorm): We need a format-independent way to represent schema.
PandasBlockSchema = collections.namedtuple("PandasBlockSchema", ["names", "types"])


class PandasBlockAccessor(TableBlockAccessor):
    def __init__(self, table: "pandas.DataFrame"):
        super().__init__(table)

    def _create_table_row(self, row: "pandas.DataFrame") -> PandasRow:
        return PandasRow(row)

    def slice(self, start: int, end: int, copy: bool) -> "pandas.DataFrame":
        view = self._table[start:end]
        if copy:
            view = view.copy(deep=True)
        return view

    def random_shuffle(self, random_seed: Optional[int]) -> "pandas.DataFrame":
        return self._table.sample(frac=1, random_state=random_seed)

    def schema(self) -> PandasBlockSchema:
        dtypes = self._table.dtypes
        schema = PandasBlockSchema(
            names=dtypes.index.tolist(), types=dtypes.values.tolist()
        )
        # Column names with non-str types of a pandas DataFrame is not
        # supported by Ray Dataset.
        if any(not isinstance(name, str) for name in schema.names):
            raise ValueError(
                "A Pandas DataFrame with column names of non-str types"
                " is not supported by Ray Dataset. Column names of this"
                f" DataFrame: {schema.names!r}."
            )
        return schema

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table

    def to_numpy(self, column: str = None) -> np.ndarray:
        if not column:
            raise ValueError(
                "`column` must be specified when calling .to_numpy() "
                "on Pandas blocks."
            )
        if column not in self._table.columns:
            raise ValueError(
                "Cannot find column {}, available columns: {}".format(
                    column, self._table.columns.tolist()
                )
            )
        return self._table[column].to_numpy()

    def to_arrow(self) -> "pyarrow.Table":
        import pyarrow

        return pyarrow.table(self._table)

    def num_rows(self) -> int:
        return self._table.shape[0]

    def size_bytes(self) -> int:
        return self._table.memory_usage(index=True, deep=True).sum()

    def _zip(self, acc: BlockAccessor) -> "pandas.DataFrame":
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

    def _sample(self, n_samples: int, key: "SortKeyT") -> "pandas.DataFrame":
        return self._table[[k[0] for k in key]].sample(n_samples, ignore_index=True)

    def sort_and_partition(
        self, boundaries: List[T], key: "SortKeyT", descending: bool
    ) -> List["pandas.DataFrame"]:
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        delegated_result = BlockAccessor.for_block(self.to_arrow()).sort_and_partition(
            boundaries, key, descending
        )
        return [BlockAccessor.for_block(_).to_pandas() for _ in delegated_result]

    def combine(self, key: KeyFn, aggs: Tuple[AggregateFn]) -> "pandas.DataFrame":
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        return BlockAccessor.for_block(self.to_arrow()).combine(key, aggs).to_pandas()

    @staticmethod
    def merge_sorted_blocks(
        blocks: List["pandas.DataFrame"], key: "SortKeyT", _descending: bool
    ) -> Tuple["pandas.DataFrame", BlockMetadata]:
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        block, metadata = ArrowBlockAccessor.merge_sorted_blocks(
            [BlockAccessor.for_block(block).to_arrow() for block in blocks],
            key,
            _descending,
        )
        return BlockAccessor.for_block(block).to_pandas(), metadata

    @staticmethod
    def aggregate_combined_blocks(
        blocks: List["pandas.DataFrame"], key: KeyFn, aggs: Tuple[AggregateFn]
    ) -> Tuple["pandas.DataFrame", BlockMetadata]:
        # TODO (kfstorm): A workaround to pass tests. Not efficient.
        block, metadata = ArrowBlockAccessor.aggregate_combined_blocks(
            [BlockAccessor.for_block(block).to_arrow() for block in blocks], key, aggs
        )
        return BlockAccessor.for_block(block).to_pandas(), metadata
