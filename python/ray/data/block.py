import collections
import logging
import time
from dataclasses import dataclass, fields
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np
import pyarrow as pa

import ray
from ray.air.util.tensor_extensions.arrow import ArrowConversionError
from ray.data._internal.util import _check_pyarrow_version, _truncated_repr
from ray.types import ObjectRef
from ray.util import log_once
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pandas
    import pyarrow

    from ray.data._internal.block_builder import BlockBuilder
    from ray.data._internal.pandas_block import PandasBlockSchema
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey
    from ray.data.aggregate import AggregateFn


T = TypeVar("T", contravariant=True)
U = TypeVar("U", covariant=True)

KeyType = TypeVar("KeyType")
AggType = TypeVar("AggType")


# Represents a batch of records to be stored in the Ray object store.
#
# Block data can be accessed in a uniform way via ``BlockAccessors`` like`
# ``ArrowBlockAccessor``.
Block = Union["pyarrow.Table", "pandas.DataFrame"]

# Represents the schema of a block, which can be either a Python type or a
# pyarrow schema. This is used to describe the structure of the data in a block.
Schema = Union[type, "PandasBlockSchema", "pyarrow.lib.Schema"]

# Represents a single column of the ``Block``
BlockColumn = Union["pyarrow.ChunkedArray", "pyarrow.Array", "pandas.Series"]


logger = logging.getLogger(__name__)


@DeveloperAPI
class BlockType(Enum):
    ARROW = "arrow"
    PANDAS = "pandas"


# User-facing data batch type. This is the data type for data that is supplied to and
# returned from batch UDFs.
DataBatch = Union["pyarrow.Table", "pandas.DataFrame", Dict[str, np.ndarray]]

# User-facing data column type. This is the data type for data that is supplied to and
# returned from column UDFs.
DataBatchColumn = Union[BlockColumn, np.ndarray]


# A class type that implements __call__.
CallableClass = type


class _CallableClassProtocol(Protocol[T, U]):
    def __call__(self, __arg: T) -> Union[U, Iterator[U]]:
        ...


# A user defined function passed to map, map_batches, ec.
UserDefinedFunction = Union[
    Callable[[T], U],
    Callable[[T], Iterator[U]],
    "_CallableClassProtocol",
]

# A list of block references pending computation by a single task. For example,
# this may be the output of a task reading a file.
BlockPartition = List[Tuple[ObjectRef[Block], "BlockMetadata"]]

# The metadata that describes the output of a BlockPartition. This has the
# same type as the metadata that describes each block in the partition.
BlockPartitionMetadata = List["BlockMetadata"]

VALID_BATCH_FORMATS = ["pandas", "pyarrow", "numpy", None]
DEFAULT_BATCH_FORMAT = "numpy"


def _is_empty_schema(schema: Optional[Schema]) -> bool:
    from ray.data._internal.pandas_block import PandasBlockSchema

    return schema is None or (
        not schema.names
        if isinstance(schema, PandasBlockSchema)
        else not schema  # pyarrow schema check
    )


def _take_first_non_empty_schema(schemas: Iterator["Schema"]) -> Optional["Schema"]:
    """Return the first non-empty schema from an iterator of schemas.

    Args:
        schemas: Iterator of schemas to check.

    Returns:
        The first non-empty schema, or None if all schemas are empty.
    """
    for schema in schemas:
        if not _is_empty_schema(schema):
            return schema
    return None


def _apply_batch_format(given_batch_format: Optional[str]) -> str:
    if given_batch_format == "default":
        given_batch_format = DEFAULT_BATCH_FORMAT
    if given_batch_format not in VALID_BATCH_FORMATS:
        raise ValueError(
            f"The given batch format {given_batch_format} isn't allowed (must be one of"
            f" {VALID_BATCH_FORMATS})."
        )
    return given_batch_format


@DeveloperAPI
def to_stats(metas: List["BlockMetadata"]) -> List["BlockStats"]:
    return [m.to_stats() for m in metas]


@DeveloperAPI
class BlockExecStats:
    """Execution stats for this block.

    Attributes:
        wall_time_s: The wall-clock time it took to compute this block.
        cpu_time_s: The CPU time it took to compute this block.
        node_id: A unique id for the node that computed this block.
        max_uss_bytes: An estimate of the maximum amount of physical memory that the
            process was using while computing this block.
    """

    def __init__(self):
        self.start_time_s: Optional[float] = None
        self.end_time_s: Optional[float] = None
        self.wall_time_s: Optional[float] = None
        self.udf_time_s: Optional[float] = 0
        self.cpu_time_s: Optional[float] = None
        self.node_id = ray.runtime_context.get_runtime_context().get_node_id()
        self.max_uss_bytes: int = 0
        self.task_idx: Optional[int] = None

    @staticmethod
    def builder() -> "_BlockExecStatsBuilder":
        return _BlockExecStatsBuilder()

    def __repr__(self):
        return repr(
            {
                "wall_time_s": self.wall_time_s,
                "cpu_time_s": self.cpu_time_s,
                "udf_time_s": self.udf_time_s,
                "node_id": self.node_id,
            }
        )


class _BlockExecStatsBuilder:
    """Helper class for building block stats.

    When this class is created, we record the start time. When build() is
    called, the time delta is saved as part of the stats.
    """

    def __init__(self):
        self._start_time = time.perf_counter()
        self._start_cpu = time.process_time()

    def build(self) -> "BlockExecStats":
        # Record end times.
        end_time = time.perf_counter()
        end_cpu = time.process_time()

        # Build the stats.
        stats = BlockExecStats()
        stats.start_time_s = self._start_time
        stats.end_time_s = end_time
        stats.wall_time_s = end_time - self._start_time
        stats.cpu_time_s = end_cpu - self._start_cpu

        return stats


@DeveloperAPI
@dataclass
class BlockStats:
    """Statistics about the block produced"""

    #: The number of rows contained in this block, or None.
    num_rows: Optional[int]
    #: The approximate size in bytes of this block, or None.
    size_bytes: Optional[int]
    #: Execution stats for this block.
    exec_stats: Optional[BlockExecStats]

    def __post_init__(self):
        if self.size_bytes is not None:
            # Require size_bytes to be int, ray.util.metrics objects
            # will not take other types like numpy.int64
            assert isinstance(self.size_bytes, int)


_BLOCK_STATS_FIELD_NAMES = {f.name for f in fields(BlockStats)}


@DeveloperAPI
@dataclass
class BlockMetadata(BlockStats):
    """Metadata about the block."""

    #: The pyarrow schema or types of the block elements, or None.
    #: The list of file paths used to generate this block, or
    #: the empty list if indeterminate.
    input_files: Optional[List[str]]

    def to_stats(self):
        return BlockStats(
            **{key: self.__getattribute__(key) for key in _BLOCK_STATS_FIELD_NAMES}
        )

    def __post_init__(self):
        super().__post_init__()

        if self.input_files is None:
            self.input_files = []


@DeveloperAPI(stability="alpha")
@dataclass
class BlockMetadataWithSchema(BlockMetadata):
    schema: Optional[Schema] = None

    def __init__(self, metadata: BlockMetadata, schema: Optional["Schema"] = None):
        super().__init__(
            input_files=metadata.input_files,
            size_bytes=metadata.size_bytes,
            num_rows=metadata.num_rows,
            exec_stats=metadata.exec_stats,
        )
        self.schema = schema

    def from_block(
        block: Block, stats: Optional["BlockExecStats"] = None
    ) -> "BlockMetadataWithSchema":
        accessor = BlockAccessor.for_block(block)
        meta = accessor.get_metadata(exec_stats=stats)
        schema = accessor.schema()
        return BlockMetadataWithSchema(metadata=meta, schema=schema)

    @property
    def metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self.num_rows,
            size_bytes=self.size_bytes,
            exec_stats=self.exec_stats,
            input_files=self.input_files,
        )


@DeveloperAPI
class BlockAccessor:
    """Provides accessor methods for a specific block.

    Ideally, we wouldn't need a separate accessor classes for blocks. However,
    this is needed if we want to support storing ``pyarrow.Table`` directly
    as a top-level Ray object, without a wrapping class (issue #17186).
    """

    def num_rows(self) -> int:
        """Return the number of rows contained in this block."""
        raise NotImplementedError

    def iter_rows(self, public_row_format: bool) -> Iterator[T]:
        """Iterate over the rows of this block.

        Args:
            public_row_format: Whether to cast rows into the public Dict row
                format (this incurs extra copy conversions).
        """
        raise NotImplementedError

    def slice(self, start: int, end: int, copy: bool = False) -> Block:
        """Return a slice of this block.

        Args:
            start: The starting index of the slice (inclusive).
            end: The ending index of the slice (exclusive).
            copy: Whether to perform a data copy for the slice.

        Returns:
            The sliced block result.
        """
        raise NotImplementedError

    def take(self, indices: List[int]) -> Block:
        """Return a new block containing the provided row indices.

        Args:
            indices: The row indices to return.

        Returns:
            A new block containing the provided row indices.
        """
        raise NotImplementedError

    def select(self, columns: List[Optional[str]]) -> Block:
        """Return a new block containing the provided columns."""
        raise NotImplementedError

    def rename_columns(self, columns_rename: Dict[str, str]) -> Block:
        """Return the block reflecting the renamed columns."""
        raise NotImplementedError

    def random_shuffle(self, random_seed: Optional[int]) -> Block:
        """Randomly shuffle this block."""
        raise NotImplementedError

    def to_pandas(self) -> "pandas.DataFrame":
        """Convert this block into a Pandas dataframe."""
        raise NotImplementedError

    def to_numpy(
        self, columns: Optional[Union[str, List[str]]] = None
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        """Convert this block (or columns of block) into a NumPy ndarray.

        Args:
            columns: Name of columns to convert, or None if converting all columns.
        """
        raise NotImplementedError

    def to_arrow(self) -> "pyarrow.Table":
        """Convert this block into an Arrow table."""
        raise NotImplementedError

    def to_block(self) -> Block:
        """Return the base block that this accessor wraps."""
        raise NotImplementedError

    def to_default(self) -> Block:
        """Return the default data format for this accessor."""
        return self.to_block()

    def to_batch_format(self, batch_format: Optional[str]) -> DataBatch:
        """Convert this block into the provided batch format.

        Args:
            batch_format: The batch format to convert this block to.

        Returns:
            This block formatted as the provided batch format.
        """
        if batch_format is None:
            return self.to_block()
        elif batch_format == "default" or batch_format == "native":
            return self.to_default()
        elif batch_format == "pandas":
            return self.to_pandas()
        elif batch_format == "pyarrow":
            return self.to_arrow()
        elif batch_format == "numpy":
            return self.to_numpy()
        else:
            raise ValueError(
                f"The batch format must be one of {VALID_BATCH_FORMATS}, got: "
                f"{batch_format}"
            )

    def size_bytes(self) -> int:
        """Return the approximate size in bytes of this block."""
        raise NotImplementedError

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the Python type or pyarrow schema of this block."""
        raise NotImplementedError

    def get_metadata(
        self,
        input_files: Optional[List[str]] = None,
        exec_stats: Optional[BlockExecStats] = None,
    ) -> BlockMetadata:
        """Create a metadata object from this block."""
        return BlockMetadata(
            num_rows=self.num_rows(),
            size_bytes=self.size_bytes(),
            input_files=input_files,
            exec_stats=exec_stats,
        )

    def zip(self, other: "Block") -> "Block":
        """Zip this block with another block of the same type and size."""
        raise NotImplementedError

    @staticmethod
    def builder() -> "BlockBuilder":
        """Create a builder for this block type."""
        raise NotImplementedError

    @classmethod
    def batch_to_block(
        cls,
        batch: DataBatch,
        block_type: Optional[BlockType] = None,
    ) -> Block:
        """Create a block from user-facing data formats."""

        if isinstance(batch, np.ndarray):
            raise ValueError(
                f"Error validating {_truncated_repr(batch)}: "
                "Standalone numpy arrays are not "
                "allowed in Ray 2.5. Return a dict of field -> array, "
                "e.g., `{'data': array}` instead of `array`."
            )

        elif isinstance(batch, collections.abc.Mapping):
            if block_type is None or block_type == BlockType.ARROW:
                try:
                    return cls.batch_to_arrow_block(batch)
                except ArrowConversionError as e:
                    if log_once("_fallback_to_pandas_block_warning"):
                        logger.warning(
                            f"Failed to convert batch to Arrow due to: {e}; "
                            f"falling back to Pandas block"
                        )

                    if block_type is None:
                        return cls.batch_to_pandas_block(batch)
                    else:
                        raise e
            else:
                assert block_type == BlockType.PANDAS
                return cls.batch_to_pandas_block(batch)
        return batch

    @classmethod
    def batch_to_arrow_block(cls, batch: Dict[str, Any]) -> Block:
        """Create an Arrow block from user-facing data formats."""
        from ray.data._internal.arrow_block import ArrowBlockBuilder

        return ArrowBlockBuilder._table_from_pydict(batch)

    @classmethod
    def batch_to_pandas_block(cls, batch: Dict[str, Any]) -> Block:
        """Create a Pandas block from user-facing data formats."""
        from ray.data._internal.pandas_block import PandasBlockBuilder

        return PandasBlockBuilder._table_from_pydict(batch)

    @staticmethod
    def for_block(block: Block) -> "BlockAccessor[T]":
        """Create a block accessor for the given block."""
        _check_pyarrow_version()
        import pandas
        import pyarrow

        if isinstance(block, pyarrow.Table):
            from ray.data._internal.arrow_block import ArrowBlockAccessor

            return ArrowBlockAccessor(block)
        elif isinstance(block, pandas.DataFrame):
            from ray.data._internal.pandas_block import PandasBlockAccessor

            return PandasBlockAccessor(block)
        elif isinstance(block, bytes):
            from ray.data._internal.arrow_block import ArrowBlockAccessor

            return ArrowBlockAccessor.from_bytes(block)
        elif isinstance(block, list):
            raise ValueError(
                f"Error validating {_truncated_repr(block)}: "
                "Standalone Python objects are not "
                "allowed in Ray 2.5. To use Python objects in a dataset, "
                "wrap them in a dict of numpy arrays, e.g., "
                "return `{'item': batch}` instead of just `batch`."
            )
        else:
            raise TypeError("Not a block type: {} ({})".format(block, type(block)))

    def sample(self, n_samples: int, sort_key: "SortKey") -> "Block":
        """Return a random sample of items from this block."""
        raise NotImplementedError

    def count(self, on: str, ignore_nulls: bool = False) -> Optional[U]:
        """Returns a count of the distinct values in the provided column"""
        raise NotImplementedError

    def sum(self, on: str, ignore_nulls: bool) -> Optional[U]:
        """Returns a sum of the values in the provided column"""
        raise NotImplementedError

    def min(self, on: str, ignore_nulls: bool) -> Optional[U]:
        """Returns a min of the values in the provided column"""
        raise NotImplementedError

    def max(self, on: str, ignore_nulls: bool) -> Optional[U]:
        """Returns a max of the values in the provided column"""
        raise NotImplementedError

    def mean(self, on: str, ignore_nulls: bool) -> Optional[U]:
        """Returns a mean of the values in the provided column"""
        raise NotImplementedError

    def sum_of_squared_diffs_from_mean(
        self,
        on: str,
        ignore_nulls: bool,
        mean: Optional[U] = None,
    ) -> Optional[U]:
        """Returns a sum of diffs (from mean) squared for the provided column"""
        raise NotImplementedError

    def sort(self, sort_key: "SortKey") -> "Block":
        """Returns new block sorted according to provided `sort_key`"""
        raise NotImplementedError

    def sort_and_partition(
        self, boundaries: List[T], sort_key: "SortKey"
    ) -> List["Block"]:
        """Return a list of sorted partitions of this block."""
        raise NotImplementedError

    def _aggregate(self, key: "SortKey", aggs: Tuple["AggregateFn"]) -> Block:
        """Combine rows with the same key into an accumulator."""
        raise NotImplementedError

    @staticmethod
    def merge_sorted_blocks(
        blocks: List["Block"], sort_key: "SortKey"
    ) -> Tuple[Block, BlockMetadataWithSchema]:
        """Return a sorted block by merging a list of sorted blocks."""
        raise NotImplementedError

    @staticmethod
    def _combine_aggregated_blocks(
        blocks: List[Block],
        sort_key: "SortKey",
        aggs: Tuple["AggregateFn"],
        finalize: bool = True,
    ) -> Tuple[Block, BlockMetadataWithSchema]:
        """Aggregate partially combined and sorted blocks."""
        raise NotImplementedError

    def _find_partitions_sorted(
        self,
        boundaries: List[Tuple[Any]],
        sort_key: "SortKey",
    ) -> List[Block]:
        """NOTE: PLEASE READ CAREFULLY

        Returns dataset partitioned using list of boundaries

        This method requires that
            - Block being sorted (according to `sort_key`)
            - Boundaries is a sorted list of tuples
        """
        raise NotImplementedError

    def block_type(self) -> BlockType:
        """Return the block type of this block."""
        raise NotImplementedError

    def _get_group_boundaries_sorted(self, keys: List[str]) -> np.ndarray:
        """
        NOTE: THIS METHOD ASSUMES THAT PROVIDED BLOCK IS ALREADY SORTED

        Compute boundaries of the groups within a block based on provided
        key (a column or a list of columns)

        NOTE: In each column, NaNs/None are considered to be the same group.

        Args:
            block: sorted block for which grouping of rows will be determined
                    based on provided key
            keys: list of columns determining the key for every row based on
                    which the block will be grouped

        Returns:
            A list of starting indices of each group and an end index of the last
            group, i.e., there are ``num_groups + 1`` entries and the first and last
            entries are 0 and ``len(array)`` respectively.
        """

        if self.num_rows() == 0:
            return np.array([], dtype=np.int32)
        elif not keys:
            # If no keys are specified, whole block is considered a single group
            return np.array([0, self.num_rows()])

        # Convert key columns to Numpy (to perform vectorized
        # ops on them)
        projected_block = self.to_numpy(keys)

        return _get_group_boundaries_sorted_numpy(list(projected_block.values()))


@DeveloperAPI(stability="beta")
class BlockColumnAccessor:
    """Provides vendor-neutral interface to apply common operations
    to block's (Pandas/Arrow) columns"""

    def __init__(self, col: BlockColumn):
        self._column = col

    def count(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        """Returns a count of the distinct values in the column"""
        raise NotImplementedError()

    def sum(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        """Returns a sum of the values in the column"""
        return NotImplementedError()

    def min(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        """Returns a min of the values in the column"""
        raise NotImplementedError()

    def max(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        """Returns a max of the values in the column"""
        raise NotImplementedError()

    def mean(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        """Returns a mean of the values in the column"""
        raise NotImplementedError()

    def quantile(
        self, *, q: float, ignore_nulls: bool, as_py: bool = True
    ) -> Optional[U]:
        """Returns requested quantile of the given column"""
        raise NotImplementedError()

    def unique(self) -> BlockColumn:
        """Returns new column holding only distinct values of the current one"""
        raise NotImplementedError()

    def flatten(self) -> BlockColumn:
        """Flattens nested lists merging them into top-level container"""

        raise NotImplementedError()

    def sum_of_squared_diffs_from_mean(
        self,
        *,
        ignore_nulls: bool,
        mean: Optional[U] = None,
        as_py: bool = True,
    ) -> Optional[U]:
        """Returns a sum of diffs (from mean) squared for the column"""
        raise NotImplementedError()

    def to_pylist(self) -> List[Any]:
        """Converts block column to a list of Python native objects"""
        raise NotImplementedError()

    def to_numpy(self, zero_copy_only: bool = False) -> np.ndarray:
        """Converts underlying column to Numpy"""
        raise NotImplementedError()

    def _as_arrow_compatible(self) -> Union[List[Any], "pyarrow.Array"]:
        """Converts block column into a representation compatible with Arrow"""
        raise NotImplementedError()

    @staticmethod
    def for_column(col: BlockColumn) -> "BlockColumnAccessor":
        """Create a column accessor for the given column"""
        _check_pyarrow_version()

        import pandas as pd

        if isinstance(col, pa.Array) or isinstance(col, pa.ChunkedArray):
            from ray.data._internal.arrow_block import ArrowBlockColumnAccessor

            return ArrowBlockColumnAccessor(col)
        elif isinstance(col, pd.Series):
            from ray.data._internal.pandas_block import PandasBlockColumnAccessor

            return PandasBlockColumnAccessor(col)
        else:
            raise TypeError(
                f"Expected either a pandas.Series or pyarrow.Array (ChunkedArray) "
                f"(got {type(col)})"
            )


def _get_group_boundaries_sorted_numpy(columns: list[np.ndarray]) -> np.ndarray:
    # There are 3 categories: general, numerics with NaN, and categorical with None.
    # We only needed to check the last element for NaNs/None, as they are assumed to
    # be sorted.
    general_arrays = []
    num_arrays_with_nan = []
    cat_arrays_with_none = []
    for arr in columns:
        if np.issubdtype(arr.dtype, np.number) and np.isnan(arr[-1]):
            num_arrays_with_nan.append(arr)
        elif not np.issubdtype(arr.dtype, np.number) and arr[-1] is None:
            cat_arrays_with_none.append(arr)
        else:
            general_arrays.append(arr)

    # Compute the difference between each pair of elements. Handle the cases
    # where neighboring elements are both NaN or None. Output as a list of
    # boolean arrays.
    diffs = []
    if len(general_arrays) > 0:
        diffs.append(
            np.vstack([arr[1:] != arr[:-1] for arr in general_arrays]).any(axis=0)
        )
    if len(num_arrays_with_nan) > 0:
        # Two neighboring numeric elements belong to the same group when they are
        # 1) both finite and equal
        # or 2) both np.nan
        diffs.append(
            np.vstack(
                [
                    (arr[1:] != arr[:-1])
                    & (np.isfinite(arr[1:]) | np.isfinite(arr[:-1]))
                    for arr in num_arrays_with_nan
                ]
            ).any(axis=0)
        )
    if len(cat_arrays_with_none) > 0:
        # Two neighboring str/object elements belong to the same group when they are
        # 1) both finite and equal
        # or 2) both None
        diffs.append(
            np.vstack(
                [
                    (arr[1:] != arr[:-1])
                    & ~(np.equal(arr[1:], None) & np.equal(arr[:-1], None))
                    for arr in cat_arrays_with_none
                ]
            ).any(axis=0)
        )

    # A series of vectorized operations to compute the boundaries:
    # - column_stack: stack the bool arrays into a single 2D bool array
    # - any() and nonzero(): find the indices where any of the column diffs are True
    # - add 1 to get the index of the first element of the next group
    # - hstack(): include the 0 and last indices to the boundaries
    boundaries = np.hstack(
        [
            [0],
            (np.column_stack(diffs).any(axis=1).nonzero()[0] + 1),
            [len(columns[0])],
        ]
    ).astype(int)

    return boundaries
