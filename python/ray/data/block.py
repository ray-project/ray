import os
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

import ray
from ray.data._internal.util import _check_pyarrow_version
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI

import psutil

try:
    import resource
except ImportError:
    resource = None


if TYPE_CHECKING:
    import pandas
    import pyarrow

    from ray.data import Dataset
    from ray.data._internal.block_builder import BlockBuilder
    from ray.data.aggregate import AggregateFn


T = TypeVar("T")
U = TypeVar("U")
KeyType = TypeVar("KeyType")
AggType = TypeVar("AggType")

# A function that extracts a concrete value from a record in a Dataset, used
# in ``sort(value_fns...)``, ``groupby(value_fn).agg(Agg(value_fn), ...)``.
# It can either be None (intepreted as the identity function), the name
# of a Dataset column, or a lambda function that extracts the desired value
# from the object.
KeyFn = Union[None, str, Callable[[T], Any]]


def _validate_key_fn(ds: "Dataset", key: KeyFn) -> None:
    """Check the key function is valid on the given dataset."""
    try:
        fmt = ds._dataset_format()
    except ValueError:
        # Dataset is empty/cleared, validation not possible.
        return
    if isinstance(key, str):
        if fmt == "simple":
            raise ValueError(
                "String key '{}' requires dataset format to be "
                "'arrow' or 'pandas', was '{}'.".format(key, fmt)
            )
        # Raises KeyError if key is not present in the schema.
        schema = ds.schema(fetch_if_missing=True)
        if len(schema.names) > 0 and key not in schema.names:
            raise ValueError(
                "The column '{}' does not exist in the "
                "schema '{}'.".format(key, schema)
            )
    elif key is None:
        if fmt != "simple":
            raise ValueError(
                "The `None` key '{}' requires dataset format to be "
                "'simple', was '{}'.".format(key, fmt)
            )
    elif callable(key):
        if fmt != "simple":
            raise ValueError(
                "Callable key '{}' requires dataset format to be "
                "'simple', was '{}'.".format(key, fmt)
            )
    else:
        raise TypeError("Invalid key type {} ({}).".format(key, type(key)))


# Represents a batch of records to be stored in the Ray object store.
#
# Block data can be accessed in a uniform way via ``BlockAccessors`` such as
# ``SimpleBlockAccessor`` and ``ArrowBlockAccessor``.
Block = Union[List[T], "pyarrow.Table", "pandas.DataFrame", bytes]

# User-facing data batch type. This is the data type for data that is supplied to and
# returned from batch UDFs.
DataBatch = Union[Block, np.ndarray, Dict[str, np.ndarray]]

# A class type that implements __call__.
CallableClass = type

# A UDF on data batches.
BatchUDF = Union[
    # TODO(Clark): Once Ray only supports Python 3.8+, use protocol to constraint batch
    # UDF type.
    # Callable[[DataBatch, ...], DataBatch]
    Callable[[DataBatch], DataBatch],
    Callable[..., DataBatch],
    CallableClass,
]

# A UDF on data rows.
RowUDF = Union[
    # TODO(Clark): Once Ray only supports Python 3.8+, use protocol to constraint batch
    # UDF type.
    # Callable[[T, ...], U]
    Callable[[T], U],
    Callable[..., U],
    CallableClass,
]

# A list of block references pending computation by a single task. For example,
# this may be the output of a task reading a file.
BlockPartition = List[Tuple[ObjectRef[Block], "BlockMetadata"]]

# The metadata that describes the output of a BlockPartition. This has the
# same type as the metadata that describes each block in the partition.
BlockPartitionMetadata = "BlockMetadata"

# TODO(ekl) replace this with just `BlockPartition` once block splitting is on
# by default. When block splitting is off, the type is a plain block.
MaybeBlockPartition = Union[Block, BlockPartition]

VALID_BATCH_FORMATS = ["native", "pandas", "pyarrow", "numpy"]


@DeveloperAPI
class BlockExecStats:
    """Execution stats for this block.

    Attributes:
        wall_time_s: The wall-clock time it took to compute this block.
        cpu_time_s: The CPU time it took to compute this block.
        node_id: A unique id for the node that computed this block.
    """

    def __init__(self):
        self.wall_time_s: Optional[float] = None
        self.cpu_time_s: Optional[float] = None
        self.node_id = ray.runtime_context.get_runtime_context().node_id.hex()
        # Max memory usage. May be an overestimate since we do not
        # differentiate from previous tasks on the same worker.
        self.max_rss_bytes: int = 0

    @staticmethod
    def builder() -> "_BlockExecStatsBuilder":
        return _BlockExecStatsBuilder()

    def __repr__(self):
        return repr(
            {
                "wall_time_s": self.wall_time_s,
                "cpu_time_s": self.cpu_time_s,
                "node_id": self.node_id,
            }
        )


class _BlockExecStatsBuilder:
    """Helper class for building block stats.

    When this class is created, we record the start time. When build() is
    called, the time delta is saved as part of the stats.
    """

    def __init__(self):
        self.start_time = time.perf_counter()
        self.start_cpu = time.process_time()

    def build(self) -> "BlockExecStats":
        stats = BlockExecStats()
        stats.wall_time_s = time.perf_counter() - self.start_time
        stats.cpu_time_s = time.process_time() - self.start_cpu
        if resource is None:
            # NOTE(swang): resource package is not supported on Windows. This
            # is only the memory usage at the end of the task, not the peak
            # memory.
            process = psutil.Process(os.getpid())
            stats.max_rss_bytes = int(process.memory_info().rss)
        else:
            stats.max_rss_bytes = int(
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1e3
            )
        return stats


@DeveloperAPI
@dataclass
class BlockMetadata:
    """Metadata about the block.

    Attributes:
        num_rows: The number of rows contained in this block, or None.
        size_bytes: The approximate size in bytes of this block, or None.
        schema: The pyarrow schema or types of the block elements, or None.
        input_files: The list of file paths used to generate this block, or
            the empty list if indeterminate.
        exec_stats: Execution stats for this block.
    """

    num_rows: Optional[int]
    size_bytes: Optional[int]
    schema: Optional[Union[type, "pyarrow.lib.Schema"]]
    input_files: Optional[List[str]]
    exec_stats: Optional[BlockExecStats]

    def __post_init__(self):
        if self.input_files is None:
            self.input_files = []


@DeveloperAPI
class BlockAccessor(Generic[T]):
    """Provides accessor methods for a specific block.

    Ideally, we wouldn't need a separate accessor classes for blocks. However,
    this is needed if we want to support storing ``pyarrow.Table`` directly
    as a top-level Ray object, without a wrapping class (issue #17186).

    There are three types of block accessors: ``SimpleBlockAccessor``, which
    operates over a plain Python list, ``ArrowBlockAccessor`` for
    ``pyarrow.Table`` type blocks, ``PandasBlockAccessor`` for ``pandas.DataFrame``
    type blocks.
    """

    def num_rows(self) -> int:
        """Return the number of rows contained in this block."""
        raise NotImplementedError

    def iter_rows(self) -> Iterator[T]:
        """Iterate over the rows of this block."""
        raise NotImplementedError

    def slice(self, start: int, end: int, copy: bool) -> Block:
        """Return a slice of this block.

        Args:
            start: The starting index of the slice.
            end: The ending index of the slice.
            copy: Whether to perform a data copy for the slice.

        Returns:
            The sliced block result.
        """
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

    def to_native(self) -> Block:
        """Return the native data format for this accessor."""
        return self.to_block()

    def to_batch_format(self, batch_format: str) -> DataBatch:
        """Convert this block into the provided batch format.

        Args:
            batch_format: The batch format to convert this block to.

        Returns:
            This block formatted as the provided batch format.
        """
        if batch_format == "native":
            return self.to_native()
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
        self, input_files: List[str], exec_stats: Optional[BlockExecStats]
    ) -> BlockMetadata:
        """Create a metadata object from this block."""
        return BlockMetadata(
            num_rows=self.num_rows(),
            size_bytes=self.size_bytes(),
            schema=self.schema(),
            input_files=input_files,
            exec_stats=exec_stats,
        )

    def zip(self, other: "Block[T]") -> "Block[T]":
        """Zip this block with another block of the same type and size."""
        raise NotImplementedError

    @staticmethod
    def builder() -> "BlockBuilder[T]":
        """Create a builder for this block type."""
        raise NotImplementedError

    @staticmethod
    def batch_to_block(batch: DataBatch) -> Block:
        """Create a block from user-facing data formats."""
        if isinstance(batch, (np.ndarray, dict)):
            from ray.data._internal.arrow_block import ArrowBlockAccessor

            return ArrowBlockAccessor.numpy_to_block(batch)
        return batch

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
            from ray.data._internal.simple_block import SimpleBlockAccessor

            return SimpleBlockAccessor(block)
        else:
            raise TypeError("Not a block type: {} ({})".format(block, type(block)))

    def sample(self, n_samples: int, key: Any) -> "Block[T]":
        """Return a random sample of items from this block."""
        raise NotImplementedError

    def sort_and_partition(
        self, boundaries: List[T], key: Any, descending: bool
    ) -> List["Block[T]"]:
        """Return a list of sorted partitions of this block."""
        raise NotImplementedError

    def combine(self, key: KeyFn, agg: "AggregateFn") -> Block[U]:
        """Combine rows with the same key into an accumulator."""
        raise NotImplementedError

    @staticmethod
    def merge_sorted_blocks(
        blocks: List["Block[T]"], key: Any, descending: bool
    ) -> Tuple[Block[T], BlockMetadata]:
        """Return a sorted block by merging a list of sorted blocks."""
        raise NotImplementedError

    @staticmethod
    def aggregate_combined_blocks(
        blocks: List[Block], key: KeyFn, agg: "AggregateFn"
    ) -> Tuple[Block[U], BlockMetadata]:
        """Aggregate partially combined and sorted blocks."""
        raise NotImplementedError
