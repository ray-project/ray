from typing import TypeVar, List, Generic, Iterator, Tuple, Any, Union, \
    Optional, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas
    import pyarrow
    from ray.data.impl.block_builder import BlockBuilder
    from ray.data.aggregate import AggregateFn
    from ray.data.grouped_dataset import GroupKeyT

from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI
from ray.data.impl.util import _check_pyarrow_version

T = TypeVar("T")
U = TypeVar("U")
KeyType = TypeVar("KeyType")
AggType = TypeVar("AggType")

# Represents a batch of records to be stored in the Ray object store.
#
# Block data can be accessed in a uniform way via ``BlockAccessors`` such as
# ``SimpleBlockAccessor`` and ``ArrowBlockAccessor``.
Block = Union[List[T], "pyarrow.Table", bytes]

# A list of block references pending computation by a single task. For example,
# this may be the output of a task reading a file.
BlockPartition = List[Tuple[ObjectRef[Block], "BlockMetadata"]]

# The metadata that describes the output of a BlockPartition. This has the
# same type as the metadata that describes each block in the partition.
BlockPartitionMetadata = "BlockMetadata"


@DeveloperAPI
class BlockMetadata:
    """Metadata about the block.

    Attributes:
        num_rows: The number of rows contained in this block, or None.
        size_bytes: The approximate size in bytes of this block, or None.
        schema: The pyarrow schema or types of the block elements, or None.
        input_files: The list of file paths used to generate this block, or
            the empty list if indeterminate.
    """

    def __init__(self, *, num_rows: Optional[int], size_bytes: Optional[int],
                 schema: Union[type, "pyarrow.lib.Schema"],
                 input_files: List[str]):
        if input_files is None:
            input_files = []
        self.num_rows: Optional[int] = num_rows
        self.size_bytes: Optional[int] = size_bytes
        self.schema: Optional[Any] = schema
        self.input_files: List[str] = input_files


@DeveloperAPI
class BlockAccessor(Generic[T]):
    """Provides accessor methods for a specific block.

    Ideally, we wouldn't need a separate accessor classes for blocks. However,
    this is needed if we want to support storing ``pyarrow.Table`` directly
    as a top-level Ray object, without a wrapping class (issue #17186).

    There are two types of block accessors: ``SimpleBlockAccessor``, which
    operates over a plain Python list, and ``ArrowBlockAccessor`` for
    ``pyarrow.Table`` type blocks.
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

    def to_numpy(self, column: str = None) -> np.ndarray:
        """Convert this block (or column of block) into a NumPy ndarray.

        Args:
            column: Name of column to convert, or None.
        """
        raise NotImplementedError

    def to_arrow(self) -> "pyarrow.Table":
        """Convert this block into an Arrow table."""
        raise NotImplementedError

    def size_bytes(self) -> int:
        """Return the approximate size in bytes of this block."""
        raise NotImplementedError

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the Python type or pyarrow schema of this block."""
        raise NotImplementedError

    def get_metadata(self, input_files: List[str]) -> BlockMetadata:
        """Create a metadata object from this block."""
        return BlockMetadata(
            num_rows=self.num_rows(),
            size_bytes=self.size_bytes(),
            schema=self.schema(),
            input_files=input_files)

    def zip(self, other: "Block[T]") -> "Block[T]":
        """Zip this block with another block of the same type and size."""
        raise NotImplementedError

    @staticmethod
    def builder() -> "BlockBuilder[T]":
        """Create a builder for this block type."""
        raise NotImplementedError

    @staticmethod
    def for_block(block: Block) -> "BlockAccessor[T]":
        """Create a block accessor for the given block."""
        _check_pyarrow_version()
        import pyarrow

        if isinstance(block, pyarrow.Table):
            from ray.data.impl.arrow_block import \
                ArrowBlockAccessor
            return ArrowBlockAccessor(block)
        elif isinstance(block, bytes):
            from ray.data.impl.arrow_block import \
                ArrowBlockAccessor
            return ArrowBlockAccessor.from_bytes(block)
        elif isinstance(block, list):
            from ray.data.impl.simple_block import \
                SimpleBlockAccessor
            return SimpleBlockAccessor(block)
        else:
            raise TypeError("Not a block type: {}".format(block))

    def sample(self, n_samples: int, key: Any) -> "Block[T]":
        """Return a random sample of items from this block."""
        raise NotImplementedError

    def sort_and_partition(self, boundaries: List[T], key: Any,
                           descending: bool) -> List["Block[T]"]:
        """Return a list of sorted partitions of this block."""
        raise NotImplementedError

    def combine(self, key: "GroupKeyT", agg: "AggregateFn") -> Block[U]:
        """Combine rows with the same key into an accumulator."""
        raise NotImplementedError

    @staticmethod
    def merge_sorted_blocks(
            blocks: List["Block[T]"], key: Any,
            descending: bool) -> Tuple[Block[T], BlockMetadata]:
        """Return a sorted block by merging a list of sorted blocks."""
        raise NotImplementedError

    @staticmethod
    def aggregate_combined_blocks(
            blocks: List[Block], key: "GroupKeyT",
            agg: "AggregateFn") -> Tuple[Block[U], BlockMetadata]:
        """Aggregate partially combined and sorted blocks."""
        raise NotImplementedError
