import abc
import functools
from typing import TYPE_CHECKING, List, Optional, Union

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator, SourceOperator
from ray.data._internal.util import unify_ref_bundles_schema
from ray.data.block import (
    Block,
    BlockMetadata,
    BlockMetadataWithSchema,
)
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow as pa

    ArrowTable = Union["pa.Table", bytes]


class AbstractFrom(LogicalOperator, SourceOperator, metaclass=abc.ABCMeta):
    """Abstract logical operator for `from_*`."""

    def __init__(
        self,
        input_blocks: List[ObjectRef[Block]],
        input_metadata: List[BlockMetadataWithSchema],
    ):
        super().__init__(
            name=self.__class__.__name__,
            input_dependencies=[],
            num_outputs=len(input_blocks),
        )

        assert len(input_blocks) == len(input_metadata), (
            len(input_blocks),
            len(input_metadata),
        )

        # `owns_blocks` is False because this op may be shared by multiple Datasets.
        self._input_data = [
            RefBundle(
                [(input_blocks[i], input_metadata[i])],
                owns_blocks=False,
                schema=input_metadata[i].schema,
            )
            for i in range(len(input_blocks))
        ]

    @property
    def input_data(self) -> List[RefBundle]:
        return self._input_data

    def output_data(self) -> Optional[List[RefBundle]]:
        return self._input_data

    @functools.cached_property
    def _cached_output_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self._num_rows(),
            size_bytes=self._size_bytes(),
            input_files=None,
            exec_stats=None,
        )

    def _num_rows(self):
        if all(bundle.num_rows() is not None for bundle in self._input_data):
            return sum(bundle.num_rows() for bundle in self._input_data)
        else:
            return None

    def _size_bytes(self):
        metadata = [m for bundle in self._input_data for m in bundle.metadata]
        if all(m.size_bytes is not None for m in metadata):
            return sum(m.size_bytes for m in metadata)
        else:
            return None

    def infer_metadata(self) -> BlockMetadata:
        return self._cached_output_metadata

    def infer_schema(self):
        return unify_ref_bundles_schema(self._input_data)

    def is_lineage_serializable(self) -> bool:
        # This operator isn't serializable because it contains ObjectRefs.
        return False


class FromItems(AbstractFrom):
    """Logical operator for `from_items`."""

    pass


class FromBlocks(AbstractFrom):
    """Logical operator for `from_blocks`."""

    pass


class FromNumpy(AbstractFrom):
    """Logical operator for `from_numpy`."""

    pass


class FromArrow(AbstractFrom):
    """Logical operator for `from_arrow`."""

    pass


class FromPandas(AbstractFrom):
    """Logical operator for `from_pandas`."""

    pass
