import abc
import functools
from dataclasses import InitVar, dataclass, field
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

__all__ = [
    "AbstractFrom",
    "FromArrow",
    "FromBlocks",
    "FromItems",
    "FromNumpy",
    "FromPandas",
]


@dataclass(frozen=True, repr=False)
class AbstractFrom(LogicalOperator, SourceOperator, metaclass=abc.ABCMeta):
    """Abstract logical operator for `from_*`."""

    input_blocks: InitVar[Optional[List[ObjectRef[Block]]]] = None
    input_metadata: InitVar[Optional[List[BlockMetadataWithSchema]]] = None
    input_data: List[RefBundle] = field(init=False)

    def __post_init__(
        self,
        input_blocks: Optional[List[ObjectRef[Block]]],
        input_metadata: Optional[List[BlockMetadataWithSchema]],
    ) -> None:
        assert input_blocks is not None
        assert input_metadata is not None
        if self.name is None:
            object.__setattr__(self, "name", self.__class__.__name__)
        object.__setattr__(self, "input_dependencies", [])
        object.__setattr__(self, "num_outputs", len(input_blocks))

        assert len(input_blocks) == len(input_metadata), (
            len(input_blocks),
            len(input_metadata),
        )

        # `owns_blocks` is False because this op may be shared by multiple Datasets.
        object.__setattr__(
            self,
            "input_data",
            [
                RefBundle(
                    [(input_blocks[i], input_metadata[i])],
                    owns_blocks=False,
                    schema=input_metadata[i].schema,
                )
                for i in range(len(input_blocks))
            ],
        )
        super().__post_init__()

    def output_data(self) -> Optional[List[RefBundle]]:
        return self.input_data

    @functools.cached_property
    def _cached_output_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self._num_rows(),
            size_bytes=self._size_bytes(),
            input_files=None,
            exec_stats=None,
        )

    def _num_rows(self):
        if all(bundle.num_rows() is not None for bundle in self.input_data):
            return sum(bundle.num_rows() for bundle in self.input_data)
        else:
            return None

    def _size_bytes(self):
        metadata = [m for bundle in self.input_data for m in bundle.metadata]
        if all(m.size_bytes is not None for m in metadata):
            return sum(m.size_bytes for m in metadata)
        else:
            return None

    def infer_metadata(self) -> BlockMetadata:
        return self._cached_output_metadata

    def infer_schema(self):
        return unify_ref_bundles_schema(self.input_data)

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
