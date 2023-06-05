import abc
from typing import TYPE_CHECKING, List, Union

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow as pa

    ArrowTable = Union["pa.Table", bytes]


class AbstractFrom(LogicalOperator, metaclass=abc.ABCMeta):
    """Abstract logical operator for `from_*`."""

    def __init__(
        self,
        input_blocks: List[ObjectRef[Block]],
        input_metadata: List[BlockMetadata],
    ):
        super().__init__(self.__class__.__name__, [])
        assert len(input_blocks) == len(input_metadata), (
            len(input_blocks),
            len(input_metadata),
        )
        # `owns_blocks` is False because this op may be shared by multiple Datasets.
        self._input_data = [
            RefBundle([(input_blocks[i], input_metadata[i])], owns_blocks=False)
            for i in range(len(input_blocks))
        ]

    @property
    def input_data(self) -> List[RefBundle]:
        return self._input_data


class FromItems(AbstractFrom):
    """Logical operator for `from_items`."""

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
