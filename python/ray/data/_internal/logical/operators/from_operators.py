import abc
from typing import TYPE_CHECKING, Any, Generic, List, TypeVar, Union

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef

T = TypeVar("T")

if TYPE_CHECKING:
    import pyarrow as pa

    ArrowTable = Union["pa.Table", bytes]


class AbstractFrom(LogicalOperator, metaclass=abc.ABCMeta):
    """Abstract logical operator for `from_*`."""

    def __init__(
        self,
        input_blocks: List[ObjectRef[Block]],
        input_metadata: List[BlockMetadata],
        owns_blocks: bool,
    ):
        super().__init__(self.op_name(), [])
        assert len(input_blocks) == len(input_metadata), (
            len(input_blocks),
            len(input_metadata),
        )
        self._input_data = [
            RefBundle([(input_blocks[i], input_metadata[i])], owns_blocks)
            for i in range(len(input_blocks))
        ]

    @abc.abstractmethod
    def op_name(self) -> str:
        """Returns the name of the operator."""
        pass

    @property
    def input_data(self) -> List[RefBundle]:
        return self._input_data


class FromItems(AbstractFrom):
    """Logical operator for `from_items`."""

    def op_name(self) -> str:
        return "FromItems"


class FromNumpy(AbstractFrom):
    """Logical operator for `from_numpy`."""

    def op_name(self) -> str:
        return "FromNumpy"


class FromArrow(AbstractFrom):
    """Logical operator for `from_arrow`."""

    def op_name(self) -> str:
        return "FromArrow"


class FromPandas(AbstractFrom):
    """Logical operator for `from_pandas`."""

    def op_name(self) -> str:
        return "FromPandas"
