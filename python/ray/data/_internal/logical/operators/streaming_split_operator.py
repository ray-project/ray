from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

from ray.data._internal.logical.interfaces import LogicalOperator

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import NodeIdStr

__all__ = [
    "StreamingSplit",
]


@dataclass(frozen=True, repr=False, eq=False)
class StreamingSplit(LogicalOperator):
    """Logical operator that represents splitting the input data to `n` splits."""

    num_splits: int
    equal: bool
    locality_hints: Optional[List["NodeIdStr"]] = None
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        object.__setattr__(self, "name", self.__class__.__name__)
        object.__setattr__(self, "num_outputs", None)
