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
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        object.__setattr__(self, "_num_outputs", None)

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs
