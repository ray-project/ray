from dataclasses import InitVar, dataclass, field
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

    input_op: InitVar[LogicalOperator]
    num_splits: int
    equal: bool
    locality_hints: Optional[List["NodeIdStr"]] = None
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self, input_op: LogicalOperator):
        assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_input_dependencies", [input_op])
        object.__setattr__(self, "_num_outputs", None)

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs
