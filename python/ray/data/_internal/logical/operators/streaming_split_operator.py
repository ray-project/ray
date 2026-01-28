from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from ray.data._internal.logical.interfaces import LogicalOperator

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import NodeIdStr

__all__ = [
    "StreamingSplit",
]


@dataclass(frozen=True, init=False, repr=False)
class StreamingSplit(LogicalOperator):
    """Logical operator that represents splitting the input data to `n` splits."""

    num_splits: int
    equal: bool
    locality_hints: Optional[List["NodeIdStr"]]

    def __init__(
        self,
        input_op: Optional[LogicalOperator] = None,
        input_dependencies: Optional[List[LogicalOperator]] = None,
        num_splits: Optional[int] = None,
        equal: Optional[bool] = None,
        locality_hints: Optional[List["NodeIdStr"]] = None,
        name: Optional[str] = None,
        num_outputs: Optional[int] = None,
    ):
        assert num_splits is not None
        assert equal is not None
        if input_dependencies is None:
            assert input_op is not None
            input_dependencies = [input_op]
        if name is None:
            name = "StreamingSplit"
        super().__init__(
            name=name, input_dependencies=input_dependencies, num_outputs=num_outputs
        )
        object.__setattr__(self, "num_splits", num_splits)
        object.__setattr__(self, "equal", equal)
        object.__setattr__(self, "locality_hints", locality_hints)
