from dataclasses import dataclass
from typing import List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)

__all__ = [
    "NAry",
    "Union",
    "Zip",
]


@dataclass(frozen=True, init=False, repr=False)
class NAry(LogicalOperator):
    """Base class for n-ary operators, which take multiple input operators."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
        input_dependencies: Optional[List[LogicalOperator]] = None,
        num_outputs: Optional[int] = None,
        name: Optional[str] = None,
    ):
        """Initialize an n-ary logical operator.

        Args:
            *input_ops: The input operators.
            input_dependencies: The input operators. If set, overrides input_ops.
            num_outputs: The output number of blocks. None means unknown.
            name: The operator name. Defaults to the class name.
        """
        if name is None:
            name = self.__class__.__name__
        if input_dependencies is None:
            input_dependencies = list(input_ops)
        super().__init__(
            name=name, input_dependencies=input_dependencies, num_outputs=num_outputs
        )


@dataclass(frozen=True, init=False, repr=False)
class Zip(NAry):
    """Logical operator for zip."""

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs = max(total_num_outputs, num_outputs)
        return total_num_outputs


@dataclass(frozen=True, init=False, repr=False)
class Union(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for union."""

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs += num_outputs
        return total_num_outputs

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Union allows pushing filter into each branch
        return PredicatePassThroughBehavior.PUSH_INTO_BRANCHES
