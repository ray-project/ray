from dataclasses import dataclass, field
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


@dataclass(frozen=True, repr=False, eq=False, init=False)
class NAry(LogicalOperator):
    """Base class for n-ary operators, which take multiple input operators."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
        num_outputs: Optional[int] = None,
    ):
        """
        Args:
            input_ops: The input operators.
        """
        super().__init__(
            _input_dependencies=list(input_ops),
            _num_outputs=num_outputs,
        )

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs

    def _with_new_input_dependencies(
        self, input_dependencies: List[LogicalOperator]
    ) -> LogicalOperator:
        return self.__class__(*input_dependencies)


@dataclass(frozen=True, repr=False, eq=False, init=False)
class Zip(NAry):
    """Logical operator for zip."""

    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        for input_op in input_ops:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_input_dependencies", list(input_ops))
        object.__setattr__(self, "_num_outputs", None)

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs = max(total_num_outputs, num_outputs)
        return total_num_outputs


@dataclass(frozen=True, repr=False, eq=False, init=False)
class Union(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for union."""

    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        for input_op in input_ops:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_input_dependencies", list(input_ops))
        object.__setattr__(self, "_num_outputs", None)

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
