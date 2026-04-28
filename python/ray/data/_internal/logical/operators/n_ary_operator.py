import enum
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)

__all__ = [
    "Mix",
    "MixStoppingCondition",
    "NAry",
    "Union",
    "Zip",
]


class MixStoppingCondition(enum.Enum):
    """Controls when a mix pipeline terminates.

    STOP_ON_SHORTEST: Pipeline ends when the shortest dataset is exhausted.
        Other datasets are truncated.
    STOP_ON_LONGEST_DROP: Pipeline ends when the longest dataset is exhausted.
        Shorter datasets drop out once exhausted; later batches are drawn
        entirely from longer datasets.
    """

    STOP_ON_SHORTEST = "stop_on_shortest"
    STOP_ON_LONGEST_DROP = "stop_on_longest_drop"


def estimate_num_mix_outputs(
    per_input_counts: List[Optional[int]],
    weights: List[float],
    stopping_condition: MixStoppingCondition,
) -> Optional[int]:
    """Estimate total output count for a mix operation.

    Used by both the logical and physical Mix operators to estimate
    num_outputs_total / num_output_rows_total.
    """
    if any(c is None for c in per_input_counts):
        return None
    if stopping_condition == MixStoppingCondition.STOP_ON_LONGEST_DROP:
        return sum(per_input_counts)
    elif stopping_condition == MixStoppingCondition.STOP_ON_SHORTEST:
        # Limited by whichever input runs out first relative to its weight.
        total_weight = sum(weights)
        return min(
            int(count / (w / total_weight))
            for count, w in zip(per_input_counts, weights)
        )
    else:
        raise ValueError(f"Unknown stopping condition: {stopping_condition}")


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
            input_dependencies=list(input_ops),
            num_outputs=num_outputs,
        )

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs


@dataclass(frozen=True, repr=False, eq=False, init=False)
class Zip(NAry):
    """Logical operator for zip."""

    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        for input_op in input_ops:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_name", self.__class__.__name__)
        object.__setattr__(self, "_input_dependencies", list(input_ops))
        object.__setattr__(self, "_num_outputs", None)

    def _apply_transform(
        self, transform: Callable[[LogicalOperator], LogicalOperator]
    ) -> LogicalOperator:
        transformed_inputs = [
            input_op._apply_transform(transform) for input_op in self.input_dependencies
        ]
        target: LogicalOperator
        if all(
            transformed_input is input_op
            for transformed_input, input_op in zip(
                transformed_inputs, self.input_dependencies
            )
        ):
            target = self
        else:
            target = Zip(*transformed_inputs)
        return transform(target)

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs = max(total_num_outputs, num_outputs)
        return total_num_outputs


class Mix(NAry):
    """Logical operator for weighted dataset mixing."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
        weights: List[float],
        stopping_condition: MixStoppingCondition = MixStoppingCondition.STOP_ON_SHORTEST,
    ):
        self.weights = weights
        self.stopping_condition = stopping_condition

        if len(input_ops) != len(weights):
            raise ValueError(
                f"Number of input operators ({len(input_ops)}) must match number of weights ({len(weights)})."
            )
        if any(weight <= 0 for weight in weights):
            raise ValueError(f"Weights must be positive. Got weights: {weights}")

        super().__init__(*input_ops)

    def estimated_num_outputs(self) -> Optional[int]:
        return estimate_num_mix_outputs(
            [dep.estimated_num_outputs() for dep in self.input_dependencies],
            self.weights,
            self.stopping_condition,
        )


@dataclass(frozen=True, repr=False, eq=False, init=False)
class Union(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for union."""

    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        for input_op in input_ops:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_name", self.__class__.__name__)
        object.__setattr__(self, "_input_dependencies", list(input_ops))
        object.__setattr__(self, "_num_outputs", None)

    def _apply_transform(
        self, transform: Callable[[LogicalOperator], LogicalOperator]
    ) -> LogicalOperator:
        transformed_inputs = [
            input_op._apply_transform(transform) for input_op in self.input_dependencies
        ]
        target: LogicalOperator
        if all(
            transformed_input is input_op
            for transformed_input, input_op in zip(
                transformed_inputs, self.input_dependencies
            )
        ):
            target = self
        else:
            target = Union(*transformed_inputs)
        return transform(target)

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
