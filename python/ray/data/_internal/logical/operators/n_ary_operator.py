import enum
from typing import List, Optional

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


class Zip(NAry):
    """Logical operator for zip."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)

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
        super().__init__(*input_ops)

    def estimated_num_outputs(self) -> Optional[int]:
        if self.stopping_condition == MixStoppingCondition.STOP_ON_SHORTEST:
            # The output is limited by whichever input runs out first
            # relative to its weight.
            min_outputs = None
            for i, input_dep in enumerate(self.input_dependencies):
                num_outputs = input_dep.estimated_num_outputs()
                if num_outputs is None:
                    return None
                # Scale by weight to estimate how many total output blocks
                # this input can sustain.
                weight = self.weights[i] / sum(self.weights)
                if weight > 0:
                    scaled = int(num_outputs / weight)
                    if min_outputs is None or scaled < min_outputs:
                        min_outputs = scaled
            return min_outputs
        else:
            # STOP_ON_LONGEST_DROP: sum of all inputs (like Union).
            total = 0
            for input_dep in self.input_dependencies:
                num_outputs = input_dep.estimated_num_outputs()
                if num_outputs is None:
                    return None
                total += num_outputs
            return total


class Union(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for union."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)

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
