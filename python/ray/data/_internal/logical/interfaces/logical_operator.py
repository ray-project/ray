from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional

from .operator import Operator
from ray.data.block import BlockMetadata
from ray.data.expressions import Expr

if TYPE_CHECKING:
    from ray.data.block import Schema


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(
        self,
        name: str,
        input_dependencies: List["LogicalOperator"],
        num_outputs: Optional[int] = None,
    ):
        super().__init__(
            name,
            input_dependencies,
        )
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x

        self._num_outputs: Optional[int] = num_outputs

    def estimated_num_outputs(self) -> Optional[int]:
        """Returns the estimated number of blocks that
        would be outputted by this logical operator.

        This method does not execute the plan, so it does not take into consideration
        block splitting. This method only considers high-level block constraints like
        `Dataset.repartition(num_blocks=X)`. A more accurate estimation can be given by
        `PhysicalOperator.num_outputs_total()` during execution.
        """
        if self._num_outputs is not None:
            return self._num_outputs
        elif len(self._input_dependencies) == 1:
            return self._input_dependencies[0].estimated_num_outputs()
        return None

    # Override the following 3 methods to correct type hints.

    @property
    def input_dependencies(self) -> List["LogicalOperator"]:
        return super().input_dependencies  # type: ignore

    @property
    def output_dependencies(self) -> List["LogicalOperator"]:
        return super().output_dependencies  # type: ignore

    def post_order_iter(self) -> Iterator["LogicalOperator"]:
        return super().post_order_iter()  # type: ignore

    def _apply_transform(
        self, transform: Callable[["LogicalOperator"], "LogicalOperator"]
    ) -> "LogicalOperator":
        return super()._apply_transform(transform)  # type: ignore

    def _get_args(self) -> Dict[str, Any]:
        """This Dict must be serializable"""
        return vars(self)

    def infer_schema(self) -> Optional["Schema"]:
        """Returns the inferred schema of the output blocks."""
        return None

    def infer_metadata(self) -> "BlockMetadata":
        """A ``BlockMetadata`` that represents the aggregate metadata of the outputs.

        This method is used by methods like :meth:`~ray.data.Dataset.schema` to
        efficiently return metadata.
        """
        return BlockMetadata(None, None, None, None)

    def is_lineage_serializable(self) -> bool:
        """Returns whether the lineage of this operator can be serialized.

        An operator is lineage serializable if you can serialize it on one machine and
        deserialize it on another without losing information. Operators that store
        object references (e.g., ``InputData``) aren't lineage serializable because the
        objects aren't available on the deserialized machine.
        """
        return True


class LogicalOperatorSupportsProjectionPushdown(LogicalOperator):
    """Mixin for reading operators supporting projection pushdown"""

    def supports_projection_pushdown(self) -> bool:
        return False

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        return None

    def apply_projection(
        self,
        projection_map: Optional[Dict[str, str]],
    ) -> LogicalOperator:
        return self


class LogicalOperatorSupportsPredicatePushdown(LogicalOperator):
    """Mixin for reading operators supporting predicate pushdown"""

    def supports_predicate_pushdown(self) -> bool:
        return False

    def get_current_predicate(self) -> Optional[Expr]:
        return None

    def apply_predicate(
        self,
        predicate_expr: Expr,
    ) -> LogicalOperator:
        return self

    def get_column_renames(self) -> Optional[Dict[str, str]]:
        """Return the column renames applied by projection pushdown, if any.

        Returns:
            A dictionary mapping old column names to new column names,
            or None if no renaming has been applied.
        """
        return None


class PredicatePassThroughBehavior(Enum):
    """Defines how predicates can be passed through an operator."""

    # Predicate can be pushed through as-is (e.g., Sort, Repartition, RandomShuffle, Limit)
    PASSTHROUGH = "passthrough"

    # Predicate can be pushed through but needs column rebinding (e.g., Project)
    PASSTHROUGH_WITH_SUBSTITUTION = "passthrough_with_substitution"

    # Predicate can be pushed into each branch (e.g., Union)
    PUSH_INTO_BRANCHES = "push_into_branches"

    # Predicate can be conditionally pushed based on columns (e.g., Join)
    CONDITIONAL = "conditional"


class LogicalOperatorSupportsPredicatePassThrough(ABC):
    """Mixin for operators that allow predicates to be pushed through them.

    This is distinct from LogicalOperatorSupportsPredicatePushdown, which is for
    operators that can *accept* predicates (like Read). This trait is for operators
    that allow predicates to *pass through* them.
    """

    @abstractmethod
    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        """Returns the predicate passthrough behavior for this operator."""
        pass

    def get_column_substitutions(self) -> Optional[Dict[str, str]]:
        """Returns column renames needed when pushing through (for PASSTHROUGH_WITH_SUBSTITUTION).

        Returns:
            Dict mapping from old_name -> new_name, or None if no rebinding needed
        """
        return None
