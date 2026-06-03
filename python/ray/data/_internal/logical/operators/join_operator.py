from dataclasses import InitVar, dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsProjectionPassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.logical.operators.n_ary_operator import NAry

if TYPE_CHECKING:
    from typing import FrozenSet, Set

    from ray.data.dataset import Schema
    from ray.data.expressions import Expr

__all__ = [
    "Join",
    "JoinSide",
    "JoinType",
]


class JoinType(Enum):
    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"
    LEFT_SEMI = "left_semi"
    RIGHT_SEMI = "right_semi"
    LEFT_ANTI = "left_anti"
    RIGHT_ANTI = "right_anti"


class JoinSide(Enum):
    """Represents which side of a join to push a predicate to.

    The enum values correspond to branch indices (0 for left, 1 for right).
    """

    LEFT = 0
    RIGHT = 1


@dataclass(frozen=True, repr=False, eq=False)
class Join(
    NAry,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorSupportsProjectionPassThrough,
):
    """Logical operator for join."""

    left_input_op: InitVar[LogicalOperator]
    right_input_op: InitVar[LogicalOperator]
    join_type: Union[JoinType, str]
    left_key_columns: Tuple[str]
    right_key_columns: Tuple[str]
    num_partitions: InitVar[int]
    left_columns_suffix: Optional[str] = None
    right_columns_suffix: Optional[str] = None
    partition_size_hint: Optional[int] = None
    aggregator_ray_remote_args: Optional[Dict[str, Any]] = None
    _input_dependencies: list[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, repr=False)

    def __post_init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
        num_partitions: int,
    ):
        try:
            join_type_enum = JoinType(self.join_type)
        except ValueError:
            raise ValueError(
                f"Invalid join type: '{self.join_type}'. "
                f"Supported join types are: {', '.join(jt.value for jt in JoinType)}."
            )

        object.__setattr__(self, "join_type", join_type_enum)
        object.__setattr__(
            self,
            "_input_dependencies",
            [left_input_op, right_input_op],
        )
        object.__setattr__(self, "_num_outputs", num_partitions)

    def _with_new_input_dependencies(
        self, input_dependencies: List[LogicalOperator]
    ) -> LogicalOperator:
        return replace(
            self,
            left_input_op=input_dependencies[0],
            right_input_op=input_dependencies[1],
            num_partitions=self.num_outputs,
        )

    @staticmethod
    def _validate_schemas(
        left_op_schema: "Schema",
        right_op_schema: "Schema",
        left_key_column_names: Tuple[str],
        right_key_column_names: Tuple[str],
    ):
        def _col_names_as_str(keys: Sequence[str]):
            keys_joined = ", ".join(map(lambda k: f"'{k}'", keys))
            return f"[{keys_joined}]"

        if len(left_key_column_names) < 1:
            raise ValueError(
                f"At least 1 column name to join on has to be provided (got "
                f"{_col_names_as_str(left_key_column_names)})"
            )

        if len(left_key_column_names) != len(right_key_column_names):
            raise ValueError(
                f"Number of columns provided for left and right datasets has to match "
                f"(got {_col_names_as_str(left_key_column_names)} and "
                f"{_col_names_as_str(right_key_column_names)})"
            )

        def _get_key_column_types(schema: "Schema", keys: Tuple[str]):
            return (
                [
                    _type
                    for name, _type in zip(schema.names, schema.types)
                    if name in keys
                ]
                if schema
                else None
            )

        right_op_key_cols = _get_key_column_types(
            right_op_schema, left_key_column_names
        )
        left_op_key_cols = _get_key_column_types(left_op_schema, right_key_column_names)

        if left_op_key_cols != right_op_key_cols:
            raise ValueError(
                f"Key columns are expected to be present and have the same types "
                "in both left and right operands of the join operation: "
                f"left has {left_op_schema}, but right has {right_op_schema}"
            )

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        return PredicatePassThroughBehavior.CONDITIONAL

    def which_side_to_push_predicate(
        self, predicate_expr: "Expr"
    ) -> Optional[JoinSide]:
        """Determine which side of the join to push a predicate to.

        Returns the side to push to, or None if pushdown is not safe.

        Predicate pushdown is safe for:
        - INNER: Can push to either side
        - LEFT_OUTER/SEMI/ANTI: Can push to left side (preserved/output side)
        - RIGHT_OUTER/SEMI/ANTI: Can push to right side (preserved/output side)
        - FULL_OUTER: Cannot push (both sides can generate nulls)

        The predicate must reference columns from exactly one side of the join,
        OR reference only join key columns that all exist on one side.
        """
        # Get predicate columns and schemas
        predicate_columns = self._get_referenced_columns(predicate_expr)
        left_schema = self.input_dependencies[0].infer_schema()
        right_schema = self.input_dependencies[1].infer_schema()

        if not left_schema or not right_schema:
            return None

        # Get column sets for each side
        left_columns = set(left_schema.names)
        right_columns = set(right_schema.names)
        left_join_keys = set(self.left_key_columns)
        right_join_keys = set(self.right_key_columns)

        # Get pushdown rules for this join type
        can_push_left, can_push_right = self._get_pushdown_rules()

        # Check if predicate can be evaluated on left side
        # Condition: ALL predicate columns must exist on left (either as regular columns or join keys)
        can_evaluate_on_left = predicate_columns.issubset(
            left_columns
        ) or predicate_columns.issubset(left_join_keys)
        if can_evaluate_on_left and can_push_left:
            return JoinSide.LEFT

        # Check if predicate can be evaluated on right side
        can_evaluate_on_right = predicate_columns.issubset(
            right_columns
        ) or predicate_columns.issubset(right_join_keys)
        if can_evaluate_on_right and can_push_right:
            return JoinSide.RIGHT

        # Cannot push down
        return None

    def _get_pushdown_rules(self) -> Tuple[bool, bool]:
        """Get pushdown rules for the current join type.

        Returns:
            Tuple of (can_push_left, can_push_right) indicating which sides
            can accept predicate pushdown for this join type.
        """
        pushdown_rules = {
            JoinType.INNER: (True, True),
            JoinType.LEFT_OUTER: (True, False),
            JoinType.RIGHT_OUTER: (False, True),
            JoinType.LEFT_SEMI: (True, False),
            JoinType.RIGHT_SEMI: (False, True),
            JoinType.LEFT_ANTI: (True, False),
            JoinType.RIGHT_ANTI: (False, True),
            JoinType.FULL_OUTER: (False, False),
        }
        return pushdown_rules.get(self.join_type, (False, False))

    def _get_referenced_columns(self, expr: "Expr") -> set[str]:
        """Extract all column names referenced in an expression."""
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            _ColumnReferenceCollector,
        )

        visitor = _ColumnReferenceCollector()
        visitor.visit(expr)
        return set(visitor.get_column_refs())

    def required_input_columns(
        self, required_output_columns: Optional["FrozenSet[str]"]
    ) -> Optional[List[Optional["Set[str]"]]]:
        """Each join input contributes the join keys (needed to perform the
        join) plus the input columns behind the output columns referenced above
        the join.

        Output columns may be suffixed (when left/right have overlapping non-key
        names), so a referenced output name is mapped back to its origin input
        column and side. Pruning columns never changes which rows match or
        null-pad, so this is safe for every join type (inner/outer/semi/anti).

        Returns ``[left_keep, right_keep]``, or ``None`` to decline when any
        schema is unknown or the output-name model doesn't reproduce the join's
        real output schema (so we never risk pruning the wrong column).
        """
        import pyarrow as pa

        if required_output_columns is None:
            # All join outputs are needed -> nothing safe to prune.
            return None

        left_schema = self.input_dependencies[0].infer_schema()
        right_schema = self.input_dependencies[1].infer_schema()
        out_schema = self.infer_schema()
        if (
            not isinstance(left_schema, pa.Schema)
            or not isinstance(right_schema, pa.Schema)
            or not isinstance(out_schema, pa.Schema)
        ):
            return None

        left_cols, right_cols = list(left_schema.names), list(right_schema.names)
        left_keys, right_keys = set(self.left_key_columns), set(self.right_key_columns)

        # Which sides appear in the output. Semi/anti joins emit only one side.
        left_only = self.join_type in (JoinType.LEFT_SEMI, JoinType.LEFT_ANTI)
        right_only = self.join_type in (JoinType.RIGHT_SEMI, JoinType.RIGHT_ANTI)
        left_in_output = not right_only
        right_in_output = not left_only

        # Suffixing only happens when both sides are in the output and share
        # non-key column names. Crucially, the suffix is collision-dependent:
        # dropping one side's colliding column would un-suffix the other's output
        # name. So whenever suffixing is in play we must retain the colliding
        # columns on both sides to keep every output name stable.
        suffixed = left_in_output and right_in_output
        collisions = (
            (set(left_cols) - left_keys) & (set(right_cols) - right_keys)
            if suffixed
            else set()
        )
        left_suffix = self.left_columns_suffix or ""
        right_suffix = self.right_columns_suffix or ""

        def _out_name(name: str, suffix: str) -> str:
            return name + suffix if name in collisions else name

        # Map each output column name back to its origin input column per side
        # (right key columns are coalesced into the left keys -> not in output).
        out_to_left = (
            {_out_name(c, left_suffix): c for c in left_cols} if left_in_output else {}
        )
        out_to_right = (
            {_out_name(c, right_suffix): c for c in right_cols if c not in right_keys}
            if right_in_output
            else {}
        )

        # If this model doesn't exactly reproduce the join's real output columns,
        # bail out rather than risk pruning the wrong column.
        if set(out_to_left) | set(out_to_right) != set(out_schema.names):
            return None

        # Each side keeps its join keys (needed to perform the join), the
        # collision columns (to keep output names stable), and the input columns
        # behind the referenced output columns.
        left_keep = set(left_keys) | collisions
        right_keep = set(right_keys) | collisions
        for output_column in required_output_columns:
            if output_column in out_to_left:
                left_keep.add(out_to_left[output_column])
            if output_column in out_to_right:
                right_keep.add(out_to_right[output_column])

        return [left_keep, right_keep]

    def infer_schema(self) -> Optional["Schema"]:
        """Infer the output schema by running the shared ``join_tables``
        utility on empty tables built from the input schemas. The same
        utility runs at execution time, so plan-time and runtime schemas
        agree by construction.
        """
        import pyarrow as pa

        from ray.data._internal.execution.operators.join import join_tables

        left_schema = self.input_dependencies[0].infer_schema()
        right_schema = self.input_dependencies[1].infer_schema()
        if not isinstance(left_schema, pa.Schema) or not isinstance(
            right_schema, pa.Schema
        ):
            return None

        join_type_enum = (
            self.join_type
            if isinstance(self.join_type, JoinType)
            else JoinType(self.join_type)
        )
        try:
            joined = join_tables(
                left_schema.empty_table(),
                right_schema.empty_table(),
                join_type=join_type_enum,
                left_key_col_names=tuple(self.left_key_columns),
                right_key_col_names=tuple(self.right_key_columns),
                left_columns_suffix=self.left_columns_suffix,
                right_columns_suffix=self.right_columns_suffix,
            )
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowKeyError, ValueError):
            return None
        return joined.schema
