from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.logical.operators.n_ary_operator import NAry

if TYPE_CHECKING:
    from ray.data.dataset import Schema
    from ray.data.expressions import Expr


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


class Join(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for join."""

    def __init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
        join_type: str,
        left_key_columns: Tuple[str],
        right_key_columns: Tuple[str],
        *,
        num_partitions: int,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
        partition_size_hint: Optional[int] = None,
        aggregator_ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            left_input_op: The input operator at left hand side.
            right_input_op: The input operator at right hand side.
            join_type: The kind of join that should be performed, one of ("inner",
               "left_outer", "right_outer", "full_outer", "left_semi", "right_semi",
               "left_anti", "right_anti").
            left_key_columns: The columns from the left Dataset that should be used as
              keys of the join operation.
            right_key_columns: The columns from the right Dataset that should be used as
              keys of the join operation.
            partition_size_hint: Hint to joining operator about the estimated
              avg expected size of the resulting partition (in bytes)
            num_partitions: Total number of expected blocks outputted by this
                operator.
        """

        try:
            join_type_enum = JoinType(join_type)
        except ValueError:
            raise ValueError(
                f"Invalid join type: '{join_type}'. "
                f"Supported join types are: {', '.join(jt.value for jt in JoinType)}."
            )

        super().__init__(left_input_op, right_input_op, num_outputs=num_partitions)

        self._left_key_columns = left_key_columns
        self._right_key_columns = right_key_columns
        self._join_type = join_type_enum

        self._left_columns_suffix = left_columns_suffix
        self._right_columns_suffix = right_columns_suffix

        self._partition_size_hint = partition_size_hint
        self._aggregator_ray_remote_args = aggregator_ray_remote_args

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

        When column suffixes are used, this method strips the suffixes to match
        against the original column names in the input schemas.
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
        left_join_keys = set(self._left_key_columns)
        right_join_keys = set(self._right_key_columns)

        # Get pushdown rules for this join type
        can_push_left, can_push_right = self._get_pushdown_rules()

        # Check if predicate can be evaluated on left side
        # Strip left suffix from predicate columns and check against left schema
        predicate_cols_without_left_suffix = self._strip_suffix_from_columns(
            predicate_columns, self._left_columns_suffix
        )
        can_evaluate_on_left = predicate_cols_without_left_suffix.issubset(
            left_columns
        ) or predicate_cols_without_left_suffix.issubset(left_join_keys)
        if can_evaluate_on_left and can_push_left:
            return JoinSide.LEFT

        # Check if predicate can be evaluated on right side
        # Strip right suffix from predicate columns and check against right schema
        predicate_cols_without_right_suffix = self._strip_suffix_from_columns(
            predicate_columns, self._right_columns_suffix
        )
        can_evaluate_on_right = predicate_cols_without_right_suffix.issubset(
            right_columns
        ) or predicate_cols_without_right_suffix.issubset(right_join_keys)
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
        return pushdown_rules.get(self._join_type, (False, False))

    def _get_referenced_columns(self, expr: "Expr") -> set[str]:
        """Extract all column names referenced in an expression."""
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            _ColumnReferenceCollector,
        )

        visitor = _ColumnReferenceCollector()
        visitor.visit(expr)
        return set(visitor.get_column_refs())

    def _strip_suffix_from_columns(
        self, columns: set[str], suffix: Optional[str]
    ) -> set[str]:
        """Strip a suffix from column names if present.

        Args:
            columns: Set of column names (potentially with suffixes)
            suffix: The suffix to strip (e.g., "_l" or "_r")

        Returns:
            Set of column names with the suffix removed (if it was present)
        """
        if suffix is None or suffix == "":
            return columns

        return {col[: -len(suffix)] if col.endswith(suffix) else col for col in columns}

    def get_column_substitutions(
        self, side: Optional[JoinSide] = None
    ) -> dict[str, str]:
        """Get column substitutions for predicate pushdown.

        This method implements the standard predicate passthrough interface.
        For Join operators, column substitutions depend on which side is being
        pushed to, so the `side` parameter must be provided.

        Args:
            side: Which side of the join to get substitutions for. Required for
                  Join operators, optional for compatibility with other operators.

        Returns:
            Dictionary mapping from original_name -> suffixed_name for the specified
            side, or empty dict if no substitutions are needed or no side specified.
        """
        if side is None:
            # Join requires side context for substitutions
            return {}

        # Get suffix and schema for the specified side
        suffix = (
            self._left_columns_suffix
            if side == JoinSide.LEFT
            else self._right_columns_suffix
        )
        input_op = self.input_dependencies[side.value]
        schema = input_op.infer_schema()

        if not schema or suffix is None:
            return {}

        # Create mapping: original_name -> suffixed_name
        # This will be inverted by _substitute_predicate_columns to map:
        # suffixed_name -> col(original_name)
        return {col_name: col_name + suffix for col_name in schema.names}
