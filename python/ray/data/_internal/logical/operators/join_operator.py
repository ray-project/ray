from dataclasses import InitVar, dataclass, field, replace
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.logical.operators.n_ary_operator import NAry
from ray.data.expressions import BinaryExpr, ColumnExpr, Expr, Operation

if TYPE_CHECKING:
    from ray.data.block import Schema as BlockSchema
    from ray.data.dataset import Schema

__all__ = [
    "Join",
    "JoinSide",
    "JoinType",
    "ResolvedJoinCondition",
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


@dataclass(frozen=True)
class ResolvedJoinCondition:
    """Keys after extraction plus the residual; see ``Join.resolve_condition``."""

    left_key_columns: Tuple[str, ...]
    right_key_columns: Tuple[str, ...]
    residual: Optional[Expr]


def _flatten_conjuncts(expr: Expr) -> List[Expr]:
    if isinstance(expr, BinaryExpr) and expr.op is Operation.AND:
        return _flatten_conjuncts(expr.left) + _flatten_conjuncts(expr.right)
    return [expr]


def _resolve_side(
    name: str, left_columns: Set[str], right_columns: Set[str]
) -> JoinSide:
    on_left = name in left_columns
    on_right = name in right_columns
    if on_left and on_right:
        raise ValueError(
            f"Column '{name}' in the join `condition` exists on both sides of "
            "the join, so it is ambiguous. Rename it on one side before joining."
        )
    if not on_left and not on_right:
        raise ValueError(
            f"Column '{name}' in the join `condition` is not a column of either "
            f"join input (left has {sorted(left_columns)}, right has "
            f"{sorted(right_columns)})."
        )
    return JoinSide.LEFT if on_left else JoinSide.RIGHT


def _as_cross_side_equality(
    conjunct: Expr, left_columns: Set[str], right_columns: Set[str]
) -> Optional[Tuple[str, str]]:
    """``(left_column, right_column)`` if ``conjunct`` is an equality between
    a column of each side (in either operand order), else ``None``."""
    if not (
        isinstance(conjunct, BinaryExpr)
        and conjunct.op is Operation.EQ
        and isinstance(conjunct.left, ColumnExpr)
        and isinstance(conjunct.right, ColumnExpr)
    ):
        return None
    first, second = conjunct.left.name, conjunct.right.name
    first_side = _resolve_side(first, left_columns, right_columns)
    second_side = _resolve_side(second, left_columns, right_columns)
    if first_side is second_side:
        return None
    return (first, second) if first_side is JoinSide.LEFT else (second, first)


@dataclass(frozen=True, repr=False, eq=False)
class Join(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for join."""

    left_input_op: InitVar[LogicalOperator]
    right_input_op: InitVar[LogicalOperator]
    join_type: Union[JoinType, str]
    left_key_columns: Tuple[str, ...]
    right_key_columns: Tuple[str, ...]
    num_partitions: int
    left_columns_suffix: Optional[str] = None
    right_columns_suffix: Optional[str] = None
    partition_size_hint: Optional[int] = None
    aggregator_ray_remote_args: Optional[Dict[str, Any]] = None
    # Over *input* column names. ``ExtractEquiJoinKeys`` moves cross-side
    # equalities into the keys; the residual stays here (ray-project/ray#66835).
    condition: Optional[Expr] = None
    _input_dependencies: list[LogicalOperator] = field(init=False, repr=False)

    def __post_init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
    ):
        try:
            join_type_enum = JoinType(self.join_type)
        except ValueError:
            raise ValueError(
                f"Invalid join type: '{self.join_type}'. "
                f"Supported join types are: {', '.join(jt.value for jt in JoinType)}."
            )

        object.__setattr__(self, "join_type", join_type_enum)
        object.__setattr__(self, "left_key_columns", tuple(self.left_key_columns))
        object.__setattr__(self, "right_key_columns", tuple(self.right_key_columns))
        if len(self.left_key_columns) < 1:
            raise ValueError(
                "At least 1 column name to join on has to be provided via `on`"
                + (
                    "; a `condition` alone is not supported"
                    if self.condition is not None
                    else ""
                )
            )
        if len(self.left_key_columns) != len(self.right_key_columns):
            raise ValueError(
                "Number of columns provided for left and right datasets has to "
                f"match (got {list(self.left_key_columns)} and "
                f"{list(self.right_key_columns)})"
            )
        object.__setattr__(
            self,
            "_input_dependencies",
            [left_input_op, right_input_op],
        )
        # Mirror the reduce-task remote args onto `ray_remote_args` so fusing a downstream map
        # into this join's reduce task respects the reduce task's actual args.
        object.__setattr__(
            self, "ray_remote_args", dict(self.aggregator_ray_remote_args or {})
        )

    @property
    def num_outputs(self) -> Optional[int]:
        return self.num_partitions

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

        if len(set(left_key_column_names)) != len(left_key_column_names):
            raise ValueError(
                f"Join key columns must be unique in the left operand: "
                f"{left_key_column_names}"
            )

        if len(set(right_key_column_names)) != len(right_key_column_names):
            raise ValueError(
                f"Join key columns must be unique in the right operand: "
                f"{right_key_column_names}"
            )

        def _get_key_column_types(schema: "Schema", keys: Tuple[str]):
            if schema is None:
                return None

            column_types = dict(zip(schema.names, schema.types))

            if any(key not in column_types for key in keys):
                return None

            return [column_types[key] for key in keys]

        right_op_key_cols = _get_key_column_types(
            right_op_schema, right_key_column_names
        )
        left_op_key_cols = _get_key_column_types(left_op_schema, left_key_column_names)

        if (
            left_op_key_cols is None
            or right_op_key_cols is None
            or left_op_key_cols != right_op_key_cols
        ):
            raise ValueError(
                f"Key columns are expected to be present and have the same types "
                "in both left and right operands of the join operation: "
                f"left has {left_op_schema}, but right has {right_op_schema}"
            )

    def resolve_condition(
        self,
        left_schema: Union["Schema", "BlockSchema", None] = None,
        right_schema: Union["Schema", "BlockSchema", None] = None,
    ) -> Optional[ResolvedJoinCondition]:
        """Resolve ``condition`` against the input schemas.

        Column references in ``condition`` name input columns (before any
        suffix is applied), so each must belong to exactly one side; an
        unknown or ambiguous column raises ``ValueError``. Returns ``None``
        when either input schema is unknown and the condition therefore can't
        be resolved. Without a condition, the result just repeats the keys.

        Callers that already hold the input schemas pass them in; otherwise
        they are inferred here. (``infer_schema`` passes its own, so a chain
        of joins doesn't infer each input twice per level.)
        """
        if self.condition is None:
            return ResolvedJoinCondition(
                self.left_key_columns, self.right_key_columns, None
            )
        if left_schema is None:
            left_schema = self.input_dependencies[0].infer_schema()
        if right_schema is None:
            right_schema = self.input_dependencies[1].infer_schema()
        if left_schema is None or right_schema is None:
            return None
        left_columns = set(left_schema.names)
        right_columns = set(right_schema.names)
        for name in self._get_referenced_columns(self.condition):
            _resolve_side(name, left_columns, right_columns)

        left_keys = list(self.left_key_columns)
        right_keys = list(self.right_key_columns)
        pairs = set(zip(left_keys, right_keys))
        residual: Optional[Expr] = None
        for conjunct in _flatten_conjuncts(self.condition):
            pair = _as_cross_side_equality(conjunct, left_columns, right_columns)
            if pair is None:
                residual = conjunct if residual is None else residual & conjunct
            elif pair not in pairs:
                pairs.add(pair)
                left_keys.append(pair[0])
                right_keys.append(pair[1])
        return ResolvedJoinCondition(tuple(left_keys), tuple(right_keys), residual)

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
        # Use the keys ExtractEquiJoinKeys will produce, so the schema is the same
        # before and after the rewrite. Don't raise on a bad condition here:
        # Dataset.__repr__ calls this and expects None for "unknown".
        try:
            resolved = self.resolve_condition(left_schema, right_schema)
        except ValueError:
            return None
        if resolved is None:
            return None
        try:
            joined = join_tables(
                left_schema.empty_table(),
                right_schema.empty_table(),
                join_type=join_type_enum,
                left_key_col_names=resolved.left_key_columns,
                right_key_col_names=resolved.right_key_columns,
                left_columns_suffix=self.left_columns_suffix,
                right_columns_suffix=self.right_columns_suffix,
            )
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowKeyError, ValueError):
            return None
        except Exception as exc:
            # Polars is imported lazily, so its exception types can only be
            # referenced once the package is installed.
            try:
                from polars.exceptions import PolarsError
            except ImportError:
                pass
            else:
                if isinstance(exc, PolarsError):
                    return None
            raise
        return joined.schema
