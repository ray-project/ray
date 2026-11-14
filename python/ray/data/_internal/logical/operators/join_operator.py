from dataclasses import InitVar, dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set, Tuple, Union

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
    """Condition keys plus the residual; see ``Join.resolve_condition``."""

    left_condition_key_columns: Tuple[str, ...]
    right_condition_key_columns: Tuple[str, ...]
    residual: Optional[Expr]


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
    # equalities into the condition keys below; the residual stays here
    condition: Optional[Expr] = None
    # Hashed on and matched like the key columns, but not coalesced: both
    # columns of a pair stay in the output, as with a SQL ``ON``.
    left_condition_key_columns: Tuple[str, ...] = ()
    right_condition_key_columns: Tuple[str, ...] = ()
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
        object.__setattr__(
            self,
            "left_condition_key_columns",
            tuple(self.left_condition_key_columns),
        )
        object.__setattr__(
            self,
            "right_condition_key_columns",
            tuple(self.right_condition_key_columns),
        )
        assert len(self.left_condition_key_columns) == len(
            self.right_condition_key_columns
        )
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
        be resolved. Without a condition, the result just repeats the
        condition keys already on the operator.

        Callers that already hold the input schemas pass them in; otherwise
        they are inferred here, and that result is cached on the operator.
        (``infer_schema`` passes its own, so a chain of joins doesn't infer
        each input twice per level.)

        Example: with ``on="k"``, ``right_on="k2"`` and the condition
        ``(col("a") == col("b")) & (col("x") < col("y"))``, where ``a`` and
        ``x`` are left columns and ``b`` and ``y`` right ones, the condition
        keys are ``("a",)`` / ``("b",)`` and the residual is
        ``col("x") < col("y")``.
        """
        if self.condition is None:
            return ResolvedJoinCondition(
                self.left_condition_key_columns,
                self.right_condition_key_columns,
                None,
            )
        if left_schema is None and right_schema is None:
            cached = self.__dict__.get("_inferred_resolution")
            if cached is None:
                cached = self._resolve_condition(
                    self.input_dependencies[0].infer_schema(),
                    self.input_dependencies[1].infer_schema(),
                )
                if cached is not None:
                    object.__setattr__(self, "_inferred_resolution", cached)
            return cached
        if left_schema is None:
            left_schema = self.input_dependencies[0].infer_schema()
        if right_schema is None:
            right_schema = self.input_dependencies[1].infer_schema()
        return self._resolve_condition(left_schema, right_schema)

    def _resolve_condition(
        self,
        left_schema: Union["Schema", "BlockSchema", None],
        right_schema: Union["Schema", "BlockSchema", None],
    ) -> Optional[ResolvedJoinCondition]:
        condition = self.condition
        assert condition is not None
        if left_schema is None or right_schema is None:
            return None
        left_columns = set(left_schema.names)
        right_columns = set(right_schema.names)
        from ray.data._internal.datasource_v2.common.pushdown_utils import (
            combine_predicate_chain,
            flatten_predicate_chain,
        )
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        for name in get_column_references(condition):
            _resolve_side(name, left_columns, right_columns)

        left_keys = list(self.left_condition_key_columns)
        right_keys = list(self.right_condition_key_columns)
        # Everything already matched on, ``on``/``right_on`` included.
        left_matched = set(self.left_key_columns) | set(left_keys)
        right_matched = set(self.right_key_columns) | set(right_keys)
        pairs = set(zip(self.left_key_columns, self.right_key_columns))
        pairs.update(zip(left_keys, right_keys))
        residual: List[Expr] = []
        for conjunct in flatten_predicate_chain(condition, Operation.AND):
            pair = _as_cross_side_equality(conjunct, left_columns, right_columns)
            if pair is None:
                residual.append(conjunct)
            elif pair not in pairs:
                # A column is hashed on only once per side.
                for name, matched, side in (
                    (pair[0], left_matched, "left"),
                    (pair[1], right_matched, "right"),
                ):
                    if name in matched:
                        raise ValueError(
                            f"Column '{name}' of the {side} input is already a "
                            f"join key, so the equality between '{pair[0]}' and "
                            f"'{pair[1]}' in `condition` can't become another one. "
                            "Apply it with `filter()` after the join instead."
                        )
                pairs.add(pair)
                left_matched.add(pair[0])
                right_matched.add(pair[1])
                left_keys.append(pair[0])
                right_keys.append(pair[1])
        return ResolvedJoinCondition(
            tuple(left_keys),
            tuple(right_keys),
            combine_predicate_chain(residual, Operation.AND),
        )

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        return PredicatePassThroughBehavior.CONDITIONAL

    def allows_predicate_pushdown_to(self, side: JoinSide) -> bool:
        """Whether filtering side's input ahead of the join cannot change the
        result: INNER either side; the preserved side of an outer / semi / anti
        join; neither side of a FULL_OUTER, since both can generate nulls.
        """
        return self._get_pushdown_rules()[side.value]

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

    def output_to_input_columns(
        self,
    ) -> Optional[Tuple[Dict[str, str], Dict[str, str]]]:
        """The mapping table of output column names to input"""
        left_schema = self.input_dependencies[0].infer_schema()
        right_schema = self.input_dependencies[1].infer_schema()
        if not left_schema or not right_schema:
            return None
        left_columns = list(left_schema.names)
        right_columns = list(right_schema.names)

        # Semi / anti joins emit one side unchanged.
        if self.join_type in (JoinType.LEFT_SEMI, JoinType.LEFT_ANTI):
            return {c: c for c in left_columns}, {}
        if self.join_type in (JoinType.RIGHT_SEMI, JoinType.RIGHT_ANTI):
            return {}, {c: c for c in right_columns}

        left_keys = set(self.left_key_columns)
        right_keys = set(self.right_key_columns)
        shared_keys = {
            l for l, r in zip(self.left_key_columns, self.right_key_columns) if l == r
        }
        collisions = set(left_columns) & (set(right_columns) - right_keys)
        left_renamed = collisions - left_keys if self.left_columns_suffix else set()
        right_renamed = collisions if self.right_columns_suffix else set()

        def output_name(column: str, renamed: Set[str], suffix: Optional[str]) -> str:
            return f"{column}{suffix}" if column in renamed else column

        # Map the output to input based on suffix
        left = {
            output_name(c, left_renamed, self.left_columns_suffix): c
            for c in left_columns
        }
        right = {
            output_name(c, right_renamed, self.right_columns_suffix): c
            for c in right_columns
        }
        # Same-name keys are the one name both sides may emit: they hold the
        # same values, so a predicate on them can go to either side.
        emitted_by_both = (left.keys() & right.keys()) - shared_keys
        return (
            {name: c for name, c in left.items() if name not in emitted_by_both},
            {name: c for name, c in right.items() if name not in emitted_by_both},
        )

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
        # Use the condition keys ExtractEquiJoinKeys will produce, so the schema
        # is the same before and after the rewrite (a pair left in the condition
        # collides when both sides name it alike; a condition key can't).
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
                left_key_col_names=self.left_key_columns,
                right_key_col_names=self.right_key_columns,
                left_condition_key_col_names=resolved.left_condition_key_columns,
                right_condition_key_col_names=resolved.right_condition_key_columns,
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
