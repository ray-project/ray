import functools
from dataclasses import InitVar, dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.logical.operators.n_ary_operator import NAry

if TYPE_CHECKING:
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
class Join(NAry, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for join."""

    left_input_op: InitVar[LogicalOperator]
    right_input_op: InitVar[LogicalOperator]
    join_type: Union[JoinType, str]
    left_key_columns: Tuple[str]
    right_key_columns: Tuple[str]
    num_partitions: int
    left_columns_suffix: Optional[str] = None
    right_columns_suffix: Optional[str] = None
    partition_size_hint: Optional[int] = None
    aggregator_ray_remote_args: Optional[Dict[str, Any]] = None
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
        """The first side (left before right) that accepts ``predicate_expr``,
        or ``None``. See :meth:`can_push_predicate_to_side`."""
        for side in JoinSide:
            if self.can_push_predicate_to_side(predicate_expr, side):
                return side
        return None

    def can_push_predicate_to_side(
        self, predicate_expr: "Expr", side: JoinSide
    ) -> bool:
        """Whether a predicate over this join's output can be applied to
        ``side``'s input instead, without changing the result.

        The join type must allow it (INNER either side; LEFT_OUTER / SEMI /
        ANTI the left side; RIGHT_* the right side; FULL_OUTER neither), and
        every column the predicate references must be one ``side`` produces in
        the output. Push the predicate through :meth:`get_column_substitutions_for_side`
        so a suffixed output name (``val_r``) is rebound to the input's own
        name (``val``).
        """
        side_columns = self._output_to_input_columns
        if side_columns is None or not self._get_pushdown_rules()[side.value]:
            return False
        referenced = self._get_referenced_columns(predicate_expr)
        return referenced.issubset(side_columns[side.value].keys())

    def get_column_substitutions_for_side(self, side: JoinSide) -> Dict[str, str]:
        """Input column name -> output column name for the columns of ``side``
        the physical join renames (a suffixed name collision); empty if none."""
        side_columns = self._output_to_input_columns
        if side_columns is None:
            return {}
        return {
            input_name: output_name
            for output_name, input_name in side_columns[side.value].items()
            if input_name != output_name
        }

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

    def __copy__(self) -> "Join":
        # A shallow copy must not carry the cached column mapping below: it is
        # derived from the inputs, which a copy exists to swap out.
        clone = object.__new__(type(self))
        clone.__dict__.update(self.__dict__)
        clone.__dict__.pop("_output_to_input_columns", None)
        return clone

    @functools.cached_property
    def _output_to_input_columns(
        self,
    ) -> Optional[Tuple[Dict[str, str], Dict[str, str]]]:
        """Per side, output column name -> the input column it comes from;
        ``None`` if an input schema is unknown.

        Mirrors the naming in ``execution/operators/join.py``: colliding
        payload names get the side's suffix, keys coalesce per join type,
        semi/anti joins emit one side unchanged. Names the mirror cannot
        place are left out, so a predicate on them stays above the join.
        Without an inferred output schema to check against (non-Arrow input
        schemas, e.g. ``from_pandas``) only names unique to one input and
        shared keys are mapped.

        Cached: ``infer_schema`` is not memoized, and pushdown asks per
        conjunct.
        """
        left_schema = self.input_dependencies[0].infer_schema()
        right_schema = self.input_dependencies[1].infer_schema()
        if not left_schema or not right_schema:
            return None
        left_columns = list(left_schema.names)
        right_columns = list(right_schema.names)
        left_keys = set(self.left_key_columns)
        right_keys = set(self.right_key_columns)

        if self.join_type in (JoinType.LEFT_SEMI, JoinType.LEFT_ANTI):
            return {c: c for c in left_columns}, {}
        if self.join_type in (JoinType.RIGHT_SEMI, JoinType.RIGHT_ANTI):
            return {}, {c: c for c in right_columns}

        shared_keys = {
            left_key
            for left_key, right_key in zip(
                self.left_key_columns, self.right_key_columns
            )
            if left_key == right_key
        }

        output_schema = self.infer_schema()
        if not output_schema:
            # Keys are coalesced away on the non-preserved side, so only a
            # shared key counts there; everything else needs a unique name.
            # A right column named like a left collision after the left suffix
            # (left ``v`` -> ``v_l`` while the right also has ``v_l``) gets
            # suffixed again by Polars, so that bare name is the left's: not
            # the right's to claim.
            left_set, right_set = set(left_columns), set(right_columns)
            is_right_outer = self.join_type is JoinType.RIGHT_OUTER
            left_renamed = (
                {
                    f"{c}{self.left_columns_suffix}"
                    for c in (left_set & (right_set - right_keys)) - left_keys
                }
                if self.left_columns_suffix
                else set()
            )
            return (
                {
                    c: c
                    for c in left_columns
                    if c in shared_keys
                    or (c not in right_set and not (is_right_outer and c in left_keys))
                },
                {
                    c: c
                    for c in right_columns
                    if c in shared_keys
                    or (
                        c not in left_set
                        and c not in left_renamed
                        and (c not in right_keys or is_right_outer)
                    )
                },
            )
        # Right keys are coalesced into the left ones, so only right non-key
        # columns can collide with left columns.
        collisions = set(left_columns) & (set(right_columns) - right_keys)
        left_suffix = self.left_columns_suffix
        right_suffix = self.right_columns_suffix
        is_right_outer = self.join_type is JoinType.RIGHT_OUTER

        left: Dict[str, str] = {}
        for c in left_columns:
            if c in left_keys:
                if not is_right_outer or c in shared_keys:
                    left[c] = c
            elif c in collisions and left_suffix:
                left[f"{c}{left_suffix}"] = c
            else:
                left[c] = c
        right: Dict[str, str] = {}
        for c in right_columns:
            if c in right_keys:
                if c in shared_keys or (is_right_outer and c not in left_columns):
                    right[c] = c
            elif c in collisions:
                if right_suffix:
                    right[f"{c}{right_suffix}"] = c
                elif c in left_keys:
                    # The left key keeps its name, so Polars suffixes this one.
                    right[f"{c}_right"] = c
                else:
                    # The left column was suffixed instead; this one keeps its name.
                    right[c] = c
            else:
                right[c] = c

        # Keep only names the join really emits, and never let both sides
        # claim one unless it is a shared key.
        output_names = set(output_schema.names)
        left = {o: i for o, i in left.items() if o in output_names}
        right = {o: i for o, i in right.items() if o in output_names}
        for name in (left.keys() & right.keys()) - shared_keys:
            del left[name]
            del right[name]
        return left, right

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
