from dataclasses import InitVar, dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.logical.operators.n_ary_operator import NAry

if TYPE_CHECKING:
    from ray.data.dataset import Schema

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
