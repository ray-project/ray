import functools
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional

from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.logical.interfaces import (
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data._internal.logical.operators.one_to_one_operator import AbstractOneToOne
from ray.data.block import UserDefinedFunction
from ray.data.expressions import Expr, StarExpr
from ray.data.preprocessor import Preprocessor

__all__ = [
    "AbstractMap",
    "AbstractUDFMap",
    "Filter",
    "FlatMap",
    "MapBatches",
    "MapRows",
    "Project",
    "StreamingRepartition",
]


logger = logging.getLogger(__name__)


@dataclass(frozen=True, repr=False)
class AbstractMap(AbstractOneToOne):
    """Abstract class for logical operators that should be converted to physical
    MapOperator.
    """

    min_rows_per_bundled_input: Optional[int] = None
    ray_remote_args: Optional[Dict[str, Any]] = None
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None
    compute: Optional[ComputeStrategy] = None
    per_block_limit: Optional[int] = None

    def __post_init__(self) -> None:
        if self.ray_remote_args is None:
            object.__setattr__(self, "ray_remote_args", {})
        if self.compute is None:
            object.__setattr__(self, "compute", TaskPoolStrategy())
        super().__post_init__()

    def set_per_block_limit(self, per_block_limit: int):
        object.__setattr__(self, "per_block_limit", per_block_limit)


@dataclass(frozen=True, repr=False)
class AbstractUDFMap(AbstractMap):
    """Abstract class for logical operators performing a UDF that should be converted
    to physical MapOperator.
    """

    fn: Optional[UserDefinedFunction] = None
    fn_args: Optional[Iterable[Any]] = None
    fn_kwargs: Optional[Dict[str, Any]] = None
    fn_constructor_args: Optional[Iterable[Any]] = None
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.name is not None and self.fn is not None and "(" not in self.name:
            object.__setattr__(
                self, "name", self._get_operator_name(self.name, self.fn)
            )
        super().__post_init__()

    def _get_operator_name(self, op_name: str, fn: UserDefinedFunction):
        """Gets the Operator name including the map `fn` UDF name."""
        # If the input `fn` is a Preprocessor, the
        # name is simply the name of the Preprocessor class.
        if inspect.ismethod(fn) and isinstance(fn.__self__, Preprocessor):
            return fn.__self__.__class__.__name__

        # Otherwise, it takes the form of `<MapOperator class>(<UDF name>)`,
        # e.g. `MapBatches(my_udf)`.
        try:
            if inspect.isclass(fn):
                # callable class
                return f"{op_name}({fn.__name__})"
            elif inspect.ismethod(fn):
                # class method
                return f"{op_name}({fn.__self__.__class__.__name__}.{fn.__name__})"
            elif inspect.isfunction(fn):
                # normal function or lambda function.
                return f"{op_name}({fn.__name__})"
            elif isinstance(fn, functools.partial):
                # functools.partial
                return f"{op_name}({fn.func.__name__})"
            else:
                # callable object.
                return f"{op_name}({fn.__class__.__name__})"
        except AttributeError as e:
            logger.error("Failed to get name of UDF %s: %s", fn, e)
            return "<unknown>"


@dataclass(frozen=True, repr=False)
class MapBatches(AbstractUDFMap):
    """Logical operator for map_batches."""

    batch_size: Optional[int] = None
    batch_format: str = "default"
    zero_copy_batch: bool = True

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(self, "name", "MapBatches")
        super().__post_init__()


@dataclass(frozen=True, repr=False)
class MapRows(AbstractUDFMap):
    """Logical operator for map."""

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(self, "name", "Map")
        object.__setattr__(self, "can_modify_num_rows", False)
        super().__post_init__()


@dataclass(frozen=True, repr=False)
class Filter(AbstractUDFMap):
    """Logical operator for filter."""

    predicate_expr: Optional[Expr] = None

    def __post_init__(self) -> None:
        provided_params = sum([self.fn is not None, self.predicate_expr is not None])
        if provided_params != 1:
            raise ValueError(
                "Exactly one of 'fn' or 'predicate_expr' must be provided "
                f"(received fn={self.fn}, predicate_expr={self.predicate_expr})"
            )
        if self.name is None:
            object.__setattr__(self, "name", "Filter")
        if self.predicate_expr is not None:
            object.__setattr__(self, "name", self._get_operator_name("Filter", self.fn))
        object.__setattr__(self, "can_modify_num_rows", True)
        super().__post_init__()

    def is_expression_based(self) -> bool:
        return self.predicate_expr is not None

    def _get_operator_name(self, op_name: str, fn: UserDefinedFunction):
        if self.is_expression_based():
            # Get a concise inline string representation of the expression
            from ray.data._internal.planner.plan_expression.expression_visitors import (
                _InlineExprReprVisitor,
            )

            expr_str = _InlineExprReprVisitor().visit(self.predicate_expr)

            # Truncate only the final result if too long
            max_length = 60
            if len(expr_str) > max_length:
                expr_str = expr_str[: max_length - 3] + "..."

            return f"{op_name}({expr_str})"
        return super()._get_operator_name(op_name, fn)


@dataclass(frozen=True, repr=False)
class Project(AbstractMap, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for all Projection Operations."""

    exprs: list["Expr"] = None  # type: ignore[assignment]
    batch_size: Optional[int] = None
    batch_format: str = "pyarrow"
    zero_copy_batch: bool = True

    def __post_init__(self) -> None:
        if self.compute is None:
            object.__setattr__(
                self, "compute", self._detect_and_get_compute_strategy(self.exprs)
            )
        if self.name is None:
            object.__setattr__(self, "name", "Project")
        object.__setattr__(self, "can_modify_num_rows", False)
        super().__post_init__()
        for expr in self.exprs:
            if expr.name is None and not isinstance(expr, StarExpr):
                raise TypeError(
                    "All Project expressions must be named (use .alias(name) or col(name)), "
                    "or be a star() expression."
                )

    def _detect_and_get_compute_strategy(self, exprs: list["Expr"]) -> ComputeStrategy:
        """Detect if expressions contain callable class UDFs and return appropriate compute strategy.

        If any expression contains a callable class UDF, returns ActorPoolStrategy.
        Otherwise returns TaskPoolStrategy.
        """
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            _CallableClassUDFCollector,
        )

        # Check all expressions for callable class UDFs
        for expr in exprs:
            collector = _CallableClassUDFCollector()
            collector.visit(expr)
            if collector.get_callable_class_udfs():
                # Found at least one callable class UDF - use actor semantics
                from ray.data._internal.compute import ActorPoolStrategy

                return ActorPoolStrategy(min_size=1, max_size=None)

        # No callable class UDFs found - use task-based execution
        from ray.data._internal.compute import TaskPoolStrategy

        return TaskPoolStrategy()

    def has_star_expr(self) -> bool:
        return self.get_star_expr() is not None

    def get_star_expr(self) -> Optional[StarExpr]:
        """Check if this projection contains a star() expression."""
        for expr in self.exprs:
            if isinstance(expr, StarExpr):
                return expr

        return None

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        return PredicatePassThroughBehavior.PASSTHROUGH_WITH_SUBSTITUTION

    def get_column_substitutions(self) -> Optional[Dict[str, str]]:
        """Returns the column renames from this projection.

        Maps source_column_name -> output_column_name. This is what we need
        to rebind predicates when pushing through.
        """
        # Reuse the existing logic from projection pushdown
        from ray.data._internal.logical.rules.projection_pushdown import (
            _extract_input_columns_renaming_mapping,
        )

        rename_map = _extract_input_columns_renaming_mapping(self.exprs)
        return rename_map if rename_map else None


@dataclass(frozen=True, repr=False)
class FlatMap(AbstractUDFMap):
    """Logical operator for flat_map."""

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(self, "name", "FlatMap")
        object.__setattr__(self, "can_modify_num_rows", True)
        super().__post_init__()


@dataclass(frozen=True, repr=False)
class StreamingRepartition(AbstractMap):
    """Logical operator for streaming repartition operation.
    Args:
        input_op: The operator preceding this operator in the plan DAG.
        target_num_rows_per_block: The target number of rows per block granularity for
           streaming repartition.
    """

    target_num_rows_per_block: int = 0

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(
                self,
                "name",
                f"StreamingRepartition[num_rows_per_block={self.target_num_rows_per_block}]",
            )
        object.__setattr__(self, "can_modify_num_rows", False)
        super().__post_init__()
