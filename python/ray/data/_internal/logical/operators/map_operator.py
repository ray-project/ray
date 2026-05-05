import functools
import inspect
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Literal, Optional, Union

from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
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


@dataclass(frozen=True, repr=False, eq=False, init=False)
class AbstractMap(AbstractOneToOne):
    """Abstract class for logical operators that should be converted to physical
    MapOperator.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        input_op: Optional[LogicalOperator] = None,
        num_outputs: Optional[int] = None,
        *,
        can_modify_num_rows: bool,
        min_rows_per_bundled_input: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        compute: Optional[ComputeStrategy] = None,
    ):
        """Initialize an ``AbstractMap`` logical operator that will later
        be converted into a physical ``MapOperator``.

        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The
                outputs of ``input_op`` will be the inputs to this operator.
            num_outputs: Number of outputs for this operator.
            can_modify_num_rows: Whether the operator can change the row count. False if
                # of input rows = # of output rows. True otherwise.
            min_rows_per_bundled_input: Minimum number of rows a single bundle of
                blocks passed on to the task must possess.
            ray_remote_args: Args to provide to :func:`ray.remote`.
            ray_remote_args_fn: A function that returns a dictionary of remote
                args passed to each map worker. The purpose of this argument is
                to generate dynamic arguments for each actor/task, and it will
                be called each time prior to initializing the worker. Args
                returned from this dict always override the args in
                ``ray_remote_args``. Note: this is an advanced, experimental
                feature.
            compute: The compute strategy, either ``TaskPoolStrategy`` (default)
                to use Ray tasks, or ``ActorPoolStrategy`` to use an
                autoscaling actor pool.
        """
        super().__init__(
            input_op=input_op,
            can_modify_num_rows=can_modify_num_rows,
            num_outputs=num_outputs,
            name=name,
        )
        object.__setattr__(
            self, "min_rows_per_bundled_input", min_rows_per_bundled_input
        )
        object.__setattr__(self, "ray_remote_args", ray_remote_args or {})
        object.__setattr__(self, "ray_remote_args_fn", ray_remote_args_fn)
        object.__setattr__(self, "compute", compute or TaskPoolStrategy())
        object.__setattr__(self, "per_block_limit", None)

    def set_per_block_limit(self, per_block_limit: int):
        object.__setattr__(self, "per_block_limit", per_block_limit)

    def _get_args(self) -> Dict[str, Any]:
        args = super()._get_args()
        for key in [
            "can_modify_num_rows",
            "min_rows_per_bundled_input",
            "ray_remote_args",
            "ray_remote_args_fn",
            "compute",
            "per_block_limit",
        ]:
            args[f"_{key}"] = getattr(self, key)
        return args


@dataclass(frozen=True, repr=False, eq=False, init=False)
class AbstractUDFMap(AbstractMap):
    """Abstract class for logical operators performing a UDF that should be converted
    to physical MapOperator.
    """

    fn: UserDefinedFunction
    fn_args: Optional[Iterable[Any]] = None
    fn_kwargs: Optional[Dict[str, Any]] = None
    fn_constructor_args: Optional[Iterable[Any]] = None
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        fn: UserDefinedFunction,
        *,
        can_modify_num_rows: bool,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        min_rows_per_bundled_input: Optional[int] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize AbstractUDFMap.

        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            fn: User-defined function to be called.
            can_modify_num_rows: Whether the UDF can change the row count. False if
                # of input rows = # of output rows. True otherwise.
            fn_args: Arguments to `fn`.
            fn_kwargs: Keyword arguments to `fn`.
            fn_constructor_args: Arguments to provide to the initializor of `fn` if
                `fn` is a callable class.
            fn_constructor_kwargs: Keyword Arguments to provide to the initializor of
                `fn` if `fn` is a callable class.
            min_rows_per_bundled_input: The target number of rows to pass to
                ``MapOperator._add_bundled_input()``.
            compute: The compute strategy, either ``TaskPoolStrategy`` (default) to use
                Ray tasks, or ``ActorPoolStrategy`` to use an autoscaling actor pool.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict will
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
            ray_remote_args: Args to provide to :func:`ray.remote`.
        """
        name = self._get_operator_name(name, fn)
        super().__init__(
            name,
            input_op,
            can_modify_num_rows=can_modify_num_rows,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
            compute=compute,
        )
        object.__setattr__(self, "fn", fn)
        object.__setattr__(self, "fn_args", fn_args)
        object.__setattr__(self, "fn_kwargs", fn_kwargs)
        object.__setattr__(self, "fn_constructor_args", fn_constructor_args)
        object.__setattr__(self, "fn_constructor_kwargs", fn_constructor_kwargs)
        object.__setattr__(self, "ray_remote_args_fn", ray_remote_args_fn)

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


@dataclass(frozen=True, repr=False, eq=False)
class MapBatches(AbstractUDFMap):
    """Logical operator for map_batches."""

    fn: UserDefinedFunction
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    can_modify_num_rows: bool = False
    batch_size: Union[Optional[int], Literal["auto"]] = None
    batch_format: Optional[str] = "default"
    zero_copy_batch: bool = True
    fn_args: Optional[Iterable[Any]] = None
    fn_kwargs: Optional[Dict[str, Any]] = None
    fn_constructor_args: Optional[Iterable[Any]] = None
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None
    min_rows_per_bundled_input: Optional[int] = None
    compute: Optional[ComputeStrategy] = None
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    per_block_limit: Optional[int] = None
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if self.compute is None:
            object.__setattr__(self, "compute", TaskPoolStrategy())
        object.__setattr__(
            self,
            "_name",
            self._get_operator_name(self.__class__.__name__, self.fn),
        )
        object.__setattr__(self, "_num_outputs", None)


@dataclass(frozen=True, repr=False, eq=False)
class MapRows(AbstractUDFMap):
    """Logical operator for map."""

    fn: UserDefinedFunction
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    fn_args: Optional[Iterable[Any]] = None
    fn_kwargs: Optional[Dict[str, Any]] = None
    fn_constructor_args: Optional[Iterable[Any]] = None
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None
    compute: Optional[ComputeStrategy] = None
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    can_modify_num_rows: bool = field(init=False, default=False)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    per_block_limit: Optional[int] = None
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if self.compute is None:
            object.__setattr__(self, "compute", TaskPoolStrategy())
        object.__setattr__(self, "_name", self._get_operator_name("Map", self.fn))
        object.__setattr__(self, "_num_outputs", None)


@dataclass(frozen=True, repr=False, eq=False)
class Filter(AbstractUDFMap):
    """Logical operator for filter."""

    predicate_expr: Optional[Expr] = None
    fn: Optional[UserDefinedFunction] = None
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    fn_args: Optional[Iterable[Any]] = None
    fn_kwargs: Optional[Dict[str, Any]] = None
    fn_constructor_args: Optional[Iterable[Any]] = None
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None
    compute: Optional[ComputeStrategy] = None
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    per_block_limit: Optional[int] = None
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        provided_params = sum([self.fn is not None, self.predicate_expr is not None])
        if provided_params != 1:
            raise ValueError(
                "Exactly one of 'fn', or 'predicate_expr' must be provided "
                f"(received fn={self.fn}, predicate_expr={self.predicate_expr})"
            )
        if self.compute is None:
            object.__setattr__(self, "compute", TaskPoolStrategy())
        object.__setattr__(
            self,
            "_name",
            self._get_operator_name(self.__class__.__name__, self.fn),
        )
        object.__setattr__(self, "_num_outputs", None)

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


@dataclass(frozen=True, repr=False, eq=False)
class Project(AbstractMap, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for all Projection Operations."""

    exprs: list["Expr"]
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    compute: Optional[ComputeStrategy] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None
    can_modify_num_rows: bool = field(init=False, default=False)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    batch_size: Optional[int] = field(init=False, default=None)
    batch_format: str = field(init=False, default="pyarrow")
    zero_copy_batch: bool = field(init=False, default=True)
    per_block_limit: Optional[int] = None
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if self.compute is None:
            object.__setattr__(
                self, "compute", self._detect_and_get_compute_strategy(self.exprs)
            )
        for expr in self.exprs:
            if expr.name is None and not isinstance(expr, StarExpr):
                raise TypeError(
                    "All Project expressions must be named (use .alias(name) or col(name)), "
                    "or be a star() expression."
                )
        object.__setattr__(self, "_num_outputs", None)

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


@dataclass(frozen=True, repr=False, eq=False)
class FlatMap(AbstractUDFMap):
    """Logical operator for flat_map."""

    fn: UserDefinedFunction
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    fn_args: Optional[Iterable[Any]] = None
    fn_kwargs: Optional[Dict[str, Any]] = None
    fn_constructor_args: Optional[Iterable[Any]] = None
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None
    compute: Optional[ComputeStrategy] = None
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    per_block_limit: Optional[int] = None
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if self.compute is None:
            object.__setattr__(self, "compute", TaskPoolStrategy())
        object.__setattr__(
            self,
            "_name",
            self._get_operator_name(self.__class__.__name__, self.fn),
        )
        object.__setattr__(self, "_num_outputs", None)


@dataclass(frozen=True, repr=False, eq=False)
class StreamingRepartition(AbstractMap, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for streaming repartition operation.

    Args:
        input_op: The operator preceding this operator in the plan DAG.
        target_num_rows_per_block: The target number of rows per block granularity for
            streaming repartition.
        strict: If True, guarantees that all output blocks, except for the last one,
            will have exactly target_num_rows_per_block rows. If False, uses best-effort
            bundling and may produce at most one block smaller than target_num_rows_per_block
            per input block without forcing exact sizes through block splitting.
            Defaults to False.
    """

    target_num_rows_per_block: int
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    strict: bool = False
    can_modify_num_rows: bool = field(init=False, default=False)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None
    compute: Optional[ComputeStrategy] = None
    per_block_limit: Optional[int] = None
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if self.target_num_rows_per_block <= 0:
            raise ValueError(
                "target_num_rows_per_block must be positive for streaming repartition, "
                f"got {self.target_num_rows_per_block}"
            )
        if self.compute is None:
            object.__setattr__(self, "compute", TaskPoolStrategy())
        object.__setattr__(
            self,
            "_name",
            f"StreamingRepartition[num_rows_per_block={self.target_num_rows_per_block},strict={self.strict}]",
        )
        object.__setattr__(self, "_num_outputs", None)

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # StreamingRepartition only re-bundles rows into different block sizes.
        # It doesn't modify schema or filter rows, so filters can safely pass through.
        return PredicatePassThroughBehavior.PASSTHROUGH
