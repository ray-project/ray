import functools
import inspect
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional

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

logger = logging.getLogger(__name__)


class AbstractMap(AbstractOneToOne):
    """Abstract class for logical operators that should be converted to physical
    MapOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: Optional[LogicalOperator] = None,
        num_outputs: Optional[int] = None,
        *,
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
        super().__init__(name, input_op, num_outputs)
        self._min_rows_per_bundled_input = min_rows_per_bundled_input
        self._ray_remote_args = ray_remote_args or {}
        self._ray_remote_args_fn = ray_remote_args_fn
        self._compute = compute or TaskPoolStrategy()
        self._per_block_limit = None

    def set_per_block_limit(self, per_block_limit: int):
        self._per_block_limit = per_block_limit


class AbstractUDFMap(AbstractMap):
    """Abstract class for logical operators performing a UDF that should be converted
    to physical MapOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        fn: UserDefinedFunction,
        *,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        min_rows_per_bundled_input: Optional[int] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            fn: User-defined function to be called.
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
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
            compute=compute,
        )
        self._fn = fn
        self._fn_args = fn_args
        self._fn_kwargs = fn_kwargs
        self._fn_constructor_args = fn_constructor_args
        self._fn_constructor_kwargs = fn_constructor_kwargs
        self._ray_remote_args_fn = ray_remote_args_fn

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


class MapBatches(AbstractUDFMap):
    """Logical operator for map_batches."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: UserDefinedFunction,
        batch_size: Optional[int] = None,
        batch_format: str = "default",
        zero_copy_batch: bool = True,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        min_rows_per_bundled_input: Optional[int] = None,
        compute: Optional[ComputeStrategy] = None,
        udf_modifying_row_count: bool = False,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "MapBatches",
            input_op,
            fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            compute=compute,
            ray_remote_args_fn=ray_remote_args_fn,
            ray_remote_args=ray_remote_args,
        )
        self._batch_size = batch_size
        self._batch_format = batch_format
        self._zero_copy_batch = zero_copy_batch
        self._udf_modifying_row_count = udf_modifying_row_count

    def can_modify_num_rows(self) -> bool:
        return self._udf_modifying_row_count


class MapRows(AbstractUDFMap):
    """Logical operator for map."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: UserDefinedFunction,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "Map",
            input_op,
            fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
            compute=compute,
            ray_remote_args_fn=ray_remote_args_fn,
            ray_remote_args=ray_remote_args,
        )

    def can_modify_num_rows(self) -> bool:
        return False


class Filter(AbstractUDFMap):
    """Logical operator for filter."""

    def __init__(
        self,
        input_op: LogicalOperator,
        predicate_expr: Optional[Expr] = None,
        fn: Optional[UserDefinedFunction] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        # Ensure exactly one of fn, or predicate_expr is provided
        provided_params = sum([fn is not None, predicate_expr is not None])
        if provided_params != 1:
            raise ValueError(
                f"Exactly one of 'fn', or 'predicate_expr' must be provided (received fn={fn}, predicate_expr={predicate_expr})"
            )

        self._predicate_expr = predicate_expr

        super().__init__(
            "Filter",
            input_op,
            fn=fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
            compute=compute,
            ray_remote_args_fn=ray_remote_args_fn,
            ray_remote_args=ray_remote_args,
        )

    def can_modify_num_rows(self) -> bool:
        return True

    def is_expression_based(self) -> bool:
        return self._predicate_expr is not None

    def _get_operator_name(self, op_name: str, fn: UserDefinedFunction):
        if self.is_expression_based():
            # Get a concise inline string representation of the expression
            from ray.data._internal.planner.plan_expression.expression_visitors import (
                _InlineExprReprVisitor,
            )

            expr_str = _InlineExprReprVisitor().visit(self._predicate_expr)

            # Truncate only the final result if too long
            max_length = 60
            if len(expr_str) > max_length:
                expr_str = expr_str[: max_length - 3] + "..."

            return f"{op_name}({expr_str})"
        return super()._get_operator_name(op_name, fn)


class Project(AbstractMap, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for all Projection Operations."""

    def __init__(
        self,
        input_op: LogicalOperator,
        exprs: list["Expr"],
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "Project",
            input_op=input_op,
            ray_remote_args=ray_remote_args,
            compute=compute,
        )
        self._batch_size = None
        self._exprs = exprs
        self._batch_format = "pyarrow"
        self._zero_copy_batch = True

        for expr in self._exprs:
            if expr.name is None and not isinstance(expr, StarExpr):
                raise TypeError(
                    "All Project expressions must be named (use .alias(name) or col(name)), "
                    "or be a star() expression."
                )

    def has_star_expr(self) -> bool:
        return self.get_star_expr() is not None

    def get_star_expr(self) -> Optional[StarExpr]:
        """Check if this projection contains a star() expression."""
        for expr in self._exprs:
            if isinstance(expr, StarExpr):
                return expr

        return None

    @property
    def exprs(self) -> List["Expr"]:
        return self._exprs

    def can_modify_num_rows(self) -> bool:
        return False

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

        rename_map = _extract_input_columns_renaming_mapping(self._exprs)
        return rename_map if rename_map else None


class FlatMap(AbstractUDFMap):
    """Logical operator for flat_map."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: UserDefinedFunction,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "FlatMap",
            input_op,
            fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
            compute=compute,
            ray_remote_args_fn=ray_remote_args_fn,
            ray_remote_args=ray_remote_args,
        )

    def can_modify_num_rows(self) -> bool:
        return True


class StreamingRepartition(AbstractMap):
    """Logical operator for streaming repartition operation.
    Args:
        target_num_rows_per_block: The target number of rows per block granularity for
           streaming repartition.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        target_num_rows_per_block: int,
    ):
        super().__init__(
            f"StreamingRepartition[num_rows_per_block={target_num_rows_per_block}]",
            input_op,
        )
        self._target_num_rows_per_block = target_num_rows_per_block

    @property
    def target_num_rows_per_block(self) -> int:
        return self._target_num_rows_per_block

    def can_modify_num_rows(self) -> bool:
        return False
