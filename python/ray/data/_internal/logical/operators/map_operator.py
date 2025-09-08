import functools
import inspect
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.one_to_one_operator import AbstractOneToOne
from ray.data.block import UserDefinedFunction
from ray.data.expressions import Expr
from ray.data.preprocessor import Preprocessor

if TYPE_CHECKING:
    import pyarrow as pa


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
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            min_rows_per_bundled_input: Min number of rows a single bundle of blocks
                passed on to the task must possess.
            ray_remote_args: Args to provide to :func:`ray.remote`.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
        """
        super().__init__(name, input_op, num_outputs)
        self._min_rows_per_bundled_input = min_rows_per_bundled_input
        self._ray_remote_args = ray_remote_args or {}
        self._ray_remote_args_fn = ray_remote_args_fn
        self._compute = compute or TaskPoolStrategy()


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
        zero_copy_batch: bool = False,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        min_rows_per_bundled_input: Optional[int] = None,
        compute: Optional[ComputeStrategy] = None,
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

    def can_modify_num_rows(self) -> bool:
        return False


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
        fn: Optional[UserDefinedFunction] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        filter_expr: Optional["pa.dataset.Expression"] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        # Ensure exactly one of fn or filter_expr is provided
        if not ((fn is None) ^ (filter_expr is None)):
            raise ValueError("Exactly one of 'fn' or 'filter_expr' must be provided")
        self._filter_expr = filter_expr

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


class Check(AbstractUDFMap):
    """Logical operator for data quality checks."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: Optional[UserDefinedFunction] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        check_expr: Optional["pa.dataset.Expression"] = None,
        max_failures: Optional[int] = None,
        on_violation: str = "warn",
        quarantine_path: Optional[str] = None,
        quarantine_format: str = "parquet",
        metadata_path: Optional[str] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize Check logical operator.

        Args:
            input_op: The operator preceding this operator in the plan DAG.
            fn: User-defined function that returns True for valid rows.
            fn_args: Arguments to `fn`.
            fn_kwargs: Keyword arguments to `fn`.
            fn_constructor_args: Arguments to provide to the initializor of `fn` if
                `fn` is a callable class.
            fn_constructor_kwargs: Keyword arguments to provide to the initializor of
                `fn` if `fn` is a callable class.
            check_expr: PyArrow expression for data quality check.
            max_failures: Maximum number of failures before stopping execution.
            on_violation: Action to take on violations: "warn", "drop", "fail", "quarantine".
            quarantine_path: Path to store quarantined (invalid) data.
            quarantine_format: Format for quarantined data (parquet, csv, json).
            metadata_path: Path to store check metadata and statistics.
            compute: The compute strategy.
            ray_remote_args_fn: Function that returns remote args for workers.
            ray_remote_args: Args to provide to ray.remote.
        """
        # Ensure exactly one of fn or check_expr is provided
        if not ((fn is None) ^ (check_expr is None)):
            raise ValueError("Exactly one of 'fn' or 'check_expr' must be provided")

        # Validate on_violation parameter
        valid_actions = {"warn", "drop", "fail", "quarantine"}
        if on_violation not in valid_actions:
            raise ValueError(
                f"on_violation must be one of {valid_actions}, got '{on_violation}'"
            )

        # Validate quarantine configuration
        if on_violation == "quarantine" and quarantine_path is None:
            raise ValueError(
                "quarantine_path must be provided when on_violation='quarantine'"
            )

        valid_formats = {"parquet", "csv", "json"}
        if quarantine_format not in valid_formats:
            raise ValueError(
                f"quarantine_format must be one of {valid_formats}, got '{quarantine_format}'"
            )

        # Validate max_failures
        if max_failures is not None and (
            not isinstance(max_failures, int) or max_failures < 0
        ):
            raise ValueError("max_failures must be a non-negative integer or None")

        # Validate expression or function
        if check_expr is not None:
            # Expression validation will be done during planning when we have the actual string
            pass
        else:
            self._validate_callable(fn)

        # Store check-specific parameters
        self._check_expr = check_expr
        self._max_failures = max_failures
        self._on_violation = on_violation
        self._quarantine_path = quarantine_path
        self._quarantine_format = quarantine_format
        self._metadata_path = metadata_path

        # Generate unique ID for this check (important for chained checks)
        import uuid

        self._check_id = f"check_{uuid.uuid4().hex[:8]}"

        super().__init__(
            "Check",
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
        """Check operator can modify number of rows when dropping violations."""
        return self._on_violation in ("drop", "fail")

    def _validate_expression(self, expr: str) -> None:
        """Validate string expression syntax and semantics."""
        if not expr or not isinstance(expr, str):
            raise ValueError("Expression must be a non-empty string")

        # Basic syntax validation
        try:
            # Try to compile as Python expression
            compile(expr, "<string>", "eval")
        except SyntaxError as e:
            raise ValueError(f"Invalid expression syntax: {e}")

        # Check for dangerous operations
        dangerous_keywords = ["import", "__", "exec", "eval", "open", "file"]
        for keyword in dangerous_keywords:
            if keyword in expr.lower():
                raise ValueError(
                    f"Expression contains potentially dangerous operation: {keyword}"
                )

    def _validate_callable(self, fn: UserDefinedFunction) -> None:
        """Validate callable function for data quality checks."""
        if not callable(fn):
            raise ValueError("Function must be callable")

        # Check function signature if possible
        import inspect

        try:
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            if len(params) == 0:
                raise ValueError(
                    "Check function must accept at least one parameter (row)"
                )
        except (ValueError, TypeError):
            # Can't inspect signature, assume it's valid
            pass

    def _get_metadata_info(self) -> Dict[str, Any]:
        """Get metadata information about this check operation."""
        return {
            "operator_type": "Check",
            "check_id": self._check_id,
            "expression_type": "string" if self._check_expr else "callable",
            "on_violation": self._on_violation,
            "max_failures": self._max_failures,
            "quarantine_enabled": self._on_violation == "quarantine",
            "quarantine_path": self._quarantine_path,
            "quarantine_format": self._quarantine_format,
            "has_metadata_path": self._metadata_path is not None,
        }

    @property
    def check_expr(self) -> Optional["pa.dataset.Expression"]:
        return self._check_expr

    @property
    def max_failures(self) -> Optional[int]:
        return self._max_failures

    @property
    def on_violation(self) -> str:
        return self._on_violation

    @property
    def quarantine_path(self) -> Optional[str]:
        return self._quarantine_path

    @property
    def quarantine_format(self) -> str:
        return self._quarantine_format

    @property
    def metadata_path(self) -> Optional[str]:
        return self._metadata_path

    @property
    def check_id(self) -> str:
        return self._check_id


class Project(AbstractMap):
    """Logical operator for select_columns."""

    def __init__(
        self,
        input_op: LogicalOperator,
        cols: Optional[List[str]] = None,
        cols_rename: Optional[Dict[str, str]] = None,
        exprs: Optional[
            Dict[str, "Expr"]
        ] = None,  # TODO Remove cols and cols_rename and replace them with corresponding exprs
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
        self._cols = cols
        self._cols_rename = cols_rename
        self._exprs = exprs
        self._batch_format = "pyarrow"
        self._zero_copy_batch = True

        if exprs is not None:
            # Validate that all values are expressions
            for name, expr in exprs.items():
                if not isinstance(expr, Expr):
                    raise TypeError(
                        f"Expected Expr for column '{name}', got {type(expr)}"
                    )

    @property
    def cols(self) -> Optional[List[str]]:
        return self._cols

    @property
    def cols_rename(self) -> Optional[Dict[str, str]]:
        return self._cols_rename

    @property
    def exprs(self) -> Optional[Dict[str, "Expr"]]:
        return self._exprs

    def can_modify_num_rows(self) -> bool:
        return False


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
        super().__init__("StreamingRepartition", input_op)
        self._target_num_rows_per_block = target_num_rows_per_block

    @property
    def target_num_rows_per_block(self) -> int:
        return self._target_num_rows_per_block

    def can_modify_num_rows(self) -> bool:
        return False
