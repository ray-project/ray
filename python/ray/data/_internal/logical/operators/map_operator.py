import inspect
import logging
from typing import Any, Callable, Dict, Iterable, Optional, Union

from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.one_to_one_operator import AbstractOneToOne
from ray.data.block import UserDefinedFunction
from ray.data.context import DEFAULT_BATCH_SIZE
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
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            min_rows_per_bundled_input: The target number of rows to pass to
                ``MapOperator._add_bundled_input()``.
            ray_remote_args: Args to provide to ray.remote.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict will
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
        """
        super().__init__(name, input_op, num_outputs)
        self._min_rows_per_bundled_input = min_rows_per_bundled_input
        self._ray_remote_args = ray_remote_args or {}
        self._ray_remote_args_fn = ray_remote_args_fn


class AbstractUDFMap(AbstractMap):
    """Abstract class for logical operators performing a UDF that should be converted
    to physical MapOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        fn: UserDefinedFunction,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        min_rows_per_bundled_input: Optional[int] = None,
        compute: Optional[Union[str, ComputeStrategy]] = None,
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
            compute: The compute strategy, either ``"tasks"`` (default) to use Ray
                tasks, or ``"actors"`` to use an autoscaling actor pool.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict will
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
            ray_remote_args: Args to provide to ray.remote.
        """
        name = self._get_operator_name(name, fn)
        super().__init__(
            name,
            input_op,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
        )
        self._fn = fn
        self._fn_args = fn_args
        self._fn_kwargs = fn_kwargs
        self._fn_constructor_args = fn_constructor_args
        self._fn_constructor_kwargs = fn_constructor_kwargs
        self._compute = compute or TaskPoolStrategy()
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
        batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
        batch_format: str = "default",
        zero_copy_batch: bool = False,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        min_rows_per_bundled_input: Optional[int] = None,
        compute: Optional[Union[str, ComputeStrategy]] = None,
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

    @property
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
        compute: Optional[Union[str, ComputeStrategy]] = None,
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

    @property
    def can_modify_num_rows(self) -> bool:
        return False


class Filter(AbstractUDFMap):
    """Logical operator for filter."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: UserDefinedFunction,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "Filter",
            input_op,
            fn,
            compute=compute,
            ray_remote_args_fn=ray_remote_args_fn,
            ray_remote_args=ray_remote_args,
        )

    @property
    def can_modify_num_rows(self) -> bool:
        return True


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
        compute: Optional[Union[str, ComputeStrategy]] = None,
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

    @property
    def can_modify_num_rows(self) -> bool:
        return True
