import inspect
from typing import Any, Dict, Iterable, Optional, Union

from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.one_to_one_operator import AbstractOneToOne
from ray.data.block import UserDefinedFunction
from ray.data.context import DEFAULT_BATCH_SIZE

logger = DatasetLogger(__name__)


class AbstractMap(AbstractOneToOne):
    """Abstract class for logical operators that should be converted to physical
    MapOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: Optional[LogicalOperator] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            ray_remote_args: Args to provide to ray.remote.
        """
        super().__init__(name, input_op)
        self._ray_remote_args = ray_remote_args or {}


def _get_udf_name(fn: UserDefinedFunction) -> str:
    try:
        if inspect.isclass(fn):
            # callable class
            return fn.__name__
        elif inspect.ismethod(fn):
            # class method
            return f"{fn.__self__.__class__.__name__}.{fn.__name__}"
        elif inspect.isfunction(fn):
            # normal function or lambda function.
            return fn.__name__
        else:
            # callable object.
            return fn.__class__.__name__
    except AttributeError as e:
        logger.get_logger().error("Failed to get name of UDF %s: %s", fn, e)
        return "<unknown>"


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
        target_block_size: Optional[int] = None,
        compute: Optional[Union[str, ComputeStrategy]] = None,
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
            target_block_size: The target size for blocks outputted by this operator.
            compute: The compute strategy, either ``"tasks"`` (default) to use Ray
                tasks, or ``"actors"`` to use an autoscaling actor pool.
            ray_remote_args: Args to provide to ray.remote.
        """
        name = f"{name}({_get_udf_name(fn)})"
        super().__init__(name, input_op, ray_remote_args)
        self._fn = fn
        self._fn_args = fn_args
        self._fn_kwargs = fn_kwargs
        self._fn_constructor_args = fn_constructor_args
        self._fn_constructor_kwargs = fn_constructor_kwargs
        self._target_block_size = target_block_size
        self._compute = compute or TaskPoolStrategy()


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
        target_block_size: Optional[int] = None,
        compute: Optional[Union[str, ComputeStrategy]] = None,
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
            target_block_size=target_block_size,
            compute=compute,
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
        fn_constructor_args: Optional[Iterable[Any]] = None,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "Map",
            input_op,
            fn,
            fn_constructor_args=fn_constructor_args,
            compute=compute,
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
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "Filter",
            input_op,
            fn,
            compute=compute,
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
        fn_constructor_args: Optional[Iterable[Any]] = None,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "FlatMap",
            input_op,
            fn,
            fn_constructor_args=fn_constructor_args,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )

    @property
    def can_modify_num_rows(self) -> bool:
        return True
