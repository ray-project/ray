import sys
from typing import Any, Dict, Iterable, Optional, Union

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.compute import (
    UDF,
    ComputeStrategy,
)
from ray.data.block import BatchUDF, RowUDF
from ray.data.context import DEFAULT_BATCH_SIZE


if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class AbstractMap(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    MapOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        fn: UDF,
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
        super().__init__(name, [input_op])
        self._fn = fn
        self._fn_args = fn_args
        self._fn_kwargs = fn_kwargs
        self._fn_constructor_args = fn_constructor_args
        self._fn_constructor_kwargs = fn_constructor_kwargs
        self._target_block_size = target_block_size
        self._compute = compute or "tasks"
        self._ray_remote_args = ray_remote_args or {}


class MapBatches(AbstractMap):
    """Logical operator for map_batches."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: BatchUDF,
        batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
        batch_format: Literal["default", "pandas", "pyarrow", "numpy"] = "default",
        prefetch_batches: int = 0,
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
        self._prefetch_batches = prefetch_batches
        self._zero_copy_batch = zero_copy_batch


class MapRows(AbstractMap):
    """Logical operator for map."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: RowUDF,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "MapRows",
            input_op,
            fn,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )


class Filter(AbstractMap):
    """Logical operator for filter."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: RowUDF,
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


class FlatMap(AbstractMap):
    """Logical operator for flat_map."""

    def __init__(
        self,
        input_op: LogicalOperator,
        fn: RowUDF,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "FlatMap",
            input_op,
            fn,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )
