import sys
from typing import Any, Dict, Iterable, Optional, Union

from ray.data._internal.compute import BlockTransform
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.compute import (
    UDF,
    ComputeStrategy,
)
from ray.data.block import BatchUDF, RowUDF


if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class AbstractMap(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    MapOperator.
    """

    # TODO: Replace `fn`, `fn_args`, `fn_kwargs`, `fn_constructor_args`, and
    # `fn_constructor_kwargs` from this API, in favor of `block_fn_args` and
    # `block_fn_kwargs`. Operators should only be concerned with `block_fn`.
    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        block_fn: BlockTransform,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        target_block_size: Optional[int] = None,
        fn: Optional[UDF] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            block_fn: The transform function to apply to each input block to produce
                output blocks.
            target_block_size: The target size for blocks outputted by this operator.
            fn: User provided UDF to be called in `block_fn`.
            fn_args: Arguments to `fn`.
            fn_kwargs: Keyword arguments to `fn`.
            fn_constructor_args: Arguments to provide to the initializor of `fn` if
                `fn` is a callable class.
            fn_constructor_kwargs: Keyword Arguments to provide to the initializor of
                `fn` if `fn` is a callable class.
            ray_remote_args: Args to provide to ray.remote.
        """
        super().__init__(name, [input_op])
        self._block_fn = block_fn
        self._compute = compute or "tasks"
        self._target_block_size = target_block_size
        self._fn = fn
        self._fn_args = fn_args
        self._fn_kwargs = fn_kwargs
        self._fn_constructor_args = fn_constructor_args
        self._fn_constructor_kwargs = fn_constructor_kwargs
        self._ray_remote_args = ray_remote_args or {}


class MapBatches(AbstractMap):
    """Logical operator for map_batches."""

    def __init__(
        self,
        input_op: LogicalOperator,
        block_fn: BlockTransform,
        fn: BatchUDF,
        batch_size: Optional[Union[int, Literal["default"]]] = "default",
        compute: Optional[Union[str, ComputeStrategy]] = None,
        batch_format: Literal["default", "pandas", "pyarrow", "numpy"] = "default",
        zero_copy_batch: bool = False,
        target_block_size: Optional[int] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "MapBatches",
            input_op,
            block_fn,
            compute=compute,
            target_block_size=target_block_size,
            fn=fn,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
            ray_remote_args=ray_remote_args,
        )
        self._batch_size = batch_size
        self._batch_format = batch_format
        self._zero_copy_batch = zero_copy_batch


class MapRows(AbstractMap):
    """Logical operator for map."""

    def __init__(
        self,
        input_op: LogicalOperator,
        block_fn: BlockTransform,
        fn: RowUDF,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "MapRows",
            input_op,
            block_fn,
            compute=compute,
            fn=fn,
            ray_remote_args=ray_remote_args,
        )


class Filter(AbstractMap):
    """Logical operator for filter."""

    def __init__(
        self,
        input_op: LogicalOperator,
        block_fn: BlockTransform,
        fn: RowUDF,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "Filter",
            input_op,
            block_fn,
            compute=compute,
            fn=fn,
            ray_remote_args=ray_remote_args,
        )


class FlatMap(AbstractMap):
    """Logical operator for flat_map."""

    def __init__(
        self,
        input_op: LogicalOperator,
        block_fn: BlockTransform,
        fn: RowUDF,
        compute: Optional[Union[str, ComputeStrategy]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "FlatMap",
            input_op,
            block_fn,
            compute=compute,
            fn=fn,
            ray_remote_args=ray_remote_args,
        )
