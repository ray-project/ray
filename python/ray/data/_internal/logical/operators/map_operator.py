import sys
from typing import Any, Dict, Iterable, Iterator, Optional, Union

import ray
from ray.data._internal.compute import BlockTransform
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.compute import (
    UDF,
    get_compute,
    CallableClass,
    ComputeStrategy,
    TaskPoolStrategy,
    ActorPoolStrategy,
)
from ray.data.block import BatchUDF, Block, RowUDF


if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class AbstractMap(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    MapOperator.

    Args:
        name: Name for this operator. This is the name that will appear when inspecting
            the logical plan of a Dataset.
        input_op: The operator preceding this operator in the plan DAG. The outputs of
            `input_op` will be the inputs to this operator.
        block_fn: The transform function to apply to each input block to produce output
            blocks.
        target_block_size: The target size for blocks outputted by this operator.
        fn: User provided UDF to be called in `block_fn`.
        fn_args: Arguments to `fn`.
        fn_kwargs: Keyword arguments to `fn`.
        fn_constructor_args: Arguments to provide to the initializor of `fn` if `fn` is
            a callable class.
        fn_constructor_kwargs: Keyword Arguments to provide to the initializor of `fn`
            if `fn` is a callable class.
        ray_remote_args: Args to provide to ray.remote.
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


def plan_map_op(op: AbstractMap, input_physical_dag: PhysicalOperator) -> MapOperator:
    """Get the corresponding physical operators DAG for AbstractMap operators."""
    compute = get_compute(op._compute)
    block_fn = op._block_fn

    if isinstance(op._fn, CallableClass):
        if isinstance(compute, TaskPoolStrategy):
            raise ValueError(
                "``compute`` must be specified when using a callable class, "
                "and must specify the actor compute strategy. "
                'For example, use ``compute="actors"`` or '
                "``compute=ActorPoolStrategy(min, max)``."
            )
        assert isinstance(compute, ActorPoolStrategy)

        fn_constructor_args = op._fn_constructor_args or ()
        fn_constructor_kwargs = op._fn_constructor_kwargs or {}
        fn_ = op._fn

        def fn(item: Any) -> Any:
            # Wrapper providing cached instantiation of stateful callable class
            # UDFs.
            if ray.data._cached_fn is None:
                ray.data._cached_cls = fn_
                ray.data._cached_fn = fn_(*fn_constructor_args, **fn_constructor_kwargs)
            else:
                # A worker is destroyed when its actor is killed, so we
                # shouldn't have any worker reuse across different UDF
                # applications (i.e. different map operators).
                assert ray.data._cached_cls == fn_
            return ray.data._cached_fn(item)

    else:
        fn = op._fn
    fn_args = (fn,)
    if op._fn_args:
        fn_args += op._fn_args
    fn_kwargs = op._fn_kwargs or {}

    def do_map(blocks: Iterator[Block]) -> Iterator[Block]:
        yield from block_fn(blocks, *fn_args, **fn_kwargs)

    return MapOperator.create(
        do_map,
        input_physical_dag,
        name=op.name,
        compute_strategy=compute,
        min_rows_per_bundle=op._target_block_size,
        ray_remote_args=op._ray_remote_args,
    )
