import sys
from typing import Any, Dict, Iterable, Iterator, Optional, Union

import ray
from ray.data._internal.compute import BlockTransform
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.compute import (
    get_compute,
    CallableClass,
    ComputeStrategy,
    TaskPoolStrategy,
    ActorPoolStrategy,
)
from ray.data.block import BatchUDF, Block


if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class MapBatches(LogicalOperator):
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
        super().__init__("MapBatches", [input_op])
        self._block_fn = block_fn
        self._fn = fn
        self._batch_size = batch_size
        self._compute = compute or "tasks"
        self._batch_format = batch_format
        self._zero_copy_batch = zero_copy_batch
        self._target_block_size = target_block_size
        self._fn_args = fn_args
        self._fn_kwargs = fn_kwargs
        self._fn_constructor_args = fn_constructor_args
        self._fn_constructor_kwargs = fn_constructor_kwargs
        self._ray_remote_args = ray_remote_args or {}


def plan_map_batches_op(
    op: MapBatches, input_physical_dag: PhysicalOperator
) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for MapBatches."""
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

    return MapOperator(
        do_map,
        input_physical_dag,
        name=op.name,
        compute_strategy=compute,
        min_rows_per_bundle=op._target_block_size,
        ray_remote_args=op._ray_remote_args,
    )
