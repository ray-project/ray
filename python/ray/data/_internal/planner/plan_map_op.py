from typing import Any, Iterator

import ray
from ray.data._internal.compute import (
    ActorPoolStrategy,
    TaskPoolStrategy,
    get_compute,
)
from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.map_operator import (
    AbstractMap,
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
)
from ray.data._internal.planner.filter import generate_filter_fn
from ray.data._internal.planner.flat_map import generate_flat_map_fn
from ray.data._internal.planner.map_batches import generate_map_batches_fn
from ray.data._internal.planner.map_rows import generate_map_rows_fn
from ray.data.block import Block, CallableClass


def _plan_map_op(op: AbstractMap, input_physical_dag: PhysicalOperator) -> MapOperator:
    """Get the corresponding physical operators DAG for AbstractMap operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    if isinstance(op, MapBatches):
        transform_fn = generate_map_batches_fn(
            batch_size=op._batch_size,
            batch_format=op._batch_format,
            prefetch_batches=op._prefetch_batches,
            zero_copy_batch=op._zero_copy_batch,
        )
    elif isinstance(op, MapRows):
        transform_fn = generate_map_rows_fn()
    elif isinstance(op, FlatMap):
        transform_fn = generate_flat_map_fn()
    elif isinstance(op, Filter):
        transform_fn = generate_filter_fn()
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    compute = get_compute(op._compute)

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

    def do_map(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        yield from transform_fn(blocks, ctx, *fn_args, **fn_kwargs)

    return MapOperator.create(
        do_map,
        input_physical_dag,
        name=op.name,
        compute_strategy=compute,
        min_rows_per_bundle=op._target_block_size,
        ray_remote_args=op._ray_remote_args,
    )
