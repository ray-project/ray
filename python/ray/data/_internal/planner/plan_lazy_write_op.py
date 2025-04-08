import itertools
from typing import Callable, Iterator, Union


from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

from ray.data._internal.logical.operators.lazy_write_operator import LazyWrite


def generate_lazy_write_fn(
    prefilter_fn: Callable[[Block], Block] | None,
    datasink_or_legacy_datasource: Union[Datasink, Datasource],
    **write_args,
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Writes the blocks to the given datasink or legacy datasource.

        Outputs the original blocks to be written."""
        # Create a copy of the iterator, so we can return the original blocks.
        it1, it2 = itertools.tee(blocks, 2)
        if isinstance(datasink_or_legacy_datasource, Datasink):
            # Apply the prefilter function to each block before writing
            if prefilter_fn is not None:
                it1 = (prefilter_fn(block) if len(block) else block for block in it1)
            ctx.kwargs["_datasink_write_return"] = datasink_or_legacy_datasource.write(
                it1, ctx
            )
        else:
            datasink_or_legacy_datasource.write(it1, ctx, **write_args)

        return it2

    return fn


def plan_lazy_write_op(
    op: LazyWrite,
    physical_children: list[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    prefilter_fn = op._prefilter_fn
    write_fn = generate_lazy_write_fn(
        prefilter_fn, op._datasink_or_legacy_datasource, **op._write_args
    )
    # collect_stats_fn = generate_collect_write_stats_fn()
    # Create a MapTransformer for a write operator
    transform_fns = [
        BlockMapTransformFn(write_fn),
    ]
    map_transformer = MapTransformer(transform_fns)
    op._datasink_or_legacy_datasource.on_write_start()
    # TODO: figure out how to handle on_write_complete()
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name="LazyWrite",
        target_max_block_size=None,
        ray_remote_args=op._ray_remote_args,
        min_rows_per_bundle=op._min_rows_per_bundled_input,
        compute_strategy=TaskPoolStrategy(op._concurrency),
    )
