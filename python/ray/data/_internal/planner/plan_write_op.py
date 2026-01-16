import itertools
import uuid
from typing import Callable, Iterator, List, Union

from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators.write_operator import Write
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

WRITE_UUID_KWARG_NAME = "write_uuid"


def generate_write_fn(
    datasink_or_legacy_datasource: Union[Datasink, Datasource], **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Writes the blocks to the given datasink or legacy datasource.

        Outputs the original blocks to be written."""
        # Create a copy of the iterator, so we can return the original blocks.
        it1, it2 = itertools.tee(blocks, 2)
        if isinstance(datasink_or_legacy_datasource, Datasink):
            ctx.kwargs["_datasink_write_return"] = datasink_or_legacy_datasource.write(
                it1, ctx
            )
        else:
            datasink_or_legacy_datasource.write(it1, ctx, **write_args)

        return it2

    return fn


def generate_collect_write_stats_fn() -> BlockMapTransformFn:
    # If the write op succeeds, the resulting Dataset is a list of
    # one Block which contain stats/metrics about the write.
    # Otherwise, an error will be raised. The Datasource can handle
    # execution outcomes with `on_write_complete()`` and `on_write_failed()``.
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Handles stats collection for block writes."""
        block_accessors = [BlockAccessor.for_block(block) for block in blocks]
        total_num_rows = sum(ba.num_rows() for ba in block_accessors)
        total_size_bytes = sum(ba.size_bytes() for ba in block_accessors)

        # NOTE: Write tasks can return anything, so we need to wrap it in a valid block
        # type.
        import pandas as pd

        block = pd.DataFrame(
            {
                "num_rows": [total_num_rows],
                "size_bytes": [total_size_bytes],
                "write_return": [ctx.kwargs.get("_datasink_write_return", None)],
            }
        )
        return iter([block])

    return BlockMapTransformFn(
        fn,
        is_udf=False,
        disable_block_shaping=True,
    )


def plan_write_op(
    op: Write,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    collect_stats_fn = generate_collect_write_stats_fn()

    return _plan_write_op_internal(
        op, physical_children, data_context, extra_transformations=[collect_stats_fn]
    )


def _plan_write_op_internal(
    op: Write,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
    extra_transformations: List[BlockMapTransformFn],
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    datasink = op._datasink_or_legacy_datasource
    write_fn = generate_write_fn(datasink, **op._write_args)

    # Create a MapTransformer for a write operator
    transform_fns = [
        BlockMapTransformFn(
            write_fn,
            is_udf=False,
            # NOTE: No need for block-shaping
            disable_block_shaping=True,
        ),
    ] + extra_transformations

    map_transformer = MapTransformer(transform_fns)

    # Set up on_start callback for datasinks.
    # This allows on_write_start to receive the schema from the first input bundle,
    # enabling schema-dependent initialization (e.g., Iceberg schema evolution).
    on_start = None
    if isinstance(datasink, Datasink):
        on_start = datasink.on_write_start

    map_op = MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name="Write",
        # Add a UUID to write tasks to prevent filename collisions. This a UUID for the
        # overall write operation, not the individual write tasks.
        map_task_kwargs={WRITE_UUID_KWARG_NAME: uuid.uuid4().hex},
        ray_remote_args=op._ray_remote_args,
        min_rows_per_bundle=op._min_rows_per_bundled_input,
        compute_strategy=TaskPoolStrategy(op._concurrency),
        on_start=on_start,
    )

    return map_op
