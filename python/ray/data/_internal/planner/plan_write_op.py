import itertools
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
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.datasource.datasource import Datasource


def generate_write_fn(
    datasink_or_legacy_datasource: Union[Datasink, Datasource], **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    def fn(blocks: Iterator[Block], ctx) -> Iterator[Block]:
        """Writes the blocks to the given datasink or legacy datasource.

        Outputs the original blocks to be written."""
        # Create a copy of the iterator, so we can return the original blocks.
        it1, it2 = itertools.tee(blocks, 2)
        if isinstance(datasink_or_legacy_datasource, Datasink):
            datasink_or_legacy_datasource.write(it1, ctx)
        else:
            datasink_or_legacy_datasource.write(it1, ctx, **write_args)
        return it2

    return fn


def generate_collect_write_stats_fn() -> Callable[
    [Iterator[Block], TaskContext], Iterator[Block]
]:
    # If the write op succeeds, the resulting Dataset is a list of
    # one Block which contain stats/metrics about the write.
    # Otherwise, an error will be raised. The Datasource can handle
    # execution outcomes with `on_write_complete()`` and `on_write_failed()``.
    def fn(blocks: Iterator[Block], ctx) -> Iterator[Block]:
        """Handles stats collection for block writes."""
        block_accessors = [BlockAccessor.for_block(block) for block in blocks]
        total_num_rows = sum(ba.num_rows() for ba in block_accessors)
        total_size_bytes = sum(ba.size_bytes() for ba in block_accessors)

        # NOTE: Write tasks can return anything, so we need to wrap it in a valid block
        # type.
        import pandas as pd

        write_result = WriteResult(num_rows=total_num_rows, size_bytes=total_size_bytes)
        block = pd.DataFrame({"write_result": [write_result]})
        return iter([block])

    return fn


def plan_write_op(
    op: Write, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    write_fn = generate_write_fn(op._datasink_or_legacy_datasource, **op._write_args)
    collect_stats_fn = generate_collect_write_stats_fn()
    # Create a MapTransformer for a write operator
    transform_fns = [
        BlockMapTransformFn(write_fn),
        BlockMapTransformFn(collect_stats_fn),
    ]
    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        name="Write",
        target_max_block_size=None,
        ray_remote_args=op._ray_remote_args,
        min_rows_per_bundle=op._min_rows_per_bundled_input,
        compute_strategy=TaskPoolStrategy(op._concurrency),
    )
