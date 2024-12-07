import itertools
import pickle
from typing import Callable, Iterable, List, Union

from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data._internal.logical.operators.write_operator import Write
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.datasource.datasource import Datasource


def generate_write_fn(
    datasink_or_legacy_datasource: Union[Datasink, Datasource], **write_args
) -> Callable[[Iterable[Block], TaskContext], Iterable[Block]]:
    stats_fn = generate_collect_write_stats_fn()

    def fn(blocks: Iterable[Block], ctx) -> Iterable[Block]:
        """Writes the blocks to the given datasink or legacy datasource.

        Outputs the original blocks to be written."""
        # Create a copy of the iterator, so we can return the original blocks.
        it1, it2 = itertools.tee(blocks, 2)
        if isinstance(datasink_or_legacy_datasource, Datasink):
            write_result = datasink_or_legacy_datasource.write(it1, ctx)
        else:
            write_result = datasink_or_legacy_datasource.write(it1, ctx, **write_args)

        import pandas as pd

        payload = pd.DataFrame({"payload": [pickle.dumps(write_result)]})

        stats = list(stats_fn(it2, ctx))
        assert len(stats) == 1
        block = pd.concat([stats[0], payload], axis=1)
        return iter([block])

    return fn


def generate_collect_write_stats_fn() -> Callable[
    [Iterable[Block], TaskContext], Iterable[Block]
]:
    # If the write op succeeds, the resulting Dataset is a list of
    # one Block which contain stats/metrics about the write.
    # Otherwise, an error will be raised. The Datasource can handle
    # execution outcomes with `on_write_complete()`` and `on_write_failed()``.
    def fn(blocks: Iterable[Block], ctx) -> Iterable[Block]:
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
    op: Write,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    write_fn = generate_write_fn(op._datasink_or_legacy_datasource, **op._write_args)
    # Create a MapTransformer for a write operator
    transform_fns: List[MapTransformFn] = [
        BlockMapTransformFn(write_fn),
    ]
    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name="Write",
        target_max_block_size=None,
        ray_remote_args=op._ray_remote_args,
        min_rows_per_bundle=op._min_rows_per_bundled_input,
        compute_strategy=TaskPoolStrategy(op._concurrency),
    )
