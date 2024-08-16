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
from ray.data.block import Block
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource


def generate_write_fn(
    datasink_or_legacy_datasource: Union[Datasink, Datasource], **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    # If the write op succeeds, the resulting Dataset is a list of
    # arbitrary objects (one object per write task). Otherwise, an error will
    # be raised. The Datasource can handle execution outcomes with the
    # on_write_complete() and on_write_failed().
    def fn(blocks: Iterator[Block], ctx) -> Iterator[Block]:
        if isinstance(datasink_or_legacy_datasource, Datasink):
            write_result = datasink_or_legacy_datasource.write(blocks, ctx)
        else:
            write_result = datasink_or_legacy_datasource.write(
                blocks, ctx, **write_args
            )

        # NOTE: Write tasks can return anything, so we need to wrap it in a valid block
        # type.
        import pandas as pd

        block = pd.DataFrame({"write_result": [write_result]})
        return [block]

    return fn


def plan_write_op(
    op: Write, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    write_fn = generate_write_fn(op._datasink_or_legacy_datasource, **op._write_args)
    # Create a MapTransformer for a write operator
    transform_fns = [
        BlockMapTransformFn(write_fn),
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
