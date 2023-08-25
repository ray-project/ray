from typing import Callable, Iterator

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators.write_operator import Write
from ray.data.block import Block
from ray.data.datasource.datasource import Datasource


def generate_write_fn(
    datasource: Datasource, **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    # If the write op succeeds, the resulting Dataset is a list of
    # WriteResult (one element per write task). Otherwise, an error will
    # be raised. The Datasource can handle execution outcomes with the
    # on_write_complete() and on_write_failed().
    def fn(blocks: Iterator[Block], ctx) -> Iterator[Block]:
        # NOTE: `WriteResult` isn't a valid block type, so we need to wrap it up.
        import pandas as pd

        block = pd.DataFrame(
            {"write_result": [datasource.write(blocks, ctx, **write_args)]}
        )
        return [block]

    return fn


def plan_write_op(op: Write, input_physical_dag: PhysicalOperator) -> PhysicalOperator:
    write_fn = generate_write_fn(op._datasource, **op._write_args)
    # Create a MapTransformer for a write operator
    transform_fns = [
        BlockMapTransformFn(write_fn),
    ]
    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        name="Write",
        ray_remote_args=op._ray_remote_args,
    )
