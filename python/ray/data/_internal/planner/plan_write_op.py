from typing import Iterator

from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.planner.write import generate_write_fn
from ray.data.block import Block


def _plan_write_op(op: Write, input_physical_dag: PhysicalOperator) -> PhysicalOperator:
    transform_fn = generate_write_fn(op._datasource, **op._write_args)

    def do_write(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        yield from transform_fn(blocks, ctx)

    return MapOperator.create(
        do_write,
        input_physical_dag,
        name="Write",
        ray_remote_args=op._ray_remote_args,
    )
