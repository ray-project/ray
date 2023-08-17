from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_data_processor import (
    create_map_data_processor_for_write_op,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.planner.write import generate_write_fn
from ray.data.block import Block


def _plan_write_op(op: Write, input_physical_dag: PhysicalOperator) -> PhysicalOperator:
    write_fn = generate_write_fn(op._datasource, **op._write_args)
    map_data_processor = create_map_data_processor_for_write_op(write_fn)
    return MapOperator.create(
        map_data_processor,
        input_physical_dag,
        name="Write",
        ray_remote_args=op._ray_remote_args,
    )
