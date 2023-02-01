from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.planner.transforms import generate_block_transform_for_op


def _plan_udf_map_op(
    op: AbstractUDFMap, input_physical_dag: PhysicalOperator
) -> MapOperator:
    """Get the corresponding physical operators DAG for AbstractUDFMap operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    do_map = generate_block_transform_for_op(op)

    return MapOperator.create(
        do_map,
        input_physical_dag,
        name=op.name,
        compute_strategy=get_compute(op._compute),
        min_rows_per_bundle=op._target_block_size,
        ray_remote_args=op._ray_remote_args,
    )
