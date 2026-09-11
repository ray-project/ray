"""ListFiles planning for generated-ID checkpointing.

The id_column path filters rows downstream (an actor-pool wrap around
``Read`` / ``ReadFiles``); the generated-ID path instead skips work at the
source: listing tasks receive the compact checkpoint block and drop
fully-checkpointed files before chunking, and the manifest rows they emit
carry the per-file checkpoint struct for reader-side row-group and row
skipping.

The checkpoint block does not exist at planning time — it is loaded by
``LoadCheckpointCallback.before_execution_starts`` — so it is attached as a
lazy map-task kwarg resolved when listing tasks launch.
"""

from typing import Callable, List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators import ListFiles
from ray.data._internal.planner.plan_list_files_op import plan_list_files_op
from ray.data.block import Block
from ray.data.checkpoint.generated_id import CHECKPOINTED_IDS_KWARG_NAME
from ray.data.context import DataContext
from ray.types import ObjectRef


def plan_list_files_op_with_checkpoint_filter(
    op: ListFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
    *,
    load_checkpoint: Callable[[], ObjectRef[Block]],
) -> MapOperator:
    """Plan ``ListFiles`` with the compact checkpoint block injected.

    Args:
        op: The ``ListFiles`` logical operator.
        physical_children: Planned physical children (always empty).
        data_context: The data context.
        load_checkpoint: Zero-arg callable returning the compact checkpoint
            block ref; typically ``LoadCheckpointCallback.load_checkpoint``.

    Returns:
        The planned ``ListFiles`` map operator.
    """
    map_op = plan_list_files_op(op, physical_children, data_context)
    map_op.add_map_task_kwargs_fn(
        lambda: {CHECKPOINTED_IDS_KWARG_NAME: load_checkpoint()}
    )
    return map_op
