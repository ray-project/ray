from typing import List, Optional

from ray.data._internal.execution.lineage_tracker import (
    LineageTaskId,
    LineageTracker,
    ParentBlockOutput,
    ReconstructionPlanId,
)


def release_inputs_and_submit(
    tracker: LineageTracker,
    lineage_task_id: LineageTaskId,
    dependencies: List[ParentBlockOutput],
    reconstruction_plan_id: Optional[ReconstructionPlanId],
) -> None:
    """Submit a task the way the operator does.

    A re-execution only runs on input its parents released for the plan, so each
    input block is queued for the plan before the task is submitted. A fresh
    task is submitted as is.
    """
    if reconstruction_plan_id is not None:
        for dependency in dependencies:
            tracker.register_block_queued(
                dependency.parent_lineage_task_id,
                dependency.output_index,
                reconstruction_plan_id,
            )
    tracker.register_task_submission(
        lineage_task_id, dependencies, reconstruction_plan_id
    )
