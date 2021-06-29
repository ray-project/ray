import logging
from typing import Any

import ray

logger = logging.getLogger(__name__)


class WorkflowExecutionError(Exception):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] failed during execution."
        super().__init__(self.message)


@ray.remote
def workflow_output_cache(workflow_id: str, output: ray.ObjectRef) -> Any:
    """Cache the output of a workflow.

    This function returns an object ref to an object ref. This enables us to
    check if the workflow finished correctly without fetching the whole output.

    Args:
        workflow_id: The ID of the workflow.
        output: The output object ref of a workflow.

    Returns:
        The resolved physical object.
    """
    try:
        while isinstance(output, ray.ObjectRef):
            output = ray.get(output)
    except Exception as e:
        raise WorkflowExecutionError(workflow_id) from e
    logger.info(f"Workflow job {workflow_id} completes successfully.")
    return output
