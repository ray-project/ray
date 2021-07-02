import logging
from typing import Any

import ray

logger = logging.getLogger(__name__)


class WorkflowExecutionError(Exception):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] failed during execution."
        super().__init__(self.message)


class _SelfDereferenceObject:
    """A object that dereferences itself during deserialization"""

    def __init__(self, workflow_id: str, nested_ref: ray.ObjectRef):
        self.workflow_id = workflow_id
        self.nested_ref = nested_ref

    def __reduce__(self):
        return _resolve_workflow_output, (self.workflow_id, self.nested_ref)


def flatten_workflow_output(workflow_id: str,
                            workflow_output: ray.ObjectRef) -> ray.ObjectRef:
    """Converts the nested ref to a direct ref of an object.

    Args:
        workflow_id: The ID of a workflow.
        workflow_output: A (nested) object ref of the workflow output.

    Returns:
        A direct ref of an object.
    """
    return ray.put(_SelfDereferenceObject(workflow_id, workflow_output))


def _resolve_workflow_output(workflow_id: str, output: ray.ObjectRef) -> Any:
    """Resolve the output of a workflow.

    Args:
        workflow_id: The ID of the workflow.
        output: The output object ref of a workflow.

    Raises:
        WorkflowExecutionError: When the workflow fails.

    Returns:
        The resolved physical object.
    """
    try:
        while isinstance(output, ray.ObjectRef):
            output = ray.get(output)
    except Exception as e:
        # re-raise the exception so we know it is a workflow failure.
        raise WorkflowExecutionError(workflow_id) from e
    logger.info(f"Get the output of workflow {workflow_id} successfully.")
    return output
