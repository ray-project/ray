import logging
from typing import Any, Dict

import ray
from ray.experimental.workflow import recovery
from ray.experimental.workflow import storage
logger = logging.getLogger(__name__)

MANAGEMENT_ACTOR_NAME = "WorkflowManagementActor"


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
        actor = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError as e:
        raise ValueError(
            "Failed to connect to the workflow management actor.") from e
    try:
        while isinstance(output, ray.ObjectRef):
            output = ray.get(output)
    except Exception as e:
        # re-raise the exception so we know it is a workflow failure.
        try:
            ray.get(actor.report_failure.remote(workflow_id))
        except Exception:
            # the actor does not exist
            logger.warning("Could not inform the workflow management actor "
                           "about the error of the workflow.")
        raise WorkflowExecutionError(workflow_id) from e
    try:
        ray.get(actor.report_success.remote(workflow_id))
    except Exception:
        # the actor does not exist
        logger.warning("Could not inform the workflow management actor "
                       "about the success of the workflow.")
    logger.info(f"Get the output of workflow {workflow_id} successfully.")
    return output


# TODO(suquark): we may use an actor pool in the future if too much
# concurrent workflow access blocks the actor.
@ray.remote
class WorkflowManagementActor:
    """Keep the ownership and manage the workflow output."""

    def __init__(self):
        self._workflow_outputs: Dict[str, ray.ObjectRef] = {}

    def run_or_resume(self, workflow_id: str,
                      storage_url: str) -> ray.ObjectRef:
        """Run or resume a workflow.

        Args:
            workflow_id: The ID of the workflow.
            storage_url: A string that represents the storage.

        Returns:
            An object reference that can be used to retrieve the
            workflow result.
        """
        if workflow_id in self._workflow_outputs:
            raise ValueError(f"The output of workflow[id={workflow_id}] "
                             "already exists.")
        store = storage.create_storage(storage_url)
        output = recovery.resume_workflow_job(workflow_id, store)
        self._workflow_outputs[workflow_id] = output
        logger.info(f"Workflow job [id={workflow_id}] started.")
        return output

    def get_output(self, workflow_id: str) -> ray.ObjectRef:
        """Get the output of a running workflow.

        Args:
            workflow_id: The ID of a  workflow job.

        Returns:
            An object reference that can be used to retrieve the
            workflow result.
        """
        if workflow_id not in self._workflow_outputs:
            raise ValueError(f"The output of workflow[id={workflow_id}] "
                             "does not exist. The workflow is either failed "
                             "or finished. Use 'workflow.resume()' to access "
                             "the workflow result.")
        return self._workflow_outputs[workflow_id]

    def report_failure(self, workflow_id: str) -> None:
        """Report the failure of a workflow_id.

        Args:
            workflow_id: The ID of the workflow.
        """
        logger.error(f"Workflow job [id={workflow_id}] failed.")
        self._workflow_outputs.pop(workflow_id, None)

    def report_success(self, workflow_id: str) -> None:
        """Report the failure of a workflow_id.

        Args:
            workflow_id: The ID of the workflow.
        """
        logger.info(f"Workflow job [id={workflow_id}] succeeded.")
        self._workflow_outputs.pop(workflow_id, None)
