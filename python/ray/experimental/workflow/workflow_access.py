import logging
from typing import Any, Dict, List, TYPE_CHECKING

import ray
from ray.experimental.workflow import common
from ray.experimental.workflow import recovery
from ray.experimental.workflow import storage
from ray.experimental.workflow import workflow_storage

if TYPE_CHECKING:
    from ray.actor import ActorHandle

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
        self._actor_initialized: Dict[str, ray.ObjectRef] = {}

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

    def init_actor(self, actor_id: str,
                   init_marker: List[ray.ObjectRef]) -> None:
        """Initialize a workflow virtual actor.

        Args:
            actor_id: The ID of a workflow virtual actor.
            init_marker: A future object (wrapped in a list) that represents
                the state of the actor. "ray.get" the object successfully
                indicates the actor is initialized successfully.
        """
        # TODO(suquark): Maybe we should raise an error if the actor_id
        # already exists?
        self._actor_initialized[actor_id] = init_marker[0]

    def actor_ready(self, actor_id: str, storage_url: str) -> ray.ObjectRef:
        """Check if a workflow virtual actor is fully initialized.

        Args:
            actor_id: The ID of a workflow virtual actor.
            storage_url: A string that represents the storage.

        Returns:
            A future object that represents the state of the actor.
            "ray.get" the object successfully indicates the actor is
            initialized successfully.
        """
        store = storage.create_storage(storage_url)
        workflow_id = common.actor_id_to_workflow_id(actor_id)
        ws = workflow_storage.WorkflowStorage(workflow_id, store)
        try:
            step_id = ws.get_entrypoint_step_id()
            output_exists = ws.inspect_step(step_id).output_object_valid
            if output_exists:
                return ray.put(None)
        except Exception:
            pass
        if actor_id not in self._actor_initialized:
            raise ValueError(f"Actor '{actor_id}' has not been created, or "
                             "it has failed before initialization.")
        return self._actor_initialized[actor_id]

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


def get_or_create_management_actor() -> "ActorHandle":
    """Get or create WorkflowManagementActor"""
    # TODO(suquark): We should not get the actor everytime. We also need to
    # resume the actor if it failed. Using a global variable to cache the
    # actor seems not enough to resume the actor, because there is no
    # aliveness detection for an actor.
    try:
        actor = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError:
        # the actor does not exist
        actor = WorkflowManagementActor.options(
            name=MANAGEMENT_ACTOR_NAME, lifetime="detached").remote()
    return actor
