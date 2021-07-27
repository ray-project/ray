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

# The name contains the namespace "workflow".
MANAGEMENT_ACTOR_NAME = "workflow/WorkflowManagementActor"


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
    return output


def cancel_job(obj: ray.ObjectRef):
    return
    # TODO (yic) Enable true canceling in ray.
    #
    # try:
    #     while isinstance(obj, ray.ObjectRef):
    #         ray.cancel(obj)
    #         obj = ray.get(obj)
    # except Exception:
    #     pass


# TODO(suquark): we may use an actor pool in the future if too much
# concurrent workflow access blocks the actor.
@ray.remote
class WorkflowManagementActor:
    """Keep the ownership and manage the workflow output."""

    def __init__(self, store: "storage.Storage"):
        self._store = store
        self._workflow_outputs: Dict[str, ray.ObjectRef] = {}
        self._actor_initialized: Dict[str, ray.ObjectRef] = {}
        self._step_status: Dict[str, Dict[str, common.WorkflowStatus]] = {}

    def get_storage_url(self) -> str:
        """Get hte storage URL."""
        return self._store.storage_url

    def run_or_resume(self, workflow_id: str) -> ray.ObjectRef:
        """Run or resume a workflow.

        Args:
            workflow_id: The ID of the workflow.

        Returns:
            An object reference that can be used to retrieve the
            workflow result.
        """
        if workflow_id in self._workflow_outputs:
            raise RuntimeError(f"The output of workflow[id={workflow_id}] "
                               "already exists.")
        output = recovery.resume_workflow_job.remote(workflow_id,
                                                     self._store.storage_url)
        self._workflow_outputs[workflow_id] = output
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        wf_store.save_workflow_meta(
            common.WorkflowMetaData(common.WorkflowStatus.RUNNING))
        self._step_status[workflow_id] = {}
        logger.info(f"Workflow job [id={workflow_id}] started.")
        return output

    def update_step_status(self, workflow_id: str, step_id: str,
                           status: common.WorkflowStatus):
        if status == common.WorkflowStatus.SUCCESSFUL:
            self._step_status[workflow_id].pop(step_id, None)
        else:
            self._step_status.setdefault(workflow_id, {})[step_id] = status
        remaining = len(self._step_status[workflow_id])

        if status != common.WorkflowStatus.FAILED and remaining != 0:
            return

        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)

        if status == common.WorkflowStatus.FAILED:
            if workflow_id in self._workflow_outputs:
                cancel_job(self._workflow_outputs.pop(workflow_id))
            wf_store.save_workflow_meta(
                common.WorkflowMetaData(common.WorkflowStatus.FAILED))
            self._step_status.pop(workflow_id)
        else:
            # remaining = 0
            wf_store.save_workflow_meta(
                common.WorkflowMetaData(common.WorkflowStatus.SUCCESSFUL))
            self._step_status.pop(workflow_id)

    def cancel_workflow(self, workflow_id: str) -> None:
        self._step_status.pop(workflow_id)
        cancel_job(self._workflow_outputs.pop(workflow_id))
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        wf_store.save_workflow_meta(
            common.WorkflowMetaData(common.WorkflowStatus.CANCELED))

    def is_workflow_running(self, workflow_id: str) -> bool:
        return workflow_id in self._step_status and \
          workflow_id in self._workflow_outputs

    def list_running_workflow(self) -> List[str]:
        return list(self._step_status.keys())

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

    def actor_ready(self, actor_id: str) -> ray.ObjectRef:
        """Check if a workflow virtual actor is fully initialized.

        Args:
            actor_id: The ID of a workflow virtual actor.

        Returns:
            A future object that represents the state of the actor.
            "ray.get" the object successfully indicates the actor is
            initialized successfully.
        """
        ws = workflow_storage.WorkflowStorage(actor_id, self._store)
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
        if workflow_id in self._workflow_outputs:
            return self._workflow_outputs[workflow_id]
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        meta = wf_store.load_workflow_meta()
        if meta is None:
            raise ValueError(f"No such workflow {workflow_id}")
        if meta == common.WorkflowStatus.FAILED:
            raise ValueError(
                f"Workflow {workflow_id} failed, please resume it")
        output = recovery.resume_workflow_job.remote(workflow_id,
                                                     self._store.storage_url)
        self._workflow_outputs[workflow_id] = output
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        wf_store.save_workflow_meta(
            common.WorkflowMetaData(common.WorkflowStatus.RUNNING))
        self._step_status[workflow_id] = {}
        return output

    def get_running_workflow(self) -> List[str]:
        return list(self._workflow_outputs.keys())

    def report_failure(self, workflow_id: str) -> None:
        """Report the failure of a workflow_id.

        Args:
            workflow_id: The ID of the workflow.
        """
        logger.error(f"Workflow job [id={workflow_id}] failed.")
        self._workflow_outputs.pop(workflow_id, None)

    def report_success(self, workflow_id: str) -> None:
        """Report the success of a workflow_id.

        Args:
            workflow_id: The ID of the workflow.
        """
        logger.info(f"Workflow job [id={workflow_id}] succeeded.")
        self._workflow_outputs.pop(workflow_id, None)


def init_management_actor() -> None:
    """Initialize WorkflowManagementActor"""
    store = storage.get_global_storage()
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
        storage_url = ray.get(workflow_manager.get_storage_url.remote())
        if storage_url != store.storage_url:
            raise RuntimeError("The workflow is using a storage "
                               f"({store.storage_url}) different from the "
                               f"workflow manager({storage_url}).")
    except ValueError:
        logger.info("Initializing workflow manager...")
        # the actor does not exist
        WorkflowManagementActor.options(
            name=MANAGEMENT_ACTOR_NAME, lifetime="detached").remote(store)


def get_or_create_management_actor() -> "ActorHandle":
    """Get or create WorkflowManagementActor"""
    # TODO(suquark): We should not get the actor everytime. We also need to
    # resume the actor if it failed. Using a global variable to cache the
    # actor seems not enough to resume the actor, because there is no
    # aliveness detection for an actor.
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError:
        store = storage.get_global_storage()
        # the actor does not exist
        logger.warning("Cannot access workflow manager. It could because "
                       "the workflow manager exited unexpectedly. A new "
                       "workflow manager is being created with storage "
                       f"'{store}'.")
        workflow_manager = WorkflowManagementActor.options(
            name=MANAGEMENT_ACTOR_NAME, lifetime="detached").remote(store)
    return workflow_manager
