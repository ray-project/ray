import logging
import time
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING

from dataclasses import dataclass
import ray
from ray.workflow import common
from ray.workflow.common import WorkflowStaticRef
from ray.workflow import recovery
from ray.workflow import storage
from ray.workflow import workflow_storage
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.workflow.common import StepID, WorkflowExecutionResult

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class WorkflowExecutionError(Exception):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] failed during execution."
        super().__init__(self.message)


class _SelfDereferenceObject:
    """A object that dereferences itself during deserialization"""

    def __init__(self, workflow_id: Optional[str], nested_ref: ray.ObjectRef):
        self.workflow_id = workflow_id
        self.nested_ref = nested_ref

    def __reduce__(self):
        return _resolve_workflow_output, (self.workflow_id, self.nested_ref)


def flatten_workflow_output(
    workflow_id: str, workflow_output: ray.ObjectRef
) -> ray.ObjectRef:
    """Converts the nested ref to a direct ref of an object.

    Args:
        workflow_id: The ID of a workflow.
        workflow_output: A (nested) object ref of the workflow output.

    Returns:
        A direct ref of an object.
    """
    return ray.put(_SelfDereferenceObject(workflow_id, workflow_output))


def _resolve_workflow_output(
    workflow_id: Optional[str], output: WorkflowStaticRef
) -> Any:
    """Resolve the output of a workflow.

    Args:
        workflow_id: The ID of the workflow. If it's set to be None,
            it won't report to workflow manager
        output: The output object ref of a workflow.

    Raises:
        WorkflowExecutionError: When the workflow fails.

    Returns:
        The resolved physical object.
    """
    if workflow_id is not None:
        try:
            actor = get_management_actor()
        except ValueError as e:
            raise ValueError(
                "Failed to connect to the workflow management actor."
            ) from e

    from ray.workflow.step_executor import _resolve_static_workflow_ref

    try:
        output = _resolve_static_workflow_ref(output)
    except Exception as e:
        if workflow_id is not None:
            # re-raise the exception so we know it is a workflow failure.
            try:
                ray.get(actor.report_failure.remote(workflow_id))
            except Exception:
                # the actor does not exist
                logger.warning(
                    "Could not inform the workflow management actor "
                    "about the error of the workflow."
                )
        raise WorkflowExecutionError(workflow_id) from e
    if workflow_id is not None:
        try:
            ray.get(actor.report_success.remote(workflow_id))
        except Exception:
            # the actor does not exist
            logger.warning(
                "Could not inform the workflow management actor "
                "about the success of the workflow."
            )
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


@dataclass
class LatestWorkflowOutput:
    output: WorkflowStaticRef
    workflow_id: str
    step_id: "StepID"


# TODO(suquark): we may use an actor pool in the future if too much
# concurrent workflow access blocks the actor.
@ray.remote(num_cpus=0)
class WorkflowManagementActor:
    """Keep the ownership and manage the workflow output."""

    def __init__(self, store: "storage.Storage"):
        self._store = store
        self._workflow_outputs: Dict[str, LatestWorkflowOutput] = {}
        # Cache step output. It is used for step output lookup of
        # "WorkflowRef". The dictionary entry is removed when the status of
        # a step is marked as finished (successful or failed).
        self._step_output_cache: Dict[Tuple[str, str], LatestWorkflowOutput] = {}
        self._actor_initialized: Dict[str, ray.ObjectRef] = {}
        self._step_status: Dict[str, Dict[str, common.WorkflowStatus]] = {}

    def get_storage_url(self) -> str:
        """Get hte storage URL."""
        return self._store.storage_url

    def get_cached_step_output(
        self, workflow_id: str, step_id: "StepID"
    ) -> ray.ObjectRef:
        """Get the cached result of a step.

        Args:
            workflow_id: The ID of the workflow.
            step_id: The ID of the step.

        Returns:
            An object reference that can be used to retrieve the
            step result. If it does not exist, return None
        """
        try:
            output = self._step_output_cache[(workflow_id, step_id)].output
            return output
        except Exception:
            return None

    def run_or_resume(
        self, workflow_id: str, ignore_existing: bool = False
    ) -> "WorkflowExecutionResult":
        """Run or resume a workflow.

        Args:
            workflow_id: The ID of the workflow.
            ignore_existing: Ignore we already have an existing output. When
            set false, raise an exception if there has already been a workflow
            running with this id

        Returns:
            Workflow execution result that contains the state and output.
        """
        if workflow_id in self._workflow_outputs and not ignore_existing:
            raise RuntimeError(
                f"The output of workflow[id={workflow_id}] already exists."
            )
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        workflow_prerun_metadata = {"start_time": time.time()}
        wf_store.save_workflow_prerun_metadata(workflow_prerun_metadata)
        step_id = wf_store.get_entrypoint_step_id()
        try:
            current_output = self._workflow_outputs[workflow_id].output
        except KeyError:
            current_output = None
        result = recovery.resume_workflow_step(
            workflow_id, step_id, self._store.storage_url, current_output
        )
        latest_output = LatestWorkflowOutput(
            result.persisted_output, workflow_id, step_id
        )
        self._workflow_outputs[workflow_id] = latest_output
        logger.info(
            f"run_or_resume: {workflow_id}, {step_id}," f"{result.persisted_output.ref}"
        )
        self._step_output_cache[(workflow_id, step_id)] = latest_output

        wf_store.save_workflow_meta(
            common.WorkflowMetaData(common.WorkflowStatus.RUNNING)
        )

        if workflow_id not in self._step_status:
            self._step_status[workflow_id] = {}
            logger.info(f"Workflow job [id={workflow_id}] started.")
        return result

    def gen_step_id(self, workflow_id: str, step_name: str) -> str:
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        idx = wf_store.gen_step_id(step_name)
        if idx == 0:
            return step_name
        else:
            return f"{step_name}_{idx}"

    def update_step_status(
        self,
        workflow_id: str,
        step_id: str,
        status: common.WorkflowStatus,
        outputs: List[WorkflowStaticRef],
    ):
        # Note: For virtual actor, we could add more steps even if
        # the workflow finishes.

        self._step_status.setdefault(workflow_id, {})
        if status == common.WorkflowStatus.SUCCESSFUL:
            self._step_status[workflow_id].pop(step_id, None)
        else:
            self._step_status.setdefault(workflow_id, {})[step_id] = status
        remaining = len(self._step_status[workflow_id])
        if status != common.WorkflowStatus.RUNNING:
            self._step_output_cache.pop((workflow_id, step_id), None)

        if status != common.WorkflowStatus.FAILED and remaining != 0:
            return

        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)

        if status == common.WorkflowStatus.FAILED:
            if workflow_id in self._workflow_outputs:
                cancel_job(self._workflow_outputs.pop(workflow_id).output)
            wf_store.save_workflow_meta(
                common.WorkflowMetaData(common.WorkflowStatus.FAILED)
            )
            self._step_status.pop(workflow_id)
        else:
            wf_store.save_workflow_meta(
                common.WorkflowMetaData(common.WorkflowStatus.SUCCESSFUL)
            )
            self._step_status.pop(workflow_id)
        workflow_postrun_metadata = {"end_time": time.time()}
        wf_store.save_workflow_postrun_metadata(workflow_postrun_metadata)

    def cancel_workflow(self, workflow_id: str) -> None:
        self._step_status.pop(workflow_id)
        cancel_job(self._workflow_outputs.pop(workflow_id).output)
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        wf_store.save_workflow_meta(
            common.WorkflowMetaData(common.WorkflowStatus.CANCELED)
        )

    def is_workflow_running(self, workflow_id: str) -> bool:
        return (
            workflow_id in self._step_status and workflow_id in self._workflow_outputs
        )

    def list_running_workflow(self) -> List[str]:
        return list(self._step_status.keys())

    def init_actor(self, actor_id: str, init_marker: List[ray.ObjectRef]) -> None:
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
            raise ValueError(
                f"Actor '{actor_id}' has not been created, or "
                "it has failed before initialization."
            )
        return self._actor_initialized[actor_id]

    def get_output(self, workflow_id: str, name: Optional[str]) -> WorkflowStaticRef:
        """Get the output of a running workflow.

        Args:
            workflow_id: The ID of a workflow job.

        Returns:
            An object reference that can be used to retrieve the
            workflow result.
        """
        if workflow_id in self._workflow_outputs and name is None:
            return self._workflow_outputs[workflow_id].output
        wf_store = workflow_storage.WorkflowStorage(workflow_id, self._store)
        meta = wf_store.load_workflow_meta()
        if meta is None:
            raise ValueError(f"No such workflow {workflow_id}")
        if meta == common.WorkflowStatus.CANCELED:
            raise ValueError(f"Workflow {workflow_id} is canceled")
        if name is None:
            # For resumable workflow, the workflow result is not ready.
            # It has to be resumed first.
            if meta == common.WorkflowStatus.RESUMABLE:
                raise ValueError(
                    f"Workflow {workflow_id} is in resumable status, "
                    "please resume it"
                )

        if name is None:
            step_id = wf_store.get_entrypoint_step_id()
        else:
            step_id = name
            output = self.get_cached_step_output(workflow_id, step_id)
            if output is not None:
                return WorkflowStaticRef.from_output(step_id, output)

        @ray.remote
        def load(wf_store, workflow_id, step_id):
            result = wf_store.inspect_step(step_id)
            if result.output_object_valid:
                # we already have the output
                return wf_store.load_step_output(step_id)
            if isinstance(result.output_step_id, str):
                actor = get_management_actor()
                return WorkflowStaticRef.from_output(
                    result.output_step_id,
                    actor.get_output.remote(workflow_id, result.output_step_id),
                )
            raise ValueError(
                f"Cannot load output from step id {step_id} "
                f"in workflow {workflow_id}"
            )

        return WorkflowStaticRef.from_output(
            step_id,
            load.remote(wf_store, workflow_id, step_id),
        )

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
        # TODO(suquark): maybe we should not report success for every
        # step of virtual actor writer?
        logger.info(f"Workflow job [id={workflow_id}] succeeded.")
        self._workflow_outputs.pop(workflow_id, None)


def init_management_actor() -> None:
    """Initialize WorkflowManagementActor"""
    store = storage.get_global_storage()
    try:
        workflow_manager = get_management_actor()
        storage_url = ray.get(workflow_manager.get_storage_url.remote())
        if storage_url != store.storage_url:
            raise RuntimeError(
                "The workflow is using a storage "
                f"({store.storage_url}) different from the "
                f"workflow manager({storage_url})."
            )
    except ValueError:
        logger.info("Initializing workflow manager...")
        # the actor does not exist
        actor = WorkflowManagementActor.options(
            name=common.MANAGEMENT_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote(store)
        # No-op to ensure the actor is created before the driver exits.
        ray.get(actor.get_storage_url.remote())


def get_management_actor() -> "ActorHandle":
    return ray.get_actor(
        common.MANAGEMENT_ACTOR_NAME, namespace=common.MANAGEMENT_ACTOR_NAMESPACE
    )


def get_or_create_management_actor() -> "ActorHandle":
    """Get or create WorkflowManagementActor"""
    # TODO(suquark): We should not get the actor everytime. We also need to
    # resume the actor if it failed. Using a global variable to cache the
    # actor seems not enough to resume the actor, because there is no
    # aliveness detection for an actor.
    try:
        workflow_manager = get_management_actor()
    except ValueError:
        store = storage.get_global_storage()
        # the actor does not exist
        logger.warning(
            "Cannot access workflow manager. It could be because "
            "the workflow manager exited unexpectedly. A new "
            "workflow manager is being created with storage "
            f"'{store}'."
        )
        workflow_manager = WorkflowManagementActor.options(
            name=common.MANAGEMENT_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote(store)
    return workflow_manager
