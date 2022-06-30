import logging
from typing import Dict, List, Set, Optional, TYPE_CHECKING

import ray

from ray.workflow import common
from ray.workflow.common import WorkflowStatus, StepID
from ray.workflow import workflow_state_from_storage
from ray.workflow import workflow_context
from ray.workflow import workflow_storage
from ray.workflow.exceptions import (
    WorkflowCancellationError,
    WorkflowNotResumableError,
)
from ray.workflow.workflow_executor import WorkflowExecutor
from ray.workflow.workflow_state import WorkflowExecutionState
from ray.workflow.workflow_context import WorkflowStepContext

if TYPE_CHECKING:
    from ray.actor import ActorHandle

logger = logging.getLogger(__name__)


class SelfResolvingObject:
    def __init__(self, x):
        self.x = x

    def __reduce__(self):
        return ray.get, (self.x,)


@ray.remote(num_cpus=0)
def load_step_output_from_storage(workflow_id: str, task_id: Optional[StepID]):
    wf_store = workflow_storage.WorkflowStorage(workflow_id)
    tid = wf_store.inspect_output(task_id)
    if tid is not None:
        return wf_store.load_step_output(tid)
    # TODO(suquark): Unify the error from "workflow.get_output" & "workflow.run_async".
    # Currently they could be different, because "workflow.get_output" could
    # get the output from a stopped workflow, it does not may sense to raise
    # "WorkflowExecutionError" as the workflow is not running.
    if task_id is not None:
        raise ValueError(
            f"Cannot load output from task id '{task_id}' in workflow '{workflow_id}'"
        )
    else:
        raise ValueError(f"Cannot load output from workflow '{workflow_id}'")


@ray.remote(num_cpus=0)
def resume_workflow_step(
    job_id: str,
    workflow_id: str,
    step_id: Optional[StepID] = None,
) -> WorkflowExecutionState:
    """Resume a step of a workflow.

    Args:
        job_id: The ID of the job that submits the workflow execution. The ID
        is used to identify the submitter of the workflow.
        workflow_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        step_id: The step to resume in the workflow.

    Raises:
        WorkflowNotResumableException: fail to resume the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    with workflow_context.workflow_logging_context(job_id):
        try:
            return workflow_state_from_storage.workflow_state_from_storage(
                workflow_id, step_id
            )
        except Exception as e:
            raise WorkflowNotResumableError(workflow_id) from e


# TODO(suquark): we may use an actor pool in the future if too much
# concurrent workflow access blocks the actor.
@ray.remote(num_cpus=0)
class WorkflowManagementActor:
    """Keep the ownership and manage the workflow output."""

    def __init__(self):
        self._workflow_executors: Dict[str, WorkflowExecutor] = {}
        # TODO(suquark): We do not cleanup "_executed_workflows" because we need to
        #  know if users are running the same workflow again long after a workflow
        #  completes. One possible alternative solution is to check the workflow
        #  status in the storage.
        self._executed_workflows: Set[str] = set()

    def gen_step_id(self, workflow_id: str, step_name: str) -> str:
        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        idx = wf_store.gen_step_id(step_name)
        if idx == 0:
            return step_name
        else:
            return f"{step_name}_{idx}"

    def submit_workflow(
        self,
        workflow_id: str,
        state: WorkflowExecutionState,
        ignore_existing: bool = False,
    ):
        """Submit workflow. A submitted workflow can be executed later.

        Args:
            workflow_id: ID of the workflow.
            state: The initial state of the workflow.
            ignore_existing: Ignore existing executed workflows.
        """
        if workflow_id in self._workflow_executors:
            raise RuntimeError(f"Workflow[id={workflow_id}] is being executed.")
        if workflow_id in self._executed_workflows and not ignore_existing:
            raise RuntimeError(f"Workflow[id={workflow_id}] has been executed.")

        if state.output_task_id is None:
            raise ValueError(
                "No root DAG specified that generates output for the workflow."
            )

        # initialize executor
        self._workflow_executors[workflow_id] = WorkflowExecutor(state)

    async def reconstruct_workflow(
        self, job_id: str, context: WorkflowStepContext
    ) -> None:
        """Reconstruct a (failed) workflow and submit it."""
        state = await resume_workflow_step.remote(job_id, context.workflow_id)
        self.submit_workflow(context.workflow_id, state, ignore_existing=True)

    async def execute_workflow(
        self,
        job_id: str,
        context: WorkflowStepContext,
    ) -> ray.ObjectRef:
        """Execute a submitted workflow.

        Args:
            job_id: The ID of the job for logging.
            context: The execution context.
        Returns:
            An object ref that represent the result.
        """
        workflow_id = context.workflow_id
        if workflow_id not in self._workflow_executors:
            raise RuntimeError(f"Workflow '{workflow_id}' has not been submitted.")
        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        executor = self._workflow_executors[workflow_id]
        try:
            await executor.run_until_complete(job_id, context, wf_store)
            return await self.get_output(workflow_id, executor.output_task_id)
        finally:
            self._workflow_executors.pop(workflow_id)
            self._executed_workflows.add(workflow_id)

    async def cancel_workflow(self, workflow_id: str) -> None:
        """Cancel workflow execution."""
        if workflow_id in self._workflow_executors:
            executor = self._workflow_executors[workflow_id]
            fut = executor.get_task_output_async(executor.output_task_id)
            executor.cancel()
            try:
                # Wait until cancelled, otherwise workflow status may not
                # get updated after "workflow.cancel()" is called.
                await fut
            except WorkflowCancellationError:
                pass
        else:
            wf_store = workflow_storage.WorkflowStorage(workflow_id)
            wf_store.update_workflow_status(WorkflowStatus.CANCELED)

    def is_workflow_running(self, workflow_id: str) -> bool:
        return workflow_id in self._workflow_executors

    def list_running_workflow(self) -> List[str]:
        return list(self._workflow_executors.keys())

    async def get_output(
        self, workflow_id: str, name: Optional[StepID]
    ) -> ray.ObjectRef:
        """Get the output of a running workflow.

        Args:
            workflow_id: The ID of a workflow job.

        Returns:
            An object reference that can be used to retrieve the
            workflow result.
        """
        # TODO(suquark): Use 'task_id' instead of 'name' for the API.
        ref = None
        if self.is_workflow_running(workflow_id):
            executor = self._workflow_executors[workflow_id]
            if name is None:
                name = executor.output_task_id
            workflow_ref = await executor.get_task_output_async(name)
            name, ref = workflow_ref.task_id, workflow_ref.ref
        if ref is None:
            ref = load_step_output_from_storage.remote(workflow_id, name)
        return SelfResolvingObject(ref)

    def ready(self) -> None:
        """A no-op to make sure the actor is ready."""


def init_management_actor() -> None:
    """Initialize WorkflowManagementActor"""
    try:
        get_management_actor()
    except ValueError:
        logger.info("Initializing workflow manager...")
        # the actor does not exist
        actor = WorkflowManagementActor.options(
            name=common.MANAGEMENT_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote()
        # No-op to ensure the actor is created before the driver exits.
        ray.get(actor.ready.remote())


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
        # the actor does not exist
        logger.warning(
            "Cannot access workflow manager. It could be because "
            "the workflow manager exited unexpectedly. A new "
            "workflow manager is being created."
        )
        workflow_manager = WorkflowManagementActor.options(
            name=common.MANAGEMENT_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote()
    return workflow_manager
