import asyncio
import logging
import queue
from typing import Dict, List, Set, Optional, TYPE_CHECKING

import ray

from ray.workflow import common
from ray.workflow.common import WorkflowStatus, TaskID
from ray.workflow import workflow_state_from_storage
from ray.workflow import workflow_context
from ray.workflow import workflow_storage
from ray.workflow.exceptions import (
    WorkflowCancellationError,
    WorkflowNotFoundError,
    WorkflowNotResumableError,
    WorkflowStillActiveError,
)
from ray.workflow.workflow_executor import WorkflowExecutor
from ray.workflow.workflow_state import WorkflowExecutionState
from ray.workflow.workflow_context import WorkflowTaskContext

if TYPE_CHECKING:
    from ray.actor import ActorHandle

logger = logging.getLogger(__name__)


class SelfResolvingObject:
    def __init__(self, x):
        self.x = x

    def __reduce__(self):
        return ray.get, (self.x,)


@ray.remote(num_cpus=0)
def load_task_output_from_storage(workflow_id: str, task_id: Optional[TaskID]):
    wf_store = workflow_storage.WorkflowStorage(workflow_id)
    tid = wf_store.inspect_output(task_id)
    if tid is not None:
        return wf_store.load_task_output(tid)
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
def resume_workflow_task(
    job_id: str,
    workflow_id: str,
    task_id: Optional[TaskID] = None,
) -> WorkflowExecutionState:
    """Resume a task of a workflow.

    Args:
        job_id: The ID of the job that submits the workflow execution. The ID
        is used to identify the submitter of the workflow.
        workflow_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        task_id: The task to resume in the workflow.

    Raises:
        WorkflowNotResumableException: fail to resume the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    with workflow_context.workflow_logging_context(job_id):
        try:
            return workflow_state_from_storage.workflow_state_from_storage(
                workflow_id, task_id
            )
        except Exception as e:
            raise WorkflowNotResumableError(workflow_id) from e


# TODO(suquark): we may use an actor pool in the future if too much
# concurrent workflow access blocks the actor.
@ray.remote(num_cpus=0)
class WorkflowManagementActor:
    """Keep the ownership and manage the workflow output."""

    def __init__(self, max_running_workflows: int, max_pending_workflows: int):
        self._workflow_executors: Dict[str, WorkflowExecutor] = {}

        self._max_running_workflows: int = max_running_workflows
        self._max_pending_workflows: int = max_pending_workflows

        # 0 means infinite for queue
        self._workflow_queue = queue.Queue(
            max_pending_workflows if max_pending_workflows != -1 else 0
        )

        self._running_workflows: Set[str] = set()
        self._queued_workflows: Dict[str, asyncio.Future] = {}
        # TODO(suquark): We do not cleanup "_executed_workflows" because we need to
        #  know if users are running the same workflow again long after a workflow
        #  completes. One possible alternative solution is to check the workflow
        #  status in the storage.
        self._executed_workflows: Set[str] = set()

    def validate_init_options(
        self, max_running_workflows: Optional[int], max_pending_workflows: Optional[int]
    ):
        if (
            max_running_workflows is not None
            and max_running_workflows != self._max_running_workflows
        ) or (
            max_pending_workflows is not None
            and max_pending_workflows != self._max_pending_workflows
        ):
            raise ValueError(
                "The workflow init is called again but the init options"
                "does not match the original ones. Original options: "
                f"max_running_workflows={self._max_running_workflows} "
                f"max_pending_workflows={self._max_pending_workflows}; "
                f"New options: max_running_workflows={max_running_workflows} "
                f"max_pending_workflows={max_pending_workflows}."
            )

    def gen_task_id(self, workflow_id: str, task_name: str) -> str:
        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        idx = wf_store.gen_task_id(task_name)
        if idx == 0:
            return task_name
        else:
            return f"{task_name}_{idx}"

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

        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        if (
            self._max_running_workflows != -1
            and len(self._running_workflows) >= self._max_running_workflows
        ):
            try:
                self._workflow_queue.put_nowait(workflow_id)
                self._queued_workflows[workflow_id] = asyncio.Future()
                wf_store.update_workflow_status(WorkflowStatus.PENDING)
            except queue.Full:
                # override with our error message
                raise queue.Full("Workflow queue has been full") from None
        else:
            self._running_workflows.add(workflow_id)
            wf_store.update_workflow_status(WorkflowStatus.RUNNING)
        # initialize executor
        self._workflow_executors[workflow_id] = WorkflowExecutor(state)

    async def reconstruct_workflow(
        self, job_id: str, context: WorkflowTaskContext
    ) -> None:
        """Reconstruct a (failed) workflow and submit it."""
        state = await resume_workflow_task.remote(job_id, context.workflow_id)
        self.submit_workflow(context.workflow_id, state, ignore_existing=True)

    async def execute_workflow(
        self,
        job_id: str,
        context: WorkflowTaskContext,
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

        pending_fut = self._queued_workflows.get(workflow_id)
        if pending_fut is not None:
            await pending_fut  # wait until this workflow is ready to go

        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        executor = self._workflow_executors[workflow_id]
        try:
            await executor.run_until_complete(job_id, context, wf_store)
            return await self.get_output(workflow_id, executor.output_task_id)
        finally:
            self._workflow_executors.pop(workflow_id)
            self._running_workflows.remove(workflow_id)
            self._executed_workflows.add(workflow_id)
            if not self._workflow_queue.empty():
                # schedule another workflow from the pending queue
                next_workflow_id = self._workflow_queue.get_nowait()
                self._running_workflows.add(next_workflow_id)
                fut = self._queued_workflows.pop(next_workflow_id)
                fut.set_result(None)

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

    def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        """Get the status of the workflow."""
        if workflow_id in self._workflow_executors:
            if workflow_id in self._queued_workflows:
                return WorkflowStatus.PENDING
            return WorkflowStatus.RUNNING
        store = workflow_storage.get_workflow_storage(workflow_id)
        status = store.load_workflow_status()
        if status == WorkflowStatus.NONE:
            raise WorkflowNotFoundError(workflow_id)
        elif status in WorkflowStatus.non_terminating_status():
            return WorkflowStatus.RESUMABLE
        return status

    def is_workflow_non_terminating(self, workflow_id: str) -> bool:
        """True if the workflow is still running or pending."""
        return workflow_id in self._workflow_executors

    def list_non_terminating_workflows(self) -> Dict[WorkflowStatus, List[str]]:
        """List workflows whose status are not of terminated status."""
        result = {WorkflowStatus.RUNNING: [], WorkflowStatus.PENDING: []}
        for wf in self._workflow_executors.keys():
            if wf in self._running_workflows:
                result[WorkflowStatus.RUNNING].append(wf)
            else:
                result[WorkflowStatus.PENDING].append(wf)
        return result

    async def get_output(
        self, workflow_id: str, task_id: Optional[TaskID]
    ) -> ray.ObjectRef:
        """Get the output of a running workflow.

        Args:
            workflow_id: The ID of a workflow job.
            task_id: If set, fetch the specific task output instead of the output
                of the workflow.

        Returns:
            An object reference that can be used to retrieve the workflow result.
        """
        ref = None
        if self.is_workflow_non_terminating(workflow_id):
            executor = self._workflow_executors[workflow_id]
            if task_id is None:
                task_id = executor.output_task_id
            workflow_ref = await executor.get_task_output_async(task_id)
            task_id, ref = workflow_ref.task_id, workflow_ref.ref
        if ref is None:
            wf_store = workflow_storage.WorkflowStorage(workflow_id)
            tid = wf_store.inspect_output(task_id)
            if tid is not None:
                ref = load_task_output_from_storage.remote(workflow_id, task_id)
            elif task_id is not None:
                raise ValueError(
                    f"Cannot load output from task id '{task_id}' in workflow "
                    f"'{workflow_id}'"
                )
            else:
                raise ValueError(f"Cannot load output from workflow '{workflow_id}'")
        return SelfResolvingObject(ref)

    def delete_workflow(self, workflow_id: str) -> None:
        """Delete a workflow, its checkpoints, and other information it may have
           persisted to storage.

        Args:
            workflow_id: The workflow to delete.

        Raises:
            WorkflowStillActiveError: The workflow is still active.
            WorkflowNotFoundError: The workflow does not exist.
        """
        if self.is_workflow_non_terminating(workflow_id):
            raise WorkflowStillActiveError("DELETE", workflow_id)
        wf_storage = workflow_storage.WorkflowStorage(workflow_id)
        wf_storage.delete_workflow()
        self._executed_workflows.discard(workflow_id)

    def create_http_event_provider(self) -> None:
        """Deploy an HTTPEventProvider as a Serve deployment with
        name = common.HTTP_EVENT_PROVIDER_NAME, if one doesn't exist
        """
        ray.serve.start(detached=True)
        try:
            ray.serve.get_deployment(common.HTTP_EVENT_PROVIDER_NAME)
        except KeyError:
            from ray.workflow.http_event_provider import HTTPEventProvider

            HTTPEventProvider.deploy()

    def ready(self) -> None:
        """A no-op to make sure the actor is ready."""


def init_management_actor(
    max_running_workflows: Optional[int], max_pending_workflows: Optional[int]
) -> None:
    """Initialize WorkflowManagementActor.

    Args:
        max_running_workflows: The maximum number of concurrently running workflows.
            Use -1 as infinity. Use 'None' for keeping the original value if the actor
            exists, or it is equivalent to infinity if the actor does not exist.
        max_pending_workflows: The maximum number of queued workflows.
            Use -1 as infinity. Use 'None' for keeping the original value if the actor
            exists, or it is equivalent to infinity if the actor does not exist.
    """
    try:
        actor = get_management_actor()
        # Check if max_running_workflows/max_pending_workflows
        # matches the previous settings.
        ray.get(
            actor.validate_init_options.remote(
                max_running_workflows, max_pending_workflows
            )
        )
    except ValueError:
        logger.info("Initializing workflow manager...")
        if max_running_workflows is None:
            max_running_workflows = -1
        if max_pending_workflows is None:
            max_pending_workflows = -1
        # the actor does not exist
        actor = WorkflowManagementActor.options(
            name=common.MANAGEMENT_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote(max_running_workflows, max_pending_workflows)
        # No-op to ensure the actor is created before the driver exits.
        ray.get(actor.ready.remote())


def get_management_actor() -> "ActorHandle":
    return ray.get_actor(
        common.MANAGEMENT_ACTOR_NAME, namespace=common.MANAGEMENT_ACTOR_NAMESPACE
    )
