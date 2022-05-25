import asyncio
import logging
import time
from typing import Set, List, Tuple, Optional, TYPE_CHECKING, Dict, Any
import uuid

import ray
from ray.workflow import workflow_context
from ray.workflow import workflow_storage
from ray.workflow.common import (
    Workflow,
    WorkflowStatus,
    StepType,
    WorkflowNotFoundError,
    validate_user_metadata,
    asyncio_run,
)
from ray.workflow.step_executor import commit_step
from ray.workflow.workflow_access import (
    flatten_workflow_output,
    get_or_create_management_actor,
    get_management_actor,
)

if TYPE_CHECKING:
    from ray.workflow.step_executor import WorkflowExecutionResult

logger = logging.getLogger(__name__)


def run(
    entry_workflow: Workflow,
    workflow_id: Optional[str] = None,
    metadata: Optional[Dict] = None,
) -> ray.ObjectRef:
    """Run a workflow asynchronously."""
    validate_user_metadata(metadata)
    metadata = metadata or {}

    from ray.workflow.api import _ensure_workflow_initialized

    _ensure_workflow_initialized()

    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{str(uuid.uuid4())}.{time.time():.9f}"
    step_type = entry_workflow.data.step_options.step_type

    logger.info(f'Workflow job created. [id="{workflow_id}"]. Type: {step_type}.')

    with workflow_context.workflow_step_context(workflow_id):
        # checkpoint the workflow
        ws = workflow_storage.get_workflow_storage(workflow_id)
        ws.save_workflow_user_metadata(metadata)

        wf_exists = True
        try:
            ws.get_entrypoint_step_id()
        except Exception:
            wf_exists = False

        # "Is growing" means we could adding steps to the (top-level)
        # workflow to grow the workflow dynamically at runtime.
        is_growing = step_type not in (StepType.FUNCTION, StepType.WAIT)

        # We only commit for
        #  - virtual actor tasks: it's dynamic tasks, so we always add
        #  - it's a new workflow
        # TODO (yic): follow up with force rerun
        if is_growing or not wf_exists:
            # We must checkpoint entry workflow.
            commit_step(ws, "", entry_workflow, exception=None)
        workflow_manager = get_or_create_management_actor()
        ignore_existing = is_growing
        # NOTE: It is important to 'ray.get' the returned output. This
        # ensures caller of 'run()' holds the reference to the workflow
        # result. Otherwise if the actor removes the reference of the
        # workflow output, the caller may fail to resolve the result.
        job_id = ray.get_runtime_context().job_id.hex()
        result: "WorkflowExecutionResult" = ray.get(
            workflow_manager.run_or_resume.remote(job_id, workflow_id, ignore_existing)
        )
        if not is_growing:
            return flatten_workflow_output(workflow_id, result.persisted_output)
        else:
            return flatten_workflow_output(workflow_id, result.volatile_output)


# TODO(suquark): support recovery with ObjectRef inputs.
def resume(workflow_id: str) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details."""
    logger.info(f'Resuming workflow [id="{workflow_id}"].')
    workflow_manager = get_or_create_management_actor()
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    job_id = ray.get_runtime_context().job_id.hex()
    result: "WorkflowExecutionResult" = ray.get(
        workflow_manager.run_or_resume.remote(
            job_id, workflow_id, ignore_existing=False
        )
    )
    logger.info(f"Workflow job {workflow_id} resumed.")
    return flatten_workflow_output(workflow_id, result.persisted_output)


def get_output(workflow_id: str, name: Optional[str]) -> ray.ObjectRef:
    """Get the output of a running workflow.
    See "api.get_output()" for details.
    """
    from ray.workflow.api import _ensure_workflow_initialized

    _ensure_workflow_initialized()
    try:
        workflow_manager = get_management_actor()
    except ValueError as e:
        raise ValueError(
            "Failed to connect to the workflow management "
            "actor. The workflow could have already failed. You can use "
            "workflow.resume() to resume the workflow."
        ) from e
    output = ray.get(workflow_manager.get_output.remote(workflow_id, name))
    return flatten_workflow_output(workflow_id, output)


def cancel(workflow_id: str) -> None:
    try:
        workflow_manager = get_management_actor()
    except ValueError:
        wf_store = workflow_storage.get_workflow_storage(workflow_id)
        # TODO(suquark): Here we update workflow status "offline", so it is likely
        # thread-safe because there is no workflow management actor updating the
        # workflow concurrently. But we should be careful if we are going to
        # update more workflow status offline in the future.
        wf_store.update_workflow_status(WorkflowStatus.CANCELED)
        return
    ray.get(workflow_manager.cancel_workflow.remote(workflow_id))


def get_status(workflow_id: str) -> Optional[WorkflowStatus]:
    try:
        workflow_manager = get_management_actor()
        running = ray.get(workflow_manager.is_workflow_running.remote(workflow_id))
    except Exception:
        running = False
    if running:
        return WorkflowStatus.RUNNING
    store = workflow_storage.get_workflow_storage(workflow_id)
    status = store.load_workflow_status()
    if status == WorkflowStatus.NONE:
        raise WorkflowNotFoundError(workflow_id)
    if status == WorkflowStatus.RUNNING:
        return WorkflowStatus.RESUMABLE
    return status


def get_metadata(workflow_id: str, name: Optional[str]) -> Dict[str, Any]:
    """Get the metadata of the workflow.
    See "api.get_metadata()" for details.
    """
    store = workflow_storage.get_workflow_storage(workflow_id)
    if name is None:
        return store.load_workflow_metadata()
    else:
        return store.load_step_metadata(name)


def list_all(status_filter: Set[WorkflowStatus]) -> List[Tuple[str, WorkflowStatus]]:
    try:
        workflow_manager = get_management_actor()
    except ValueError:
        workflow_manager = None

    if workflow_manager is None:
        runnings = []
    else:
        runnings = ray.get(workflow_manager.list_running_workflow.remote())
    if WorkflowStatus.RUNNING in status_filter and len(status_filter) == 1:
        return [(r, WorkflowStatus.RUNNING) for r in runnings]

    runnings = set(runnings)
    # Here we don't have workflow id, so use empty one instead
    store = workflow_storage.get_workflow_storage("")

    exclude_running = False
    if (
        WorkflowStatus.RESUMABLE in status_filter
        and WorkflowStatus.RUNNING not in status_filter
    ):
        # Here we have to add "RUNNING" to the status filter, because some "RESUMABLE"
        # workflows are converted from "RUNNING" workflows below.
        exclude_running = True
        status_filter.add(WorkflowStatus.RUNNING)
    status_from_storage = store.list_workflow(status_filter)
    ret = []
    for (k, s) in status_from_storage:
        if s == WorkflowStatus.RUNNING:
            if k not in runnings:
                s = WorkflowStatus.RESUMABLE
            elif exclude_running:
                continue
        if s in status_filter:
            ret.append((k, s))
    return ret


def resume_all(with_failed: bool) -> List[Tuple[str, ray.ObjectRef]]:
    filter_set = {WorkflowStatus.RESUMABLE}
    if with_failed:
        filter_set.add(WorkflowStatus.FAILED)
    all_failed = list_all(filter_set)
    try:
        workflow_manager = get_management_actor()
    except Exception as e:
        raise RuntimeError("Failed to get management actor") from e

    async def _resume_one(wid: str) -> Tuple[str, Optional[ray.ObjectRef]]:
        try:
            job_id = ray.get_runtime_context().job_id.hex()
            result: "WorkflowExecutionResult" = (
                await workflow_manager.run_or_resume.remote(job_id, wid)
            )
            obj = flatten_workflow_output(wid, result.persisted_output)
            return wid, obj
        except Exception:
            logger.error(f"Failed to resume workflow {wid}")
            return (wid, None)

    ret = asyncio_run(asyncio.gather(*[_resume_one(wid) for (wid, _) in all_failed]))
    return [(wid, obj) for (wid, obj) in ret if obj is not None]
