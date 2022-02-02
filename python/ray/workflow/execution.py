import asyncio
import json
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
    WorkflowMetaData,
    StepType,
    WorkflowNotFoundError,
)
from ray.workflow.step_executor import commit_step
from ray.workflow.storage import get_global_storage
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
    if metadata is not None:
        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a dict.")
        for k, v in metadata.items():
            try:
                json.dumps(v)
            except TypeError as e:
                raise ValueError(
                    "metadata values must be JSON serializable, "
                    "however '{}' has a value whose {}.".format(k, e)
                )
    metadata = metadata or {}

    store = get_global_storage()
    assert ray.is_initialized()
    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{str(uuid.uuid4())}.{time.time():.9f}"
    step_type = entry_workflow.data.step_options.step_type

    logger.info(
        f'Workflow job created. [id="{workflow_id}", storage_url='
        f'"{store.storage_url}"]. Type: {step_type}.'
    )

    with workflow_context.workflow_step_context(workflow_id, store.storage_url):
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
            commit_step(ws, "", entry_workflow, exception=None)
        workflow_manager = get_or_create_management_actor()
        ignore_existing = is_growing
        # NOTE: It is important to 'ray.get' the returned output. This
        # ensures caller of 'run()' holds the reference to the workflow
        # result. Otherwise if the actor removes the reference of the
        # workflow output, the caller may fail to resolve the result.
        result: "WorkflowExecutionResult" = ray.get(
            workflow_manager.run_or_resume.remote(workflow_id, ignore_existing)
        )
        if not is_growing:
            return flatten_workflow_output(workflow_id, result.persisted_output)
        else:
            return flatten_workflow_output(workflow_id, result.volatile_output)


# TODO(suquark): support recovery with ObjectRef inputs.
def resume(workflow_id: str) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details."""
    storage = get_global_storage()
    logger.info(
        f'Resuming workflow [id="{workflow_id}", storage_url='
        f'"{storage.storage_url}"].'
    )
    workflow_manager = get_or_create_management_actor()
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    result: "WorkflowExecutionResult" = ray.get(
        workflow_manager.run_or_resume.remote(workflow_id, ignore_existing=False)
    )
    logger.info(f"Workflow job {workflow_id} resumed.")
    return flatten_workflow_output(workflow_id, result.persisted_output)


def get_output(workflow_id: str, name: Optional[str]) -> ray.ObjectRef:
    """Get the output of a running workflow.
    See "api.get_output()" for details.
    """
    assert ray.is_initialized()
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
        ray.get(workflow_manager.cancel_workflow.remote(workflow_id))
    except ValueError:
        wf_store = workflow_storage.get_workflow_storage(workflow_id)
        wf_store.save_workflow_meta(WorkflowMetaData(WorkflowStatus.CANCELED))


def get_status(workflow_id: str) -> Optional[WorkflowStatus]:
    try:
        workflow_manager = get_management_actor()
        running = ray.get(workflow_manager.is_workflow_running.remote(workflow_id))
    except Exception:
        running = False
    if running:
        return WorkflowStatus.RUNNING
    store = workflow_storage.get_workflow_storage(workflow_id)
    meta = store.load_workflow_meta()
    if meta is None:
        raise WorkflowNotFoundError(workflow_id)
    if meta.status == WorkflowStatus.RUNNING:
        return WorkflowStatus.RESUMABLE
    return meta.status


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
    ret = []
    for (k, s) in store.list_workflow():
        if s == WorkflowStatus.RUNNING and k not in runnings:
            s = WorkflowStatus.RESUMABLE
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
            result: "WorkflowExecutionResult" = (
                await workflow_manager.run_or_resume.remote(wid)
            )
            obj = flatten_workflow_output(wid, result.persisted_output)
            return wid, obj
        except Exception:
            logger.error(f"Failed to resume workflow {wid}")
            return (wid, None)

    ret = workflow_storage.asyncio_run(
        asyncio.gather(*[_resume_one(wid) for (wid, _) in all_failed])
    )
    return [(wid, obj) for (wid, obj) in ret if obj is not None]
