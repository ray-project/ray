import asyncio
import logging
import time
from typing import Set, Dict, Tuple, Optional

import ray

from ray.experimental.workflow import workflow_storage
from ray.experimental.workflow.common import (Workflow, WorkflowStatus,
                                              WorkflowMetaData)
from ray.experimental.workflow.step_executor import commit_step
from ray.experimental.workflow.storage import get_global_storage
from ray.experimental.workflow.workflow_access import (
    MANAGEMENT_ACTOR_NAME, flatten_workflow_output,
    get_or_create_management_actor)

logger = logging.getLogger(__name__)


def run(entry_workflow: Workflow,
        workflow_id: Optional[str] = None) -> ray.ObjectRef:
    """Run a workflow asynchronously. See "api.run()" for details."""
    store = get_global_storage()
    assert ray.is_initialized()
    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{entry_workflow.id}.{time.time():.9f}"
    logger.info(f"Workflow job created. [id=\"{workflow_id}\", storage_url="
                f"\"{store.storage_url}\"].")

    # checkpoint the workflow
    ws = workflow_storage.get_workflow_storage(workflow_id)
    commit_step(ws, "", entry_workflow)
    workflow_manager = get_or_create_management_actor()
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    output = ray.get(workflow_manager.run_or_resume.remote(workflow_id))
    return flatten_workflow_output(workflow_id, output)


# TODO(suquark): support recovery with ObjectRef inputs.
def resume(workflow_id: str) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details.
    """
    storage = get_global_storage()
    logger.info(f"Resuming workflow [id=\"{workflow_id}\", storage_url="
                f"\"{storage.storage_url}\"].")
    workflow_manager = get_or_create_management_actor()
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    output = ray.get(workflow_manager.run_or_resume.remote(workflow_id))
    direct_output = flatten_workflow_output(workflow_id, output)
    logger.info(f"Workflow job {workflow_id} resumed.")
    return direct_output


def get_output(workflow_id: str) -> ray.ObjectRef:
    """Get the output of a running workflow.
    See "api.get_output()" for details.
    """
    assert ray.is_initialized()
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError as e:
        raise ValueError(
            "Failed to connect to the workflow management "
            "actor. The workflow could have already failed. You can use "
            "workflow.resume() to resume the workflow.") from e
    output = ray.get(workflow_manager.get_output.remote(workflow_id))
    return flatten_workflow_output(workflow_id, output)


def cancel(workflow_id: str) -> None:
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
        ray.get(workflow_manager.cancel_workflow.remote(workflow_id))
    except ValueError:
        wf_store = workflow_storage.get_workflow_storage(workflow_id)
        wf_store.save_workflow_meta(WorkflowMetaData(WorkflowStatus.CANCELED))


def get_status(workflow_id: str) -> Optional[WorkflowStatus]:
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
        running = ray.get(
            workflow_manager.is_workflow_running.remote(workflow_id))
    except Exception:
        running = False
    if running:
        return WorkflowStatus.RUNNING
    store = workflow_storage.get_workflow_storage(workflow_id)
    meta = store.load_workflow_meta()
    if meta is None:
        return meta
    return meta.status


def list_all(status_filter: Set[WorkflowStatus]) -> Dict[str, WorkflowStatus]:
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError:
        workflow_manager = None

    if workflow_manager is None:
        runnings = []
    else:
        runnings = ray.get(workflow_manager.list_running_workflow.remote())
    if WorkflowStatus.RUNNING in status_filter and len(status_filter) == 1:
        return {r: WorkflowStatus.RUNNING for r in runnings}

    runnings = set(runnings)
    # Here we don't have workflow id, so use empty one instead
    store = workflow_storage.get_workflow_storage("")
    ret = {}
    for (k, s) in store.list_workflow():
        if s == WorkflowStatus.RUNNING and k not in runnings:
            s = WorkflowStatus.RESUMABLE
        if s in status_filter:
            ret[k] = s
    return ret


def resume_all() -> Dict[str, ray.ObjectRef]:
    all_failed = list_all({WorkflowStatus.RESUMABLE})
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except Exception as e:
        raise RuntimeError("Failed to get management actor") from e

    async def _resume_one(wid: str) -> Tuple[str, Optional[ray.ObjectRef]]:
        try:
            obj = await workflow_manager.run_or_resume.remote(wid)
            return (wid, flatten_workflow_output(wid, obj))
        except Exception:
            logger.error(f"Failed to resume workflow {wid}")
            return (wid, None)

    ret = workflow_storage.asyncio_run(
        asyncio.gather(*[_resume_one(wid) for wid in all_failed.keys()]))
    return {wid: obj for (wid, obj) in ret if obj is not None}
