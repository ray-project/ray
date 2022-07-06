import logging
import time
from typing import Set, List, Tuple, Optional, Dict, Any
import uuid

import ray
from ray.dag import DAGNode, DAGInputData

from ray.workflow import workflow_context
from ray.workflow import workflow_storage
from ray.workflow.common import (
    WorkflowStatus,
    WorkflowNotFoundError,
    validate_user_metadata,
)
from ray.workflow.workflow_access import (
    get_or_create_management_actor,
    get_management_actor,
    load_step_output_from_storage,
)
from ray.workflow.workflow_state_from_dag import workflow_state_from_dag


logger = logging.getLogger(__name__)


def run(
    dag: DAGNode,
    dag_inputs: DAGInputData,
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

    state = workflow_state_from_dag(dag, dag_inputs, workflow_id)
    logger.info(f'Workflow job created. [id="{workflow_id}"].')

    context = workflow_context.WorkflowStepContext(workflow_id=workflow_id)
    with workflow_context.workflow_step_context(context):
        # checkpoint the workflow
        ws = workflow_storage.get_workflow_storage(workflow_id)
        ws.save_workflow_user_metadata(metadata)

        job_id = ray.get_runtime_context().job_id.hex()

        try:
            ws.get_entrypoint_step_id()
            wf_exists = True
        except Exception:
            # The workflow does not exist. We must checkpoint entry workflow.
            ws.save_workflow_execution_state("", state)
            wf_exists = False
        workflow_manager = get_or_create_management_actor()
        if ray.get(workflow_manager.is_workflow_running.remote(workflow_id)):
            raise RuntimeError(f"Workflow '{workflow_id}' is already running.")
        if wf_exists:
            return resume(workflow_id)
        ignore_existing = ws.load_workflow_status() == WorkflowStatus.NONE
        ray.get(
            workflow_manager.submit_workflow.remote(
                workflow_id, state, ignore_existing=ignore_existing
            )
        )
        return workflow_manager.execute_workflow.remote(job_id, context)


# TODO(suquark): support recovery with ObjectRef inputs.
def resume(workflow_id: str) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details."""
    logger.info(f'Resuming workflow [id="{workflow_id}"].')
    workflow_manager = get_or_create_management_actor()
    if ray.get(workflow_manager.is_workflow_running.remote(workflow_id)):
        raise RuntimeError(f"Workflow '{workflow_id}' is already running.")
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    job_id = ray.get_runtime_context().job_id.hex()

    context = workflow_context.WorkflowStepContext(workflow_id=workflow_id)
    ray.get(workflow_manager.reconstruct_workflow.remote(job_id, context))
    result = workflow_manager.execute_workflow.remote(job_id, context)
    logger.info(f"Workflow job {workflow_id} resumed.")
    return result


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

    try:
        # check storage first
        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        tid = wf_store.inspect_output(name)
        if tid is not None:
            return load_step_output_from_storage.remote(workflow_id, name)
    except ValueError:
        pass

    return workflow_manager.get_output.remote(workflow_id, name)


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

    # This is the tricky part: the status "RESUMABLE" neither come from
    # the workflow management actor nor the storage. It is the status where
    # the storage says it is running but the workflow management actor
    # is not running it. This usually happened when there was a sudden crash
    # of the whole Ray runtime or the workflow management actor
    # (due to cluster etc.). So we includes 'RUNNING' status in the storage
    # filter to get "RESUMABLE" candidates.
    storage_status_filter = status_filter.copy()
    storage_status_filter.add(WorkflowStatus.RUNNING)
    status_from_storage = store.list_workflow(storage_status_filter)
    ret = []
    for (k, s) in status_from_storage:
        if s == WorkflowStatus.RUNNING:
            if k not in runnings:
                s = WorkflowStatus.RESUMABLE
            else:
                continue
        if s in status_filter:
            ret.append((k, s))
    if WorkflowStatus.RUNNING in status_filter:
        ret.extend((k, WorkflowStatus.RUNNING) for k in runnings)
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

    job_id = ray.get_runtime_context().job_id.hex()
    reconstructed_refs = []
    reconstructed_workflows = []
    for wid, _ in all_failed:
        context = workflow_context.WorkflowStepContext(workflow_id=wid)
        reconstructed_refs.append(
            (context, workflow_manager.reconstruct_workflow.remote(job_id, context))
        )
    for context, ref in reconstructed_refs:
        try:
            ray.get(ref)  # make sure the workflow is already reconstructed
            reconstructed_workflows.append(
                (
                    context.workflow_id,
                    workflow_manager.execute_workflow.remote(job_id, context),
                )
            )
        except Exception:
            # TODO(suquark): Here some workflows got resumed successfully but some
            #  failed and the user has no idea about this, which is very wired.
            # Maybe we should raise an exception here instead?
            logger.error(f"Failed to resume workflow {context.workflow_id}")

    return reconstructed_workflows
