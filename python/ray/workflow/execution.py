import logging
import time
from typing import Set, List, Tuple, Optional, Dict
import uuid

import ray
from ray.dag import DAGNode, DAGInputData

from ray.workflow import workflow_context
from ray.workflow import workflow_storage
from ray.workflow.common import (
    WorkflowStatus,
    validate_user_metadata,
)
from ray.workflow.workflow_access import (
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
        workflow_manager = get_management_actor()
        if ray.get(workflow_manager.is_workflow_non_terminating.remote(workflow_id)):
            raise RuntimeError(
                f"Workflow '{workflow_id}' is already running or pending."
            )
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
    workflow_manager = get_management_actor()
    if ray.get(workflow_manager.is_workflow_non_terminating.remote(workflow_id)):
        raise RuntimeError(f"Workflow '{workflow_id}' is already running or pending.")
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
    workflow_manager = get_management_actor()
    return ray.get(workflow_manager.get_workflow_status.remote(workflow_id))


def list_all(status_filter: Set[WorkflowStatus]) -> List[Tuple[str, WorkflowStatus]]:
    try:
        workflow_manager = get_management_actor()
    except ValueError:
        workflow_manager = None

    if workflow_manager is None:
        non_terminating_workflows = {}
    else:
        non_terminating_workflows = ray.get(
            workflow_manager.list_non_terminating_workflows.remote()
        )

    ret = []
    if set(non_terminating_workflows.keys()).issuperset(status_filter):
        for status, workflows in non_terminating_workflows.items():
            if status in status_filter:
                for w in workflows:
                    ret.append((w, status))
        return ret

    ret = []
    # Here we don't have workflow id, so use empty one instead
    store = workflow_storage.get_workflow_storage("")
    modified_status_filter = status_filter.copy()
    # Here we have to add non-terminating status to the status filter, because some
    # "RESUMABLE" workflows are converted from non-terminating workflows below.
    # This is the tricky part: the status "RESUMABLE" neither come from
    # the workflow management actor nor the storage. It is the status where
    # the storage says it is non-terminating but the workflow management actor
    # is not running it. This usually happened when there was a sudden crash
    # of the whole Ray runtime or the workflow management actor
    # (due to cluster etc.). So we includes non terminating status in the storage
    # filter to get "RESUMABLE" candidates.
    modified_status_filter.update(WorkflowStatus.non_terminating_status())
    status_from_storage = store.list_workflow(modified_status_filter)
    non_terminating_workflows = {
        k: set(v) for k, v in non_terminating_workflows.items()
    }
    resume_running = []
    resume_pending = []
    for (k, s) in status_from_storage:
        if s in non_terminating_workflows and k not in non_terminating_workflows[s]:
            if s == WorkflowStatus.RUNNING:
                resume_running.append(k)
            elif s == WorkflowStatus.PENDING:
                resume_pending.append(k)
            else:
                assert False, "This line of code should not be reachable."
            continue
        if s in status_filter:
            ret.append((k, s))
    if WorkflowStatus.RESUMABLE in status_filter:
        # The running workflows ranks before the pending workflows.
        for w in resume_running:
            ret.append((w, WorkflowStatus.RESUMABLE))
        for w in resume_pending:
            ret.append((w, WorkflowStatus.RESUMABLE))
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
    reconstructed_workflows = []
    for wid, _ in all_failed:
        context = workflow_context.WorkflowStepContext(workflow_id=wid)
        # TODO(suquark): This is not very efficient, but it makes sure
        #  running workflows has higher priority when getting reconstructed.
        try:
            ray.get(workflow_manager.reconstruct_workflow.remote(job_id, context))
        except Exception as e:
            # TODO(suquark): Here some workflows got resumed successfully but some
            #  failed and the user has no idea about this, which is very wired.
            # Maybe we should raise an exception here instead?
            logger.error(f"Failed to resume workflow {context.workflow_id}", exc_info=e)
            raise
        reconstructed_workflows.append(context)

    results = []
    for context in reconstructed_workflows:
        results.append(
            (
                context.workflow_id,
                workflow_manager.execute_workflow.remote(job_id, context),
            )
        )
    return results
