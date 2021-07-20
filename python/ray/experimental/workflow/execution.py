import logging
import re
import time
from typing import List, Tuple, Union, Optional

import ray

from ray.experimental.workflow import workflow_storage
from ray.experimental.workflow.common import (Workflow, WorkflowStatus)
#                                              WorkflowMeta)
from ray.experimental.workflow.step_executor import commit_step
from ray.experimental.workflow.storage import (Storage, create_storage,
                                               get_global_storage)
from ray.experimental.workflow.workflow_access import (
    MANAGEMENT_ACTOR_NAME, flatten_workflow_output,
    get_or_create_management_actor)

logger = logging.getLogger(__name__)


def _is_anonymous_namespace():
    namespace = ray.get_runtime_context().namespace
    regex = re.compile(
        r"^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?"
        r"[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z", re.I)
    match = regex.match(namespace)
    return bool(match)


def _get_storage(storage: Optional[Union[str, Storage]]) -> Storage:
    if storage is None:
        return get_global_storage()
    elif isinstance(storage, str):
        return create_storage(storage)
    elif isinstance(storage, Storage):
        return storage
    else:
        raise TypeError("'storage' should be None, str, or Storage type.")


def _get_storage_url(storage: Optional[Union[str, Storage]]) -> str:
    if storage is None:
        return get_global_storage().storage_url
    elif isinstance(storage, str):
        return storage
    elif isinstance(storage, Storage):
        return storage.storage_url
    else:
        raise TypeError("'storage' should be None, str, or Storage type.")


def run(entry_workflow: Workflow,
        storage: Optional[Union[str, Storage]] = None,
        workflow_id: Optional[str] = None) -> ray.ObjectRef:
    """Run a workflow asynchronously. See "api.run()" for details."""
    assert ray.is_initialized()
    if _is_anonymous_namespace():
        raise ValueError("Must use a namespace in 'ray.init()' to access "
                         "workflows properly. Current namespace seems to "
                         "be anonymous.")
    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{entry_workflow.id}.{time.time():.9f}"
    store = _get_storage(storage)
    logger.info(f"Workflow job created. [id=\"{workflow_id}\", storage_url="
                f"\"{store.storage_url}\"].")

    # checkpoint the workflow
    ws = workflow_storage.WorkflowStorage(workflow_id, store)
    commit_step(ws, "", entry_workflow)
    workflow_manager = get_or_create_management_actor()
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    output = ray.get(
        workflow_manager.run_or_resume.remote(workflow_id, store.storage_url))
    return flatten_workflow_output(workflow_id, output)


def resume(workflow_id: str,
           storage: Optional[Union[str, Storage]] = None) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details.
    """
    assert ray.is_initialized()
    if _is_anonymous_namespace():
        raise ValueError("Must use a namespace in 'ray.init()' to access "
                         "workflows properly. Current namespace seems to "
                         "be anonymous.")
    storage_url = _get_storage_url(storage)
    logger.info(f"Resuming workflow [id=\"{workflow_id}\", storage_url="
                f"\"{storage_url}\"].")
    workflow_manager = get_or_create_management_actor()
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    output = ray.get(
        workflow_manager.run_or_resume.remote(workflow_id, storage_url))
    direct_output = flatten_workflow_output(workflow_id, output)
    logger.info(f"Workflow job {workflow_id} resumed.")
    return direct_output


def get_output(workflow_id: str) -> ray.ObjectRef:
    """Get the output of a running workflow.
    See "api.get_output()" for details.
    """
    assert ray.is_initialized()
    if _is_anonymous_namespace():
        raise ValueError("Must use a namespace in 'ray.init()' to access "
                         "workflows properly. Current namespace seems to "
                         "be anonymous.")
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError as e:
        raise ValueError(
            "Failed to connect to the workflow management "
            "actor. The workflow could have already failed. You can use "
            "workflow.resume() to resume the workflow.") from e
    output = ray.get(workflow_manager.get_output.remote(workflow_id))
    return flatten_workflow_output(workflow_id, output)


def cancel(workflow_id: str,
           storage: Optional[Union[str, Storage]] = None) -> None:
    pass
    # store = _get_storage(storage)
    # ws = workflow_storage.WorkflowStorage(workflow_id, store)

    # output = None
    # try:
    #     workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    #     output = ray.get(workflow_manager.get_output.remote(workflow_id))
    # except ValueError:
    #     pass

    # if output is not None:
    #     # workflow is currently running
    #     ray.cancel(output)
    # else:
    #     status = get_status(workflow_id)
    #     if status == WorkflowStatus.CANCELED:
    #         raise RuntimeError(f"{workflow_id} has been canceled.")
    # meta = WorkflowMeta(status=WorkflowStatus.CANCELED)
    # get_global_storage().save_workflow_meta(meta)


def get_status(workflow_id: str, storage: Optional[Union[str, Storage]] = None
               ) -> Optional[WorkflowStatus]:
    running = False
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError:
        work
    pass
    # output = None
    # storage_url = get_global_storage().storage_url
    # with workflow_context.workflow_step_context(workflow_id, storage_url):
    #     try:
    #         workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    #         output = ray.get(workflow_manager.get_output.remote(workflow_id))
    #     except ValueError:
    #         pass
    #     if output is not None:
    #         return WorkflowStatus.RUNNING


def list_all(status: Optional[WorkflowStatus],
             storage: Optional[Union[str, Storage]] = None
             ) -> List[Tuple[str, WorkflowStatus]]:
    try:
        workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError:
        workflow_manager = None

    if workflow_manager is None:
        runnings = []
    else:
        runnings = ray.get(workflow_manager.list_running_workflow.remote())
    if status == WorkflowStatus.RUNNING:
        return [(r, WorkflowStatus.RUNNING) for r in runnings]

    from ray.experimental.workflow.workflow_storage import WorkflowStorage
    runnings = set(runnings)
    store = WorkflowStorage(get_global_storage())
    ret = []
    for (k, s) in store.list_workflow():
        if s == WorkflowStatus.RUNNING and k not in runnings:
            s = WorkflowStatus.FAILED
        if status == None or s == status:
            ret.append((k, s))
    return ret
