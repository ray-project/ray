import logging
import time
from typing import Union, Optional

import ray

from ray.experimental.workflow import workflow_storage
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow.storage import (Storage, create_storage,
                                               get_global_storage)
from ray.experimental.workflow.workflow_access import (
    MANAGEMENT_ACTOR_NAME, flatten_workflow_output,
    get_or_create_management_actor)
from ray.experimental.workflow.step_executor import commit_step

logger = logging.getLogger(__name__)


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


# TODO(suquark): support recovery with ObjectRef inputs.


def resume(workflow_id: str,
           storage: Optional[Union[str, Storage]] = None) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details.
    """
    assert ray.is_initialized()
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
    try:
        actor = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError as e:
        raise ValueError(
            "Failed to connect to the workflow management "
            "actor. The workflow could have already failed. You can use "
            "workflow.resume() to resume the workflow.") from e
    output = ray.get(actor.get_output.remote(workflow_id))
    return flatten_workflow_output(workflow_id, output)
