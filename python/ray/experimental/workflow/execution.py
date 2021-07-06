import logging
import time
from typing import Union, Optional

import ray

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import recovery
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow.step_executor import commit_step
from ray.experimental.workflow.storage import (
    Storage, create_storage, get_global_storage, set_global_storage)

logger = logging.getLogger(__name__)

# TODO(suquark): Raise an error when calling run() on an existing workflow.
# Maybe we can also add a run_or_resume() call.


def run(entry_workflow: Workflow,
        storage: Optional[Union[str, Storage]] = None,
        workflow_id: Optional[str] = None) -> ray.ObjectRef:
    """Run a workflow asynchronously. See "api.run()" for details."""
    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{entry_workflow.id}.{time.time():.9f}"
    if isinstance(storage, str):
        set_global_storage(create_storage(storage))
    elif isinstance(storage, Storage):
        set_global_storage(storage)
    elif storage is not None:
        raise TypeError("'storage' should be None, str, or Storage type.")
    storage_url = get_global_storage().storage_url
    logger.info(f"Workflow job created. [id=\"{workflow_id}\", storage_url="
                f"\"{storage_url}\"].")
    try:
        workflow_context.init_workflow_step_context(workflow_id, storage_url)
        commit_step(entry_workflow)
        # TODO(suquark): Move this to a detached named actor,
        # so the workflow shares fate with the actor.
        # The current plan is resuming the workflow on the detached named
        # actor. This is extremely simple to implement, but I am not sure
        # of its performance.
        output = recovery.resume_workflow_job(workflow_id,
                                              get_global_storage())
        logger.info(f"Workflow job {workflow_id} started.")
    finally:
        workflow_context.set_workflow_step_context(None)
    return output


# TODO(suquark): support recovery with ObjectRef inputs.


def resume(workflow_id: str,
           storage: Optional[Union[str, Storage]] = None) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details.
    """
    if isinstance(storage, str):
        store = create_storage(storage)
    elif isinstance(storage, Storage):
        store = storage
    elif storage is None:
        store = get_global_storage()
    else:
        raise TypeError("'storage' should be None, str, or Storage type.")
    logger.info(f"Resuming workflow [id=\"{workflow_id}\", storage_url="
                f"\"{store.storage_url}\"].")
    output = recovery.resume_workflow_job(workflow_id, store)
    logger.info(f"Workflow job {workflow_id} resumed.")
    return output
