import logging
import time
from typing import Union, Optional

import ray

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import recovery
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow.step_executor import (commit_step,
                                                     execute_workflow)
from ray.experimental.workflow.storage import (
    Storage, create_storage, get_global_storage, set_global_storage)
from ray.experimental.workflow.workflow_access import flatten_workflow_output

logger = logging.getLogger(__name__)


def run(entry_workflow: Workflow,
        storage: Optional[Union[str, Storage]] = None,
        workflow_id: Optional[str] = None,
        detached: bool = True) -> ray.ObjectRef:
    """
    Run a workflow asynchronously.

    Args:
        entry_workflow: The workflow to run.
        storage: The storage or the URL of an external storage used for
            checkpointing.
        workflow_id: The ID of the workflow. The ID is used to identify
            the workflow.
        detached: If True, the execution of workflow is detached form
            the driver. Otherwise the workflow shares fate with the driver
            (i.e. if the driver failed, the workflow also failed).

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
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
        if detached:
            # TODO(suquark): Move this to a detached named actor,
            # so the workflow shares fate with the actor.
            # The current plan is resuming the workflow on the detached named
            # actor. This is extremely simple to implement, but I am not sure
            # of its performance.
            output = recovery.resume_workflow_job(workflow_id,
                                                  get_global_storage())
        else:
            rref = execute_workflow(entry_workflow)
            output = flatten_workflow_output(workflow_id, rref)
        logger.info(f"Workflow job {workflow_id} started.")
    finally:
        workflow_context.set_workflow_step_context(None)
    return output


# TODO(suquark): support recovery with ObjectRef inputs.


def resume(workflow_id: str,
           storage: Optional[Union[str, Storage]] = None) -> ray.ObjectRef:
    """
    Resume a workflow asynchronously. This workflow maybe fail previously.

    Args:
        workflow_id: The ID of the workflow. The ID is used to identify
            the workflow.
        storage: The storage or the URL of an external storage used for
            checkpointing.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
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
