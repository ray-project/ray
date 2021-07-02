import logging
import time
import types
from typing import Union, Optional

import ray

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import recovery
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow.workflow_manager import WorkflowStepFunction
from ray.experimental.workflow.step_executor import postprocess_workflow_step
from ray.experimental.workflow.storage import (
    Storage, create_storage, get_global_storage, set_global_storage)
from ray.experimental.workflow.workflow_access import flatten_workflow_output

logger = logging.getLogger(__name__)


def step(func) -> WorkflowStepFunction:
    """
    A decorator wraps over a function to turn it into a workflow step function.
    """
    if not isinstance(func, types.FunctionType):
        raise TypeError("The @step decorator must wraps over a function.")
    return WorkflowStepFunction(func)


# TODO(suquark): Raise an error when calling run() on an existing workflow.
# Maybe we can also add a run_or_resume() call.


def run(entry_workflow: Workflow,
        storage: Optional[Union[str, Storage]] = None,
        workflow_id: Optional[str] = None) -> ray.ObjectRef:
    """
    Run a workflow asynchronously.

    Args:
        entry_workflow: The workflow to run.
        storage: The storage or the URL of an external storage used for
            checkpointing.
        workflow_id: The ID of the workflow. The ID is used to identify
            the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    assert ray.is_initialized()
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
        rref = postprocess_workflow_step(entry_workflow)
        logger.info(f"Workflow job {workflow_id} started.")
        output = flatten_workflow_output(workflow_id, rref)
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
    assert ray.is_initialized()
    if isinstance(storage, str):
        store = create_storage(storage)
    elif isinstance(storage, Storage):
        store = storage
    elif storage is None:
        store = get_global_storage()
    else:
        raise TypeError("'storage' should be None, str, or Storage type.")
    r = recovery.resume_workflow_job(workflow_id, store)
    logger.info(f"Resuming workflow [id=\"{workflow_id}\", storage_url="
                f"\"{store.storage_url}\"].")
    if isinstance(r, ray.ObjectRef):
        return r
    # skip saving the DAG of a recovery workflow
    r.skip_saving_workflow_dag = True
    return run(r, store, workflow_id)


__all__ = ("step", "run")
