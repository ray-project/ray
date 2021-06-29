import logging
import time
import types
import uuid

import ray

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import recovery
from ray.experimental.workflow import storage
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow.workflow_manager import WorkflowStepFunction
from ray.experimental.workflow.step_executor import postprocess_workflow_step
from ray.experimental.workflow.workflow_access import workflow_output_cache

logger = logging.getLogger(__name__)

# TODO(suquark): some readability improvements:
# 1. Humanly readable default WorkflowID and StepID
# 2. Better logging message during workflow.run and workflow.resume
#    e.g. print information about storage.


def step(func) -> WorkflowStepFunction:
    """
    A decorator wraps over a function to turn it into a workflow step function.
    """
    if not isinstance(func, types.FunctionType):
        raise TypeError("The @step decorator must wraps over a function.")
    return WorkflowStepFunction(func)


# TODO(suquark): Raise an error when calling run() on an existing workflow.
# Maybe we can also add a run_or_resume() call.


def run(entry_workflow: Workflow, storage_url=None,
        workflow_id=None) -> ray.ObjectRef:
    """
    Run a workflow asynchronously.

    Args:
        entry_workflow: The workflow to run.
        storage_url: The URL of an external storage used for checkpointing.
        workflow_id: The ID of the workflow. The ID is used to identify
            the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    assert ray.is_initialized()
    if workflow_id is None:
        # TODO(suquark): include the name of the workflow in the default ID,
        # this makes the ID more readable.
        # Workflow ID format: {UUID}.{Unix time to nanoseconds}
        workflow_id = f"{uuid.uuid4().hex}.{time.time():.9f}"
    logger.info(f"Workflow job {workflow_id} created.")
    try:
        workflow_context.init_workflow_step_context(workflow_id, storage_url)
        rref = postprocess_workflow_step(entry_workflow)
        logger.info(f"Workflow job {workflow_id} started.")
        output = workflow_output_cache.remote(workflow_id, rref)
    finally:
        workflow_context.set_workflow_step_context(None)
    return output


# TODO(suquark): support recovery with ObjectRef inputs.


def resume(workflow_id: str, storage_url=None) -> ray.ObjectRef:
    """
    Resume a workflow asynchronously. This workflow maybe fail previously.

    Args:
        workflow_id: The ID of the workflow. The ID is used to identify
            the workflow.
        storage_url: The path of an external storage used for
            checkpointing.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    assert ray.is_initialized()
    if storage_url is not None:
        store = storage.create_storage(storage_url)
    else:
        store = storage.get_global_storage()
    r = recovery.resume_workflow_job(workflow_id, store)
    if isinstance(r, ray.ObjectRef):
        return r
    # skip saving the DAG of a recovery workflow
    r.skip_saving_workflow_dag = True
    return run(r, storage_url, workflow_id)


__all__ = ("step", "run")
