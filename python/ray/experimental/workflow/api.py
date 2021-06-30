import logging
import time
import types
import uuid

import ray
from ray.experimental.workflow.workflow_manager import (
    WorkflowStepFunction, Workflow, resolve_object_ref,
    postprocess_workflow_step)
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import recovery
from ray.experimental.workflow import storage

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
# TODO(suquark): Rename "workflow_root_dir" to "storage_url".


def run(entry_workflow: Workflow, workflow_root_dir=None,
        workflow_id=None) -> ray.ObjectRef:
    """
    Run a workflow asynchronously.

    Args:
        entry_workflow: The workflow to run.
        workflow_root_dir: The path of an external storage used for
            checkpointing.
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
        workflow_context.init_workflow_step_context(workflow_id,
                                                    workflow_root_dir)
        rref = postprocess_workflow_step(entry_workflow)
        logger.info(f"Workflow job {workflow_id} started.")
        # TODO(suquark): although we do not return the resolved object to user,
        # the object was resolved temporarily to the driver script.
        # We may need a helper step for storing the resolved object
        # instead later.
        output = resolve_object_ref(rref)[1]
    finally:
        workflow_context.set_workflow_step_context(None)
    return output


# TODO(suquark): support recovery with ObjectRef inputs.


def resume(workflow_id: str, workflow_root_dir=None) -> ray.ObjectRef:
    """
    Resume a workflow asynchronously. This workflow maybe fail previously.

    Args:
        workflow_id: The ID of the workflow. The ID is used to identify
            the workflow.
        workflow_root_dir: The path of an external storage used for
            checkpointing.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    assert ray.is_initialized()
    if workflow_root_dir is not None:
        store = storage.create_storage(workflow_root_dir)
    else:
        store = storage.get_global_storage()
    r = recovery.resume_workflow_job(workflow_id, store)
    if isinstance(r, ray.ObjectRef):
        return r
    # skip saving the DAG of a recovery workflow
    r.skip_saving_workflow_dag = True
    return run(r, workflow_root_dir, workflow_id)


__all__ = ("step", "run")
