import logging
import time
import types
import uuid

import ray
from ray.experimental.workflow.workflow_manager import (
    WorkflowStepFunction, Workflow, resolve_object_ref)
from ray.experimental.workflow import workflow_context

logger = logging.getLogger(__name__)


def step(func) -> WorkflowStepFunction:
    """
    A decorator wraps over a function to turn it into a workflow step function.
    """
    if not isinstance(func, types.FunctionType):
        raise TypeError("The @step decorator must wraps over a function.")
    return WorkflowStepFunction(func)


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
    if workflow_id is None:
        # TODO(suquark): include the name of the workflow in the default ID,
        # this makes the ID more readable.
        # Workflow ID format: {UUID}.{Unix time to nanoseconds}
        workflow_id = f"{uuid.uuid4().hex}.{time.time():.9f}"
    logger.info(f"Workflow job {workflow_id} created.")
    try:
        workflow_context.init_workflow_step_context(workflow_id,
                                                    workflow_root_dir)
        rref = entry_workflow.execute()
        logger.info(f"Workflow job {workflow_id} started.")
        # TODO(suquark): although we do not return the resolved object to user,
        # the object was resolved temporarily to the driver script.
        # We may need a helper step for storing the resolved object
        # instead later.
        output = resolve_object_ref(rref)[1]
    finally:
        workflow_context.set_workflow_step_context(None)
    return output


__all__ = ("step", "run")
