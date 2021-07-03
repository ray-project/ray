import logging
import types
from typing import Union, Optional, TYPE_CHECKING

import ray

from ray.experimental.workflow import execution
from ray.experimental.workflow.step_function import WorkflowStepFunction

if TYPE_CHECKING:
    from ray.experimental.workflow.storage import Storage
    from ray.experimental.workflow.common import Workflow

logger = logging.getLogger(__name__)


def step(func) -> WorkflowStepFunction:
    """
    A decorator wraps over a function to turn it into a workflow step function.
    """
    if not isinstance(func, types.FunctionType):
        raise TypeError("The @step decorator must wraps over a function.")
    return WorkflowStepFunction(func)


def run(entry_workflow: "Workflow",
        storage: "Optional[Union[str, Storage]]" = None,
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
    return execution.run(entry_workflow, storage, workflow_id)


def resume(workflow_id: str,
           storage: "Optional[Union[str, Storage]]" = None) -> ray.ObjectRef:
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
    return execution.resume(workflow_id, storage)


__all__ = ("step", "run")
