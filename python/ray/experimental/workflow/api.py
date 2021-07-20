import logging
import types
from typing import List, Tuple, Union, Optional, TYPE_CHECKING

import ray
from ray.experimental.workflow import execution
from ray.experimental.workflow.step_function import WorkflowStepFunction
# avoid collision with arguments & APIs
from ray.experimental.workflow import virtual_actor_class
from ray.experimental.workflow import storage as storage_base
from ray.experimental.workflow.common import WorkflowStatus

if TYPE_CHECKING:
    from ray.experimental.workflow.storage import Storage
    from ray.experimental.workflow.virtual_actor_class import (
        VirtualActorClass, VirtualActor)

logger = logging.getLogger(__name__)


def step(func: types.FunctionType) -> WorkflowStepFunction:
    """A decorator used for creating workflow steps.

    Examples:
        >>> @workflow.step
        ... def book_flight(origin: str, dest: str) -> Flight:
        ...    return Flight(...)

    Args:
        func: The function to turn into a workflow step.
    """
    if not isinstance(func, types.FunctionType):
        raise TypeError(
            "The @workflow.step decorator can only wrap a function.")
    return WorkflowStepFunction(func)


class _VirtualActorDecorator:
    """A decorator used for creating a virtual actor based on a class.
    The class that is based on must have the "__getstate__" and
     "__setstate__" method.

    Examples:
        >>> @workflow.virtual_actor
        ... class Counter:
        ... def __init__(self, x: int):
        ...     self.x = x
        ...
        ... # Mark a method as a readonly method. It would not modify the
        ... # state of the virtual actor.
        ... @workflow.virtual_actor.readonly
        ... def get(self):
        ...     return self.x
        ...
        ... def incr(self):
        ...     self.x += 1
        ...     return self.x
        ...
        ... def __getstate__(self):
        ...     return self.x
        ...
        ... def __setstate__(self, state):
        ...     self.x = state
        ...
        ... # Create and run a virtual actor.
        ... counter = Counter.get_or_create(actor_id="Counter", x=1)
        ... assert ray.get(counter.run(incr)) == 2
    """

    @classmethod
    def __call__(cls, _cls: type) -> "VirtualActorClass":
        return virtual_actor_class.decorate_actor(_cls)

    @classmethod
    def readonly(cls, method: types.FunctionType) -> types.FunctionType:
        if not isinstance(method, types.FunctionType):
            raise TypeError("The @workflow.virtual_actor.readonly "
                            "decorator can only wrap a method.")
        method.__virtual_actor_readonly__ = True
        return method


virtual_actor = _VirtualActorDecorator()


def get_actor(actor_id: str, storage: "Optional[Union[str, Storage]]" = None
              ) -> "VirtualActor":
    """Get an virtual actor.

    Args:
        actor_id: The ID of the actor.
        storage: The storage of the actor.

    Returns:
        A virtual actor.
    """
    if storage is None:
        storage = storage_base.get_global_storage()
    elif isinstance(storage, str):
        storage = storage_base.create_storage(storage)
    return virtual_actor_class.get_actor(actor_id, storage)


def resume(workflow_id: str,
           storage: "Optional[Union[str, Storage]]" = None) -> ray.ObjectRef:
    """Resume a workflow.

    Resume a workflow and retrieve its output. If the workflow was incomplete,
    it will be re-executed from its checkpointed outputs. If the workflow was
    complete, returns the result immediately.

    Examples:
        >>> res1 = workflow.run(trip, workflow_id="trip1")
        >>> res2 = workflow.resume("trip1")
        >>> assert ray.get(res1) == ray.get(res2)

    Args:
        workflow_id: The id of the workflow to resume.
        storage: The external storage URL or a custom storage class. If not
            specified, ``/tmp/ray/workflow_data`` will be used.

    Returns:
        An object reference that can be used to retrieve the workflow result.
    """
    return execution.resume(workflow_id, storage)


def get_output(workflow_id: str) -> ray.ObjectRef:
    """Get the output of a running workflow.

    Args:
        workflow_id: The ID of the running workflow job.

    Examples:
        >>> res1 = workflow.run(trip, workflow_id="trip1")
        >>> # you could "get_output()" in another machine
        >>> res2 = workflow.get_output("trip1")
        >>> assert ray.get(res1) == ray.get(res2)

    Returns:
        An object reference that can be used to retrieve the workflow result.
    """
    return execution.get_output(workflow_id)


def list_all(status: Optional[WorkflowStatus]=None) -> List[Tuple[str, WorkflowStatus]]:
    return execution.list_all(status)


def resume_all() -> List[Tuple[str, WorkflowStatus]]:
    return execution.resume_all()


def get_status(workflow_id: str) -> WorkflowStatus:
    if not isinstance(workflow_id, str):
        raise ValueError("workflow_id has to be a string type.")
    return execution.get_status(workflow_id)


def cancel(workflow_id: str) -> None:
    if not isinstance(workflow_id, str):
        raise ValueError("workflow_id has to be a string type.")
    return execution.cancel(workflow_id)


__all__ = ("step", "virtual_actor", "resume", "get_output", "get_actor",
           "resume_all", "get_status", "cancel")
