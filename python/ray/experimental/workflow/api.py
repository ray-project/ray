import logging
import os
import types
from typing import Dict, Set, List, Tuple, Union, Optional, Any, TYPE_CHECKING

import ray
from ray.experimental.workflow import execution
from ray.experimental.workflow.step_function import WorkflowStepFunction
# avoid collision with arguments & APIs
from ray.experimental.workflow import virtual_actor_class
from ray.experimental.workflow import storage as storage_base
from ray.experimental.workflow.common import WorkflowStatus
from ray.experimental.workflow.storage import Storage
from ray.experimental.workflow import workflow_access

if TYPE_CHECKING:
    from ray.experimental.workflow.virtual_actor_class import (
        VirtualActorClass, VirtualActor)

logger = logging.getLogger(__name__)


def init(storage: "Optional[Union[str, Storage]]" = None) -> None:
    """Initialize workflow.

    Args:
        storage: The external storage URL or a custom storage class. If not
            specified, ``/tmp/ray/workflow_data`` will be used.
    """
    if storage is None:
        storage = os.environ.get("RAY_WORKFLOW_STORAGE")

    if storage is None:
        # We should use get_temp_dir_path, but for ray client, we don't
        # have this one. We need a flag to tell whether it's a client
        # or a driver to use the right dir.
        # For now, just use /tmp/ray/workflow_data
        storage = "file:///tmp/ray/workflow_data"
    if isinstance(storage, str):
        logger.info(f"Using storage: {storage}")
        storage = storage_base.create_storage(storage)
    elif not isinstance(storage, Storage):
        raise TypeError("'storage' should be None, str, or Storage type.")

    try:
        _storage = storage_base.get_global_storage()
    except RuntimeError:
        pass
    else:
        # we have to use the 'else' branch because we would raise a
        # runtime error, but we do not want to be captured by 'except'
        if _storage.storage_url == storage.storage_url:
            logger.warning("Calling 'workflow.init()' again with the same "
                           "storage.")
        else:
            raise RuntimeError("Calling 'workflow.init()' again with a "
                               "different storage")
    storage_base.set_global_storage(storage)
    workflow_access.init_management_actor()


def make_step_decorator(step_options: Dict[str, Any]):
    def decorator(func):
        return WorkflowStepFunction(func, **step_options)

    return decorator


def step(*args, **kwargs):
    """A decorator used for creating workflow steps.

    Examples:
        >>> @workflow.step
        ... def book_flight(origin: str, dest: str) -> Flight:
        ...    return Flight(...)

        >>> @workflow.step(max_retries=3, catch_exceptions=True)
        ... def book_hotel(dest: str) -> Hotel:
        ...    return Hotel(...)

    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return make_step_decorator({})(args[0])
    if len(args) != 0:
        raise ValueError(f"Invalid arguments for step decorator {args}")
    step_options = {}
    max_retries = kwargs.pop("max_retries", None)
    if max_retries is not None:
        step_options["max_retries"] = max_retries
    catch_exceptions = kwargs.pop("catch_exceptions", None)
    if catch_exceptions is not None:
        step_options["catch_exceptions"] = catch_exceptions
    if len(kwargs) != 0:
        step_options["ray_options"] = kwargs
    return make_step_decorator(step_options)


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


def get_actor(actor_id: str) -> "VirtualActor":
    """Get an virtual actor.

    Args:
        actor_id: The ID of the actor.

    Returns:
        A virtual actor.
    """
    return virtual_actor_class.get_actor(actor_id,
                                         storage_base.get_global_storage())


def resume(workflow_id: str) -> ray.ObjectRef:
    """Resume a workflow.

    Resume a workflow and retrieve its output. If the workflow was incomplete,
    it will be re-executed from its checkpointed outputs. If the workflow was
    complete, returns the result immediately.

    Examples:
        >>> trip = start_trip.step()
        >>> res1 = trip.async_run(workflow_id="trip1")
        >>> res2 = workflow.resume("trip1")
        >>> assert ray.get(res1) == ray.get(res2)

    Args:
        workflow_id: The id of the workflow to resume.

    Returns:
        An object reference that can be used to retrieve the workflow result.
    """
    return execution.resume(workflow_id)


def get_output(workflow_id: str) -> ray.ObjectRef:
    """Get the output of a running workflow.

    Args:
        workflow_id: The ID of the running workflow job.

    Examples:
        >>> trip = start_trip.step()
        >>> res1 = trip.async_run(workflow_id="trip1")
        >>> # you could "get_output()" in another machine
        >>> res2 = workflow.get_output("trip1")
        >>> assert ray.get(res1) == ray.get(res2)

    Returns:
        An object reference that can be used to retrieve the workflow result.
    """
    return execution.get_output(workflow_id)


def list_all(status_filter: Optional[Union[Union[WorkflowStatus, str], Set[
        Union[WorkflowStatus, str]]]] = None
             ) -> List[Tuple[str, WorkflowStatus]]:
    """List the workflow status. If status is given, it'll filter by that.

    Args:
        status: If given, only return workflow with that status. It can
            be a set workflow status,
            i.e., "RUNNING"/"FAILED"/"SUCCESSFUL"/"CANCELED"/"RESUMABLE"

    Examples:
        >>> workflow_step = long_running_job.step()
        >>> wf = workflow_step.async_run(workflow_id="long_running_job")
        >>> jobs = workflow.list_all()
        >>> assert jobs == [ ("long_running_job", workflow.RUNNING) ]
        >>> ray.get(wf)
        >>> jobs = workflow.list_all({workflow.RUNNING})
        >>> assert jobs == []
        >>> jobs = workflow.list_all(workflow.SUCCESSFUL)
        >>> assert jobs == [ ("long_running_job", workflow.SUCCESSFUL) ]

    Returns:
        A list of tuple with workflow id and workflow status
    """
    if isinstance(status_filter, str):
        status_filter = set({WorkflowStatus(status_filter)})
    elif isinstance(status_filter, WorkflowStatus):
        status_filter = set({status_filter})
    elif isinstance(status_filter, set):
        if all([isinstance(s, str) for s in status_filter]):
            status_filter = {WorkflowStatus(s) for s in status_filter}
        elif not all([isinstance(s, WorkflowStatus) for s in status_filter]):
            raise TypeError("status_filter contains element which is not"
                            " a type of `WorkflowStatus or str`."
                            f" {status_filter}")
    elif status_filter is None:
        status_filter = set(WorkflowStatus.__members__.keys())
    else:
        raise TypeError(
            "status_filter must be WorkflowStatus or a set of WorkflowStatus.")
    return execution.list_all(status_filter)


def resume_all(include_failed: bool = False) -> Dict[str, ray.ObjectRef]:
    """Resume all resumable workflow jobs.

    This usually is used after ray cluster shutdown to resume all tasks.


    Args:
        with_failed: Whether to include the failed workflow.

    Examples:
        >>> workflow_step = failed_job.step()
        >>> output = workflow_step.async_run(workflow_id="failed_job")
        >>> try:
        >>>     ray.get(output)
        >>> except Exception:
        >>>     print("JobFailed")
        >>> jobs = workflow.list_all()
        >>> assert jobs == [("failed_job", workflow.FAILED)]
        >>> assert workflow.resume_all(
        >>>   include_failed=True).get("failed_job") is not None

    Returns:
        Workflow resumed. It'll be a list of (workflow_id, returned_obj_ref).
    """
    return execution.resume_all(include_failed)


def get_status(workflow_id: str) -> WorkflowStatus:
    """Get the status for a given workflow.

    Args:
        workflow_id: The workflow id

    Examples:
        >>> workflow_step = trip.step()
        >>> output = workflow_step.run(workflow_id="trip")
        >>> assert workflow.SUCCESSFUL == workflow.get_status("trip")

    Returns:
        The status of that workflow
    """
    if not isinstance(workflow_id, str):
        raise TypeError("workflow_id has to be a string type.")
    return execution.get_status(workflow_id)


def cancel(workflow_id: str) -> None:
    """Cancel a workflow.

    Args:
        workflow_id: The workflow to cancel

    Examples:
        >>> workflow_step = some_job.step()
        >>> output = workflow_step.async_run(workflow_id="some_job")
        >>> workflow.cancel(workflow_id="some_job")
        >>> assert [("some_job", workflow.CANCELED)] == workflow.list_all()

    Returns:
        None
    """
    if not isinstance(workflow_id, str):
        raise TypeError("workflow_id has to be a string type.")
    return execution.cancel(workflow_id)


__all__ = ("step", "virtual_actor", "resume", "get_output", "get_actor",
           "resume_all", "get_status", "cancel")
