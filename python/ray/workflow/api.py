import logging
import os
import types
from typing import Dict, Set, List, Tuple, Union, Optional, Any, TYPE_CHECKING
import time

import ray
from ray.experimental.dag import DAGNode
from ray.experimental.dag.input_node import DAGInputData

from ray.workflow import execution
from ray.workflow.step_function import WorkflowStepFunction

# avoid collision with arguments & APIs

from ray.workflow import virtual_actor_class
from ray.workflow import storage as storage_base
from ray.workflow.common import (
    WorkflowStatus,
    ensure_ray_initialized,
    Workflow,
    Event,
    WorkflowRunningError,
    WorkflowNotFoundError,
    WorkflowStepRuntimeOptions,
    StepType,
    asyncio_run,
)
from ray.workflow import serialization
from ray.workflow.event_listener import EventListener, EventListenerType, TimerListener
from ray.workflow.storage import Storage
from ray.workflow import workflow_access
from ray.workflow.workflow_storage import get_workflow_storage
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.workflow.virtual_actor_class import VirtualActorClass, VirtualActor

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
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
            logger.warning("Calling 'workflow.init()' again with the same storage.")
        else:
            raise RuntimeError(
                "Calling 'workflow.init()' again with a different storage"
            )
    storage_base.set_global_storage(storage)
    workflow_access.init_management_actor()
    serialization.init_manager()


def make_step_decorator(
    step_options: "WorkflowStepRuntimeOptions",
    name: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
):
    def decorator(func):
        return WorkflowStepFunction(
            func, step_options=step_options, name=name, metadata=metadata
        )

    return decorator


@PublicAPI(stability="beta")
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
        options = WorkflowStepRuntimeOptions.make(step_type=StepType.FUNCTION)
        return make_step_decorator(options)(args[0])
    if len(args) != 0:
        raise ValueError(f"Invalid arguments for step decorator {args}")
    max_retries = kwargs.pop("max_retries", None)
    catch_exceptions = kwargs.pop("catch_exceptions", None)
    name = kwargs.pop("name", None)
    metadata = kwargs.pop("metadata", None)
    allow_inplace = kwargs.pop("allow_inplace", False)
    checkpoint = kwargs.pop("checkpoint", None)
    ray_options = kwargs

    options = WorkflowStepRuntimeOptions.make(
        step_type=StepType.FUNCTION,
        catch_exceptions=catch_exceptions,
        max_retries=max_retries,
        allow_inplace=allow_inplace,
        checkpoint=checkpoint,
        ray_options=ray_options,
    )
    return make_step_decorator(options, name, metadata)


@PublicAPI(stability="beta")
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
            raise TypeError(
                "The @workflow.virtual_actor.readonly "
                "decorator can only wrap a method."
            )
        method.__virtual_actor_readonly__ = True
        return method


virtual_actor = _VirtualActorDecorator()


@PublicAPI(stability="beta")
def get_actor(actor_id: str) -> "VirtualActor":
    """Get an virtual actor.

    Args:
        actor_id: The ID of the actor.

    Returns:
        A virtual actor.
    """
    ensure_ray_initialized()
    return virtual_actor_class.get_actor(actor_id, storage_base.get_global_storage())


@PublicAPI(stability="beta")
def resume(workflow_id: str) -> ray.ObjectRef:
    """Resume a workflow.

    Resume a workflow and retrieve its output. If the workflow was incomplete,
    it will be re-executed from its checkpointed outputs. If the workflow was
    complete, returns the result immediately.

    Examples:
        >>> trip = start_trip.step()
        >>> res1 = trip.run_async(workflow_id="trip1")
        >>> res2 = workflow.resume("trip1")
        >>> assert ray.get(res1) == ray.get(res2)

    Args:
        workflow_id: The id of the workflow to resume.

    Returns:
        An object reference that can be used to retrieve the workflow result.
    """
    ensure_ray_initialized()
    return execution.resume(workflow_id)


@PublicAPI(stability="beta")
def get_output(workflow_id: str, *, name: Optional[str] = None) -> ray.ObjectRef:
    """Get the output of a running workflow.

    Args:
        workflow_id: The workflow to get the output of.
        name: If set, fetch the specific step instead of the output of the
            workflow.

    Examples:
        >>> trip = start_trip.options(name="trip").step()
        >>> res1 = trip.run_async(workflow_id="trip1")
        >>> # you could "get_output()" in another machine
        >>> res2 = workflow.get_output("trip1")
        >>> assert ray.get(res1) == ray.get(res2)
        >>> step_output = workflow.get_output("trip1", "trip")
        >>> assert ray.get(step_output) == ray.get(res1)

    Returns:
        An object reference that can be used to retrieve the workflow result.
    """
    ensure_ray_initialized()
    return execution.get_output(workflow_id, name)


@PublicAPI(stability="beta")
def list_all(
    status_filter: Optional[
        Union[Union[WorkflowStatus, str], Set[Union[WorkflowStatus, str]]]
    ] = None
) -> List[Tuple[str, WorkflowStatus]]:
    """List all workflows matching a given status filter.

    Args:
        status: If given, only returns workflow with that status. This can
            be a single status or set of statuses. The string form of the
            status is also acceptable, i.e.,
            "RUNNING"/"FAILED"/"SUCCESSFUL"/"CANCELED"/"RESUMABLE".

    Examples:
        >>> workflow_step = long_running_job.step()
        >>> wf = workflow_step.run_async(workflow_id="long_running_job")
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
    ensure_ray_initialized()
    if isinstance(status_filter, str):
        status_filter = set({WorkflowStatus(status_filter)})
    elif isinstance(status_filter, WorkflowStatus):
        status_filter = set({status_filter})
    elif isinstance(status_filter, set):
        if all(isinstance(s, str) for s in status_filter):
            status_filter = {WorkflowStatus(s) for s in status_filter}
        elif not all(isinstance(s, WorkflowStatus) for s in status_filter):
            raise TypeError(
                "status_filter contains element which is not"
                " a type of `WorkflowStatus or str`."
                f" {status_filter}"
            )
    elif status_filter is None:
        status_filter = set(WorkflowStatus.__members__.keys())
    else:
        raise TypeError(
            "status_filter must be WorkflowStatus or a set of WorkflowStatus."
        )
    return execution.list_all(status_filter)


@PublicAPI(stability="beta")
def resume_all(include_failed: bool = False) -> Dict[str, ray.ObjectRef]:
    """Resume all resumable workflow jobs.

    This can be used after cluster restart to resume all tasks.

    Args:
        with_failed: Whether to resume FAILED workflows.

    Examples:
        >>> workflow_step = failed_job.step()
        >>> output = workflow_step.run_async(workflow_id="failed_job")
        >>> try:
        >>>     ray.get(output)
        >>> except Exception:
        >>>     print("JobFailed")
        >>> jobs = workflow.list_all()
        >>> assert jobs == [("failed_job", workflow.FAILED)]
        >>> assert workflow.resume_all(
        >>>   include_failed=True).get("failed_job") is not None

    Returns:
        A list of (workflow_id, returned_obj_ref) resumed.
    """
    ensure_ray_initialized()
    return execution.resume_all(include_failed)


@PublicAPI(stability="beta")
def get_status(workflow_id: str) -> WorkflowStatus:
    """Get the status for a given workflow.

    Args:
        workflow_id: The workflow to query.

    Examples:
        >>> workflow_step = trip.step()
        >>> output = workflow_step.run(workflow_id="trip")
        >>> assert workflow.SUCCESSFUL == workflow.get_status("trip")

    Returns:
        The status of that workflow
    """
    ensure_ray_initialized()
    if not isinstance(workflow_id, str):
        raise TypeError("workflow_id has to be a string type.")
    return execution.get_status(workflow_id)


@PublicAPI(stability="beta")
def wait_for_event(
    event_listener_type: EventListenerType, *args, **kwargs
) -> Workflow[Event]:
    if not issubclass(event_listener_type, EventListener):
        raise TypeError(
            f"Event listener type is {event_listener_type.__name__}"
            ", which is not a subclass of workflow.EventListener"
        )

    @step
    def get_message(event_listener_type: EventListenerType, *args, **kwargs) -> Event:
        event_listener = event_listener_type()
        return asyncio_run(event_listener.poll_for_event(*args, **kwargs))

    @step
    def message_committed(
        event_listener_type: EventListenerType, event: Event
    ) -> Event:
        event_listener = event_listener_type()
        asyncio_run(event_listener.event_checkpointed(event))
        return event

    return message_committed.step(
        event_listener_type, get_message.step(event_listener_type, *args, **kwargs)
    )


@PublicAPI(stability="beta")
def sleep(duration: float) -> Workflow[Event]:
    """
    A workfow that resolves after sleeping for a given duration.
    """

    @step
    def end_time():
        return time.time() + duration

    return wait_for_event(TimerListener, end_time.step())


@PublicAPI(stability="beta")
def get_metadata(workflow_id: str, name: Optional[str] = None) -> Dict[str, Any]:
    """Get the metadata of the workflow.

    This will return a dict of metadata of either the workflow (
    if only workflow_id is given) or a specific workflow step (if
    both workflow_id and step name are given). Exception will be
    raised if the given workflow id or step name does not exist.

    If only workflow id is given, this will return metadata on
    workflow level, which includes running status, workflow-level
    user metadata and workflow-level running stats (e.g. the
    start time and end time of the workflow).

    If both workflow id and step name are given, this will return
    metadata on workflow step level, which includes step inputs,
    step-level user metadata and step-level running stats (e.g.
    the start time and end time of the step).


    Args:
        workflow_id: The workflow to get the metadata of.
        name: If set, fetch the metadata of the specific step instead of
            the metadata of the workflow.

    Examples:
        >>> workflow_step = trip.options(
        ...     name="trip", metadata={"k1": "v1"}).step()
        >>> workflow_step.run(workflow_id="trip1", metadata={"k2": "v2"})
        >>> workflow_metadata = workflow.get_metadata("trip1")
        >>> assert workflow_metadata["status"] == "SUCCESSFUL"
        >>> assert workflow_metadata["user_metadata"] == {"k2": "v2"}
        >>> assert "start_time" in workflow_metadata["stats"]
        >>> assert "end_time" in workflow_metadata["stats"]
        >>> step_metadata = workflow.get_metadata("trip1", "trip")
        >>> assert step_metadata["step_type"] == "FUNCTION"
        >>> assert step_metadata["user_metadata"] == {"k1": "v1"}
        >>> assert "start_time" in step_metadata["stats"]
        >>> assert "end_time" in step_metadata["stats"]

    Returns:
        A dictionary containing the metadata of the workflow.

    Raises:
        ValueError: if given workflow or workflow step does not exist.
    """
    ensure_ray_initialized()
    return execution.get_metadata(workflow_id, name)


@PublicAPI(stability="beta")
def cancel(workflow_id: str) -> None:
    """Cancel a workflow. Workflow checkpoints will still be saved in storage. To
       clean up saved checkpoints, see `workflow.delete()`.

    Args:
        workflow_id: The workflow to cancel.

    Examples:
        >>> workflow_step = some_job.step()
        >>> output = workflow_step.run_async(workflow_id="some_job")
        >>> workflow.cancel(workflow_id="some_job")
        >>> assert [("some_job", workflow.CANCELED)] == workflow.list_all()

    Returns:
        None

    """
    ensure_ray_initialized()
    if not isinstance(workflow_id, str):
        raise TypeError("workflow_id has to be a string type.")
    return execution.cancel(workflow_id)


@PublicAPI(stability="beta")
def delete(workflow_id: str) -> None:
    """Delete a workflow, its checkpoints, and other information it may have
       persisted to storage. To stop a running workflow, see
       `workflow.cancel()`.

        NOTE: The caller should ensure that the workflow is not currently
        running before deleting it.

    Args:
        workflow_id: The workflow to delete.

    Examples:
        >>> workflow_step = some_job.step()
        >>> output = workflow_step.run_async(workflow_id="some_job")
        >>> workflow.delete(workflow_id="some_job")
        >>> assert [] == workflow.list_all()

    Returns:
        None

    """

    try:
        status = get_status(workflow_id)
        if status == WorkflowStatus.RUNNING:
            raise WorkflowRunningError("DELETE", workflow_id)
    except ValueError:
        raise WorkflowNotFoundError(workflow_id)

    wf_storage = get_workflow_storage(workflow_id)
    wf_storage.delete_workflow()


WaitResult = Tuple[List[Any], List[Workflow]]


@PublicAPI(stability="beta")
def wait(
    workflows: List[Workflow], num_returns: int = 1, timeout: Optional[float] = None
) -> Workflow[WaitResult]:
    """Return a list of result of workflows that are ready and a list of
    workflows that are pending.

    Examples:
        >>> tasks = [task.step() for _ in range(3)]
        >>> wait_step = workflow.wait(tasks, num_returns=1)
        >>> print(wait_step.run())
        ([result_1], [<Workflow object>, <Workflow object>])

        >>> tasks = [task.step() for _ in range(2)] + [forever.step()]
        >>> wait_step = workflow.wait(tasks, num_returns=3, timeout=10)
        >>> print(wait_step.run())
        ([result_1, result_2], [<Workflow object>])

    If timeout is set, the function returns either when the requested number of
    workflows are ready or when the timeout is reached, whichever occurs first.
    If it is not set, the function simply waits until that number of workflows
    is ready and returns that exact number of workflows.

    This method returns two lists. The first list consists of workflows
    references that correspond to workflows that are ready. The second
    list corresponds to the rest of the workflows (which may or may not be
    ready).

    Ordering of the input list of workflows is preserved. That is, if A
    precedes B in the input list, and both are in the ready list, then A will
    precede B in the ready list. This also holds true if A and B are both in
    the remaining list.

    This method will issue a warning if it's running inside an async context.

    Args:
        workflows (List[Workflow]): List of workflows that may
            or may not be ready. Note that these workflows must be unique.
        num_returns (int): The number of workflows that should be returned.
        timeout (float): The maximum amount of time in seconds to wait before
            returning.

    Returns:
        A list of ready workflow results that are ready and a list of the
        remaining workflows.
    """
    from ray.workflow import serialization_context
    from ray.workflow.common import WorkflowData

    for w in workflows:
        if not isinstance(w, Workflow):
            raise TypeError("The input of workflow.wait should be a list of workflows.")
    wait_inputs = serialization_context.make_workflow_inputs(workflows)
    step_options = WorkflowStepRuntimeOptions.make(
        step_type=StepType.WAIT,
        # Pass the options through Ray options. "num_returns" conflicts with
        # the "num_returns" for Ray remote functions, so we need to wrap it
        # under "wait_options".
        ray_options={
            "wait_options": {
                "num_returns": num_returns,
                "timeout": timeout,
            }
        },
    )
    workflow_data = WorkflowData(
        func_body=None,
        inputs=wait_inputs,
        step_options=step_options,
        name="workflow.wait",
        user_metadata={},
    )
    return Workflow(workflow_data)


@PublicAPI(stability="beta")
def create(dag_node: "DAGNode", *args, **kwargs) -> Workflow:
    """Converts a DAG into a workflow.

    Args:
        dag_node: The DAG to be converted.
        args: Positional arguments of the DAG input node.
        kwargs: Keyword arguments of the DAG input node.
    """
    from ray.workflow.dag_to_workflow import transform_ray_dag_to_workflow

    if not isinstance(dag_node, DAGNode):
        raise TypeError("Input should be a DAG.")
    input_context = DAGInputData(*args, **kwargs)
    return transform_ray_dag_to_workflow(dag_node, input_context)


@PublicAPI(stability="beta")
def continuation(dag_node: "DAGNode") -> Union[Workflow, ray.ObjectRef]:
    """Converts a DAG into a continuation.

    The result depends on the context. If it is inside a workflow, it
    returns a workflow; otherwise it executes and get the result of
    the DAG.

    Args:
        dag_node: The DAG to be converted.
    """
    from ray.workflow.workflow_context import in_workflow_execution

    if not isinstance(dag_node, DAGNode):
        raise TypeError("Input should be a DAG.")

    if in_workflow_execution():
        return create(dag_node)
    return ray.get(dag_node.execute())


__all__ = (
    "step",
    "virtual_actor",
    "resume",
    "get_output",
    "get_actor",
    "resume_all",
    "get_status",
    "get_metadata",
    "cancel",
)
