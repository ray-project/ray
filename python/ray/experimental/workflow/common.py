from collections import deque
import re
from typing import (Tuple, Dict, List, Optional, Callable, Set, Iterator, Any,
                    Union, TYPE_CHECKING)
import unicodedata
import uuid

from dataclasses import dataclass

import ray
from ray import ObjectRef

# Alias types
StepID = str
WorkflowOutputType = ObjectRef
WorkflowInputTuple = Tuple[ObjectRef, List["Workflow"], List[ObjectRef]]
StepExecutionFunction = Callable[
    [StepID, WorkflowInputTuple, Optional[StepID]], WorkflowOutputType]
SerializedStepFunction = str

if TYPE_CHECKING:
    from ray.experimental.workflow.storage import Storage


@dataclass
class WorkflowInputs:
    # The workflow step function body.
    func_body: Callable
    # The object ref of the input arguments.
    args: ObjectRef
    # The hex string of object refs in the arguments.
    object_refs: List[str]
    # The ID of workflows in the arguments.
    workflows: List[str]
    # The num of retry for application exception
    step_max_retries: int
    # Whether the user want to handle the exception mannually
    catch_exceptions: bool
    # ray_remote options
    ray_options: Dict[str, Any]


def slugify(value: str, allow_unicode=False) -> str:
    """Adopted from
    https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, dots or hyphens. Also strip leading and
    trailing whitespace.
    """
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = unicodedata.normalize("NFKD", value).encode(
            "ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w.\-]", "", value).strip()
    return re.sub(r"[-\s]+", "-", value)


class Workflow:
    def __init__(self, original_function: Callable,
                 step_execution_function: StepExecutionFunction,
                 input_placeholder: ObjectRef,
                 input_workflows: List["Workflow"],
                 input_object_refs: List[ObjectRef], step_max_retries: int,
                 catch_exceptions: bool, ray_options: Dict[str, Any]):
        self._input_placeholder: ObjectRef = input_placeholder
        self._input_workflows: List[Workflow] = input_workflows
        self._input_object_refs: List[ObjectRef] = input_object_refs
        # we need the original function for checkpointing
        self._original_function: Callable = original_function
        self._step_execution_function: StepExecutionFunction = (
            step_execution_function)
        self._executed: bool = False
        self._output: Optional[WorkflowOutputType] = None
        self._step_id: StepID = slugify(
            original_function.__qualname__) + "." + uuid.uuid4().hex
        self._step_max_retries: int = step_max_retries
        self._catch_exceptions: bool = catch_exceptions
        self._ray_options: Dict[str, Any] = ray_options

    @property
    def executed(self) -> bool:
        return self._executed

    @property
    def output(self) -> WorkflowOutputType:
        if not self._executed:
            raise Exception("The workflow has not been executed.")
        return self._output

    @property
    def id(self) -> StepID:
        return self._step_id

    def execute(self,
                outer_most_step_id: Optional[StepID] = None) -> ObjectRef:
        """Trigger workflow execution recursively.

        Args:
            outer_most_step_id: See
                "step_executor.execute_workflow" for explanation.
        """
        if self.executed:
            return self._output
        workflow_outputs = [w.execute() for w in self._input_workflows]
        # NOTE: Input placeholder is only a placeholder. It only can be
        # deserialized under a proper serialization context. Directly
        # deserialize the placeholder without a context would raise
        # an exception. If we pass the placeholder to _step_execution_function
        # as a direct argument, it would be deserialized by Ray without a
        # proper context. To prevent it, we put it inside a tuple.
        step_inputs = (self._input_placeholder, workflow_outputs,
                       self._input_object_refs)
        output = self._step_execution_function(
            self._step_id, step_inputs, self._catch_exceptions,
            self._step_max_retries, self._ray_options, outer_most_step_id)
        if not isinstance(output, WorkflowOutputType):
            raise TypeError("Unexpected return type of the workflow.")
        self._output = output
        self._executed = True
        return output

    def iter_workflows_in_dag(self) -> Iterator["Workflow"]:
        """Collect all workflows in the DAG linked to the workflow
        using BFS."""
        # deque is used instead of queue.Queue because queue.Queue is aimed
        # at multi-threading. We just need a pure data structure here.
        visited_workflows: Set[Workflow] = {self}
        q = deque([self])
        while q:  # deque's pythonic way to check emptyness
            w: Workflow = q.popleft()
            for p in w._input_workflows:
                if p not in visited_workflows:
                    visited_workflows.add(p)
                    q.append(p)
            yield w

    def get_inputs(self) -> WorkflowInputs:
        """Get the inputs of the workflow."""
        return WorkflowInputs(
            func_body=self._original_function,
            args=self._input_placeholder,
            object_refs=[r.hex() for r in self._input_object_refs],
            workflows=[w.id for w in self._input_workflows],
            step_max_retries=self._step_max_retries,
            catch_exceptions=self._catch_exceptions,
            ray_options=self._ray_options,
        )

    def __reduce__(self):
        raise ValueError(
            "Workflow is not supposed to be serialized by pickle. "
            "Maybe you are passing it to a Ray remote function, "
            "returning it from a Ray remote function, or using "
            "'ray.put()' with it?")

    def run(self,
            workflow_id: Optional[str] = None,
            storage: "Optional[Union[str, Storage]]" = None) -> Any:
        """Run a workflow.

        Examples:
            >>> @workflow.step
            ... def book_flight(origin: str, dest: str) -> Flight:
            ...    return Flight(...)

            >>> @workflow.step
            ... def book_hotel(location: str) -> Reservation:
            ...    return Reservation(...)

            >>> @workflow.step
            ... def finalize_trip(bookings: List[Any]) -> Trip:
            ...    return Trip(...)

            >>> flight1 = book_flight.step("OAK", "SAN")
            >>> flight2 = book_flight.step("SAN", "OAK")
            >>> hotel = book_hotel.step("SAN")
            >>> trip = finalize_trip.step([flight1, flight2, hotel])
            >>> result = trip.run()

        Args:
            workflow_id: A unique identifier that can be used to resume the
                workflow. If not specified, a random id will be generated.
            storage: The external storage URL or a custom storage class. If not
                specified, ``/tmp/ray/workflow_data`` will be used.
        """
        return ray.get(self.run_async(workflow_id, storage))

    def run_async(
            self,
            workflow_id: Optional[str] = None,
            storage: "Optional[Union[str, Storage]]" = None) -> ObjectRef:
        """Run a workflow asynchronously.

        Examples:
            >>> @workflow.step
            ... def book_flight(origin: str, dest: str) -> Flight:
            ...    return Flight(...)

            >>> @workflow.step
            ... def book_hotel(location: str) -> Reservation:
            ...    return Reservation(...)

            >>> @workflow.step
            ... def finalize_trip(bookings: List[Any]) -> Trip:
            ...    return Trip(...)

            >>> flight1 = book_flight.step("OAK", "SAN")
            >>> flight2 = book_flight.step("SAN", "OAK")
            >>> hotel = book_hotel.step("SAN")
            >>> trip = finalize_trip.step([flight1, flight2, hotel])
            >>> result = ray.get(trip.run_async())

        Args:
            workflow_id: A unique identifier that can be used to resume the
                workflow. If not specified, a random id will be generated.
            storage: The external storage URL or a custom storage class. If not
                specified, ``/tmp/ray/workflow_data`` will be used.
        """
        # avoid cyclic importing
        from ray.experimental.workflow.execution import run
        return run(self, storage, workflow_id)
