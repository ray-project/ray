from enum import Enum, unique
from collections import deque
import re
from typing import Dict, List, Optional, Callable, Set, Iterator, Any
import unicodedata
import uuid

from dataclasses import dataclass

import ray
from ray import ObjectRef

# Alias types
StepID = str
WorkflowOutputType = ObjectRef


@dataclass
class WorkflowRef:
    """This class represents a dynamic reference of a workflow output.

    See 'step_executor._resolve_dynamic_workflow_refs' for how we handle
    workflow refs."""
    # The ID of the step that produces the output of the workflow.
    step_id: StepID

    def __reduce__(self):
        return WorkflowRef, (self.step_id, )

    def __hash__(self):
        return hash(self.step_id)


@unique
class WorkflowStatus(str, Enum):
    # There is at least a remote task running in ray cluster
    RUNNING = "RUNNING"
    # It got canceled and can't be resumed later.
    CANCELED = "CANCELED"
    # The workflow runs successfully.
    SUCCESSFUL = "SUCCESSFUL"
    # The workflow failed with an applicaiton error.
    # It can be resumed.
    FAILED = "FAILED"
    # The workflow failed with a system error, i.e., ray shutdown.
    # It can be resumed.
    RESUMABLE = "RESUMABLE"


@unique
class StepType(str, Enum):
    """All step types."""
    FUNCTION = "FUNCTION"
    ACTOR_METHOD = "ACTOR_METHOD"
    READONLY_ACTOR_METHOD = "READONLY_ACTOR_METHOD"


@dataclass
class WorkflowInputs:
    # The object ref of the input arguments.
    args: ObjectRef
    # The object refs in the arguments.
    object_refs: List[ObjectRef]
    # TODO(suquark): maybe later we can replace it with WorkflowData.
    # The workflows in the arguments.
    workflows: "List[Workflow]"
    # The dynamic refs of workflows in the arguments.
    workflow_refs: List[WorkflowRef]


@dataclass
class WorkflowData:
    # The workflow step function body.
    func_body: Callable
    # The type of the step
    step_type: StepType
    # The arguments of a workflow.
    inputs: WorkflowInputs
    # The num of retry for application exception
    max_retries: int
    # Whether the user want to handle the exception mannually
    catch_exceptions: bool
    # ray_remote options
    ray_options: Dict[str, Any]

    def to_metadata(self) -> Dict[str, Any]:
        f = self.func_body
        return {
            "name": f.__module__ + "." + f.__qualname__,
            "step_type": self.step_type,
            "object_refs": [r.hex() for r in self.inputs.object_refs],
            "workflows": [w.id for w in self.inputs.workflows],
            "max_retries": self.max_retries,
            "workflow_refs": [wr.step_id for wr in self.inputs.workflow_refs],
            "catch_exceptions": self.catch_exceptions,
            "ray_options": self.ray_options,
        }


@dataclass
class WorkflowExecutionResult:
    """Dataclass for holding workflow execution result."""
    # Part of result to persist in a storage and pass to the next step.
    persisted_output: "ObjectRef"
    # Part of result to return to the user but does not require persistence.
    volatile_output: "ObjectRef"

    def __reduce__(self):
        return WorkflowExecutionResult, (self.persisted_output,
                                         self.volatile_output)


@dataclass
class WorkflowMetaData:
    # The current status of the workflow
    status: WorkflowStatus


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
    def __init__(self, workflow_data: WorkflowData):
        if workflow_data.ray_options.get("num_returns", 1) > 1:
            raise ValueError("Workflow should have one return value.")
        self._data = workflow_data
        self._executed: bool = False
        self._result: Optional[WorkflowExecutionResult] = None
        self._step_id: StepID = slugify(
            self._data.func_body.__qualname__) + "." + uuid.uuid4().hex

    @property
    def executed(self) -> bool:
        return self._executed

    @property
    def result(self) -> WorkflowExecutionResult:
        if not self._executed:
            raise Exception("The workflow has not been executed.")
        return self._result

    @property
    def id(self) -> StepID:
        return self._step_id

    def iter_workflows_in_dag(self) -> Iterator["Workflow"]:
        """Collect all workflows in the DAG linked to the workflow
        using BFS."""
        # deque is used instead of queue.Queue because queue.Queue is aimed
        # at multi-threading. We just need a pure data structure here.
        visited_workflows: Set[Workflow] = {self}
        q = deque([self])
        while q:  # deque's pythonic way to check emptyness
            w: Workflow = q.popleft()
            for p in w._data.inputs.workflows:
                if p not in visited_workflows:
                    visited_workflows.add(p)
                    q.append(p)
            yield w

    @property
    def data(self) -> WorkflowData:
        """Get the workflow data."""
        return self._data

    def __reduce__(self):
        raise ValueError(
            "Workflow is not supposed to be serialized by pickle. "
            "Maybe you are passing it to a Ray remote function, "
            "returning it from a Ray remote function, or using "
            "'ray.put()' with it?")

    def run(self, workflow_id: Optional[str] = None) -> Any:
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
        """
        return ray.get(self.run_async(workflow_id))

    def run_async(self, workflow_id: Optional[str] = None) -> ObjectRef:
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
        """
        # TODO(suquark): avoid cyclic importing
        from ray.experimental.workflow.execution import run
        return run(self, workflow_id)
