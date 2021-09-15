import base64
from ray import cloudpickle
from collections import deque
from enum import Enum, unique
import hashlib
import re
from typing import Dict, List, Optional, Callable, Set, Iterator, Any
import unicodedata

from dataclasses import dataclass

import ray
from ray import ObjectRef
from ray.util.annotations import PublicAPI

# Alias types
StepID = str
WorkflowOutputType = ObjectRef

MANAGEMENT_ACTOR_NAMESPACE = "workflow"
MANAGEMENT_ACTOR_NAME = "WorkflowManagementActor"
STORAGE_ACTOR_NAME = "StorageManagementActor"


def get_module(f):
    return f.__module__ if hasattr(f, "__module__") else "__anonymous_module__"


def get_qualname(f):
    return f.__qualname__ if hasattr(f,
                                     "__qualname__") else "__anonymous_func__"


def ensure_ray_initialized():
    ray.worker.global_worker.check_connected()


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


@PublicAPI(stability="beta")
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


@ray.remote
def _hash(obj: Any) -> bytes:
    m = hashlib.sha256()
    m.update(cloudpickle.dumps(obj))
    return m.digest()


def calculate_identifiers(object_refs: List[ObjectRef]) -> List[str]:
    """
    Calculate identifiers for an object ref based on the contents. (i.e. a hash
    of the contents).
    """
    hashes = ray.get([_hash.remote(obj) for obj in object_refs])
    encoded = map(base64.urlsafe_b64encode, hashes)
    return [encoded_hash.decode("ascii") for encoded_hash in encoded]


@ray.remote
def calculate_identifier(obj: Any) -> str:
    """Calculate a url-safe identifier for an object."""

    # Step 1: Serialize the object.
    # Step 2: Calculate its sha256 hash.
    # Step 3: Get the url safe, base64 representation of it.

    # TODO (Alex): Ideally we should use the existing ObjectRef serializer to
    # avoid duplicate serialization passes and support nested object refs.
    m = hashlib.sha256()
    m.update(cloudpickle.dumps(obj))
    hash = m.digest()
    encoded = base64.urlsafe_b64encode(hash).decode("ascii")
    return encoded


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
    # name of the step
    name: str

    # Cache the intended locations of object refs. These are expensive
    # calculations since they require computing the hash over a large value.
    _cached_refs: List[ObjectRef] = None
    _cached_locs: List[str] = None

    def to_metadata(self) -> Dict[str, Any]:
        if self._cached_refs != self.inputs.object_refs:
            self._cached_refs = self.inputs.object_refs
            self._cached_locs = calculate_identifiers(self._cached_refs)

        f = self.func_body
        metadata = {
            "name": get_module(f) + "." + get_qualname(f),
            "step_type": self.step_type,
            "object_refs": self._cached_locs,
            "workflows": [w.step_id for w in self.inputs.workflows],
            "max_retries": self.max_retries,
            "workflow_refs": [wr.step_id for wr in self.inputs.workflow_refs],
            "catch_exceptions": self.catch_exceptions,
            "ray_options": self.ray_options,
        }
        return metadata


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
    def __init__(self,
                 workflow_data: WorkflowData,
                 prepare_inputs: Optional[Callable] = None):
        if workflow_data.ray_options.get("num_returns", 1) > 1:
            raise ValueError("Workflow steps can only have one return.")
        self._data: WorkflowData = workflow_data
        self._prepare_inputs: Callable = prepare_inputs
        if self._data.inputs is None:
            assert prepare_inputs is not None
        self._executed: bool = False
        self._result: Optional[WorkflowExecutionResult] = None
        # step id will be generated during runtime
        self._step_id: StepID = None

    @property
    def _workflow_id(self):
        from ray.workflow.workflow_context import \
            get_current_workflow_id
        return get_current_workflow_id()

    @property
    def executed(self) -> bool:
        return self._executed

    @property
    def result(self) -> WorkflowExecutionResult:
        if not self._executed:
            raise Exception("The workflow has not been executed.")
        return self._result

    @property
    def _name(self) -> str:
        if self.data.name:
            name = self.data.name
        else:
            f = self._data.func_body
            name = f"{get_module(f)}.{slugify(get_qualname(f))}"
        return name

    @property
    def step_id(self) -> StepID:
        if self._step_id is not None:
            return self._step_id

        from ray.workflow.workflow_access import \
            get_or_create_management_actor
        mgr = get_or_create_management_actor()
        self._step_id = ray.get(
            mgr.gen_step_id.remote(self._workflow_id, self._name))
        return self._step_id

    def _iter_workflows_in_dag(self) -> Iterator["Workflow"]:
        """Collect all workflows in the DAG linked to the workflow
        using BFS."""
        # deque is used instead of queue.Queue because queue.Queue is aimed
        # at multi-threading. We just need a pure data structure here.
        visited_workflows: Set[Workflow] = {self}
        q = deque([self])
        while q:  # deque's pythonic way to check emptyness
            w: Workflow = q.popleft()
            for p in w.data.inputs.workflows:
                if p not in visited_workflows:
                    visited_workflows.add(p)
                    q.append(p)
            yield w

    @property
    def data(self) -> WorkflowData:
        """Get the workflow data."""
        if self._data.inputs is None:
            self._data.inputs = self._prepare_inputs()
            del self._prepare_inputs
        return self._data

    def __reduce__(self):
        raise ValueError(
            "Workflow[T] objects are not serializable. "
            "This means they cannot be passed or returned from Ray "
            "remote, or stored in Ray objects.")

    @PublicAPI(stability="beta")
    def run(self, workflow_id: Optional[str] = None) -> Any:
        """Run a workflow.

        If the workflow with the given id already exists, it will be resumed.

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

    @PublicAPI(stability="beta")
    def run_async(self, workflow_id: Optional[str] = None) -> ObjectRef:
        """Run a workflow asynchronously.

        If the workflow with the given id already exists, it will be resumed.

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
        from ray.workflow.execution import run
        self._step_id = None
        return run(self, workflow_id)
