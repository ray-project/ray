import base64
from ray import cloudpickle
from collections import deque
from enum import Enum, unique
import hashlib
import re
from typing import (Dict, Generic, List, Optional, Callable, Set, TypeVar,
                    Iterator, Any)
import unicodedata

from dataclasses import dataclass

import ray
from ray import ObjectRef
from ray.util.annotations import PublicAPI

# Alias types
Event = Any
StepID = str

MANAGEMENT_ACTOR_NAMESPACE = "workflow"
MANAGEMENT_ACTOR_NAME = "WorkflowManagementActor"
STORAGE_ACTOR_NAME = "StorageManagementActor"


def get_module(f):
    return f.__module__ if hasattr(f, "__module__") else "__anonymous_module__"


def get_qualname(f):
    return f.__qualname__ if hasattr(f,
                                     "__qualname__") else "__anonymous_func__"


def ensure_ray_initialized():
    if not ray.is_initialized():
        ray.init()


@dataclass
class WorkflowRef:
    """This class represents a reference of a workflow output.

    The workflow reference could be a **dynamic reference**,
    **static reference** depending on its content.

    When a reference only contains the step ID, it is a **dynamic reference**.
    Dynamic reference is used to refer a workflow whose status is unknown.
    This means the workflow could:

    1. have not executed yet
    2. have been running
    3. have failed
    4. have finished

    Dynamic reference is mostly used for virtual actors in the codebase.

    If we also have the object ref in workflow reference, it is a
    **unresolved static reference**.Unresolved static reference could be
    used when you want to return a running workflow from a workflow step.
    For example, the remaining workflows returned by 'workflow.wait' contain
    static refs to these pending workflows.

    If we have the output object instance in the workflow reference,
    it is a **resolved static reference**. Resolved reference represents
    workflow outputs that are both ready and local. Currently it is used for
    holding outputs of inplace workflows or to cache outputs loaded from
    storage during recovery.

    See 'workflow_ref.resolve_workflow_refs' for how we resolve workflow refs.
    """
    # The ID of the step that produces the output of the workflow.
    step_id: StepID
    # The object ref that points to the live output of the workflow.
    output_ref: Optional[ObjectRef] = None
    # True if we have the output of the workflow.
    has_output: bool = False
    # The output of the workflow.
    output: Optional[Any] = None

    @classmethod
    def from_workflow_output(cls, step_id: StepID, output) -> "WorkflowRef":
        """A helper function to convert a workflow and it output
        to a workflow

        Args:
            step_id: The ID of the workflow.
            output: The workflow that produces the output.

        Returns:
            A reference to workflow output.
        """
        if isinstance(output, ray.ObjectRef):
            return WorkflowRef(step_id=step_id, output_ref=output)
        return WorkflowRef(step_id=step_id, has_output=True, output=output)

    @property
    def is_resolved(self):
        return self.has_output

    @property
    def is_dynamic(self):
        return not self.is_resolved and self.output_ref is None

    def __reduce__(self):
        return WorkflowRef, (self.step_id, self.output_ref, self.has_output,
                             self.output)

    def __hash__(self):
        s = self.step_id
        if self.output_ref is not None:
            s += "," + self.output_ref.hex()
        return hash(s) + hash(self.output)


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
    # The workflows in the arguments.
    workflows: "List[Workflow]"


@ray.remote
def _hash(obj: Any) -> bytes:
    m = hashlib.sha256()
    m.update(cloudpickle.dumps(obj))
    return m.digest()


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
class WorkflowStepRuntimeOptions:
    """Options that will affect a workflow step at runtime."""
    # Type of the step.
    step_type: "StepType"
    # Whether the user want to handle the exception manually.
    catch_exceptions: bool
    # The num of retry for application exception.
    max_retries: int
    # Run the workflow step inplace.
    allow_inplace: bool
    # ray_remote options
    ray_options: Dict[str, Any]

    @classmethod
    def make(cls,
             *,
             step_type,
             catch_exceptions=None,
             max_retries=None,
             allow_inplace=False,
             ray_options=None):
        if max_retries is None:
            max_retries = 3
        elif not isinstance(max_retries, int) or max_retries < 1:
            raise ValueError("max_retries should be greater or equal to 1.")
        if catch_exceptions is None:
            catch_exceptions = False
        if max_retries is None:
            max_retries = 3
        if ray_options is None:
            ray_options = {}
        elif not isinstance(ray_options, dict):
            raise ValueError("ray_options must be a dict.")
        return cls(
            step_type=step_type,
            catch_exceptions=catch_exceptions,
            max_retries=max_retries,
            allow_inplace=allow_inplace,
            ray_options=ray_options)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_type": self.step_type,
            "max_retries": self.max_retries,
            "catch_exceptions": self.catch_exceptions,
            "allow_inplace": self.allow_inplace,
            "ray_options": self.ray_options,
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]):
        return cls(
            step_type=StepType[value["step_type"]],
            max_retries=value["max_retries"],
            catch_exceptions=value["catch_exceptions"],
            allow_inplace=value["allow_inplace"],
            ray_options=value["ray_options"],
        )


@dataclass
class WorkflowData:
    # The workflow step function body.
    func_body: Callable
    # The arguments of a workflow.
    inputs: WorkflowInputs
    # name of the step
    name: str
    # Workflow step options provided by users.
    step_options: WorkflowStepRuntimeOptions
    # meta data to store
    user_metadata: Dict[str, Any]

    def to_metadata(self) -> Dict[str, Any]:
        f = self.func_body
        # TODO(suquark): "name" is the field of WorkflowData,
        # however it is not used in the metadata below.
        # "name" in the metadata is different from the "name" field.
        # Maybe rename it to avoid confusion.
        metadata = {
            "name": get_module(f) + "." + get_qualname(f),
            "workflows": [w.step_id for w in self.inputs.workflows],
            "step_options": self.step_options.to_dict(),
            "user_metadata": self.user_metadata,
        }
        return metadata


@dataclass
class WorkflowExecutionResult:
    """Dataclass for holding workflow execution result."""
    # Part of result to persist in a storage and pass to the next step.
    persisted_output: WorkflowRef
    # Part of result to return to the user but does not require persistence.
    volatile_output: WorkflowRef

    @classmethod
    def from_workflow_outputs(cls, step_id: StepID, persisted_output,
                              volatile_output) -> "WorkflowExecutionResult":
        """Convert workflow outputs to WorkflowExecutionResult."""
        return cls(
            WorkflowRef.from_workflow_output(step_id, persisted_output),
            WorkflowRef.from_workflow_output(step_id, volatile_output))

    def to_workflow_outputs(self):
        """Convert WorkflowExecutionResult to workflow outputs."""

        def _helper(x: WorkflowRef):
            if x.has_output:
                return x.output
            if x.output_ref is not None:
                return x.output_ref
            raise ValueError(
                "The workflow ref is dynamic, so we cannot return "
                "a object ref or a value.")

        return _helper(self.persisted_output), _helper(self.volatile_output)

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


T = TypeVar("T")


class Workflow(Generic[T]):
    """This class represents a workflow.

    It would either be a workflow that is not executed, or it is a reference
    to a running workflow when 'workflow.ref' is not None.
    """

    def __init__(self,
                 workflow_data: WorkflowData,
                 prepare_inputs: Optional[Callable] = None):
        if workflow_data.step_options.ray_options.get("num_returns", 1) > 1:
            raise ValueError("Workflow steps can only have one return.")
        self._data: WorkflowData = workflow_data
        self._prepare_inputs: Callable = prepare_inputs
        if self._data.inputs is None:
            assert prepare_inputs is not None
        self._executed: bool = False
        self._result: Optional[WorkflowExecutionResult] = None
        # step id will be generated during runtime
        self._step_id: StepID = None
        self._ref: Optional[WorkflowRef] = None

    @property
    def _workflow_id(self):
        from ray.workflow.workflow_context import \
            get_current_workflow_id
        return get_current_workflow_id()

    @property
    def executed(self) -> bool:
        return self._executed or (self.ref is not None
                                  and not self.ref.is_dynamic)

    @property
    def result(self) -> WorkflowExecutionResult:
        if not self.executed:
            raise Exception("The workflow has not been executed.")
        if self.ref is not None and not self.ref.is_dynamic:
            null_ref = WorkflowRef(
                step_id=self.ref.step_id, has_output=True, output=None)
            return WorkflowExecutionResult(self.ref, null_ref)
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
        if self._ref is not None:
            return self._ref.step_id

        from ray.workflow.workflow_access import \
            get_or_create_management_actor
        mgr = get_or_create_management_actor()
        self._step_id = ray.get(
            mgr.gen_step_id.remote(self._workflow_id, self._name))
        return self._step_id

    @property
    def ref(self) -> Optional[WorkflowRef]:
        """Return the workflow ref contained in the workflow."""
        return self._ref

    @classmethod
    def from_ref(cls, workflow_ref: WorkflowRef) -> "Workflow":
        """Construct a workflow from the workflow ref."""
        inputs = WorkflowInputs(args=None, workflows=[])
        data = WorkflowData(
            func_body=None,
            inputs=inputs,
            name=None,
            step_options=WorkflowStepRuntimeOptions.make(
                step_type=StepType.FUNCTION),
            user_metadata={})
        wf = Workflow(data)
        wf._ref = workflow_ref
        return wf

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
        """Serialization helper for workflow.

        By default Workflow[T] objects are not serializable, except
        it is a reference to a workflow (when workflow.ref is not 'None').
        The reference can be passed around, but the workflow must
        be processed locally so we can capture it in the DAG and
        checkpoint its inputs properly.
        """
        if self._ref is None:
            raise ValueError(
                "Workflow[T] objects are not serializable. "
                "This means they cannot be passed or returned from Ray "
                "remote, or stored in Ray objects.")
        return Workflow.from_ref, (self._ref, )

    @PublicAPI(stability="beta")
    def run(self,
            workflow_id: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None) -> Any:
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
            metadata: The metadata to add to the workflow. It has to be able
                to serialize to json.

        Returns:
            The running result.
        """
        return ray.get(self.run_async(workflow_id, metadata))

    @PublicAPI(stability="beta")
    def run_async(self,
                  workflow_id: Optional[str] = None,
                  metadata: Optional[Dict[str, Any]] = None) -> ObjectRef:
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
            metadata: The metadata to add to the workflow. It has to be able
                to serialize to json.

        Returns:
           The running result as ray.ObjectRef.

        """
        # TODO(suquark): avoid cyclic importing
        from ray.workflow.execution import run
        self._step_id = None
        return run(self, workflow_id, metadata)


@PublicAPI(stability="beta")
class WorkflowNotFoundError(Exception):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] was referenced but " \
                        "doesn't exist."
        super().__init__(self.message)


@PublicAPI(stability="beta")
class WorkflowRunningError(Exception):
    def __init__(self, operation: str, workflow_id: str):
        self.message = f"{operation} couldn't be completed becasue " \
                       f"Workflow[id={workflow_id}] is still running."
        super().__init__(self.message)
