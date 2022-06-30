import base64
import asyncio
import json

from ray import cloudpickle
from enum import Enum, unique
import hashlib
from typing import Dict, Optional, Any

from dataclasses import dataclass

import ray
from ray import ObjectRef
from ray.util.annotations import PublicAPI

# Alias types
Event = Any
StepID = str
WorkflowOutputType = ObjectRef

MANAGEMENT_ACTOR_NAMESPACE = "workflow"
MANAGEMENT_ACTOR_NAME = "WorkflowManagementActor"
STORAGE_ACTOR_NAME = "StorageManagementActor"
WORKFLOW_OPTIONS = "workflow.io/options"


def asyncio_run(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def validate_user_metadata(metadata):
    if metadata is not None:
        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a dict.")
        try:
            json.dumps(metadata)
        except TypeError as e:
            raise ValueError(
                "metadata must be JSON serializable, instead, "
                "we got 'TypeError: {}'".format(e)
            )


@dataclass
class WorkflowRef:
    """This class represents a reference of a workflow output.

    A reference means the workflow has already been executed,
    and we have both the workflow step ID and the object ref to it
    living outputs.

    This could be used when you want to return a running workflow
    from a workflow step. For example, the remaining workflows
    returned by 'workflow.wait' contains a static ref to these
    pending workflows.
    """

    # The ID of the step that produces the output of the workflow.
    task_id: StepID
    # The ObjectRef of the output. If it is "None", then the output has been
    # saved in the storage, and we need to check the workflow management actor
    # for the object ref.
    ref: Optional[ObjectRef] = None

    @classmethod
    def from_output(cls, task_id: str, output: Any):
        """Create static ref from given output."""
        if not isinstance(output, cls):
            if not isinstance(output, ray.ObjectRef):
                output = ray.put(output)
            output = cls(task_id=task_id, ref=output)
        return output

    def __hash__(self):
        return hash(self.task_id)


@PublicAPI(stability="beta")
@unique
class WorkflowStatus(str, Enum):
    # No status is set for this workflow.
    NONE = "NONE"
    # There is at least a remote task running in ray cluster
    RUNNING = "RUNNING"
    # It got canceled and can't be resumed later.
    CANCELED = "CANCELED"
    # The workflow runs successfully.
    SUCCESSFUL = "SUCCESSFUL"
    # The workflow failed with an application error.
    # It can be resumed.
    FAILED = "FAILED"
    # The workflow failed with a system error, i.e., ray shutdown.
    # It can be resumed.
    RESUMABLE = "RESUMABLE"


@unique
class StepType(str, Enum):
    """All step types."""

    FUNCTION = "FUNCTION"
    WAIT = "WAIT"


CheckpointModeType = bool


@unique
class CheckpointMode(Enum):
    """All checkpoint modes."""

    # Keep the checkpoint of the workflow step.
    SYNC = True
    # Skip the checkpoint of the workflow step.
    SKIP = False


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
    # Checkpoint mode.
    checkpoint: CheckpointModeType
    # ray_remote options
    ray_options: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_type": self.step_type,
            "max_retries": self.max_retries,
            "catch_exceptions": self.catch_exceptions,
            "allow_inplace": self.allow_inplace,
            "checkpoint": self.checkpoint,
            "ray_options": self.ray_options,
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]):
        return cls(
            step_type=StepType[value["step_type"]],
            max_retries=value["max_retries"],
            catch_exceptions=value["catch_exceptions"],
            allow_inplace=value["allow_inplace"],
            checkpoint=value["checkpoint"],
            ray_options=value["ray_options"],
        )


@dataclass
class WorkflowExecutionMetadata:
    """Dataclass for the metadata of the workflow execution."""

    # True if the workflow task returns a workflow DAG.
    is_output_workflow: bool = False


@dataclass
class WorkflowMetaData:
    # The current status of the workflow
    status: WorkflowStatus


@PublicAPI(stability="beta")
class WorkflowNotFoundError(Exception):
    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] was referenced but doesn't exist."
        super().__init__(self.message)


@PublicAPI(stability="beta")
class WorkflowRunningError(Exception):
    def __init__(self, operation: str, workflow_id: str):
        self.message = (
            f"{operation} couldn't be completed becasue "
            f"Workflow[id={workflow_id}] is still running."
        )
        super().__init__(self.message)
