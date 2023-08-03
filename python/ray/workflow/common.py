import base64
import json

from ray import cloudpickle
from enum import Enum, unique
import hashlib
from typing import Dict, Optional, Any, Tuple

from dataclasses import dataclass

import ray
from ray import ObjectRef
from ray._private.utils import get_or_create_event_loop
from ray.util.annotations import PublicAPI

# Alias types
Event = Any
TaskID = str
WorkflowOutputType = ObjectRef

MANAGEMENT_ACTOR_NAMESPACE = "workflow"
MANAGEMENT_ACTOR_NAME = "WorkflowManagementActor"
HTTP_EVENT_PROVIDER_NAME = "WorkflowHttpEventProvider"
STORAGE_ACTOR_NAME = "StorageManagementActor"
WORKFLOW_OPTIONS = "workflow.io/options"


def asyncio_run(coro):
    return get_or_create_event_loop().run_until_complete(coro)


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
    and we have both the workflow task ID and the object ref to it
    living outputs.

    This could be used when you want to return a running workflow
    from a workflow task. For example, the remaining workflows
    returned by 'workflow.wait' contains a static ref to these
    pending workflows.
    """

    # The ID of the task that produces the output of the workflow.
    task_id: TaskID
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


@PublicAPI(stability="alpha")
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
    # The workflow is queued and waited to be executed.
    PENDING = "PENDING"

    @classmethod
    def non_terminating_status(cls) -> "Tuple[WorkflowStatus, ...]":
        return cls.RUNNING, cls.PENDING


@unique
class TaskType(str, Enum):
    """All task types."""

    FUNCTION = "FUNCTION"
    WAIT = "WAIT"


CheckpointModeType = bool


@unique
class CheckpointMode(Enum):
    """All checkpoint modes."""

    # Keep the checkpoint of the workflow task.
    SYNC = True
    # Skip the checkpoint of the workflow task.
    SKIP = False


@ray.remote
def _hash(obj: Any) -> bytes:
    m = hashlib.sha256()
    m.update(cloudpickle.dumps(obj))
    return m.digest()


@ray.remote
def calculate_identifier(obj: Any) -> str:
    """Calculate a url-safe identifier for an object."""

    # Task 1: Serialize the object.
    # Task 2: Calculate its sha256 hash.
    # Task 3: Get the url safe, base64 representation of it.

    # TODO (Alex): Ideally we should use the existing ObjectRef serializer to
    # avoid duplicate serialization passes and support nested object refs.
    m = hashlib.sha256()
    m.update(cloudpickle.dumps(obj))
    hash = m.digest()
    encoded = base64.urlsafe_b64encode(hash).decode("ascii")
    return encoded


@dataclass
class WorkflowTaskRuntimeOptions:
    """Options that will affect a workflow task at runtime."""

    # Type of the task.
    task_type: "TaskType"
    # Whether the user want to handle the exception manually.
    catch_exceptions: bool
    # Whether application-level errors should be retried.
    retry_exceptions: bool
    # The num of retry for application exceptions & system failures.
    max_retries: int
    # Checkpoint mode.
    checkpoint: CheckpointModeType
    # ray_remote options
    ray_options: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_type": self.task_type,
            "max_retries": self.max_retries,
            "catch_exceptions": self.catch_exceptions,
            "retry_exceptions": self.retry_exceptions,
            "checkpoint": self.checkpoint,
            "ray_options": self.ray_options,
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]):
        return cls(
            task_type=TaskType[value["task_type"]],
            max_retries=value["max_retries"],
            catch_exceptions=value["catch_exceptions"],
            retry_exceptions=value["retry_exceptions"],
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
