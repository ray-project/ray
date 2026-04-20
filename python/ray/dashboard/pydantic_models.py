from enum import Enum
from typing import Any, Dict, List, Optional, Union

from ray._common.pydantic_compat import PYDANTIC_INSTALLED, BaseModel, Field
from ray.dashboard.modules.job.common import JobStatus
from ray.util.annotations import PublicAPI

if PYDANTIC_INSTALLED:

    class RayletSchema(BaseModel):
        """
        Pydantic schema matching the frontend Raylet TypeScript interface.
        Ensures backend changes don't break the UI or History Server.
        """

        nodeId: str = Field(..., description="Unique ID of the node")
        state: str = Field(..., description="Node state (e.g., ALIVE, DEAD)")
        stateMessage: Optional[str] = Field(
            None, description="Optional reason for state"
        )

        numWorkers: int = Field(..., description="Number of workers")
        pid: int = Field(..., description="Process ID of raylet")
        nodeManagerPort: int = Field(..., description="Port of node manager")
        startTime: int = Field(..., description="Start time timestamp")
        terminateTime: Optional[int] = Field(
            None, description="Terminate time timestamp"
        )
        objectStoreAvailableMemory: int = Field(
            ..., description="Available object store memory"
        )
        objectStoreUsedMemory: int = Field(..., description="Used object store memory")
        isHeadNode: bool = Field(..., description="Whether this is the head node")

        labels: Dict[str, str] = Field(
            default_factory=dict, description="Labels associated with the node"
        )

    class NodeDetailSchema(BaseModel):
        """
        Schema for the top-level node object returned by the API.
        Matches structure expected by frontend NodeDetail type.
        """

        hostname: str = Field(..., description="Hostname of the node")
        ip: str = Field(..., description="IP address of the node")
        cpu: float = Field(..., description="CPU usage percentage")
        raylet: RayletSchema = Field(..., description="Enforced raylet schema")

    class AddressSchema(BaseModel):
        nodeId: str
        ipAddress: str
        port: int
        workerId: str

    class ActorSchema(BaseModel):
        """
        Pydantic schema matching the frontend Actor TypeScript interface.
        """

        actorId: str
        jobId: str
        placementGroupId: Optional[str] = None
        state: str
        pid: Optional[int] = None
        address: AddressSchema
        name: str
        numRestarts: str
        actorClass: str
        startTime: Optional[int] = None
        endTime: Optional[int] = None
        requiredResources: Dict[str, float] = Field(default_factory=dict)
        exitDetail: str
        reprName: str
        callSite: Optional[str] = None
        labelSelector: Optional[Dict[str, str]] = None

    class BundleSchema(BaseModel):
        bundle_id: str
        node_id: Optional[str] = None
        unit_resources: Dict[str, float] = Field(default_factory=dict)
        label_selector: Optional[Dict[str, str]] = None

    class PlacementGroupSchema(BaseModel):
        """
        Pydantic schema matching the frontend PlacementGroup TypeScript interface.
        """

        placement_group_id: str
        name: str
        creator_job_id: str
        state: str
        stats: Optional[Dict[str, Union[int, float, str]]] = None
        bundles: List[BundleSchema] = Field(default_factory=list)

    class TaskSchema(BaseModel):
        """
        Pydantic schema matching the TaskState dataclass.
        """

        task_id: str
        attempt_number: int
        name: str
        state: str
        job_id: str
        actor_id: Optional[str] = None
        type: str
        func_or_class_name: str
        parent_task_id: str
        node_id: Optional[str] = None
        worker_id: Optional[str] = None
        worker_pid: Optional[int] = None
        error_type: Optional[str] = None
        language: Optional[str] = None
        required_resources: Optional[Dict[str, float]] = None
        runtime_env_info: Optional[Dict[str, Any]] = None
        placement_group_id: Optional[str] = None
        events: Optional[List[Dict[str, Any]]] = None
        profiling_data: Optional[Dict[str, Any]] = None
        creation_time_ms: Optional[int] = None
        start_time_ms: Optional[int] = None
        end_time_ms: Optional[int] = None
        task_log_info: Optional[Dict[str, Any]] = None
        error_message: Optional[str] = None
        is_debugger_paused: Optional[bool] = None
        call_site: Optional[str] = None
        label_selector: Optional[Dict[str, str]] = None
        fallback_strategy: Optional[Dict[str, Any]] = None

    @PublicAPI(stability="beta")
    class DriverInfo(BaseModel):
        id: str = Field(..., description="The id of the driver")
        node_ip_address: str = Field(
            ..., description="The IP address of the node the driver is running on."
        )
        pid: str = Field(
            ..., description="The PID of the worker process the driver is using."
        )

    @PublicAPI(stability="beta")
    class JobType(str, Enum):
        SUBMISSION = "SUBMISSION"
        DRIVER = "DRIVER"

    @PublicAPI(stability="beta")
    class JobDetails(BaseModel):
        type: JobType = Field(..., description="The type of job.")
        job_id: Optional[str] = Field(None, description="The job ID.")
        submission_id: Optional[str] = Field(None, description="A submission ID.")
        driver_info: Optional[DriverInfo] = Field(
            None, description="The driver related to this job."
        )
        status: JobStatus = Field(..., description="The status of the job.")
        entrypoint: str = Field(..., description="The entrypoint command for this job.")
        message: Optional[str] = Field(
            None, description="A message describing the status in more detail."
        )
        error_type: Optional[str] = Field(
            None, description="Internal error or user script error."
        )
        start_time: Optional[int] = Field(
            None, description="The time when the job was started."
        )
        end_time: Optional[int] = Field(
            None, description="The time when the job moved into a terminal state."
        )
        metadata: Optional[Dict[str, str]] = Field(
            None, description="Arbitrary user-provided metadata for the job."
        )
        runtime_env: Optional[Dict[str, Any]] = Field(
            None, description="The runtime environment for the job."
        )
        driver_agent_http_address: Optional[str] = Field(
            None, description="The HTTP address of the JobAgent."
        )
        driver_node_id: Optional[str] = Field(
            None,
            description="The ID of the node the job entrypoint command is running on.",
        )
        driver_exit_code: Optional[int] = Field(
            None, description="The driver process exit code."
        )

else:
    # Fallbacks for minimal install
    RayletSchema = None
    NodeDetailSchema = None
    AddressSchema = None
    ActorSchema = None
    BundleSchema = None
    PlacementGroupSchema = None
    TaskSchema = None
    DriverInfo = None
    JobType = None
    JobDetails = None
