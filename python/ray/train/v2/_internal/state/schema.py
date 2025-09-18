from enum import Enum
from typing import Dict, List, Optional

from ray._common.pydantic_compat import BaseModel, Field
from ray.dashboard.modules.job.pydantic_models import JobDetails
from ray.util.annotations import DeveloperAPI

MAX_ERROR_STACK_TRACE_LENGTH = 50000


@DeveloperAPI
class RunStatus(str, Enum):
    """Enumeration of the possible statuses for a Train run."""

    # ====== Active States ======
    # The Train run is currently in the process of initializing.
    INITIALIZING = "INITIALIZING"
    # The Train run is waiting to be scheduled.
    SCHEDULING = "SCHEDULING"
    # The Train run is currently in progress.
    RUNNING = "RUNNING"
    # The Train run is recovering from a failure or restart.
    RESTARTING = "RESTARTING"
    # The Train run is resizing.
    RESIZING = "RESIZING"

    # ===== Terminal States ======
    # The Train run completed successfully.
    FINISHED = "FINISHED"
    # The Train run failed due to an error in the training workers.
    ERRORED = "ERRORED"
    # The Train run was terminated due to system or controller errors.
    ABORTED = "ABORTED"

    def is_terminal(self) -> bool:
        return self in [RunStatus.FINISHED, RunStatus.ERRORED, RunStatus.ABORTED]


@DeveloperAPI
class RunAttemptStatus(str, Enum):
    """Enumeration of the possible statuses for a Train run attempt."""

    # ====== Active States ======
    # The run attempt is waiting to be scheduled.
    PENDING = "PENDING"
    # The run attempt is currently in progress.
    RUNNING = "RUNNING"

    # ===== Terminal States =====
    # The run attempt completed successfully.
    FINISHED = "FINISHED"
    # The run attempt failed due to an error in the training workers.
    ERRORED = "ERRORED"
    # The run attempt was terminated due to system or controller errors.
    ABORTED = "ABORTED"

    def is_terminal(self) -> bool:
        return self in [
            RunAttemptStatus.FINISHED,
            RunAttemptStatus.ERRORED,
            RunAttemptStatus.ABORTED,
        ]


@DeveloperAPI
class ActorStatus(str, Enum):
    """Enumeration of the statuses for a Train worker actor."""

    # The actor is currently active.
    ALIVE = "ALIVE"
    # The actor is no longer active.
    DEAD = "DEAD"


@DeveloperAPI
class TrainResources(BaseModel):
    """Resources allocated for a Train worker or run."""

    resources: Dict[str, float] = Field(
        description="A dictionary specifying the types and amounts of resources "
        "allocated (e.g., CPU, GPU)."
    )


@DeveloperAPI
class TrainWorker(BaseModel):
    """Metadata about a Ray Train worker."""

    world_rank: int = Field(
        description="The global rank of the worker in the training cluster."
    )
    local_rank: int = Field(description="The local rank of the worker on its node.")
    node_rank: int = Field(description="The rank of the worker's node in the cluster.")
    actor_id: str = Field(description="The unique ID of the worker's actor.")
    node_id: str = Field(
        description="The unique ID of the node where the worker is running."
    )
    node_ip: str = Field(
        description="The IP address of the node where the worker is running."
    )
    pid: int = Field(description="The process ID of the worker.")
    gpu_ids: List[int] = Field(description="A list of GPU IDs allocated to the worker.")
    status: Optional[ActorStatus] = Field(
        description="The current status of the worker actor."
    )
    resources: TrainResources = Field(
        description="The resources allocated to this Train worker."
    )
    log_file_path: Optional[str] = Field(
        description="The path to the log file for the Train worker."
    )


@DeveloperAPI
class MemoryInfo(BaseModel):
    """Memory usage information for a process."""

    rss: int = Field(description="The resident set size (RSS) memory usage in bytes.")
    vms: int = Field(description="The virtual memory size (VMS) usage in bytes.")
    pfaults: Optional[int] = Field(description="The number of page faults.")
    pageins: Optional[int] = Field(description="The number of page-ins.")


@DeveloperAPI
class ProcessStats(BaseModel):
    """CPU and memory statistics for a process."""

    cpuPercent: float = Field(description="The percentage of CPU usage.")
    mem: Optional[List[int]] = Field(
        description="Memory statistics, including total memory, free memory, "
        "and memory usage ratio."
    )
    memoryInfo: MemoryInfo = Field(description="Detailed memory usage information.")


class ProcessGPUUsage(BaseModel):
    """GPU usage statistics for a process."""

    pid: int = Field(description="The process ID.")
    gpuMemoryUsage: int = Field(description="The GPU memory usage in bytes.")


@DeveloperAPI
class GPUStats(BaseModel):
    """Statistics for a GPU."""

    uuid: str = Field(description="The unique identifier of the GPU.")
    index: int = Field(description="The index of the GPU.")
    name: str = Field(description="The name of the GPU.")
    utilizationGpu: Optional[float] = Field(
        description="The percentage utilization of the GPU."
    )
    memoryUsed: float = Field(description="The amount of GPU memory used in bytes.")
    memoryTotal: float = Field(description="The total amount of GPU memory in bytes.")
    processInfo: ProcessGPUUsage = Field(
        description="GPU usage statistics for the associated process."
    )


@DeveloperAPI
class DecoratedTrainWorker(TrainWorker):
    """Detailed metadata for a Ray Train worker, including process and GPU stats."""

    processStats: Optional[ProcessStats] = Field(
        None, description="CPU and memory statistics for the worker process."
    )
    gpus: List[GPUStats] = Field(
        default_factory=list,
        description="A list of GPUs used by the worker process,"
        " with detailed statistics.",
    )


@DeveloperAPI
class TrainRunAttempt(BaseModel):
    """Metadata for an individual attempt to execute a Train run."""

    run_id: str = Field(description="Unique identifier for the parent Train run.")
    attempt_id: str = Field(
        description="Unique identifier for this specific Train run attempt."
    )
    status: RunAttemptStatus = Field(
        description="The current execution status of the Train run attempt."
    )
    status_detail: Optional[str] = Field(
        description="Additional details about the status,"
        " including error messages if applicable."
    )
    start_time_ns: int = Field(
        description="The UNIX timestamp (in nanoseconds)"
        " when the Train run attempt started."
    )
    end_time_ns: Optional[int] = Field(
        description="The UNIX timestamp (in nanoseconds)"
        " when the Train run attempt ended. "
        "If null, the attempt is still ongoing."
    )
    resources: List[TrainResources] = Field(
        description="The resources (e.g., CPU, GPU) allocated to the Train run attempt."
    )
    workers: List[TrainWorker] = Field(
        description="List of Train workers participating in this attempt, "
        "sorted by global ranks."
    )


@DeveloperAPI
class DecoratedTrainRunAttempt(TrainRunAttempt):
    """Detailed metadata for a Train run attempt, including decorated worker data."""

    workers: List[DecoratedTrainWorker] = Field(
        description="A list of Train workers with detailed statistics, "
        "sorted by global ranks."
    )


@DeveloperAPI
class TrainRun(BaseModel):
    """Metadata for a Ray Train run, including its details and status."""

    id: str = Field(description="Unique identifier for the Train run.")
    name: str = Field(description="Human-readable name assigned to the Train run.")
    job_id: str = Field(description="The Ray Job ID associated with this Train run.")
    controller_actor_id: str = Field(
        description="Unique ID of the actor managing the Train run."
    )
    status: RunStatus = Field(
        description="The current execution status of the Train run."
    )
    status_detail: Optional[str] = Field(
        description="Additional details about the current status, "
        "including error messages if applicable."
    )
    start_time_ns: int = Field(
        description="The UNIX timestamp (in nanoseconds) when the Train run started."
    )
    end_time_ns: Optional[int] = Field(
        description="The UNIX timestamp (in nanoseconds) when the Train run ended. "
        "If null, the run is still in progress."
    )
    controller_log_file_path: Optional[str] = Field(
        description="The path to the log file for the Train run controller."
    )


@DeveloperAPI
class DecoratedTrainRun(TrainRun):
    """Detailed metadata for a Ray Train run, including attempts and job details."""

    attempts: List[DecoratedTrainRunAttempt] = Field(
        description="A list of attempts made to execute the Train run."
    )
    job_details: Optional[JobDetails] = Field(
        None,
        description="Detailed information about the job that initiated this Train run.",
    )


@DeveloperAPI
class TrainRunsResponse(BaseModel):
    """Response containing a list of decorated Train runs."""

    train_runs: List[DecoratedTrainRun] = Field(
        description="A list of Train runs with detailed metadata."
    )
