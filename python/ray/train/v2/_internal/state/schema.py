from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from ray._common.pydantic_compat import BaseModel, Field
from ray.dashboard.modules.job.pydantic_models import JobDetails
from ray.train.v2._internal.util import TrainingFramework
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
class DataConfig(BaseModel):
    """Configuration for dataset splitting and execution options within Ray Train."""

    datasets_to_split: Union[Literal["all"], List[str]] = Field(
        description="Which datasets to split; either 'all' or a list of dataset names.",
    )
    execution_options: Optional[Dict] = Field(description="Data execution options")
    enable_shard_locality: bool = Field(
        description="Whether to enable shard locality optimization."
    )


@DeveloperAPI
class ScalingConfig(BaseModel):
    """Scaling config for a Train run."""

    num_workers: int = Field(description="The number of workers for the Train run.")
    use_gpu: bool = Field(description="Whether to use GPUs for the Train run.")
    resources_per_worker: Optional[Dict[str, float]] = Field(
        description="The resources per worker for a Train run."
    )
    placement_strategy: str = Field(
        description="The placement strategy for the Train run."
    )
    accelerator_type: Optional[str] = Field(
        description="The accelerator type for the Train run."
    )
    use_tpu: bool = Field(description="Whether to use TPUs for the Train run.")
    topology: Optional[str] = Field(description="The topology for the Train run.")
    bundle_label_selector: Optional[
        Union[Dict[str, str], List[Dict[str, str]]]
    ] = Field(description="The bundle label selector for the Train run.")


@DeveloperAPI
class FailureConfig(BaseModel):
    """Failure config for a Train run."""

    max_failures: int = Field(
        description="The maximum number of failures for a Train run."
    )
    controller_failure_limit: int = Field(
        description="The maximum number of controller failures to tolerate."
    )


@DeveloperAPI
class CheckpointConfig(BaseModel):
    """Checkpoint config for a Train run."""

    num_to_keep: Optional[int] = Field(
        description="The number of most recent checkpoints to keep. Older checkpoints may be deleted.",
    )
    checkpoint_score_attribute: Optional[str] = Field(
        description="Attribute used to score and rank checkpoints; can be a metric key or attribute.",
    )
    checkpoint_score_order: Literal["max", "min"] = Field(
        description="Order to rank checkpoint scores, 'max' for higher-is-better, 'min' for lower-is-better.",
    )


@DeveloperAPI
class RunConfig(BaseModel):
    """Run configuration parameters for a Train run, encompassing failure,
    runtime environment, checkpoint settings, and storage path."""

    failure_config: FailureConfig = Field(
        description="The failure config for a Train run."
    )
    worker_runtime_env: Dict[str, Any] = Field(
        description="The worker runtime env for a Train run."
    )
    checkpoint_config: CheckpointConfig = Field(
        description="The checkpoint config for a Train run."
    )
    storage_path: str = Field(description="The storage path for a Train run.")


@DeveloperAPI
class BackendConfig(BaseModel):
    """Backend config for a Train run."""

    framework: Optional[TrainingFramework] = Field(
        description="The training framework for this backend config."
    )
    config: Dict[str, Any] = Field(
        description="Training framework-specific configuration fields."
    )


@DeveloperAPI
class RunSettings(BaseModel):
    """Settings for a Train run, primarily consisting of configs set before a train run starts.

    This includes the train loop config, backend config, scaling config, dataset configs,
    and runtime configuration.
    """

    train_loop_config: Optional[Dict] = Field(
        description="The user defined train loop config for a Train run."
    )
    backend_config: BackendConfig = Field(
        description="The backend config for a Train run. Can vary with the framework (e.g. TorchConfig)"
    )
    scaling_config: ScalingConfig = Field(
        description="The scaling config for this Train run."
    )
    datasets: List[str] = Field(
        description="A list of dataset names for a Train run.",
    )
    data_config: DataConfig = Field(
        description="The data config for a Train run.",
    )
    run_config: RunConfig = Field(
        description="Run configuration for this Train run, including failure, runtime environment, checkpoint settings, and storage path."
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
    framework_versions: Dict[str, str] = Field(
        description="The relevant framework versions for this Train run,"
        "including the Ray version and training framework version."
    )
    run_settings: RunSettings = Field(
        description="The run settings for this Train run, including train loop config, "
        "backend config, scaling config, dataset details, and runtime configuration."
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
