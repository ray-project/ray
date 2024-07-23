from enum import Enum
from typing import List, Optional

from ray._private.pydantic_compat import BaseModel, Field
from ray.dashboard.modules.job.pydantic_models import JobDetails
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class RunStatusEnum(str, Enum):
    """Enumeration for the status of a train run."""

    # The train run has started
    STARTED = "STARTED"
    # The train run was terminated as expected
    FINISHED = "FINISHED"
    # The train run was terminated early due to errors in the training function
    ERRORED = "ERRORED"
    # The train run was terminated early due to system errors or controller errors
    ABORTED = "ABORTED"


@DeveloperAPI
class ActorStatusEnum(str, Enum):
    DEAD = "DEAD"
    ALIVE = "ALIVE"


@DeveloperAPI
class TrainWorkerInfo(BaseModel):
    """Metadata of a Ray Train worker."""

    actor_id: str = Field(description="Actor ID of the worker.")
    world_rank: int = Field(description="World rank of the worker.")
    local_rank: int = Field(description="Local rank of the worker.")
    node_rank: int = Field(description="Node rank of the worker.")
    node_id: str = Field(description="ID of the node that the worker is running on.")
    node_ip: str = Field(
        description="IP address of the node that the worker is running on."
    )
    pid: int = Field(description="Process ID of the worker.")
    gpu_ids: List[int] = Field(
        description="A list of GPU ids allocated to that worker."
    )
    status: Optional[ActorStatusEnum] = Field(
        description="The status of the train worker actor. It can be ALIVE or DEAD."
    )


@DeveloperAPI
class TrainDatasetInfo(BaseModel):
    name: str = Field(
        description="The key of the dataset dict specified in Ray Train Trainer."
    )
    dataset_uuid: str = Field(description="The uuid of the dataset.")
    dataset_name: Optional[str] = Field(description="The name of the dataset.")


@DeveloperAPI
class TrainRunInfo(BaseModel):
    """Metadata for a Ray Train run and information about its workers."""

    name: str = Field(description="The name of the Train run.")
    id: str = Field(description="The unique identifier for each Train run.")
    job_id: str = Field(description="The Ray Job ID.")
    controller_actor_id: str = Field(description="Actor Id of the Train controller.")
    workers: List[TrainWorkerInfo] = Field(
        description="A List of Train workers sorted by global ranks."
    )
    datasets: List[TrainDatasetInfo] = Field(
        description="A List of dataset info for this Train run."
    )
    run_status: RunStatusEnum = Field(
        description="The current status of the train run. It can be one of the "
        "following: STARTED, FINISHED, ERRORED, or ABORTED."
    )
    status_detail: str = Field(
        description="Detailed information about the current run status, "
        "such as error messages."
    )
    start_time_ms: int = Field(
        description="The UNIX timestamp of the start time of this Train run."
    )
    end_time_ms: Optional[int] = Field(
        description="The UNIX timestamp of the end time of this Train run. "
        "If null, the Train run has not ended yet."
    )


@DeveloperAPI
class TrainRunInfoWithDetails(TrainRunInfo):
    """Metadata for a Ray Train run and information about its workers."""

    job_details: Optional[JobDetails] = Field(
        None, description="Details of the job that started this Train run."
    )


@DeveloperAPI
class TrainRunsResponse(BaseModel):
    train_runs: List[TrainRunInfoWithDetails]
