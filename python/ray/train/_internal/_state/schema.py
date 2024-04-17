from typing import List

from ray._private.pydantic_compat import BaseModel, Field
from ray.util.annotations import DeveloperAPI


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
    pid: str = Field(description="Process ID of the worker.")
    gpu_ids: List[int] = Field(
        description="A list of GPU ids allocated to that worker."
    )


@DeveloperAPI
class TrainDatasetInfo(BaseModel):
    name: str = Field(
        description="The key of the dataset dict specified in Ray Train Trainer."
    )
    plan_name: str = Field(description="The name of the internal dataset plan.")
    plan_uuid: str = Field(description="The uuid of the internal dataset plan.")


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
