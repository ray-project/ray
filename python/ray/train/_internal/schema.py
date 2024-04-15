from typing import List, Optional

from ray._private.pydantic_compat import BaseModel, Field
from ray.util.annotations import DeveloperAPI

try:
    import pydantic  # noqa: F401
except ImportError:
    raise ModuleNotFoundError(
        "pydantic isn't installed. "
        "To install pydantic, please run 'pip install pydantic'"
    )


@DeveloperAPI
class TrainWorkerInfo(BaseModel):
    """Metadata of a Ray Train worker."""

    actor_id: str = Field(description="Actor ID of the worker.")
    world_rank: int = Field(description="World rank.")
    local_rank: int = Field(description="Local rank.")
    node_rank: int = Field(description="Node rank.")
    gpu_ids: Optional[List[str]] = Field(
        description="A list of GPU ids allocated to that worker."
    )
    node_id: Optional[str] = Field(
        description="ID of the node that the worker is running on."
    )
    node_ip: Optional[str] = Field(
        description="IP address of the node that the worker is running on."
    )
    pid: Optional[str] = Field(description="PID of the worker.")


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
    job_id: str = Field(description="Ray Job ID.")
    trial_name: str = Field(
        description=(
            "Trial name. It should be different among different Train runs, "
            "except for those that are restored from checkpoints."
        )
    )
    trainer_actor_id: str = Field(description="Actor Id of the Trainer.")
    workers: List[TrainWorkerInfo] = Field(
        description="A List of Train workers sorted by global ranks."
    )
    datasets: List[TrainDatasetInfo] = Field(
        description="A List of dataset info for this Train run."
    )
