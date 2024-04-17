import threading
from typing import List, Optional

from pydantic import BaseModel, Field

import ray
from ray.util.annotations import DeveloperAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@DeveloperAPI
class TrainWorkerInfo(BaseModel):
    """Metadata of a Ray Train worker."""

    actor_id: str = Field(description="Actor ID of the worker.")
    world_rank: int = Field(description="World rank.")
    local_rank: int = Field(description="Local rank.")
    node_rank: int = Field(description="Node rank.")
    node_id: str = Field(description="ID of the node that the worker is running on.")
    node_ip: str = Field(
        description="IP address of the node that the worker is running on."
    )
    pid: str = Field(description="PID of the worker.")
    gpu_ids: Optional[List[str]] = Field(
        description="A list of GPU ids allocated to that worker."
    )


@DeveloperAPI
class TrainRunInfo(BaseModel):
    """Metadata for a Ray Train run and information about its workers."""

    id: str = Field(description="The unique identifier for each Train run.")
    name: str = Field(description="The name of the Train run.")
    trial_name: Optional[str] = Field(
        description=(
            "Trial name. It should be different among different Train runs, "
            "except for those that are restored from checkpoints."
        )
    )
    job_id: str = Field(description="The Ray Job ID.")
    trainer_actor_id: str = Field(description="Actor Id of the Trainer.")
    workers: List[TrainWorkerInfo] = Field(
        description="A List of Train workers sorted by global ranks."
    )
    dataset_ids: Optional[List[str]] = Field(
        description="A List of dataset ids for this Train run."
    )


# Creating/getting an actor from multiple threads is not safe.
# https://github.com/ray-project/ray/issues/41324
_stats_actor_lock: threading.RLock = threading.RLock()


@ray.remote(num_cpus=0)
class _StatsActor:
    def __init__(self):
        # Generate some made-up trainruninfo data.
        self._train_run_info = [
            TrainRunInfo(
                id="1",
                name="train_run_1",
                trial_name="trial_1",
                job_id="job_1",
                trainer_actor_id="trainer_actor_1",
                workers=[
                    TrainWorkerInfo(
                        actor_id="worker_actor_1",
                        world_rank=0,
                        local_rank=0,
                        node_rank=0,
                        node_id="node_1",
                        node_ip="123.1412.123.123",
                        pid="1234",
                        gpu_ids=["0"],
                    ),
                    TrainWorkerInfo(
                        actor_id="worker_actor_2",
                        world_rank=1,
                        local_rank=1,
                        node_rank=1,
                        node_id="node_2",
                        node_ip="123.1412.123.124",
                        pid="1235",
                        gpu_ids=["1"],
                    ),
                ],
                dataset_ids=["1", "2"],
            ),
            TrainRunInfo(
                id="2",
                name="train_run_2",
                trial_name="trial_2",
                job_id="job_2",
                trainer_actor_id="trainer_actor_2",
                workers=[
                    TrainWorkerInfo(
                        actor_id="worker_actor_3",
                        world_rank=0,
                        local_rank=0,
                        node_rank=0,
                        node_id="node_1",
                        node_ip="123.1412.123.123",
                        pid="1236",
                        gpu_ids=["0"],
                    ),
                    TrainWorkerInfo(
                        actor_id="worker_actor_4",
                        world_rank=1,
                        local_rank=1,
                        node_rank=1,
                        node_id="node_2",
                        node_ip="123.1412.123.124",
                        pid="1237",
                        gpu_ids=["1"],
                    ),
                ],
                dataset_ids=["3", "4"],
            ),
        ]

    def get_train_runs(self) -> List[TrainRunInfo]:
        return self._train_run_info


def _get_or_create_stats_actor():
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
    with _stats_actor_lock:
        return _StatsActor.options(
            name="experimental-train-actor-name",
            namespace="experimental-train-actor-namespace",
            get_if_exists=True,
            lifetime="detached",
            scheduling_strategy=scheduling_strategy,
        ).remote()
