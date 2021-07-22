from dataclasses import dataclass
from typing import Optional

from ray.util.sgd.v2.worker_placement_strategy import WorkerPlacementStrategy


@dataclass
class WorkerConfig:
    num_workers: int
    num_cpus_per_worker: int = 1
    num_gpus_per_worker: int = 0
    placement_strategy: Optional[WorkerPlacementStrategy] = None
    timeout_s: int = 60  # TODO fix this.
