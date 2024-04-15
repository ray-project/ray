import threading
from typing import Dict, Optional

import ray
from ray.train._internal.schema import TrainRunInfo


@ray.remote(num_cpus=0)
class TrainStatsActor:
    def __init__(self):
        self.train_runs = dict()

    def register_train_run(self, run_info: TrainRunInfo):
        # Register a new train run.
        self.train_runs[run_info.id] = run_info

    def get_train_run(self, run_id: str) -> Optional[TrainRunInfo]:
        # Retrieve a registered run with its id
        return self.train_runs.get(run_id, None)

    def get_all_train_runs(self) -> Dict[str, TrainRunInfo]:
        # Retrieve all registered train runs
        return self.train_runs


TRAIN_STATS_ACTOR_NAME = "train_stats_actor"
TRAIN_STATS_ACTOR_NAMESPACE = "_train_stats_actor"

_stats_actor_lock: threading.RLock = threading.RLock()


def get_or_create_stats_actor():
    with _stats_actor_lock:
        return TrainStatsActor.options(
            name=TRAIN_STATS_ACTOR_NAME,
            namespace=TRAIN_STATS_ACTOR_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
            resources={"node:__internal_head__": 0.001},
        ).remote()
