import logging
import os
import threading
from typing import Dict, Optional

import ray
from ray.data import Dataset
from ray.train._internal.schema import TrainDatasetInfo, TrainRunInfo, TrainWorkerInfo
from ray.train._internal.utils import check_for_failure
from ray.train._internal.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


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


def get_or_launch_stats_actor():
    """Create or launch a `TrainStatsActor` on the head node."""
    with _stats_actor_lock:
        return TrainStatsActor.options(
            name=TRAIN_STATS_ACTOR_NAME,
            namespace=TRAIN_STATS_ACTOR_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
            resources={"node:__internal_head__": 0.001},
        ).remote()


class TrainRunStatsManager:
    """A class that aggregates and reports train run info to TrainStatsActor.

    This manager class is created on the train controller layer for each run.
    """

    def __init__(self) -> None:
        self.stats_actor = get_or_launch_stats_actor()

    def register_train_run(
        self,
        run_id: str,
        run_name: str,
        trial_name: str,
        trainer_actor_id: str,
        datasets: Dict[str, Dataset],
        worker_group: WorkerGroup,
    ) -> None:
        """Collect Train Run Info and report to StatsActor."""

        def collect_train_worker_info():
            train_context = ray.train.get_context()
            core_context = ray.runtime_context.get_runtime_context()

            return TrainWorkerInfo(
                world_rank=train_context.get_world_rank(),
                local_rank=train_context.get_local_rank(),
                node_rank=train_context.get_node_rank(),
                actor_id=core_context.get_actor_id(),
                node_id=core_context.get_node_id(),
                node_ip=core_context.get_node_ip_address(),
                gpu_ids=core_context.get_accelerator_ids().get("GPU", []),
                pid=os.getpid(),
            )

        futures = [
            worker_group.execute_single_async(index, collect_train_worker_info)
            for index in range(len(worker_group))
        ]
        success, exception = check_for_failure(futures)

        if not success:
            logger.warning("Failed to collect infomation for Ray Train Worker.")
            return

        worker_info_list = ray.get(futures)
        worker_info_list = sorted(worker_info_list, key=lambda info: info.world_rank)

        dataset_info_list = [
            TrainDatasetInfo(
                name=ds_name,
                plan_name=ds._plan._dataset_name,
                plan_uuid=ds._plan._dataset_uuid,
            )
            for ds_name, ds in datasets.items()
        ]

        train_run_info = TrainRunInfo(
            id=run_id,
            name=run_name,
            trial_name=trial_name,
            trainer_actor_id=trainer_actor_id,
            workers=worker_info_list,
            datasets=dataset_info_list,
        )

        self.stats_actor.register_train_run.remote(train_run_info)
