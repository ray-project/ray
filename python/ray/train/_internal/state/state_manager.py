import logging
import os
from typing import Dict

import ray
from ray.data import Dataset
from ray.train._internal.state.schema import (
    TrainDatasetInfo,
    TrainRunInfo,
    TrainWorkerInfo,
)
from ray.train._internal.state.state_actor import (
    TRAIN_STATE_ACTOR_NAME,
    TRAIN_STATE_ACTOR_NAMESPACE,
)
from ray.train._internal.utils import check_for_failure
from ray.train._internal.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class TrainRunStateManager:
    """A class that aggregates and reports train run info to TrainStateActor.

    This manager class is created on the train controller layer for each run.
    """

    def __init__(self, worker_group: WorkerGroup) -> None:
        self.state_actor = ray.get_actor(
            name=TRAIN_STATE_ACTOR_NAME, namespace=TRAIN_STATE_ACTOR_NAMESPACE
        )
        self.worker_group = worker_group

    def register_train_run(
        self,
        run_id: str,
        job_id: str,
        run_name: str,
        controller_actor_id: str,
        datasets: Dict[str, Dataset],
    ) -> None:
        """Collect Train Run Info and report to StateActor."""

        def collect_train_worker_info():
            train_context = ray.train.get_context()
            core_context = ray.runtime_context.get_runtime_context()

            return TrainWorkerInfo(
                world_rank=train_context.get_world_rank(),
                local_rank=train_context.get_local_rank(),
                node_rank=train_context.get_node_rank(),
                actor_id=core_context.get_actor_id(),
                node_id=core_context.get_node_id(),
                node_ip=ray.util.get_node_ip_address(),
                gpu_ids=ray.get_gpu_ids(),
                pid=os.getpid(),
            )

        futures = [
            self.worker_group.execute_single_async(index, collect_train_worker_info)
            for index in range(len(self.worker_group))
        ]
        success, exception = check_for_failure(futures)

        if not success:
            logger.error(
                "Failed to collect run information from the Ray Train "
                f"workers:\n{exception}"
            )
            return

        worker_info_list = ray.get(futures)
        worker_info_list = sorted(worker_info_list, key=lambda info: info.world_rank)

        dataset_info_list = [
            TrainDatasetInfo(
                name=ds_name,
                dataset_name=ds._plan._dataset_name,
                dataset_uuid=ds._plan._dataset_uuid,
            )
            for ds_name, ds in datasets.items()
        ]

        train_run_info = TrainRunInfo(
            id=run_id,
            job_id=job_id,
            name=run_name,
            controller_actor_id=controller_actor_id,
            workers=worker_info_list,
            datasets=dataset_info_list,
        )

        ray.get(self.state_actor.register_train_run.remote(train_run_info))
