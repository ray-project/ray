import logging
import os
from collections import defaultdict
from typing import Any, Dict, List

import ray
from ray.data import Dataset
from ray.train._internal.state.schema import (
    ActorStatusEnum,
    RunStatusEnum,
    TrainDatasetInfo,
    TrainRunInfo,
    TrainWorkerInfo,
)
from ray.train._internal.utils import check_for_failure
from ray.train._internal.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class TrainRunStateManager:
    """A class that aggregates and reports train run info to TrainStateActor.

    This manager class is created on the train controller layer for each run.
    """

    def __init__(self, state_actor) -> None:
        self.state_actor = state_actor
        self.train_run_info_dict = defaultdict(dict)

    def register_train_run(
        self,
        run_id: str,
        job_id: str,
        run_name: str,
        run_status: str,
        controller_actor_id: str,
        datasets: Dict[str, Dataset],
        worker_group: WorkerGroup,
        start_time_ms: float,
        resources: List[Dict[str, float]],
        status_detail: str = "",
    ) -> None:
        """Collect Train Run Info and report to StateActor."""

        if not self.state_actor:
            logger.warning(
                "Unable to register train run since `TrainStateActor` is not started."
            )
            return

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
                resources=resources[0],
                status=ActorStatusEnum.ALIVE,
            )

        futures = [
            worker_group.execute_single_async(index, collect_train_worker_info)
            for index in range(len(worker_group))
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

        updates = dict(
            id=run_id,
            job_id=job_id,
            name=run_name,
            controller_actor_id=controller_actor_id,
            workers=worker_info_list,
            datasets=dataset_info_list,
            start_time_ms=start_time_ms,
            run_status=run_status,
            status_detail=status_detail,
            resources=resources,
        )

        # Clear the cached info to avoid registering the same run twice
        self.train_run_info_dict[run_id] = {}
        self._update_train_run_info(run_id, updates)

    def end_train_run(
        self,
        run_id: str,
        run_status: RunStatusEnum,
        status_detail: str,
        end_time_ms: int,
    ):
        """Update the train run status when the training is finished."""
        updates = dict(
            run_status=run_status,
            status_detail=status_detail,
            end_time_ms=end_time_ms,
        )
        self._update_train_run_info(run_id, updates)

    def _update_train_run_info(self, run_id: str, updates: Dict[str, Any]) -> None:
        """Update specific fields of a registered TrainRunInfo instance."""
        if run_id in self.train_run_info_dict:
            self.train_run_info_dict[run_id].update(updates)
            train_run_info = TrainRunInfo(**self.train_run_info_dict[run_id])
            ray.get(self.state_actor.register_train_run.remote(train_run_info))
