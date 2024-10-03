import logging
import threading
from typing import Dict, Optional

import ray
from ray.actor import ActorHandle
from ray.train._internal.state.schema import TrainRunInfo
from ray._private import ray_constants
from ray._private.event.export_event_logger import get_export_event_logger

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class TrainStateActor:
    def __init__(self):
        from ray.core.generated.export_event_pb2 import ExportEvent
        self._run_infos: Dict[str, TrainRunInfo] = {}
        self._export_train_run_info_logger: logging.Logger = None
        try:
            if (
                ray_constants.RAY_ENABLE_EXPORT_API_WRITE
            ):
                self._export_train_run_info_logger = get_export_event_logger(
                    ExportEvent.SourceType.EXPORT_TRAIN_RUN,
                    "/tmp/ray/session_latest/logs",
                )
        except Exception:
            logger.exception(
                "Unable to initialize export event logger so no export "
                "events will be written."
            )

    def register_train_run(self, run_info: TrainRunInfo) -> None:
        # Register a new train run.
        self._run_infos[run_info.id] = run_info
        self._write_train_run_export_event(run_info)

    def get_train_run(self, run_id: str) -> Optional[TrainRunInfo]:
        # Retrieve a registered run with its id
        return self._run_infos.get(run_id, None)

    def get_all_train_runs(self) -> Dict[str, TrainRunInfo]:
        # Retrieve all registered train runs
        return self._run_infos
    
    def _write_train_run_export_event(self, run_info: TrainRunInfo) -> None:
        from ray.core.generated.export_train_run_info_pb2 import (
            ExportTrainRunInfo,
        )
        if not self._export_train_run_info_logger:
            return
        export_run_info = ExportTrainRunInfo(
            name=run_info.name,
            run_id=run_info.id,
            job_id=run_info.job_id,
            controller_actor_id=run_info.controller_actor_id,
            workers=[
                ExportTrainRunInfo.TrainWorkerInfo(
                    actor_id=worker.actor_id,
                    world_rank=worker.world_rank,
                    local_rank=worker.local_rank,
                    node_rank=worker.node_rank,
                    node_id=worker.node_id,
                    node_ip=worker.node_ip,
                    pid=worker.pid,
                    gpu_ids=worker.gpu_ids,
                    status=worker.status,
                ) for worker in run_info.workers
            ],
            datasets=[
                ExportTrainRunInfo.TrainDatasetInfo(
                    name=dataset.name,
                    dataset_uuid=dataset.dataset_uuid,
                    dataset_name=dataset.dataset_name,
                ) for dataset in run_info.datasets
            ],
            run_status=run_info.run_status,
            status_detail=run_info.status_detail,
            start_time_ms=run_info.start_time_ms,
            end_time_ms=run_info.end_time_ms
        )
        self._export_train_run_info_logger.send_event(export_run_info)
        


TRAIN_STATE_ACTOR_NAME = "train_state_actor"
TRAIN_STATE_ACTOR_NAMESPACE = "_train_state_actor"

_state_actor_lock: threading.RLock = threading.RLock()


def get_or_create_state_actor() -> ActorHandle:
    """Get or create a `TrainStateActor` on the head node."""
    with _state_actor_lock:
        state_actor = TrainStateActor.options(
            name=TRAIN_STATE_ACTOR_NAME,
            namespace=TRAIN_STATE_ACTOR_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
            resources={"node:__internal_head__": 0.001},
            # Escape from the parent's placement group
            scheduling_strategy="DEFAULT",
        ).remote()

    # Ensure the state actor is ready
    ray.get(state_actor.__ray_ready__.remote())
    return state_actor


def get_state_actor() -> Optional[ActorHandle]:
    """Get the `TrainStateActor` if exists, otherwise return None."""
    try:
        return ray.get_actor(
            name=TRAIN_STATE_ACTOR_NAME,
            namespace=TRAIN_STATE_ACTOR_NAMESPACE,
        )
    except ValueError:
        return None
