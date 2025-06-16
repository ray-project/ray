import logging
import os
import threading
from typing import Dict, Optional

import ray
from ray._private.event.export_event_logger import (
    EventLogType,
    check_export_api_enabled,
    get_export_event_logger,
)
from ray.actor import ActorHandle
from ray.train._internal.state.schema import TrainRunInfo

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class TrainStateActor:
    def __init__(self):
        self._run_infos: Dict[str, TrainRunInfo] = {}

        (
            self._export_logger,
            self._is_train_run_export_api_enabled,
            self._is_train_run_attempt_export_api_enabled,
        ) = self._init_export_logger()

    def register_train_run(self, run_info: TrainRunInfo) -> None:
        # Register a new train run.
        self._run_infos[run_info.id] = run_info

        self._maybe_export_train_run(run_info)
        self._maybe_export_train_run_attempt(run_info)

    def get_train_run(self, run_id: str) -> Optional[TrainRunInfo]:
        # Retrieve a registered run with its id
        return self._run_infos.get(run_id, None)

    def get_all_train_runs(self) -> Dict[str, TrainRunInfo]:
        # Retrieve all registered train runs
        return self._run_infos

    # ============================
    # Export API
    # ============================

    def is_export_api_enabled(self) -> bool:
        return self._export_logger is not None

    def _init_export_logger(self) -> tuple[Optional[logging.Logger], bool, bool]:
        """Initialize the export logger and check if the export API is enabled.

        Returns:
            A tuple containing:
                - The export logger (or None if export API is not enabled).
                - A boolean indicating if the export API is enabled for train runs.
                - A boolean indicating if the export API is enabled for train run attempts.
        """
        # Proto schemas should be imported within the scope of TrainStateActor to
        # prevent serialization errors.
        from ray.core.generated.export_event_pb2 import ExportEvent

        is_train_run_export_api_enabled = check_export_api_enabled(
            ExportEvent.SourceType.EXPORT_TRAIN_RUN
        )
        is_train_run_attempt_export_api_enabled = check_export_api_enabled(
            ExportEvent.SourceType.EXPORT_TRAIN_RUN_ATTEMPT
        )
        export_api_enabled = (
            is_train_run_export_api_enabled or is_train_run_attempt_export_api_enabled
        )

        if not export_api_enabled:
            return None, False, False

        log_directory = os.path.join(
            ray._private.worker._global_node.get_session_dir_path(), "logs"
        )
        logger = None
        try:
            logger = get_export_event_logger(
                EventLogType.TRAIN_STATE,
                log_directory,
            )
        except Exception:
            logger.exception(
                "Unable to initialize the export event logger, so no Train export "
                "events will be written."
            )

        if logger is None:
            return None, False, False

        return (
            logger,
            is_train_run_export_api_enabled,
            is_train_run_attempt_export_api_enabled,
        )

    def _maybe_export_train_run(self, run_info: TrainRunInfo) -> None:
        if not self._is_train_run_export_api_enabled:
            return

        from ray.train._internal.state.export import train_run_info_to_proto_run

        run_proto = train_run_info_to_proto_run(run_info)
        self._export_logger.send_event(run_proto)

    def _maybe_export_train_run_attempt(self, run_info: TrainRunInfo) -> None:
        if not self._is_train_run_attempt_export_api_enabled:
            return

        from ray.train._internal.state.export import train_run_info_to_proto_attempt

        run_attempt_proto = train_run_info_to_proto_attempt(run_info)
        self._export_logger.send_event(run_attempt_proto)


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
