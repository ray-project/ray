from collections import defaultdict
import logging
import os
import threading
from typing import Dict, Optional

import ray
from ray._private.event.export_event_logger import (
    EventLogType,
    get_export_event_logger,
    check_export_api_enabled,
)
from ray.actor import ActorHandle
from ray.train.v2._internal.state.schema import TrainRun, TrainRunAttempt

logger = logging.getLogger(__name__)


class TrainStateActor:
    def __init__(self):
        # NOTE: All runs and attempts are stored in memory.
        # This may be a memory issue for large runs.
        self._runs: Dict[str, TrainRun] = {}
        # {run_id: {attempt_id: TrainRunAttempt}}
        self._run_attempts: Dict[str, Dict[str, TrainRunAttempt]] = defaultdict(dict)
        (
            self._export_logger,
            self._is_train_run_export_api_enabled,
            self._is_train_run_attempt_export_api_enabled,
        ) = self._init_export_logger()

    def create_or_update_train_run(self, run: TrainRun) -> None:
        self._runs[run.id] = run
        self._maybe_export_train_run(run)

    def create_or_update_train_run_attempt(self, run_attempt: TrainRunAttempt):
        self._run_attempts[run_attempt.run_id][run_attempt.attempt_id] = run_attempt
        self._maybe_export_train_run_attempt(run_attempt)

    def get_train_runs(self) -> Dict[str, TrainRun]:
        return self._runs

    def get_train_run_attempts(self) -> Dict[str, Dict[str, TrainRunAttempt]]:
        return self._run_attempts

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

    def _maybe_export_train_run(self, run: TrainRun) -> None:
        if not self._is_train_run_export_api_enabled:
            return

        from ray.train.v2._internal.state.export import train_run_to_proto

        run_proto = train_run_to_proto(run)
        self._export_logger.send_event(run_proto)

    def _maybe_export_train_run_attempt(self, run_attempt: TrainRunAttempt) -> None:
        if not self._is_train_run_attempt_export_api_enabled:
            return

        from ray.train.v2._internal.state.export import train_run_attempt_to_proto

        run_attempt_proto = train_run_attempt_to_proto(run_attempt)
        self._export_logger.send_event(run_attempt_proto)


TRAIN_STATE_ACTOR_NAME = "train_v2_state_actor"
TRAIN_STATE_ACTOR_NAMESPACE = "_train_state_actor"

_state_actor_lock: threading.RLock = threading.RLock()


def get_or_create_state_actor() -> ActorHandle:
    """Get or create the Ray Train state actor singleton.

    This is a long-living, detached actor living on the head node
    that gets initialized when the first Train run happens on the
    Ray cluster.
    """
    with _state_actor_lock:
        state_actor = (
            ray.remote(TrainStateActor)
            .options(
                num_cpus=0,
                name=TRAIN_STATE_ACTOR_NAME,
                namespace=TRAIN_STATE_ACTOR_NAMESPACE,
                get_if_exists=True,
                lifetime="detached",
                resources={"node:__internal_head__": 0.001},
                # Escape from the parent's placement group
                scheduling_strategy="DEFAULT",
                max_restarts=-1,
                max_task_retries=-1,
            )
            .remote()
        )

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
