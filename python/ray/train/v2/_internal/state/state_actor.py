import logging
import os
import threading
import time
from collections import defaultdict
from typing import Dict, Optional

import ray
from ray._private.event.export_event_logger import (
    EventLogType,
    check_export_api_enabled,
    get_export_event_logger,
)
from ray.actor import ActorHandle
from ray.train.v2._internal.constants import (
    DEFAULT_STATE_ACTOR_POLL_INTERVAL_S,
    STATE_ACTOR_POLL_INTERVAL_S_ENV_VAR,
    get_env_vars_to_propagate,
)
from ray.train.v2._internal.state.schema import (
    TrainRun,
    TrainRunAttempt,
    is_terminal_run_status,
)
from ray.train.v2._internal.state.util import (
    update_train_run_aborted,
    update_train_run_attempt_aborted,
)
from ray.train.v2._internal.util import time_monotonic
from ray.util.state import get_actor

logger = logging.getLogger(__name__)


class TrainStateActor:
    def __init__(self):
        # NOTE: All runs and attempts are stored in memory.
        # This may be a memory issue for large runs.
        # TODO: consider cleaning up runs over time.
        self._runs: Dict[str, TrainRun] = {}
        # {run_id: {attempt_id: TrainRunAttempt}}
        self._run_attempts: Dict[str, Dict[str, TrainRunAttempt]] = defaultdict(dict)
        (
            self._export_logger,
            self._is_train_run_export_api_enabled,
            self._is_train_run_attempt_export_api_enabled,
        ) = self._init_export_logger()
        self._poll_interval_s = float(
            os.getenv(
                STATE_ACTOR_POLL_INTERVAL_S_ENV_VAR, DEFAULT_STATE_ACTOR_POLL_INTERVAL_S
            )
        )
        self._latest_poll_time = float("-inf")
        self._start_controller_polling_thread()

    def _start_controller_polling_thread(self) -> None:
        def _poll_controller():
            while True:
                # Wait for the poll interval to elapse.
                time_since_last_poll = time_monotonic() - self._latest_poll_time
                if time_since_last_poll < self._poll_interval_s:
                    remaining_time = max(
                        self._poll_interval_s - time_since_last_poll, 0
                    )
                    time.sleep(remaining_time)

                # Abort live runs/attempts whose controllers are dead.
                for run_id, run in self._runs.items():
                    if is_terminal_run_status(run.status):
                        continue
                    actor_state = get_actor(run.controller_actor_id)
                    if not actor_state or actor_state.state == "DEAD":
                        update_train_run_aborted(run)
                        self.create_or_update_train_run(run)
                        for run_attempt in self._run_attempts.get(run_id, {}).values():
                            update_train_run_attempt_aborted(run_attempt)
                            self.create_or_update_train_run_attempt(run_attempt)

                self._latest_poll_time = time_monotonic()

        thread = threading.Thread(target=_poll_controller)
        thread.start()

    def create_or_update_train_run(self, run: TrainRun) -> None:
        self._runs[run.id] = run
        self._maybe_export_train_run(run)

    def create_or_update_train_run_attempt(self, run_attempt: TrainRunAttempt) -> None:
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
                runtime_env={"env_vars": get_env_vars_to_propagate()},
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
