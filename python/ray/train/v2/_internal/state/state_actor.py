import copy
import logging
import os
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Dict, Optional

import ray
from ray._private import ray_constants
from ray._private.event.export_event_logger import (
    EventLogType,
    check_export_api_enabled,
    get_export_event_logger,
)
from ray.actor import ActorHandle
from ray.train.v2._internal.constants import (
    CONTROLLERS_TO_POLL_PER_ITERATION,
    DEFAULT_ENABLE_STATE_ACTOR_RECONCILIATION,
    DEFAULT_STATE_ACTOR_RECONCILIATION_INTERVAL_S,
    ENABLE_STATE_ACTOR_RECONCILIATION_ENV_VAR,
    GET_ACTOR_TIMEOUT_S,
    STATE_ACTOR_RECONCILIATION_INTERVAL_S_ENV_VAR,
)
from ray.train.v2._internal.state.schema import (
    TrainRun,
    TrainRunAttempt,
)
from ray.train.v2._internal.state.util import (
    is_actor_alive,
    update_train_run_aborted,
    update_train_run_attempt_aborted,
)
from ray.train.v2._internal.util import time_monotonic

logger = logging.getLogger(__name__)


class TrainStateActor:
    def __init__(
        self,
        # TODO: group into single config if we need to do similar polling elsewhere
        enable_state_actor_reconciliation: bool = False,
        reconciliation_interval_s: float = 30,
        get_actor_timeout_s: int = GET_ACTOR_TIMEOUT_S,
        controllers_to_poll_per_iteration: int = CONTROLLERS_TO_POLL_PER_ITERATION,
    ):
        # NOTE: All runs and attempts are stored in memory.
        # This may be a memory issue for large runs.
        # TODO: consider cleaning up runs over time.
        self._runs: Dict[str, TrainRun] = OrderedDict()
        # {run_id: {attempt_id: TrainRunAttempt}}
        self._run_attempts: Dict[str, OrderedDict[str, TrainRunAttempt]] = defaultdict(
            OrderedDict
        )
        (
            self._export_logger,
            self._is_train_run_export_api_enabled,
            self._is_train_run_attempt_export_api_enabled,
        ) = self._init_export_logger()

        # TODO: consider row level locking if loop takes too long.
        self._runs_lock = threading.RLock()
        self._run_attempts_lock = threading.RLock()

        # Set env vars related to reconciling train run/attempt state.
        if enable_state_actor_reconciliation:
            self._reconciliation_interval_s = reconciliation_interval_s
            self._controllers_to_poll_per_iteration = controllers_to_poll_per_iteration
            self._get_actor_timeout_s = get_actor_timeout_s
            self._start_run_state_reconciliation_thread()

    def _abort_live_runs_with_dead_controllers(
        self, last_poll_run_id: Optional[str]
    ) -> str:
        aborted_run_ids = []
        with self._runs_lock:
            runs = list(self._runs.values())

            # Start iterating from poll index.
            starting_poll_index = 0
            if last_poll_run_id is not None:
                for poll_index, run in enumerate(runs):
                    if run.id == last_poll_run_id:
                        starting_poll_index = (poll_index + 1) % len(runs)
                        break

            # Abort runs.
            num_polled_runs = 0
            poll_index = starting_poll_index
            while (
                poll_index < starting_poll_index + len(runs)
                and num_polled_runs < self._controllers_to_poll_per_iteration
            ):
                run = runs[poll_index % len(runs)]
                poll_index += 1
                last_poll_run_id = run.id
                if run.status.is_terminal():
                    continue
                try:
                    if not is_actor_alive(
                        run.controller_actor_id, self._get_actor_timeout_s
                    ):
                        update_train_run_aborted(run, False)
                        self.create_or_update_train_run(run)
                        aborted_run_ids.append(run.id)
                except ray.util.state.exception.RayStateApiException:
                    logger.exception(
                        "State API unavailable when checking if actor is alive. "
                        "Will check again on next poll."
                    )
                num_polled_runs += 1

        # Abort run attempts.
        with self._run_attempts_lock:
            for run_id in aborted_run_ids:
                latest_run_attempt = self._get_latest_run_attempt(run_id)
                if latest_run_attempt and not latest_run_attempt.status.is_terminal():
                    update_train_run_attempt_aborted(latest_run_attempt, False)
                    self.create_or_update_train_run_attempt(latest_run_attempt)

        return last_poll_run_id

    def _start_run_state_reconciliation_thread(self) -> None:
        def _reconciliation_loop():
            last_poll_run_id = None
            latest_poll_time = float("-inf")
            while True:
                # Wait for the poll interval to elapse.
                time_since_last_poll = time_monotonic() - latest_poll_time
                if time_since_last_poll < self._reconciliation_interval_s:
                    remaining_time = (
                        self._reconciliation_interval_s - time_since_last_poll
                    )
                    time.sleep(remaining_time)

                last_poll_run_id = self._abort_live_runs_with_dead_controllers(
                    last_poll_run_id
                )
                latest_poll_time = time_monotonic()

        threading.Thread(target=_reconciliation_loop, daemon=True).start()

    def _get_latest_run_attempt(self, run_id: str) -> Optional[TrainRunAttempt]:
        with self._run_attempts_lock:
            # NOTE: run_attempts is OrderedDict from attempt_id to TrainRunAttempt.
            run_attempts = self._run_attempts.get(run_id, {})
            if not run_attempts:
                return None
            return next(reversed(run_attempts.values()))

    def create_or_update_train_run(self, run: TrainRun) -> None:
        with self._runs_lock:
            self._runs[run.id] = run
            run_copy = copy.deepcopy(run)
        self._maybe_export_train_run(run_copy)

    def create_or_update_train_run_attempt(self, run_attempt: TrainRunAttempt) -> None:
        with self._run_attempts_lock:
            self._run_attempts[run_attempt.run_id][run_attempt.attempt_id] = run_attempt
            run_attempt_copy = copy.deepcopy(run_attempt)
        self._maybe_export_train_run_attempt(run_attempt_copy)

    def get_train_runs(self) -> Dict[str, TrainRun]:
        with self._runs_lock:
            return self._runs

    def get_train_run_attempts(self) -> Dict[str, Dict[str, TrainRunAttempt]]:
        with self._run_attempts_lock:
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
            .remote(
                enable_state_actor_reconciliation=ray_constants.env_bool(
                    ENABLE_STATE_ACTOR_RECONCILIATION_ENV_VAR,
                    DEFAULT_ENABLE_STATE_ACTOR_RECONCILIATION,
                ),
                reconciliation_interval_s=float(
                    os.getenv(
                        STATE_ACTOR_RECONCILIATION_INTERVAL_S_ENV_VAR,
                        DEFAULT_STATE_ACTOR_RECONCILIATION_INTERVAL_S,
                    )
                ),
            )
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
