import logging
import threading
from collections import defaultdict
from typing import Dict, Optional

import ray
from ray.actor import ActorHandle
from ray.train.v2._internal.state.schema import TrainRun, TrainRunAttempt

logger = logging.getLogger(__name__)

# TODO: Add export API functionality.


class TrainStateActor:
    def __init__(self):
        # NOTE: All runs and attempts are stored in memory.
        # This may be a memory issue for large runs.
        self._runs: Dict[str, TrainRun] = {}
        # {run_id: {attempt_id: TrainRunAttempt}}
        self._run_attempts: Dict[str, Dict[str, TrainRunAttempt]] = defaultdict(dict)

    def create_or_update_train_run(self, run: TrainRun) -> None:
        self._runs[run.id] = run

    def create_or_update_train_run_attempt(self, run_attempt: TrainRunAttempt):
        self._run_attempts[run_attempt.run_id][run_attempt.attempt_id] = run_attempt

    def get_train_runs(self) -> Dict[str, TrainRun]:
        return self._runs

    def get_train_run_attempts(self) -> Dict[str, Dict[str, TrainRunAttempt]]:
        return self._run_attempts


TRAIN_STATE_ACTOR_NAME = "train_v2_state_actor"
TRAIN_STATE_ACTOR_NAMESPACE = "_train_state_actor"

_state_actor_lock: threading.RLock = threading.RLock()


def get_or_create_state_actor() -> ActorHandle:
    """Get or create the Ray Train State Actor."""

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
