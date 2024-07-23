import logging
import threading
from typing import Dict, Optional

import ray
from ray.actor import ActorHandle
from ray.train._internal.state.schema import TrainRunInfo

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class TrainStateActor:
    def __init__(self):
        self._run_infos: Dict[str, TrainRunInfo] = {}

    def register_train_run(self, run_info: TrainRunInfo) -> None:
        # Register a new train run.
        self._run_infos[run_info.id] = run_info

    def get_train_run(self, run_id: str) -> Optional[TrainRunInfo]:
        # Retrieve a registered run with its id
        return self._run_infos.get(run_id, None)

    def get_all_train_runs(self) -> Dict[str, TrainRunInfo]:
        # Retrieve all registered train runs
        return self._run_infos


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
