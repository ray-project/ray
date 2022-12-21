from typing import Mapping, Optional, Any
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actor_manager import FaultTolerantActorManager


class TrainerRunner(FaultTolerantActorManager):
    def update_sync(
        self,
        batch: Optional[MultiAgentBatch] = None,
        update: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        pass

    def update_async(
        self,
        batch: Optional[MultiAgentBatch] = None,
        update: Optional[Mapping[str, Any]] = None,
    ) -> int:
        pass

    def get_weights(self) -> dict:
        pass

    def get_state(self):
        pass

    def set_state(self, state):
        pass
