import logging
import random
from typing import (
    List,
    Optional,
)

from ray.llm._internal.serve.request_router.kv_event_manager import KVEventManager
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)
from ray.serve._private.request_router.common import (
    PendingRequest,
)
from ray.serve._private.request_router.replica_wrapper import (
    RunningReplica,
)
from ray.serve._private.request_router.request_router import (
    FIFOMixin,
    LocalityMixin,
    MultiplexMixin,
    RequestRouter,
)
from ray.serve._private.request_router import PowerOfTwoChoicesRequestRouter

logger = logging.getLogger(SERVE_LOGGER_NAME)


class KvEventRequestRouter(LocalityMixin, MultiplexMixin, RequestRouter):
    """Chooses a replica for each request using the vLLM KV event router."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracked_replica_ids = set()
        print(f"[seiji] starting/getting kv event manager")
        self.manager = KVEventManager.options(name="kv_event_manager", namespace="ray_serve_llm", get_if_exists=True, lifetime="detached").remote()
        print(f"[seiji] kv event manager: {self.manager}")
        print(f"[seiji] starting kv event manager run()")
        self.manager.start_listening.remote()
        print(f"[seiji] kv event manager run() started")

    def update_replicas(self, replicas: List[RunningReplica]):
        super().update_replicas(replicas)
        """Update the set of available replicas to be considered for routing.

        When the set of replicas changes, we may spawn additional routing tasks
        if there are pending requests.
        """
        # Get reference to the KVEventManager actor

        # Get current replica IDs
        current_replica_ids = {replica.replica_id for replica in replicas}

        # Subscribe to new replicas
        new_replica_ids = current_replica_ids - self.tracked_replica_ids
        for replica_id in new_replica_ids:
            self.manager.subscribe_to_replica_topic.remote(replica_id.unique_id)
            logger.info(f"Subscribed to KV events for replica: {replica_id}")

        # Unsubscribe from removed replicas
        removed_replica_ids = self.tracked_replica_ids - current_replica_ids
        for replica_id in removed_replica_ids:
            self.manager.unsubscribe_from_replica_topic.remote(replica_id.unique_id)
            logger.info(f"Unsubscribed from KV events for replica: {replica_id}")

        # Update tracked replica IDs
        self.tracked_replica_ids = current_replica_ids

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """One iteration of the power of two choices procedure that chooses
         (at most) two random available replicas.

        For multiplexing, this will first attempt to choose replicas that have the
        requested model ID for a configured timeout. If no replicas with the matching
        model ID are available after that timeout, it will fall back to the regular
        procedure.
        """
        return await PowerOfTwoChoicesRequestRouter.choose_replicas(self, candidate_replicas=candidate_replicas, pending_request=pending_request)
