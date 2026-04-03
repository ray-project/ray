# flake8: noqa

# __begin_define_capacity_queue_router__
import asyncio
import logging
from typing import Dict, List, Optional

import ray
from ray.exceptions import RayActorError
from ray.serve._private.common import ReplicaID
from ray.serve._private.constants import (
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve._private.request_router.pow_2_router import (
    PowerOfTwoChoicesRequestRouter,
)
from ray.serve.request_router import (
    LocalityMixin,
    MultiplexMixin,
    PendingRequest,
    ReplicaResult,
    RequestRouter,
    RunningReplica,
)

logger = logging.getLogger("ray.serve")


class CapacityQueueRouter(LocalityMixin, MultiplexMixin, RequestRouter):
    """Custom request router that uses a CapacityQueue deployment actor.

    Acquires capacity tokens from a centralized CapacityQueue before routing
    requests. Each token guarantees the target replica has capacity.

    If the queue is unavailable (dead, not yet discovered, or times out),
    falls back to the standard power-of-two-choices algorithm so that
    routing is never blocked by queue issues.
    """

    # Short timeout for token acquisition — if the queue doesn't respond
    # quickly, fall back to power-of-two-choices rather than blocking.
    ACQUIRE_TIMEOUT_S = 3.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._capacity_queue = None
        self._capacity_queue_actor_name: str = "capacity_queue"

        # Tokens acquired in choose_replicas but not yet confirmed by
        # on_request_routed. Released on retry/backoff or cancellation.
        self._pending_tokens: Dict[str, str] = {}

        # Tokens confirmed by on_request_routed. Released when the request
        # completes or is cancelled.
        self._confirmed_tokens: Dict[str, str] = {}

    def initialize_state(self, **kwargs):
        if "capacity_queue_actor_name" in kwargs:
            self._capacity_queue_actor_name = kwargs["capacity_queue_actor_name"]

    def _try_discover_capacity_queue(self) -> bool:
        """Try to discover the CapacityQueue deployment actor.

        Returns True if discovered, False otherwise (does not raise).
        """
        prefix = (
            f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}"
            f"{self._deployment_id.app_name}::"
            f"{self._deployment_id.name}::"
        )
        suffix = f"::{self._capacity_queue_actor_name}"

        try:
            actors = ray.util.list_named_actors(all_namespaces=True)
            for actor_info in actors:
                if actor_info["namespace"] == SERVE_NAMESPACE:
                    name = actor_info["name"]
                    if name.startswith(prefix) and name.endswith(suffix):
                        self._capacity_queue = ray.get_actor(
                            name, namespace=SERVE_NAMESPACE
                        )
                        return True
        except Exception:
            pass
        return False

    def _safe_release(self, replica_id: str) -> None:
        """Release a token, handling queue actor death."""
        if self._capacity_queue is None:
            return
        try:
            self._capacity_queue.release.remote(replica_id)
        except RayActorError:
            self._capacity_queue = None

    async def _try_acquire_token(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest],
    ) -> Optional[RunningReplica]:
        """Try to acquire a token from the capacity queue.

        Returns the matched RunningReplica if successful, None if the queue
        is unavailable, times out, or returns an unknown replica.
        """
        if self._capacity_queue is None:
            if not self._try_discover_capacity_queue():
                return None

        acquire_ref = None
        try:
            acquire_ref = self._capacity_queue.acquire.remote(
                timeout_s=self.ACQUIRE_TIMEOUT_S,
            )
            acquired_replica_id = await asyncio.wait_for(
                acquire_ref,
                timeout=self.ACQUIRE_TIMEOUT_S + 1,
            )
        except asyncio.CancelledError:
            if acquire_ref is not None:
                ray.cancel(acquire_ref)
            raise
        except RayActorError:
            self._capacity_queue = None
            return None
        except Exception:
            return None

        if acquired_replica_id is None:
            return None

        # Find matching replica in the candidate list.
        for replica in candidate_replicas:
            if replica.replica_id.unique_id == acquired_replica_id:
                if pending_request is not None:
                    rid = pending_request.metadata.internal_request_id
                    self._pending_tokens[rid] = acquired_replica_id
                return replica

        # Replica not in local candidate list — release and fall back.
        self._safe_release(acquired_replica_id)
        return None

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """Choose replicas using the capacity queue with power-of-two-choices fallback.

        1. Try to acquire a token from the CapacityQueue.
        2. If successful, return the token-guaranteed replica as the sole candidate.
        3. If the queue is unavailable or times out, fall back to power-of-two-choices.

        On retry (previous token-chosen replica rejected): do NOT release the
        old token. The rejection means that replica is genuinely at capacity,
        so keeping the queue's in_flight elevated teaches it the correct state.
        Then acquire a fresh token — the queue will pick a different replica
        because the rejected one now has higher in_flight.
        """
        # Get power-of-two-choices fallback first (same pattern as
        # PrefixCacheAffinityRouter).
        fallback_replicas = await PowerOfTwoChoicesRequestRouter.choose_replicas(
            self,
            candidate_replicas=candidate_replicas,
            pending_request=pending_request,
        )

        # Can only track tokens when we have a request ID.
        if pending_request is None:
            return fallback_replicas

        # If this request already has a pending token from a previous
        # choose_replicas call, the token-chosen replica rejected. Do NOT
        # release — keeping in_flight elevated teaches the queue that the
        # replica is saturated. Just discard our reference.
        rid = pending_request.metadata.internal_request_id
        self._pending_tokens.pop(rid, None)

        matched_replica = await self._try_acquire_token(
            candidate_replicas,
            pending_request,
        )
        if matched_replica is not None:
            return [[matched_replica]]

        return fallback_replicas

    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        """Promote a pending token to confirmed once the replica accepts."""
        rid = pending_request.metadata.internal_request_id
        token = self._pending_tokens.pop(rid, None)
        if token is not None:
            self._confirmed_tokens[rid] = token

    def on_request_completed(
        self,
        replica_id: ReplicaID,
        internal_request_id: str,
    ):
        """Release the capacity token when a request completes."""
        token = self._confirmed_tokens.pop(internal_request_id, None)
        if token is not None:
            self._safe_release(token)

    def on_request_cancelled(
        self,
        internal_request_id: str,
    ):
        """Release the capacity token when a request is cancelled."""
        token = self._pending_tokens.pop(internal_request_id, None)
        if token is None:
            token = self._confirmed_tokens.pop(internal_request_id, None)
        if token is not None:
            self._safe_release(token)


# __end_define_capacity_queue_router__
