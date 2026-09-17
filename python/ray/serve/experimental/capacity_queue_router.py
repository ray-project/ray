# flake8: noqa

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


DEFAULT_MAX_FAULT_RETRIES = 3


class CapacityQueueRouter(LocalityMixin, MultiplexMixin, RequestRouter):
    """Custom request router that uses a CapacityQueue deployment actor.

    Acquires capacity tokens from a centralized CapacityQueue before routing
    requests. Each token guarantees the target replica has capacity.

    If the capacity queue is unavailable (dead, not yet discovered, or times out),
    the router retries with exponential backoff up to MAX_FAULT_RETRIES times,
    then falls back to the standard power-of-two-choices algorithm for that
    request.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._capacity_queue = None
        self._capacity_queue_actor_name: Optional[str] = None
        self._capacity_queue_full_name: Optional[str] = None

        # Tokens acquired but not yet accepted by the replica (pending the
        # rejection protocol handshake). On rejection the token is
        # intentionally leaked so the CQ's in_flight stays elevated, teaching
        # it that the replica is busy.
        self._pending_tokens: Dict[str, str] = {}

        # Tokens whose replica accepted the request. Released when the
        # request completes or is cancelled.
        self._acquired_tokens: Dict[str, str] = {}

        # Reverse index: unique_id string -> ReplicaID for O(1) lookup.
        # Rebuilt by update_replicas whenever the replica set changes.
        self._uid_to_replica_id: Dict[str, ReplicaID] = {}

    def initialize_state(self, **kwargs):
        if "capacity_queue_actor_name" not in kwargs:
            raise ValueError(
                "CapacityQueueRouter requires 'capacity_queue_actor_name' in "
                "request_router_kwargs."
            )
        self._capacity_queue_actor_name = kwargs["capacity_queue_actor_name"]
        self._max_fault_retries = kwargs.get(
            "max_fault_retries", DEFAULT_MAX_FAULT_RETRIES
        )

    def _try_discover_capacity_queue(self) -> bool:
        """Try to discover the CapacityQueue deployment actor.

        TODO (jeffreywang): Find a better way to discover the CapacityQueue actor.

        Returns True if discovered, False otherwise (does not raise).
        """
        if self._capacity_queue_full_name is not None:
            try:
                self._capacity_queue = ray.get_actor(
                    self._capacity_queue_full_name, namespace=SERVE_NAMESPACE
                )
                return True
            except Exception:
                pass
            return False

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
                        self._capacity_queue_full_name = name
                        return True
        except Exception:
            pass
        return False

    def update_replicas(self, replicas: List[RunningReplica]):
        super().update_replicas(replicas)
        self._uid_to_replica_id = {rid.unique_id: rid for rid in self._replicas}

    def _safe_release(self, replica_id: str) -> None:
        """Release a token, handling queue actor death."""
        if self._capacity_queue is None:
            return
        try:
            self._capacity_queue.release.remote(replica_id)
        except RayActorError:
            self._capacity_queue = None

    async def _choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> RunningReplica:
        """Choose a replica by acquiring a token from the capacity queue.

        This overrides the base-class method to bypass the routing-task
        machinery (choose_replicas -> probe -> retry loop). The CQ acquire
        runs directly in the caller's coroutine so the event loop stays
        responsive.

        On transient CQ faults (actor death, discovery failure), retries with
        exponential backoff up to MAX_FAULT_RETRIES times, then falls back to
        power-of-two-choices for this request.
        """
        internal_request_id = pending_request.metadata.internal_request_id

        # On retry (replica rejected), drop the old pending token WITHOUT
        # releasing it. The leaked token keeps in_flight elevated on the CQ,
        # teaching it that the replica is busy. The TTL reaper eventually
        # reclaims it.
        if is_retry:
            self._pending_tokens.pop(internal_request_id, None)

        # Wait for at least one replica.
        while len(self._replicas) == 0:
            self._replicas_updated_event.clear()
            await self._replicas_updated_event.wait()

        fault_attempt = 0
        capacity_depleted_attempt = 0
        acquired_replica_id = None
        try:
            while True:
                # Too many consecutive faults — fall back to Pow2.
                if fault_attempt >= self._max_fault_retries:
                    return await super()._choose_replica_for_request(
                        pending_request, is_retry=is_retry
                    )

                # Discover the CQ actor if we don't have a handle.
                if self._capacity_queue is None:
                    if not self._try_discover_capacity_queue():
                        await self._backoff(fault_attempt)
                        fault_attempt += 1
                        continue

                # Acquire a token from the CQ.
                acquire_ref = None
                try:
                    acquire_ref = self._capacity_queue.acquire.remote()
                    acquired_replica_id = await acquire_ref
                except asyncio.CancelledError:
                    if acquire_ref is not None:
                        ray.cancel(acquire_ref)
                    raise
                except RayActorError:
                    logger.warning(
                        "Lost connection to CapacityQueue actor: "
                        f"{self._capacity_queue_full_name}."
                    )
                    self._capacity_queue = None
                    await self._backoff(fault_attempt)
                    fault_attempt += 1
                    continue
                except Exception:
                    logger.warning(
                        "Unexpected error acquiring from CapacityQueue: "
                        f"{self._capacity_queue_full_name}.",
                        exc_info=True,
                    )
                    await self._backoff(fault_attempt)
                    fault_attempt += 1
                    continue

                # CQ returned None — capacity exhausted. This is normal
                # backpressure, not a fault, so it doesn't increment
                # fault_attempt or trigger Pow2 fallback. Apply backoff
                # to avoid a hot loop when acquire_timeout_s is low.
                if acquired_replica_id is None:
                    await self._backoff(capacity_depleted_attempt)
                    capacity_depleted_attempt += 1
                    continue

                # O(1) lookup via reverse index.
                rid = self._uid_to_replica_id.get(acquired_replica_id)
                replica = self._replicas.get(rid) if rid is not None else None
                if replica is not None:
                    # Store as pending until on_request_routed confirms
                    # acceptance. If the replica rejects, the token is
                    # intentionally NOT released.
                    self._pending_tokens[internal_request_id] = acquired_replica_id
                    acquired_replica_id = None
                    fault_attempt = 0
                    capacity_depleted_attempt = 0
                    return replica

                # Replica not found — release token and wait for replica update.
                # This happens during replica transitions when the CQ knows about
                # a new replica before the local router does. Without waiting,
                # the CQ fast path creates a hot loop returning the same unknown
                # replica.
                logger.debug(
                    f"CapacityQueue returned unknown replica {acquired_replica_id}; "
                    "releasing token and waiting for replica update."
                )
                self._safe_release(acquired_replica_id)
                acquired_replica_id = None

                # Wait for the router's local replica state to be updated via long polling.
                # This ensures we don't create a hot loop when the queue is ahead of us.
                self._replicas_updated_event.clear()
                await self._replicas_updated_event.wait()
        except asyncio.CancelledError:
            # If cancelled after acquiring a token but before storing it in
            # _pending_tokens, release it to avoid leaking capacity.
            if acquired_replica_id is not None:
                self._safe_release(acquired_replica_id)
            raise

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """Pow2 candidate selection — no CQ calls.

        Only reached when _choose_replica_for_request falls back to the
        base class after MAX_FAULT_RETRIES consecutive CQ faults.
        """
        return await PowerOfTwoChoicesRequestRouter.choose_replicas(
            self,
            candidate_replicas=candidate_replicas,
            pending_request=pending_request,
        )

    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        """Promote a pending token to acquired once the replica accepts.

        Only called when the rejection protocol is active and the replica
        accepts. On rejection this is NOT called, so the token stays in
        _pending_tokens and is intentionally leaked on retry to teach the
        CQ that the replica is busy.
        """
        rid = pending_request.metadata.internal_request_id
        token = self._pending_tokens.pop(rid, None)
        if token is not None:
            self._acquired_tokens[rid] = token

    def on_request_completed(
        self,
        replica_id: ReplicaID,
        internal_request_id: str,
    ):
        """
        Release the capacity token when a request completes.
        """
        token = self._acquired_tokens.pop(internal_request_id, None)
        if token is None:
            token = self._pending_tokens.pop(internal_request_id, None)
        if token is not None:
            self._safe_release(token)

    def on_request_cancelled(
        self,
        internal_request_id: str,
    ):
        """Release the capacity token when a request is cancelled."""
        token = self._acquired_tokens.pop(internal_request_id, None)
        if token is None:
            token = self._pending_tokens.pop(internal_request_id, None)
        if token is not None:
            self._safe_release(token)
