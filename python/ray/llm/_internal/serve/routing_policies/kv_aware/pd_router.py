"""Router-owned state for direct-streaming prefill/decode routing.

This module deliberately keeps only control-plane state.  Prompt token IDs live
in replica-local :class:`TokenStore` instances and KV tensors travel directly
between the selected prefill and decode engines.  A ticket is consequently a
small, expiring capability that identifies a pair of already-reserved workers;
it is never a container for a request body, token payload, or a Serve
``ReplicaSelection``.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    REQUEST_TRACKING_TTL_S,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    KVTokenTracker,
)

logger = get_logger(__name__)

PD_ROUTING_VERSION = "1"

PD_TOKEN_KEY_HEADER = "x-serve-router-kv-token-key"
PD_P_REPLICA_ID_HEADER = "x-serve-router-pd-p-replica-id"
PD_D_REPLICA_ID_HEADER = "x-serve-router-pd-d-replica-id"
PD_P_RESERVATION_ID_HEADER = "x-serve-router-pd-p-reservation-id"
PD_D_RESERVATION_ID_HEADER = "x-serve-router-pd-d-reservation-id"
PD_EXPIRY_MS_HEADER = "x-serve-router-pd-expiry-ms"
PD_VERSION_HEADER = "x-serve-router-pd-version"

# The owner is an internal routing detail needed by PDDecodeServer to contact
# the ingress replica that owns mutable ticket state.  It is still under the
# protected HAProxy prefix and never comes from the client.
PD_OWNER_REPLICA_ID_HEADER = "x-serve-router-pd-owner-replica-id"


class PDTicketState(str, Enum):
    ROUTED = "routed"
    PREFILL_CLAIMED = "prefill_claimed"
    DECODE_ACTIVE = "decode_active"
    RELEASED = "released"


class PDTicketError(ValueError):
    """A malformed, expired, or no-longer-usable P/D ticket."""


@dataclass
class PDTicket:
    request_id: str
    p_reservation_id: str
    d_reservation_id: str
    p_route: Dict[str, Any]
    d_route: Dict[str, Any]
    expires_at: float
    expires_at_epoch_ms: int
    state: PDTicketState = PDTicketState.ROUTED
    token_key: Optional[str] = None
    prefill_released: bool = False

    @property
    def expiry_ms(self) -> int:
        return self.expires_at_epoch_ms


class PDPairTracker:
    """Owns the P/D selection saga and ticket lifecycle for one LLMRouter.

    ``KVTokenTracker`` remains the Dynamo integration.  We maintain one scoped
    tracker for the prefill fleet and one for the decode fleet, rather than
    mixing workers from the two deployments into one selection service.  The
    decode reservation disables reuse credit because a decode engine must make
    room for the *full* prompt KV regardless of cache locality on prefill.
    """

    def __init__(
        self,
        *,
        prefill_config: Any,
        decode_config: Any,
        prefill_deployment_id: Any,
        decode_deployment_id: Any,
        ticket_ttl_s: float,
        pending_decode_load_scale: float,
        selection_policy: str = "kv_aware",
    ):
        if ticket_ttl_s <= 0:
            raise ValueError("pd_ticket_ttl_s must be positive")
        if pending_decode_load_scale < 0:
            raise ValueError("pending_decode_load_scale must be non-negative")
        if selection_policy not in {"kv_aware", "round_robin"}:
            raise ValueError(
                "pd_selection_policy must be 'kv_aware' or 'round_robin'"
            )

        self.prefill = KVTokenTracker(
            indexer_threads=prefill_config.experimental_configs.get(
                "KV_INDEXER_THREADS", 4
            ),
            serve_deployment_id=prefill_deployment_id,
        )
        self.decode = KVTokenTracker(
            indexer_threads=decode_config.experimental_configs.get(
                "KV_INDEXER_THREADS", 4
            ),
            serve_deployment_id=decode_deployment_id,
        )
        self._ticket_ttl_s = ticket_ttl_s
        self._pending_decode_load_scale = pending_decode_load_scale
        self._selection_policy = selection_policy
        self._next_prefill_index = 0
        self._next_decode_index = 0
        self._tickets_by_d_reservation: Dict[str, PDTicket] = {}
        self._d_reservation_by_request: Dict[str, str] = {}
        self._cleanup_task: Optional[asyncio.Task] = None

    def start_cleanup(self) -> None:
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    def close(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            self._cleanup_task = None

    async def _cleanup_loop(self) -> None:
        # A short bounded cadence makes expiry independent of subsequent
        # traffic, while keeping timer overhead negligible even with the
        # requested two ingress replicas per node.
        interval_s = min(30.0, max(1.0, self._ticket_ttl_s / 2))
        try:
            while True:
                await asyncio.sleep(interval_s)
                await self.evict_expired()
        except asyncio.CancelledError:
            return

    async def reserve_pair(
        self,
        *,
        request_id: str,
        prefill_token_ids: List[int],
        decode_token_ids: List[int],
        expected_output_tokens: Optional[int],
    ) -> PDTicket:
        """Atomically-enough select D then P, compensating on every failure.

        Dynamo atomically selects and reserves within each pool.  The cross-pool
        operation is a saga: D is booked first, P second, and D is immediately
        released if P selection or route metadata resolution fails.
        """
        await self.evict_expired()
        p_block_size = self.prefill.get_block_size()
        d_block_size = self.decode.get_block_size()
        if p_block_size is None or d_block_size is None:
            raise RuntimeError("P/D KV geometry is unavailable before replica registration")
        if p_block_size != d_block_size:
            raise RuntimeError(
                "P/D KV block-size mismatch "
                f"(prefill={p_block_size}, decode={d_block_size})"
            )
        d_reservation_id = f"pd-d-{uuid.uuid4().hex}"
        p_reservation_id = f"pd-p-{uuid.uuid4().hex}"

        d_selection = await self.decode.select_worker(
            d_reservation_id,
            decode_token_ids,
            self._eligible_workers(self.decode, "decode"),
            expected_output_tokens,
            # Decode must not borrow P's overlap credit.  Dynamo's normal
            # lifecycle transition turns this pending reservation into decode
            # blocks on activation; prefill_load_scale is the explicit P/D
            # pending-load tuning knob.
            router_config_override={
                "assume_kv_reuse": False,
                "prefill_load_scale": self._pending_decode_load_scale,
            },
        )
        try:
            d_route = self.decode.get_replica_route(d_selection["worker_id"])
            d_route["worker_id"] = d_selection["worker_id"]
            if not d_route.get("host") or not d_route.get("port"):
                raise RuntimeError("selected decode replica has no backend HTTP endpoint")

            p_selection = await self.prefill.select_worker(
                p_reservation_id,
                prefill_token_ids,
                self._eligible_workers(self.prefill, "prefill"),
                expected_output_tokens=None,
            )
            try:
                p_route = self.prefill.get_replica_route(p_selection["worker_id"])
                p_route["worker_id"] = p_selection["worker_id"]
            except BaseException:
                await self.prefill.on_request_completed(p_reservation_id)
                raise
        except BaseException:
            await self.decode.on_request_completed(d_reservation_id)
            raise

        ticket = PDTicket(
            request_id=request_id,
            p_reservation_id=p_reservation_id,
            d_reservation_id=d_reservation_id,
            p_route=p_route,
            d_route=d_route,
            expires_at=time.monotonic() + self._ticket_ttl_s,
            expires_at_epoch_ms=int((time.time() + self._ticket_ttl_s) * 1000),
        )
        self._tickets_by_d_reservation[d_reservation_id] = ticket
        self._d_reservation_by_request[request_id] = d_reservation_id
        return ticket

    def _eligible_workers(self, tracker: KVTokenTracker, pool: str) -> List[int]:
        """Return all workers for KV-aware routing or one stable RR target.

        The one-worker RoundRobin mode deliberately keeps Dynamo's reservation
        boundary and ticket lifecycle intact.  It differs only in which worker
        is eligible for each selection, making it a like-for-like P/D baseline
        rather than a separate data path.
        """
        worker_ids = sorted(tracker._replica_id_by_worker)
        if self._selection_policy != "round_robin" or not worker_ids:
            return worker_ids
        if pool == "prefill":
            next_index = self._next_prefill_index
            self._next_prefill_index += 1
        else:
            next_index = self._next_decode_index
            self._next_decode_index += 1
        return [worker_ids[next_index % len(worker_ids)]]

    def set_token_key(self, ticket: PDTicket, token_key: Optional[str]) -> None:
        self._require_live(ticket)
        ticket.token_key = token_key

    def claim_prefill(
        self,
        *,
        d_reservation_id: str,
        p_reservation_id: str,
        d_replica_id: str,
        p_replica_id: str,
    ) -> PDTicket:
        """Validate and atomically claim the P half of a ticket.

        This method intentionally has no await points.  Two duplicate decode
        HTTP attempts therefore cannot both transition a routed ticket from
        ``ROUTED`` to ``PREFILL_CLAIMED`` on the owning LLMRouter event loop.
        """
        ticket = self._get_ticket(d_reservation_id, p_reservation_id)
        self._require_live(ticket)
        if ticket.d_route["replica_id"] != d_replica_id:
            raise PDTicketError("decode replica does not match P/D ticket")
        if ticket.p_route["replica_id"] != p_replica_id:
            raise PDTicketError("prefill replica does not match P/D ticket")
        if ticket.state == PDTicketState.ROUTED:
            ticket.state = PDTicketState.PREFILL_CLAIMED
        elif ticket.state != PDTicketState.PREFILL_CLAIMED:
            raise PDTicketError(f"P/D ticket is already {ticket.state.value}")
        return ticket

    async def prefill_complete(self, d_reservation_id: str) -> None:
        """Release P only after its transfer-safe completion, then activate D."""
        ticket = self._get_ticket(d_reservation_id)
        self._require_live(ticket)
        if ticket.state == PDTicketState.DECODE_ACTIVE:
            return
        if ticket.state != PDTicketState.PREFILL_CLAIMED:
            raise PDTicketError("prefill completion arrived before ticket claim")
        try:
            await self.prefill.on_prefill_complete(ticket.p_reservation_id)
            await self.prefill.on_request_completed(ticket.p_reservation_id)
            ticket.prefill_released = True
            await self.decode.on_prefill_complete(ticket.d_reservation_id)
        except BaseException:
            await self.release(d_reservation_id)
            raise
        # The short configured TTL protects the unclaimed routing capability.
        # Once P completed and D is active, normal decode lifecycle events own
        # release; use the same bounded leak guard as KVTokenTracker so a long
        # legitimate response cannot have its D reservation reclaimed midway.
        ticket.expires_at = time.monotonic() + REQUEST_TRACKING_TTL_S
        ticket.expires_at_epoch_ms = int((time.time() + REQUEST_TRACKING_TTL_S) * 1000)
        ticket.state = PDTicketState.DECODE_ACTIVE

    async def release(self, d_reservation_id: str) -> None:
        """Idempotently release both sides of a terminal/expired ticket."""
        ticket = self._tickets_by_d_reservation.pop(d_reservation_id, None)
        if ticket is None:
            return
        self._d_reservation_by_request.pop(ticket.request_id, None)
        ticket.state = PDTicketState.RELEASED
        releases = [self.decode.on_request_completed(ticket.d_reservation_id)]
        if not ticket.prefill_released:
            releases.append(self.prefill.on_request_completed(ticket.p_reservation_id))
        results = await asyncio.gather(*releases, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Failed to release P/D reservation: %s", result)

    async def evict_expired(self) -> None:
        now = time.monotonic()
        expired = [
            d_reservation_id
            for d_reservation_id, ticket in self._tickets_by_d_reservation.items()
            if ticket.expires_at <= now
        ]
        for d_reservation_id in expired:
            logger.warning("Expiring uncompleted P/D ticket %s", d_reservation_id)
            await self.release(d_reservation_id)

    async def on_decode_lifecycle_events(
        self, worker_id: int, events: List[tuple]
    ) -> None:
        """Apply decode progress/completion events to the matching D ticket."""
        for hook_name, args in events:
            if not args:
                continue
            d_reservation_id = self._d_reservation_by_request.get(args[0])
            if d_reservation_id is None:
                continue
            ticket = self._tickets_by_d_reservation.get(d_reservation_id)
            if ticket is None or ticket.d_route.get("worker_id") not in (None, worker_id):
                # ``worker_id`` is not retained in route metadata today; the
                # replica-id check below is performed at claim time.  Keep this
                # branch forward-compatible with trackers that add it.
                continue
            if hook_name == "on_prefill_complete":
                if ticket.state == PDTicketState.PREFILL_CLAIMED:
                    await self.prefill_complete(d_reservation_id)
            elif hook_name == "on_decode_progress" and len(args) == 2:
                await self.decode.on_decode_progress(
                    ticket.d_reservation_id, args[1]
                )
            elif hook_name == "on_request_completed":
                await self.release(d_reservation_id)

    def _get_ticket(
        self, d_reservation_id: str, p_reservation_id: Optional[str] = None
    ) -> PDTicket:
        ticket = self._tickets_by_d_reservation.get(d_reservation_id)
        if ticket is None:
            raise PDTicketError("unknown or released P/D ticket")
        if p_reservation_id is not None and ticket.p_reservation_id != p_reservation_id:
            raise PDTicketError("prefill reservation does not match P/D ticket")
        return ticket

    def _require_live(self, ticket: PDTicket) -> None:
        if ticket.expires_at <= time.monotonic():
            raise PDTicketError("P/D ticket expired")
        if ticket.state == PDTicketState.RELEASED:
            raise PDTicketError("P/D ticket released")
