"""Router-owned state for direct-streaming prefill/decode routing.

This module deliberately keeps only control-plane state.  Prompt token IDs live
in replica-local :class:`TokenStore` instances and KV tensors travel directly
between the selected prefill and decode engines.  A ticket is consequently a
small, expiring capability that identifies a pair of already-reserved workers;
it is never a container for a request body, token payload, or a Serve
``ReplicaSelection``.
"""

import asyncio
import heapq
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    REQUEST_TRACKING_TTL_S,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    KVTokenTracker,
    ReservationBroadcastForwarder,
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


class PDReservationEvent(TypedDict):
    """A terminal P/D reservation transition replicated to peer routers."""

    source_ingress_replica_id: str
    event: str
    p_reservation_id: str
    d_reservation_id: str


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
            raise ValueError("pd_selection_policy must be 'kv_aware' or 'round_robin'")

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
        # Entries are append-only. A ticket that is claimed or released leaves
        # a stale entry behind, which is ignored when it reaches the heap head.
        self._ticket_expiries: List[Tuple[float, str]] = []
        self._cleanup_task: Optional[asyncio.Task] = None
        self._event_forwarder: Optional[ReservationBroadcastForwarder] = None
        self._ingress_replica_id: Optional[str] = None

    def start_reservation_broadcast(self, handle: Any, ingress_replica_id: str) -> None:
        """Replicate both pool reservations and terminal transitions.

        Each pool keeps the standard KVTokenTracker reservation protocol. The
        P/D-specific stream only carries the two cross-pool transitions that
        engines cannot report themselves: transfer-safe P completion and
        compensation on failures/expiry.
        """
        self.prefill.start_reservation_broadcast(handle, pool="prefill")
        self.decode.start_reservation_broadcast(handle, pool="decode")
        self._event_forwarder = ReservationBroadcastForwarder(
            handle,
            method_name="on_pd_reservation_events",
            event_name="P/D reservation",
        )
        self._ingress_replica_id = ingress_replica_id

    def start_cleanup(self) -> None:
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    def close(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            self._cleanup_task = None
        if self._event_forwarder is not None:
            self._event_forwarder.close()
            self._event_forwarder = None

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
        if prefill_token_ids != decode_token_ids:
            raise ValueError(
                "P/D routing requires identical prefill and decode token IDs"
            )
        p_block_size = self.prefill.get_block_size()
        d_block_size = self.decode.get_block_size()
        if p_block_size is None or d_block_size is None:
            raise RuntimeError(
                "P/D KV geometry is unavailable before replica registration"
            )
        if p_block_size != d_block_size:
            raise RuntimeError(
                "P/D KV block-size mismatch "
                f"(prefill={p_block_size}, decode={d_block_size})"
            )
        # Both pool-local selection services can use the engine request id as
        # their selection id. This keeps the standard lifecycle broadcast
        # usable on every ingress replica without a second id translation.
        d_reservation_id = request_id
        p_reservation_id = request_id

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
                "overlap_score_credit": 0.0,
                "prefill_load_scale": self._pending_decode_load_scale,
            },
        )
        try:
            d_route = self.decode.get_replica_route(d_selection["worker_id"])
            d_route["worker_id"] = d_selection["worker_id"]
            if not d_route.get("host") or not d_route.get("port"):
                raise RuntimeError(
                    "selected decode replica has no backend HTTP endpoint"
                )

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
            self._report_event("release", p_reservation_id, d_reservation_id)
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
        self._schedule_expiry(ticket)
        # Match the normal aggregated KVAwareRouter behavior: every router
        # applies both pool reservations before P/D execution starts, so later
        # lifecycle broadcasts cannot be applied to a missing reservation.
        await asyncio.gather(
            self.prefill.flush_reservation_broadcast(),
            self.decode.flush_reservation_broadcast(),
        )
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
            # The short ticket TTL protects an unclaimed capability. A claimed
            # prefill may legitimately outlive it while transferring KV, so use
            # the same bounded leak guard as normal request tracking.
            self._set_ticket_expiry(ticket, REQUEST_TRACKING_TTL_S)
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
        # Keep the active decode under the normal request-tracking leak guard.
        self._set_ticket_expiry(ticket, REQUEST_TRACKING_TTL_S)
        ticket.state = PDTicketState.DECODE_ACTIVE
        self._report_event(
            "prefill_complete", ticket.p_reservation_id, ticket.d_reservation_id
        )

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
        self._report_event("release", ticket.p_reservation_id, ticket.d_reservation_id)

    async def evict_expired(self) -> None:
        now = time.monotonic()
        while self._ticket_expiries and self._ticket_expiries[0][0] <= now:
            expires_at, d_reservation_id = heapq.heappop(self._ticket_expiries)
            ticket = self._tickets_by_d_reservation.get(d_reservation_id)
            if ticket is None or ticket.expires_at != expires_at:
                continue
            logger.warning("Expiring uncompleted P/D ticket %s", d_reservation_id)
            await self.release(d_reservation_id)

    async def on_reservations_created(self, reservations: List[Dict[str, Any]]) -> None:
        """Apply standard KV reservations to the matching P/D pool."""
        by_pool = {"prefill": [], "decode": []}
        for reservation in reservations:
            pool = reservation.get("pool")
            if pool in by_pool:
                by_pool[pool].append(reservation)
            else:
                logger.warning(
                    "Ignoring P/D reservation without a valid pool: %s", pool
                )
        await asyncio.gather(
            self.prefill.apply_reservations_from_peer(by_pool["prefill"]),
            self.decode.apply_reservations_from_peer(by_pool["decode"]),
        )

    async def on_reservation_events(self, events: List[PDReservationEvent]) -> None:
        """Apply transfer-safe P completion and compensation from another router."""
        for event in events:
            if event["source_ingress_replica_id"] == self._ingress_replica_id:
                continue
            if event["event"] == "prefill_complete":
                await self.prefill.on_prefill_complete(event["p_reservation_id"])
                await self.prefill.on_request_completed(event["p_reservation_id"])
                await self.decode.on_prefill_complete(event["d_reservation_id"])
            elif event["event"] == "release":
                await self.prefill.on_request_completed(event["p_reservation_id"])
                await self.decode.on_request_completed(event["d_reservation_id"])
            else:
                logger.warning(
                    "Ignoring unknown P/D reservation event %s", event["event"]
                )

    async def on_decode_lifecycle_events(
        self, worker_id: int, events: List[tuple]
    ) -> None:
        """Apply decode progress/completion events to the matching D ticket."""
        for hook_name, args in events:
            if not args:
                continue
            request_id = args[0]
            d_reservation_id = self._d_reservation_by_request.get(request_id)
            ticket = (
                self._tickets_by_d_reservation.get(d_reservation_id)
                if d_reservation_id is not None
                else None
            )
            if ticket is not None and ticket.d_route.get("worker_id") not in (
                None,
                worker_id,
            ):
                continue
            if hook_name == "on_decode_progress" and len(args) == 2:
                await self.decode.on_decode_progress(request_id, args[1])
            elif hook_name == "on_request_completed":
                if ticket is not None:
                    await self.release(ticket.d_reservation_id)
                else:
                    await self.decode.on_request_completed(request_id)

    def _set_ticket_expiry(self, ticket: PDTicket, ttl_s: float) -> None:
        ticket.expires_at = time.monotonic() + ttl_s
        ticket.expires_at_epoch_ms = int((time.time() + ttl_s) * 1000)
        self._schedule_expiry(ticket)

    def _schedule_expiry(self, ticket: PDTicket) -> None:
        heapq.heappush(
            self._ticket_expiries,
            (ticket.expires_at, ticket.d_reservation_id),
        )

    def _report_event(
        self, event: str, p_reservation_id: str, d_reservation_id: str
    ) -> None:
        if self._event_forwarder is None or self._ingress_replica_id is None:
            return
        self._event_forwarder.report(
            {
                "source_ingress_replica_id": self._ingress_replica_id,
                "event": event,
                "p_reservation_id": p_reservation_id,
                "d_reservation_id": d_reservation_id,
            }
        )

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
