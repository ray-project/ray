import asyncio
import hashlib
import logging
import math
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, TypedDict

import ray
from ray import serve
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_INDEXER_THREADS,
    DEFAULT_SESSION_AFFINITY_TTL_S,
    KV_INDEXER_THREADS_KEY,
    LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
    REQUEST_TRACKING_TTL_S,
    SESSION_AFFINITY_MODE,
)
from ray.serve._private.common import DeploymentTargetInfo
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Dynamo's selection service keys all worker, indexer, and load state by
# (model_name, routing_group). KVTokenTracker instantiates a selection service and
# serves exactly one model, so a single fixed key scopes all of its workers
# together.
_MODEL_NAME = "default"
_ROUTING_GROUP = "default"

# Hooks a replica may invoke through ``KVTokenTracker.on_lifecycle_events``.
LIFECYCLE_HOOKS = frozenset(
    {
        "on_prefill_complete",
        "on_decode_progress",
        "on_request_completed",
    }
)


def get_worker_id(replica_unique_id: str) -> int:
    """Deterministically derive a Dynamo worker id from a replica's unique id."""
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


@dataclass
class RequestLifecycle:
    """In-flight request load state while the request is served by a replica."""

    worker_id: int
    prompt_tokens: int = 0
    # Client-provided output-length estimate (``sampling_params.max_tokens``);
    # weights each decode block's load by how much generation remains.
    expected_output_tokens: Optional[int] = None
    prefill_completed: bool = False
    output_tokens: int = 0
    # Running count of KV blocks (prompt + output) the request occupies; the
    # cursor for booking each newly crossed decode block.
    total_blocks: int = 0
    # Monotonic admission time, for the TTL eviction sweep.
    created_at: float = field(default_factory=time.monotonic)


class WorkerSelection(TypedDict):
    """The worker chosen by ``KVTokenTracker.select_worker`` for a request."""

    # The chosen LLMServer worker.
    worker_id: int
    # Data-parallel rank within the worker.
    dp_rank: int
    # Matched prompt tokens available on the selected worker.
    overlap_tokens: int
    # Prompt tokens that still need prefill on the selected worker.
    effective_prefill_tokens: int


class PeerUpdateForwarder:
    """Broadcast this ingress's routing state to its peer ingress replicas.

    Each update is one of:
      1. ``load_reservation``: the booking for a request this ingress routed,
         so every peer counts the same token load on the selected worker.
      2. ``session_affinity``: a session binding this ingress committed or
         refreshed, so every peer pins the session's later turns to that worker.

    ``report`` only enqueues. A background task batches pending updates into
    one broadcast, off the request's selection and dispatch path.
    """

    def __init__(self, handle: Any, source_writer_id: int):
        self._handle = handle
        # Breaks ties when ingresses bind one session concurrently (newest
        # (sequence, writer_id) wins); receivers also skip their own broadcasts.
        self._source_writer_id = source_writer_id
        if getattr(handle, "is_initialized", True) is False:
            # Keep broadcast routing off the ingress replica's request loop.
            handle._init(_run_router_in_separate_loop=True)
        self._updates: asyncio.Queue = asyncio.Queue()
        self._delivery_task: Optional[asyncio.Task] = None

    def report(self, kind: str, event: Dict[str, Any]) -> None:
        if self._delivery_task is None or self._delivery_task.done():
            self._delivery_task = asyncio.get_running_loop().create_task(
                self._deliver()
            )
        self._updates.put_nowait({"kind": kind, "event": event})

    async def _deliver(self) -> None:
        while True:
            batch = [await self._updates.get()]
            while not self._updates.empty():
                batch.append(self._updates.get_nowait())
            try:
                results = await self._handle.broadcast(
                    "on_replica_updates", self._source_writer_id, batch
                ).results_async(
                    timeout_s=LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
                    return_exceptions=True,
                )
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    logger.warning(
                        "KV peer broadcasts dropped on %d/%d ingress replicas: %s",
                        len(errors),
                        len(results),
                        errors[0],
                    )
            except Exception as e:
                logger.warning("Dropping selection service peer broadcast: %s", e)
            finally:
                for _ in batch:
                    self._updates.task_done()

    async def flush(self) -> None:
        """Wait until every reported replica broadcast has been attempted."""
        await self._updates.join()

    def close(self) -> None:
        if self._delivery_task is not None:
            self._delivery_task.cancel()
            self._delivery_task = None


class KVTokenTracker:
    """Tracks per-replica KV-cache overlap, token load, and session affinity
    inside the LLMRouter ingress replica.

    Built by the LLMRouter ingress replica via ``build_kv_token_tracker``, so
    ``select_worker`` is a local call on the ingress event loop.

    1. Owns a router-local Dynamo ``SelectionService``.
    2. Tracks live replicas via a ``LongPollClient`` on ``DEPLOYMENT_TARGETS``,
       mapping each running replica to a Dynamo worker id.
    3. The ``SelectionService`` maintains a global KV index radix tree, fed by
       every replica's KV events; each node records which workers hold that KV block.
    4. Scoring (``select_worker``) atomically ranks candidate workers by
       KV-cache overlap and current token load, reserves the chosen worker, and
       records local lifecycle state. A request with a session id reuses its
       session's bound worker while that worker remains a candidate; otherwise
       the selected worker becomes the session's new binding. A separate
       select-then-reserve flow causes herding because concurrent requests can
       select the same worker from stale load state before any reservation is
       visible.
    5. Load reservations and session affinity bindings are replicated to peer
       ingress replicas in the background (see ``PeerUpdateForwarder``), so
       every selection service applies the engine's subsequent lifecycle
       events and pins a session's later turns to the same worker.
    """

    def __init__(
        self,
        indexer_threads: int = DEFAULT_KV_INDEXER_THREADS,
        serve_deployment_id: Optional[Any] = None,
        session_affinity_ttl_s: float = DEFAULT_SESSION_AFFINITY_TTL_S,
    ):
        # The tracked LLMServer deployment id, passed in by the LLMRouter
        # that builds the tracker.
        self._serve_deployment_id = serve_deployment_id
        # KV-cache block size, learned once from the first replica's reported
        # engine config and passed to the selection service, which uses it to
        # track the worker's active load and index its KV blocks for overlap.
        self._block_size: Optional[int] = None
        self._indexer_threads = indexer_threads
        self._session_affinity_ttl_s = session_affinity_ttl_s
        # _replica_id_by_worker maps a Dynamo worker id to the running replica's full
        # id string, kept in sync with the deployment's live replicas over LongPoll.
        # NOTE (jeffreywang): _replica_id_by_worker is later used by select_worker
        # to get candidate workers to route among.
        self._replica_id_by_worker: Dict[int, str] = {}
        # Per-request state that the lifecycle hooks need, keyed by request id, serves
        # the following purposes:
        #   1. Block cursor: Turn cumulative decode tokens into add_output_block deltas.
        #   2. expected_output_tokens for decode-block decay weighting.
        #   3. In-flight request set: Free reservation exactly once.
        # Ordered oldest-first so the TTL sweep pops stale entries off the front.
        self._requests: "OrderedDict[str, RequestLifecycle]" = OrderedDict()
        # Reverse index of in-flight request ids per worker, kept in lockstep with
        # _requests, so remove_worker is O(k) in the worker's requests, not O(N).
        self._request_ids_by_worker: Dict[int, Set[str]] = {}
        # Request ids whose completion arrived before their reservation broadcast.
        # Ordered oldest-first so the stale sweep can bound memory.
        self._completed_request_ids: "OrderedDict[str, float]" = OrderedDict()
        self._pending_tasks: Set[asyncio.Task] = set()
        # Identifies this ingress: tags its broadcasts so it skips its own, and
        # versions the session affinity bindings it writes.
        self._replication_writer_id = get_worker_id(uuid.uuid4().hex)
        # Sends this ingress's load reservations and session affinity bindings to peers.
        self._replica_forwarder: Optional[PeerUpdateForwarder] = None
        # Streams session affinity bindings from the selection service to the forwarder.
        self._session_affinity_task: Optional[asyncio.Task] = None
        # Peer load reservations and session affinity bindings awaiting apply.
        self._replica_updates: asyncio.Queue = asyncio.Queue()
        # Applies queued peer updates off the RPC path.
        self._replica_apply_task: Optional[asyncio.Task] = None
        self._long_poll_client: Optional[LongPollClient] = None
        self._create_selection_service()
        self._start_replica_tracking()

    def get_block_size(self) -> int:
        """Return the KV-cache block size used for decode-block accounting."""
        return self._block_size

    def start_replica_broadcast(self, handle: Any) -> None:
        """Broadcast load reservations and session affinity bindings to peers."""
        if self._svc is not None:
            self._replica_forwarder = PeerUpdateForwarder(
                handle, self._replication_writer_id
            )
            if self._session_affinity_ttl_s:
                self._session_affinity_task = asyncio.create_task(
                    self._forward_session_bindings()
                )

    async def _forward_session_bindings(self) -> None:
        # One consumer per service; the forwarder fans out to peer ingress replicas.
        async for bindings in self._svc.session_binding_updates():
            for binding in bindings:
                self._replica_forwarder.report("session_affinity", binding)

    async def on_replica_updates(
        self, source_writer_id: int, updates: List[Dict[str, Any]]
    ) -> None:
        """Queue a peer's load reservations and session affinity bindings
        without blocking this ingress's request handling."""
        if self._svc is None or source_writer_id == self._replication_writer_id:
            return
        if self._replica_apply_task is None or self._replica_apply_task.done():
            self._replica_apply_task = asyncio.create_task(
                self._apply_replica_updates()
            )
        self._replica_updates.put_nowait(updates)

    async def _apply_replica_updates(self) -> None:
        while True:
            batches = [await self._replica_updates.get()]
            while not self._replica_updates.empty():
                batches.append(self._replica_updates.get_nowait())
            try:
                await self._evict_stale_requests()
                bookings = []
                for update in [update for batch in batches for update in batch]:
                    if update["kind"] == "session_affinity":
                        self._apply_session_binding(update["event"])
                    else:
                        bookings.append(update["event"])
                # Book the round's reservations together: awaiting one at a time
                # costs an event-loop round trip per booking, so under load this
                # replica falls behind its peers and skips bookings whose requests
                # completed in the meantime.
                await asyncio.gather(*map(self._apply_reservation, bookings))
            except Exception:
                logger.exception("Failed to apply selection service peer updates.")
            finally:
                for _ in batches:
                    self._replica_updates.task_done()

    def _apply_session_binding(self, binding: Dict[str, Any]) -> None:
        try:
            self._svc.apply_session_binding(binding)
        except Exception:
            logger.exception("Failed to apply a peer session affinity binding.")

    async def _apply_reservation(self, booking: Dict[str, Any]) -> None:
        # A failed booking must not fail the rest of the round.
        try:
            request_id = booking["selection_id"]
            if (
                request_id in self._requests
                or request_id in self._completed_request_ids
            ):
                return
            await self._svc.create_reservation(booking)
            # Completion can arrive while Dynamo is admitting the mirrored load.
            if request_id in self._completed_request_ids:
                await self._svc.free_reservation(request_id)
                return
            self._track_request_state(
                request_id,
                booking["worker_id"],
                booking["isl_tokens"],
                booking["expected_output_tokens"],
            )
        except Exception:
            logger.exception("Failed to apply a peer load reservation.")

    def _create_selection_service(self) -> None:
        """Create the router-local Dynamo selection service for this deployment."""
        # Imported here, not at module scope, to keep Dynamo's pyo3 extension off
        # the import path of every process that imports this module; only the
        # ingress replica that builds the tracker needs it.
        try:
            from dynamo.llm import SelectionService
        except ImportError:
            self._svc = None
            logger.warning(
                "ai-dynamo is not installed; KV-aware routing requires ai-dynamo."
            )
            return

        self._svc = SelectionService(
            indexer_threads=self._indexer_threads,
            session_affinity_ttl_secs=self._session_affinity_ttl_s or None,
            session_affinity_mode=SESSION_AFFINITY_MODE,
            session_replication_writer_id=(
                self._replication_writer_id if self._session_affinity_ttl_s else None
            ),
        )
        logger.info(
            "Dynamo SelectionService created (indexer threads %d).",
            self._indexer_threads,
        )

    def _start_replica_tracking(self) -> None:
        """Subscribe to this deployment's running replicas via LongPollClient."""
        deployment_id = self._serve_deployment_id
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
        self._long_poll_client = LongPollClient(
            controller,
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    deployment_id,
                ): self._on_deployment_targets,
            },
            # Built inside the LLMRouter ingress replica's async __init__, so this
            # binds LongPoll callbacks to that event loop.
            call_in_event_loop=asyncio.get_running_loop(),
            client_id=f"{type(self).__name__}:{deployment_id}",
        )

    def _schedule(self, awaitable) -> None:
        """Schedule an awaitable (coroutine or future) on the ingress replica's
        event loop, holding a reference until it completes.
        """

        async def _run():
            await awaitable

        task = asyncio.create_task(_run())
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    def _register_block_size(self, block_size: int, replica_id: str) -> None:
        """Pin the deployment's KV-cache block size from the first replica's
        reported engine config.
        """
        if self._block_size is None:
            self._block_size = block_size
            logger.info("KV router block size set to %d.", block_size)
        elif block_size != self._block_size:
            # Replicas of a deployment are expected to resolve the same block
            # size, so a mismatch is unexpected. We still register the worker so
            # the selection service spawns its KV-event listener, but the indexer
            # only ingests blocks whose size matches the pinned block size, so a
            # genuinely mismatched replica's KV events would be dropped (its KV
            # cache never indexed).
            logger.error(
                "Replica %s reports KV block size %d but the KV router is "
                "pinned at %d; registering it at the pinned size (replicas of a "
                "deployment are expected to agree).",
                replica_id,
                block_size,
                self._block_size,
            )

    def _on_deployment_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: reconcile tracked workers against the running-replica
        snapshot.

        Each replica advertises its KV-events endpoint via ``record_routing_stats``
        (carried in ``RunningReplicaInfo.routing_stats``); newly advertised replicas
        are registered with the selection service and departed ones evicted.
        """
        members: Dict[int, tuple] = {}
        for replica in target_info.running_replicas:
            worker_id = get_worker_id(replica.replica_id.unique_id)
            kv_event_metadata = replica.routing_stats.get("kv_event_metadata")
            if kv_event_metadata is not None:
                members[worker_id] = (
                    replica.replica_id.to_full_id_str(),
                    kv_event_metadata,
                )

        registered = set(self._replica_id_by_worker)
        added = members.keys() - registered
        removed = registered - members.keys()

        for worker_id in removed:
            self.remove_worker(worker_id)
            self._replica_id_by_worker.pop(worker_id, None)
        for worker_id in added:
            replica_id, kv_event_metadata = members[worker_id]
            self._register_block_size(kv_event_metadata["block_size"], replica_id)
            self._replica_id_by_worker[worker_id] = replica_id
            self._schedule(
                self._upsert_worker(worker_id, replica_id, kv_event_metadata)
            )

        if added or removed:
            logger.info(
                "KV router replica membership updated: +%d -%d, tracking %d worker(s).",
                len(added),
                len(removed),
                len(self._replica_id_by_worker),
            )

    def remove_worker(self, worker_id: int) -> None:
        """Evict a departed replica's worker and its KV blocks from the
        selection service.
        """
        # Drop the departed replica's in-flight requests; their completions can
        # never arrive, so they would otherwise leak. delete_worker below frees
        # their load in the service, so no per-request free_reservation is needed.
        for request_id in self._request_ids_by_worker.pop(worker_id, set()):
            self._requests.pop(request_id, None)
        if self._svc is None:
            return
        self._schedule(self._svc.delete_worker(worker_id))

    async def _upsert_worker(
        self, worker_id: int, replica_id: str, kv_event_metadata: Dict[str, Any]
    ) -> None:
        """Register a replica's KV-event endpoint with the selection service.

        The selection service spawns a connect-out ZMQ listener to the
        replica's ``endpoint`` and indexes its live KV events.
        """
        if self._svc is None:
            return
        dp_rank = kv_event_metadata["dp_rank"]
        await self._svc.upsert_worker(
            {
                "worker_id": worker_id,
                "model_name": _MODEL_NAME,
                "routing_group": _ROUTING_GROUP,
                # NOTE: SelectionService requires endpoint to be non-empty although it's left
                # unused under an external runtime like Ray Serve LLM.
                # TODO (jeffreywang): Allow empty endpoints upstream.
                "endpoint": f"ray://{replica_id}",
                "block_size": self._block_size,
                # NOTE: max_num_batched_tokens is a proxy of load capacity for load-based
                # scoring in the selection service.
                "max_num_batched_tokens": kv_event_metadata["max_num_batched_tokens"],
                "data_parallel_start_rank": dp_rank,
                # TODO (jeffreywang): Support KV-aware routing for data parallel deployments.
                "data_parallel_size": 1,
                "kv_events_endpoints": {dp_rank: kv_event_metadata["endpoint"]},
                # The listener dials this on a sequence gap (slow-joiner) to replay
                # the events it missed before its SUB connected; without it those
                # events are dropped and never indexed.
                "replay_endpoint": kv_event_metadata.get("replay_endpoint"),
            }
        )
        logger.info(
            "Registered KV event worker %d for replica %s at %s.",
            worker_id,
            replica_id,
            kv_event_metadata["endpoint"],
        )

    async def select_worker(
        self,
        request_id: str,
        token_ids: List[int],
        allowed_worker_ids: List[int],
        expected_output_tokens: Optional[int] = None,
        lora_name: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> WorkerSelection:
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Session requests reuse their selected worker while it remains eligible
        and session affinity hasn't expired. Every request still reserves load.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.
            expected_output_tokens: The request's output-token cap. With
                select-time reservation this lets selection service decay decode
                load without per-token progress events.
            lora_name: The adapter name vLLM stamps on the request's KV-cache
                events. The selection service salts its KV hashes with it, so
                omitting it on a LoRA request scores against the wrong
                namespace and never matches.
            session_id: The request's session, if any.

        Returns:
            The selected worker (see ``WorkerSelection``).
        """
        if token_ids is None or len(token_ids) == 0:
            raise ValueError("KV aware routing requires non-empty token_ids.")

        if self._svc is None:
            # ai-dynamo is not installed, so this deployment cannot score requests.
            # Fail fast and surface RuntimeError to the client as a 503 via LLMRouter.
            raise RuntimeError(
                "KV-aware routing is unavailable because ai-dynamo is not "
                "installed in the deployment's environment."
            )
        await self._evict_stale_requests()
        request = {
            "model_name": _MODEL_NAME,
            "routing_group": _ROUTING_GROUP,
            "selection_id": request_id,
            "token_ids": token_ids,
            "allowed_worker_ids": allowed_worker_ids,
            "expected_output_tokens": expected_output_tokens,
            "lora_name": lora_name,
            "session_id": session_id,
        }
        selection = await self._svc.select_and_reserve(request)
        self._track_request_state(
            request_id,
            selection["worker_id"],
            len(token_ids),
            expected_output_tokens,
        )
        if self._replica_forwarder is not None:
            self._replica_forwarder.report(
                "load_reservation",
                {
                    "model_name": _MODEL_NAME,
                    "routing_group": _ROUTING_GROUP,
                    "selection_id": request_id,
                    "worker_id": selection["worker_id"],
                    "dp_rank": selection["dp_rank"],
                    "sequence_hashes": selection["sequence_hashes"],
                    "isl_tokens": selection["isl_tokens"],
                    "expected_output_tokens": expected_output_tokens,
                    "effective_prefill_tokens": selection["effective_prefill_tokens"],
                    "lora_name": lora_name,
                },
            )
        return {
            "worker_id": selection["worker_id"],
            "dp_rank": selection["dp_rank"],
            "overlap_tokens": selection["overlap"]["longest_matched"],
            "effective_prefill_tokens": selection["effective_prefill_tokens"],
        }

    def _track_request_state(
        self,
        request_id: str,
        worker_id: int,
        prompt_tokens: int,
        expected_output_tokens: Optional[int],
    ) -> None:
        old = self._requests.pop(request_id, None)
        if old is not None:
            self._untrack_worker_request(request_id, old.worker_id)
        self._completed_request_ids.pop(request_id, None)
        if self._block_size is None:
            raise RuntimeError(
                "KV block size is unavailable before worker registration."
            )
        block_size = self._block_size
        self._requests[request_id] = RequestLifecycle(
            worker_id=worker_id,
            prompt_tokens=prompt_tokens,
            expected_output_tokens=expected_output_tokens,
            total_blocks=math.ceil(prompt_tokens / block_size),
        )
        self._request_ids_by_worker.setdefault(worker_id, set()).add(request_id)

    async def on_lifecycle_events(self, events: List[tuple]) -> None:
        """Apply a replica's ``(hook_name, args)`` lifecycle events in order.

        The hooks are order-sensitive (e.g. a completion arriving before its
        admission would resurrect an evicted request) so a replica sends its
        events in submission order, batched into one call.
        """
        if self._svc is None:
            return

        for hook_name, args in events:
            if hook_name not in LIFECYCLE_HOOKS:
                logger.warning("Ignoring unknown lifecycle hook %s", hook_name)
                continue
            try:
                await getattr(self, hook_name)(*args)
            except Exception:
                # One hook raising must not abort the batch and drop other events.
                logger.exception(
                    "KV lifecycle hook %s failed; skipping it and continuing.",
                    hook_name,
                )

    async def on_prefill_complete(self, request_id: str) -> None:
        """Record a request's prefill -> decode transition, dropping its prefill
        load in the selection service."""
        state = self._requests.get(request_id)
        if state is None:
            return
        state.prefill_completed = True
        await self._svc.prefill_complete(request_id)

    async def on_decode_progress(
        self, request_id: str, cumulative_output_tokens: int
    ) -> None:
        """Advance ``request_id`` to an exact cumulative output-token count,
        booking one decode block in the selection service per crossed boundary.
        """
        state = self._requests.get(request_id)
        if state is None:
            return
        state.output_tokens = cumulative_output_tokens
        new_total_blocks = math.ceil(
            (state.prompt_tokens + cumulative_output_tokens) / self._block_size
        )
        decay_fraction = self._get_decay_fraction(state)
        while new_total_blocks > state.total_blocks:
            state.total_blocks += 1
            self._svc.add_output_block(request_id, decay_fraction=decay_fraction)

    async def on_request_completed(self, request_id: str) -> None:
        """Free ``request_id`` from the selection service's active load and the
        local view."""
        state = self._requests.pop(request_id, None)
        self._mark_request_completed(request_id)
        if state is None:
            return
        self._untrack_worker_request(request_id, state.worker_id)
        await self._svc.free_reservation(request_id)

    def _mark_request_completed(self, request_id: str) -> None:
        """Remember completions that beat reservation admission on this ingress."""
        self._completed_request_ids.pop(request_id, None)
        self._completed_request_ids[request_id] = time.monotonic()

    def _untrack_worker_request(self, request_id: str, worker_id: int) -> None:
        """Drop a request from the per-worker reverse index, keeping it in
        lockstep with ``_requests``."""
        request_ids = self._request_ids_by_worker.get(worker_id)
        if request_ids is not None:
            request_ids.discard(request_id)
            if not request_ids:
                del self._request_ids_by_worker[worker_id]

    async def _evict_stale_requests(self) -> None:
        """Backstop for a lost completion on a live replica: evict requests tracked
        past ``REQUEST_TRACKING_TTL_S``, freeing their reservations.
        """
        cutoff = time.monotonic() - REQUEST_TRACKING_TTL_S
        while self._completed_request_ids:
            request_id, completed_at = next(iter(self._completed_request_ids.items()))
            if completed_at > cutoff:
                break
            self._completed_request_ids.pop(request_id, None)
        while self._requests:
            request_id, state = next(iter(self._requests.items()))
            if state.created_at > cutoff:
                break
            self._requests.popitem(last=False)
            self._untrack_worker_request(request_id, state.worker_id)
            logger.warning(
                "Evicting stale KV request %s (tracked > %ds without completion); "
                "freeing its reservation.",
                request_id,
                REQUEST_TRACKING_TTL_S,
            )
            await self._svc.free_reservation(request_id)

    def _get_decay_fraction(self, state: RequestLifecycle) -> Optional[float]:
        """Fraction of output still expected, or ``None`` without an estimate;
        weights each decode block by how much generation remains."""
        if not state.expected_output_tokens:
            return None
        return max(0.0, 1.0 - state.output_tokens / state.expected_output_tokens)


_KV_TOKEN_TRACKER: Optional["KVTokenTracker"] = None


def set_kv_token_tracker(tracker: "KVTokenTracker") -> None:
    global _KV_TOKEN_TRACKER
    _KV_TOKEN_TRACKER = tracker


def get_kv_token_tracker() -> Optional["KVTokenTracker"]:
    return _KV_TOKEN_TRACKER


# The LLMRouter ingress deployment name (``serve.deployment(LLMRouter)`` with no
# name override -> the class name). Engine replicas RPC its ``on_lifecycle_events``
# handle method to book request load.
LLM_ROUTER_DEPLOYMENT_NAME = "LLMRouter"


def get_llm_router_handle():
    """Handle to the in-app LLMRouter deployment, for engine replicas to reach
    its ``on_lifecycle_events`` method. Resolved in the current Serve app.
    """
    app_name = serve.get_replica_context().app_name
    return serve.get_deployment_handle(LLM_ROUTER_DEPLOYMENT_NAME, app_name=app_name)


def build_kv_token_tracker(
    llm_config: "LLMConfig", serve_deployment_id: Any
) -> "KVTokenTracker":
    """Build the ``KVTokenTracker`` and register it in this process's global
    so the same-process ``KVAwareRouter`` can reach it. Must be called from the
    ingress replica's event loop (the tracker binds a LongPollClient to it).
    """
    router_config = llm_config.deployment_config.get("request_router_config") or {}
    router_kwargs = (
        router_config.get("request_router_kwargs", {})
        if isinstance(router_config, dict)
        else router_config.request_router_kwargs
    )
    tracker = KVTokenTracker(
        session_affinity_ttl_s=router_kwargs.get(
            "session_affinity_ttl_s", DEFAULT_SESSION_AFFINITY_TTL_S
        ),
        indexer_threads=llm_config.experimental_configs.get(
            KV_INDEXER_THREADS_KEY, DEFAULT_KV_INDEXER_THREADS
        ),
        serve_deployment_id=serve_deployment_id,
    )
    set_kv_token_tracker(tracker)
    return tracker
