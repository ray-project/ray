import asyncio
import hashlib
import logging
import math
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, TypedDict

import ray
from ray import serve
from ray._common.network_utils import find_free_port
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_INDEXER_THREADS,
    DEFAULT_SESSION_AFFINITY_TTL_S,
    KV_INDEXER_THREADS_KEY,
    KV_REPLICA_SYNC_METADATA_KEY,
    KV_TOKEN_METADATA_KEY,
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

# Attempts to bind the replica mesh on a free port before giving up.
_REPLICA_SYNC_BIND_ATTEMPTS = 3

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


def get_token_endpoint(routing_stats: Dict[str, Any]) -> Optional[str]:
    """Return the prompt-token endpoint a replica advertises in its routing stats."""
    token_metadata = routing_stats.get(KV_TOKEN_METADATA_KEY)
    if isinstance(token_metadata, dict):
        return token_metadata.get("endpoint") or None
    return None


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
    5. Engines report a request's lifecycle events to the LLMRouter that
       selected it. Its selection service replicates the booking, prefill
       completion, free and session bindings to peer LLMRouters over Dynamo's
       replica mesh, so all routers score on the same load and pin sessions
       alike. The mesh syncs on Dynamo's threads, off the router's event loop,
       which stays free for routing.
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
        # Peer routers' replica-mesh endpoints, from the latest LongPoll update.
        self._replica_sync_peers: Set[str] = set()
        self._replica_sync_lock = asyncio.Lock()
        # This LLMRouter replica. The other replicas of its deployment are its
        # mesh peers.
        self._router_replica_id = serve.get_replica_context().replica_id
        self._lifecycle_receiver = None
        self._lifecycle_endpoint: Optional[str] = None
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
        # Connects to the token endpoints of newly joined workers.
        self._on_token_endpoints: Optional[Callable[[List[str]], None]] = None
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
        self._pending_tasks: Set[asyncio.Task] = set()
        self._long_poll_client: Optional[LongPollClient] = None
        self._create_selection_service()
        self._start_replica_tracking()

    def get_block_size(self) -> int:
        """Return the KV-cache block size used for decode-block accounting."""
        return self._block_size

    def subscribe_token_endpoints(self, callback: Callable[[List[str]], None]) -> None:
        """Call ``callback`` with the token endpoints of workers as they join.

        Must be called before the event loop runs the first replica update.
        """
        self._on_token_endpoints = callback

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

        # Dynamo rejects port 0. Until it supports binding 0 and returning the
        # assigned port, probing and binding race; retry on a fresh port.
        for attempt in range(1, _REPLICA_SYNC_BIND_ATTEMPTS + 1):
            self._replica_sync_port = find_free_port()
            try:
                # Session bindings replicate over the replica mesh with the load.
                self._svc = SelectionService(
                    indexer_threads=self._indexer_threads,
                    replica_sync_port=self._replica_sync_port,
                    session_affinity_ttl_secs=self._session_affinity_ttl_s or None,
                    session_affinity_mode=SESSION_AFFINITY_MODE,
                )
                break
            except Exception as e:
                if attempt == _REPLICA_SYNC_BIND_ATTEMPTS:
                    raise
                logger.warning(
                    "Failed to create the selection service on replica mesh "
                    "port %d; retrying on another port: %s",
                    self._replica_sync_port,
                    e,
                )
        logger.info(
            "Dynamo SelectionService created (indexer threads %d, replica mesh "
            "port %s).",
            self._indexer_threads,
            self._replica_sync_port,
        )

    def _start_replica_tracking(self) -> None:
        """Subscribe to this deployment's running replicas via LongPollClient."""
        deployment_id = self._serve_deployment_id
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
        key_listeners = {
            # LLMServer replicas: discover KV-event endpoints and add/remove
            # the workers that can be selected for requests.
            (
                LongPollNamespace.DEPLOYMENT_TARGETS,
                deployment_id,
            ): self._on_deployment_targets,
            # LLMRouter replicas: discover mesh endpoints so selectors share
            # load reservations and session bindings.
            (
                LongPollNamespace.DEPLOYMENT_TARGETS,
                self._router_replica_id.deployment_id,
            ): self._on_router_targets,
        }
        self._long_poll_client = LongPollClient(
            controller,
            key_listeners,
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
        token_endpoints: Dict[int, str] = {}
        for replica in target_info.running_replicas:
            worker_id = get_worker_id(replica.replica_id.unique_id)
            kv_event_metadata = replica.routing_stats.get("kv_event_metadata")
            if kv_event_metadata is not None:
                members[worker_id] = (
                    replica.replica_id.to_full_id_str(),
                    kv_event_metadata,
                )
                token_endpoint = get_token_endpoint(replica.routing_stats)
                if token_endpoint:
                    token_endpoints[worker_id] = token_endpoint

        registered = set(self._replica_id_by_worker)
        added = members.keys() - registered
        removed = registered - members.keys()

        for worker_id in removed:
            self.remove_worker(worker_id)
            self._replica_id_by_worker.pop(worker_id, None)
        new_token_endpoints = []
        for worker_id in added:
            replica_id, kv_event_metadata = members[worker_id]
            self._register_block_size(kv_event_metadata["block_size"], replica_id)
            self._replica_id_by_worker[worker_id] = replica_id
            if worker_id in token_endpoints:
                new_token_endpoints.append(token_endpoints[worker_id])
            self._schedule(
                self._upsert_worker(worker_id, replica_id, kv_event_metadata)
            )
        if new_token_endpoints and self._on_token_endpoints is not None:
            self._on_token_endpoints(new_token_endpoints)

        if added or removed:
            logger.info(
                "KV router replica membership updated: +%d -%d, tracking %d worker(s).",
                len(added),
                len(removed),
                len(self._replica_id_by_worker),
            )

    @property
    def lifecycle_endpoint(self) -> Optional[str]:
        """Where engines report lifecycle events of requests this router
        selected, or None until ``start_lifecycle_receiver``."""
        return self._lifecycle_endpoint

    async def start_lifecycle_receiver(self) -> None:
        """Receive the lifecycle events of requests this router selected."""
        from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
            LifecycleReceiver,
        )

        receiver = LifecycleReceiver(handler=self.on_lifecycle_events)
        port = await receiver.start()
        self._lifecycle_receiver = receiver
        self._lifecycle_endpoint = f"tcp://{ray.util.get_node_ip_address()}:{port}"

    async def shutdown(self) -> None:
        """Stop membership updates and release lifecycle and mesh sockets."""
        if self._long_poll_client is not None:
            self._long_poll_client.stop()
            self._long_poll_client = None
        tasks = list(self._pending_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._lifecycle_receiver is not None:
            await self._lifecycle_receiver.close()
            self._lifecycle_receiver = None
            self._lifecycle_endpoint = None
        if self._svc is not None:
            await self._svc.shutdown_async()
            self._svc = None

    def replica_sync_routing_stats(self) -> Dict[str, Any]:
        """Routing stats advertising this router's replica-mesh endpoint."""
        if self._svc is None:
            return {}
        endpoint = f"tcp://{ray.util.get_node_ip_address()}:{self._replica_sync_port}"
        return {KV_REPLICA_SYNC_METADATA_KEY: {"endpoint": endpoint}}

    def _on_router_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: keep the selection service's replica-mesh peers in
        sync with the running LLMRouter replicas that advertise an endpoint."""
        peers: Set[str] = set()
        for replica in target_info.running_replicas:
            metadata = replica.routing_stats.get(KV_REPLICA_SYNC_METADATA_KEY)
            if replica.replica_id == self._router_replica_id or not isinstance(
                metadata, dict
            ):
                continue
            if metadata.get("endpoint"):
                peers.add(metadata["endpoint"])
        if peers == self._replica_sync_peers:
            return
        self._replica_sync_peers = peers
        self._schedule(self._sync_replica_peers())

    async def _sync_replica_peers(self) -> None:
        """Reconcile the selection service's peers with ``_replica_sync_peers``.

        Scheduled only when LongPoll changes the peer endpoint set, never by
        request selection. Only membership reconciliation takes this lock.

        Idempotent: each pass diffs the desired peers against the service's
        actual peers, so a repeated or stale update converges. The lock
        serializes passes, because Dynamo applies peer calls asynchronously and an
        overlapping pass would diff against peers that haven't updated yet. ZMQ
        redials a router restarted on the same endpoint, so it keeps its connection.
        """
        async with self._replica_sync_lock:
            peers = self._replica_sync_peers
            connected = set(self._svc.list_replica_peers())
            removed, added = connected - peers, peers - connected
            results = await asyncio.gather(
                *map(self._svc.deregister_replica_peer, removed),
                *map(self._svc.register_replica_peer, added),
                return_exceptions=True,
            )
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Failed to update a KV replica mesh peer: %s", result)
        if removed or added:
            logger.info(
                "KV replica mesh peers updated: +%d -%d, %d peer(s).",
                len(added),
                len(removed),
                len(peers),
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
        reserve = asyncio.ensure_future(self._svc.select_and_reserve(request))
        try:
            selection = await asyncio.shield(reserve)
        except asyncio.CancelledError:
            # The caller can give up (HAProxy times out a slow route) after the
            # service booked the request but before this coroutine resumes. Free
            # that booking, or it stays booked until the service's own expiry.
            self._schedule(self._free_abandoned_reservation(request_id, reserve))
            raise
        self._track_request_state(
            request_id,
            selection["worker_id"],
            len(token_ids),
            expected_output_tokens,
        )
        return {
            "worker_id": selection["worker_id"],
            "dp_rank": selection["dp_rank"],
            "overlap_tokens": selection["overlap"]["longest_matched"],
            "effective_prefill_tokens": selection["effective_prefill_tokens"],
        }

    async def _free_abandoned_reservation(
        self, request_id: str, reserve: "asyncio.Future"
    ) -> None:
        """Free the booking of a selection whose caller was cancelled."""
        try:
            await reserve
        except Exception:
            return  # The selection failed, so nothing was booked.
        # Assumes request ids are unique among in-flight requests, so freeing by
        # id can't release a later request's booking that reused this id.
        try:
            await self._svc.free_reservation(request_id)
        except Exception:
            logger.warning(
                "Failed to free the reservation of abandoned request %s.",
                request_id,
                exc_info=True,
            )

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
        if state is None:
            return
        self._untrack_worker_request(request_id, state.worker_id)
        await self._svc.free_reservation(request_id)

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
            try:
                await self._svc.free_reservation(request_id)
            except Exception as e:
                # Dynamo's own expiry may have freed it already.
                logger.debug("Stale KV request %s was already freed: %s", request_id, e)

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
