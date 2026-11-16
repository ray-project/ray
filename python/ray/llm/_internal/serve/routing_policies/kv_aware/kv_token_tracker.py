import asyncio
import hashlib
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    TypedDict,
)

import ray
from ray import serve
from ray._common.network_utils import find_free_port
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_INDEXER_THREADS,
    DEFAULT_SESSION_AFFINITY_TTL_S,
    KV_INDEXER_THREADS_KEY,
    KV_LIFECYCLE_METADATA_KEY,
    KV_REPLICA_SYNC_METADATA_KEY,
    KV_TOKEN_METADATA_KEY,
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
    SELECTION_MODEL_NAME,
    SELECTION_ROUTING_GROUP,
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

# Attempts to bind the replica mesh port before giving up.
_REPLICA_SYNC_BIND_ATTEMPTS = 3

# Backoff bounds for retrying a failed endpoint update.
_ENDPOINT_SYNC_RETRY_INITIAL_S = 1.0
_ENDPOINT_SYNC_RETRY_MAX_S = 30.0


def get_worker_id(replica_unique_id: str) -> int:
    """Deterministically derive a Dynamo worker id from a replica's unique id."""
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


def _get_endpoint(routing_stats: Dict[str, Any], key: str) -> Optional[str]:
    """Return the endpoint a replica advertises under ``key`` in its routing stats."""
    metadata = routing_stats.get(key)
    if isinstance(metadata, dict):
        return metadata.get("endpoint") or None
    return None


def get_token_endpoint(routing_stats: Dict[str, Any]) -> Optional[str]:
    """Return the prompt-token endpoint a replica advertises in its routing stats."""
    return _get_endpoint(routing_stats, KV_TOKEN_METADATA_KEY)


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


class _SubscriptionReconciler:
    """Keeps one set of selection service subscriptions (LLMServer lifecycle
    publishers, or other LLMRouters' replica mesh endpoints) equal to the
    endpoints in the latest LongPoll snapshot passed to ``update``.

    Invariants:
    - One background pass runs at a time.
    - Each pass diffs the latest snapshot against the service's actual set, so
      repeated or stale snapshots are idempotent.
    - Failed calls are retried with backoff; a new snapshot ends the wait.
    - The task exits only after a pass fully applies the latest snapshot.

    ``kind`` names the set in log messages.
    """

    def __init__(
        self,
        kind: str,
        list_endpoints: Callable[[], List[str]],
        register: Callable[[str], Awaitable[bool]],
        deregister: Callable[[str], Awaitable[bool]],
    ):
        self._kind = kind
        self._list = list_endpoints
        self._register = register
        self._deregister = deregister
        self._desired: Set[str] = set()
        # Set when ``_desired`` changes; cleared at the start of each pass.
        self._changed = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def update(self, desired: Set[str]) -> None:
        """Converge on ``desired`` in the background."""
        if desired == self._desired:
            return
        self._desired = desired
        self._changed.set()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._sync())

    async def _sync(self) -> None:
        delay = _ENDPOINT_SYNC_RETRY_INITIAL_S
        while True:
            self._changed.clear()
            desired = self._desired
            try:
                actual = set(self._list())
                removed, added = actual - desired, desired - actual
                results = await asyncio.gather(
                    *map(self._deregister, removed),
                    *map(self._register, added),
                    return_exceptions=True,
                )
            except Exception as e:
                removed, added, results = set(), set(), [e]
            if removed or added:
                logger.info(
                    "KV %ss updated: +%d -%d, %d expected.",
                    self._kind,
                    len(added),
                    len(removed),
                    len(desired),
                )
            errors = [result for result in results if isinstance(result, Exception)]
            if errors:
                logger.warning(
                    "Failed to update %d KV %s(s); retrying in %.0fs: %s",
                    len(errors),
                    self._kind,
                    delay,
                    errors[0],
                )
                # Retry after the backoff, or as soon as membership changes.
                try:
                    await asyncio.wait_for(self._changed.wait(), delay)
                except asyncio.TimeoutError:
                    pass
                delay = min(2 * delay, _ENDPOINT_SYNC_RETRY_MAX_S)
            elif not self._changed.is_set():
                return

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


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
       KV-cache overlap and current token load, and reserves the chosen
       worker. A request with a session id reuses its
       session's bound worker while that worker remains a candidate; otherwise
       the selected worker becomes the session's new binding. A separate
       select-then-reserve flow causes herding because concurrent requests can
       select the same worker from stale load state before any reservation is
       visible.
    5. The selection service replicates bookings and session bindings to peer
       LLMRouters over the replica mesh.
    6. Every LLMServer publishes its requests' lifecycle events and every
       router's selection service subscribes to all of them, so a request's
       load is released on every router even if the one that selected it is gone.

    Both run in the selection service, off the router's event loop. Only events
    sent after a subscription connects are delivered; the selection service
    expires a booking DYN_ROUTER_ACTIVE_REQUEST_EXPIRY_SECS (300 s by default)
    after it's made.
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
        # The other replicas of this LLMRouter deployment are its mesh peers.
        self._router_replica_id = serve.get_replica_context().replica_id
        # Set with the selection service.
        self._replica_peers: Optional[_SubscriptionReconciler] = None
        self._lifecycle_sources: Optional[_SubscriptionReconciler] = None
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
        self._autoscaling_metrics_callback: Optional[
            Callable[[Dict[str, int]], None]
        ] = None
        self._pending_tasks: Set[asyncio.Task] = set()
        self._long_poll_client: Optional[LongPollClient] = None
        self._stopping = False
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

    def set_autoscaling_metrics_callback(
        self, callback: Callable[[Dict[str, int]], None]
    ) -> None:
        """Set the local callback for router-owned autoscaling metrics."""
        self._autoscaling_metrics_callback = callback

    async def get_autoscaling_snapshot(self) -> Dict[str, int]:
        """Return the workload of all in-flight requests, as this router's
        selection service sees it."""
        return self._get_autoscaling_snapshot()

    def _get_autoscaling_snapshot(self) -> Dict[str, int]:
        # The selection service applies every router's bookings and every
        # LLMServer's lifecycle events, so this covers all routers' requests.
        # Its prefill estimate decays the oldest prefill per worker over the
        # prefill's expected duration.
        outstanding_prefill_tokens = 0
        if self._svc is not None:
            for model in self._svc.loads(
                model_name=SELECTION_MODEL_NAME, routing_group=SELECTION_ROUTING_GROUP
            ):
                for load in model["loads"]:
                    outstanding_prefill_tokens += load["potential_prefill_tokens"]
        return {OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: outstanding_prefill_tokens}

    def _publish_autoscaling_snapshot(self) -> None:
        if self._autoscaling_metrics_callback is None:
            return
        # A raising callback must not fail the selection that triggered it.
        try:
            self._autoscaling_metrics_callback(self._get_autoscaling_snapshot())
        except Exception:
            logger.exception("Autoscaling metrics callback failed.")

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

        # The selection service rejects port 0, so probe a free port and retry if
        # another process takes it first.
        for attempt in range(1, _REPLICA_SYNC_BIND_ATTEMPTS + 1):
            self._replica_sync_port = find_free_port()
            try:
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
                    "Failed to create the selection service on Dynamo mesh "
                    "port %d; retrying on another port: %s",
                    self._replica_sync_port,
                    e,
                )
        logger.info(
            "Selection service created (indexer threads %d, Dynamo mesh port %s).",
            self._indexer_threads,
            self._replica_sync_port,
        )
        self._replica_peers = _SubscriptionReconciler(
            "LLMRouter mesh peer",
            self._svc.list_replica_peers,
            self._svc.register_replica_peer,
            self._svc.deregister_replica_peer,
        )
        self._lifecycle_sources = _SubscriptionReconciler(
            "LLMServer -> LLMRouter lifecycle event source",
            self._svc.list_worker_publishers,
            self._svc.register_worker_publisher,
            self._svc.deregister_worker_publisher,
        )

    def _start_replica_tracking(self) -> None:
        """Subscribe to this deployment's running replicas via LongPollClient."""
        deployment_id = self._serve_deployment_id
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
        key_listeners = {
            # LLMServer replicas: the selectable workers.
            (
                LongPollNamespace.DEPLOYMENT_TARGETS,
                deployment_id,
            ): self._on_deployment_targets,
            # LLMRouter replicas: the replica mesh peers.
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
        are registered with the selection service and departed ones evicted, and
        their lifecycle endpoints are subscribed to.
        """
        if self._stopping:
            return
        members: Dict[int, tuple] = {}
        token_endpoints: Dict[int, str] = {}
        lifecycle_sources: Set[str] = set()
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
                lifecycle_endpoint = _get_endpoint(
                    replica.routing_stats, KV_LIFECYCLE_METADATA_KEY
                )
                if lifecycle_endpoint:
                    lifecycle_sources.add(lifecycle_endpoint)
        if self._lifecycle_sources is not None:
            self._lifecycle_sources.update(lifecycle_sources)

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

    async def shutdown(self) -> None:
        """Stop membership updates and release the selection service's sockets."""
        # stop() does not cancel LongPoll callbacks already queued on this loop.
        self._stopping = True
        if self._long_poll_client is not None:
            self._long_poll_client.stop()
            self._long_poll_client = None
        tasks = list(self._pending_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for endpoints in (self._replica_peers, self._lifecycle_sources):
            if endpoints is not None:
                await endpoints.stop()
        if self._svc is not None:
            await self._svc.shutdown_async()
            self._svc = None

    def replica_sync_routing_stats(self) -> Dict[str, Any]:
        """Routing stats advertising this router's replica mesh endpoint."""
        if self._svc is None:
            return {}
        endpoint = f"tcp://{ray.util.get_node_ip_address()}:{self._replica_sync_port}"
        return {KV_REPLICA_SYNC_METADATA_KEY: {"endpoint": endpoint}}

    def _on_router_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: sync the replica mesh peers with the other running
        LLMRouter replicas."""
        if self._stopping or self._replica_peers is None:
            return
        peers: Set[str] = set()
        for replica in target_info.running_replicas:
            endpoint = _get_endpoint(
                replica.routing_stats, KV_REPLICA_SYNC_METADATA_KEY
            )
            if endpoint and replica.replica_id != self._router_replica_id:
                peers.add(endpoint)
        self._replica_peers.update(peers)

    def remove_worker(self, worker_id: int) -> None:
        """Evict a departed replica's worker, its KV blocks and the load of its
        in-flight requests from the selection service.
        """
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
                "model_name": SELECTION_MODEL_NAME,
                "routing_group": SELECTION_ROUTING_GROUP,
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
        request = {
            "model_name": SELECTION_MODEL_NAME,
            "routing_group": SELECTION_ROUTING_GROUP,
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
        self._publish_autoscaling_snapshot()
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
