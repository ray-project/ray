import asyncio
import hashlib
import logging
import math
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TypedDict,
    Union,
)

from typing_extensions import Unpack

import ray
from ray import serve
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_INDEXER_THREADS,
    KV_INDEXER_THREADS_KEY,
    LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
    LLM_ROUTER_DEPLOYMENT_NAME,
    PD_ROUTING_STAGE,
    REQUEST_TRACKING_TTL_S,
    RoutingStage,
)
from ray.serve._private.common import DeploymentID, DeploymentTargetInfo
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

try:
    from dynamo.llm import SelectionService
except ImportError:
    SelectionService = None

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Dynamo's selection service keys all worker, indexer, and load state by
# (model_name, tenant_id). KVTokenTracker instantiates a selection service and
# serves exactly one model, so a single fixed key scopes all of its workers
# together.
_MODEL_NAME = "default"
_TENANT_ID = "default"

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


LifecycleEvent = Tuple[str, Tuple[Union[str, int], ...]]


class CachedLifecycle(TypedDict, total=False):
    prefill_completed: bool
    output_tokens: int


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
    reservation: Optional["ReservationBroadcast"] = None


class WorkerSelection(TypedDict):
    """The worker chosen by ``KVTokenTracker.select_worker`` for a request."""

    # The chosen worker.
    worker_id: int
    # Data-parallel rank within the worker.
    dp_rank: int
    # Matched prompt tokens available on the selected worker.
    overlap_tokens: int
    # Prompt tokens that still need prefill on the selected worker.
    effective_prefill_tokens: int


class ReservationBroadcast(TypedDict, total=False):
    """Selected-worker booking state replicated to peer ingress routers."""

    request_id: str
    source_ingress_replica_id: Optional[str]
    deployment_id: Optional[DeploymentID]
    completed: bool
    track_prefill_tokens: bool
    worker_id: int
    dp_rank: int
    sequence_hashes: List[int]
    isl_tokens: int
    expected_output_tokens: Optional[int]
    effective_prefill_tokens: int


class ReservationBroadcastForwarder:
    """Best-effort background replication of selected-worker reservations.

    ``report`` only enqueues the selected-worker booking facts Dynamo already
    returned. Sending the broadcast and waiting for its results happen on the
    delivery task, off the request's selection and dispatch path.
    """

    def __init__(self, handle: DeploymentHandle):
        self._handle = handle
        if getattr(handle, "is_initialized", True) is False:
            # Keep broadcast routing off the ingress replica's request loop.
            handle._init(_run_router_in_separate_loop=True)
        self._reservations: asyncio.Queue[ReservationBroadcast] = asyncio.Queue()
        self._delivery_task: Optional[asyncio.Task[None]] = None

    def report(self, reservation: ReservationBroadcast) -> None:
        if self._delivery_task is None or self._delivery_task.done():
            self._delivery_task = asyncio.get_running_loop().create_task(
                self._deliver()
            )
        self._reservations.put_nowait(reservation)

    async def _deliver(self) -> None:
        while True:
            batch = [await self._reservations.get()]
            while not self._reservations.empty():
                batch.append(self._reservations.get_nowait())
            try:
                results = await self._handle.broadcast(
                    "on_reservations_created", batch
                ).results_async(
                    timeout_s=LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
                    return_exceptions=True,
                )
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    logger.warning(
                        "KV reservation broadcasts dropped on %d/%d ingress "
                        "replicas: %s",
                        len(errors),
                        len(results),
                        errors[0],
                    )
            except Exception as e:
                logger.warning(
                    "Dropping selection service reservation broadcast: %s", e
                )
            finally:
                for _ in batch:
                    self._reservations.task_done()

    async def flush(self) -> None:
        """Wait until every reported reservation broadcast has been attempted."""
        await self._reservations.join()

    def close(self) -> None:
        if self._delivery_task is not None:
            self._delivery_task.cancel()
            self._delivery_task = None


class KVTokenTracker:
    """Tracks per-replica KV-cache overlap and token load inside the LLMRouter
    ingress replica.

    Built by the LLMRouter ingress replica via ``build_kv_token_tracker``, so
    ``select_worker`` is a local call on the ingress event loop.

    1. Owns a router-local Dynamo ``SelectionService``.
    2. Tracks live replicas via a ``LongPollClient`` on ``DEPLOYMENT_TARGETS``,
       mapping each running replica to a Dynamo worker id.
    3. The ``SelectionService`` maintains a global KV index radix tree, fed by
       every replica's KV events; each node records which workers hold that KV block.
    4. Scoring (``select_worker``) ranks candidates and books the chosen worker
       before another local selection can run. Reservations are replicated to
       peer ingress replicas in the background so their selection services can
       apply the engine's subsequent lifecycle events.
    """

    def __init__(
        self,
        indexer_threads: int = DEFAULT_KV_INDEXER_THREADS,
        serve_deployment_id: Optional[DeploymentID] = None,
        ingress_replica_id: Optional[str] = None,
        routing_stage: Optional[RoutingStage] = None,
    ):
        # The tracked LLMServer deployment id, passed in by the LLMRouter
        # that builds the tracker.
        self._serve_deployment_id: Optional[DeploymentID] = serve_deployment_id
        self._ingress_replica_id: Optional[str] = ingress_replica_id
        if self._ingress_replica_id is None:
            try:
                self._ingress_replica_id = (
                    serve.get_replica_context().replica_id.unique_id
                )
            except RayServeException:
                self._ingress_replica_id = None
        # KV-cache block size, learned once from the first replica's reported
        # engine config and passed to the selection service, which uses it to
        # track the worker's active load and index its KV blocks for overlap.
        self._block_size: Optional[int] = None
        self._indexer_threads = indexer_threads
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
        self._pending_tasks: Set[asyncio.Task[None]] = set()
        self._reservation_forwarder: Optional[ReservationBroadcastForwarder] = None
        self._reservation_updates: asyncio.Queue[
            List[ReservationBroadcast]
        ] = asyncio.Queue()
        self._reservation_apply_task: Optional[asyncio.Task[None]] = None
        self._long_poll_client: Optional[LongPollClient] = None
        self._selection_override: Optional[Dict[str, Union[float, bool]]] = None
        self._routing_stage = routing_stage
        if routing_stage == RoutingStage.PREFILL:
            self._selection_override = {"track_prefill_tokens": True}
        elif routing_stage == RoutingStage.DECODE:
            self._selection_override = {
                "overlap_score_credit": 0.0,
                "assume_kv_reuse": False,
                "track_prefill_tokens": False,
            }
        self._prefill_selection_lock: asyncio.Lock = asyncio.Lock()
        self._prefill_workers: Set[int] = set()
        self._prefill_workers_changed = asyncio.Event()
        self._lifecycle_cache: OrderedDict[str, CachedLifecycle] = OrderedDict()
        self._create_selection_service()
        self._start_replica_tracking()

    def get_block_size(self) -> int:
        """Return the KV-cache block size used for decode-block accounting."""
        return self._block_size

    def start_reservation_broadcast(self, handle: DeploymentHandle) -> None:
        """Configure background reservation replication to this deployment."""
        self._reservation_forwarder = ReservationBroadcastForwarder(handle)

    def _create_selection_service(self) -> None:
        """Create the router-local Dynamo selection service for this deployment."""
        if SelectionService is None:
            self._svc = None
            logger.warning(
                "ai-dynamo is not installed; KV-aware routing requires ai-dynamo."
            )
            return

        self._svc = SelectionService(indexer_threads=self._indexer_threads)
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

    def close(self) -> None:
        if self._long_poll_client is not None:
            self._long_poll_client.stop()
        if self._reservation_forwarder is not None:
            self._reservation_forwarder.close()
        if self._reservation_apply_task is not None:
            self._reservation_apply_task.cancel()
        for task in self._pending_tasks:
            task.cancel()
        if self._svc is not None:
            self._svc.shutdown()

    def remove_worker(self, worker_id: int) -> None:
        """Evict a departed replica's worker and its KV blocks from the
        selection service.
        """
        # Drop the departed replica's in-flight requests; their completions can
        # never arrive, so they would otherwise leak. delete_worker below frees
        # their load in the service, so no per-request free_reservation is needed.
        if self._routing_stage == RoutingStage.PREFILL:
            self._prefill_workers.discard(worker_id)
        for request_id in self._request_ids_by_worker.pop(worker_id, set()):
            self._requests.pop(request_id, None)
            if self._routing_stage is not None:
                self._mark_request_completed(request_id)
                self._lifecycle_cache.pop(request_id, None)
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
                "tenant_id": _TENANT_ID,
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
        if self._routing_stage == RoutingStage.PREFILL:
            self._prefill_workers.add(worker_id)
            self._prefill_workers_changed.set()
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
    ) -> WorkerSelection:
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.
            expected_output_tokens: The request's output-token cap, used to
                weight output blocks when decode progress is reported.

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
        # Serve may retry dispatch within the same P/D routing attempt.
        existing = self._requests.get(request_id)
        if (
            self._routing_stage is not None
            and existing is not None
            and existing.reservation is not None
        ):
            if existing.worker_id in allowed_worker_ids:
                booking = existing.reservation
                return {
                    "worker_id": existing.worker_id,
                    "dp_rank": booking["dp_rank"],
                    "overlap_tokens": len(token_ids)
                    - booking["effective_prefill_tokens"],
                    "effective_prefill_tokens": booking["effective_prefill_tokens"],
                }
            await self.release_request(request_id)
            raise RuntimeError("Selected worker became unavailable during routing")
        request = {
            "model_name": _MODEL_NAME,
            "tenant_id": _TENANT_ID,
            "selection_id": request_id,
            "token_ids": token_ids,
            "allowed_worker_ids": allowed_worker_ids,
            "expected_output_tokens": expected_output_tokens,
        }
        if self._selection_override is not None:
            request["router_config_override"] = self._selection_override
        selection = await self._select_and_reserve(request)
        if (
            self._routing_stage is not None
            and request_id in self._completed_request_ids
        ):
            await self._svc.free_reservation(request_id)
            raise RuntimeError("Routing attempt was cancelled during selection")
        self._track_request_state(
            request_id,
            selection["worker_id"],
            len(token_ids),
            expected_output_tokens,
        )
        reservation: ReservationBroadcast = {
            "request_id": request_id,
            "source_ingress_replica_id": self._ingress_replica_id,
            "deployment_id": self._serve_deployment_id,
            "worker_id": selection["worker_id"],
            "dp_rank": selection["dp_rank"],
            "sequence_hashes": selection["sequence_hashes"],
            "isl_tokens": selection["isl_tokens"],
            "expected_output_tokens": expected_output_tokens,
            "effective_prefill_tokens": selection["effective_prefill_tokens"],
            "track_prefill_tokens": selection.get("track_prefill_tokens", True),
        }
        self._requests[request_id].reservation = reservation
        if self._reservation_forwarder is not None:
            self._reservation_forwarder.report(reservation)
        return {
            "worker_id": selection["worker_id"],
            "dp_rank": selection["dp_rank"],
            "overlap_tokens": selection["overlap"]["longest_matched"],
            "effective_prefill_tokens": selection["effective_prefill_tokens"],
        }

    async def _select_and_reserve(self, request: Dict[str, Any]) -> Dict[str, Any]:
        if self._routing_stage != RoutingStage.PREFILL:
            return await self._svc.select_and_reserve(request)

        # Dynamo's prefill router tracks pending tokens but no active KV blocks.
        # SelectionService cannot disable block tracking per request, so book
        # explicitly without block hashes. Serialize local selections until booked.
        async with self._prefill_selection_lock:
            # Handle membership can arrive before Dynamo's worker registration.
            while not (
                workers := self._prefill_workers.intersection(
                    request["allowed_worker_ids"]
                )
            ):
                self._prefill_workers_changed.clear()
                await self._prefill_workers_changed.wait()
            request = {**request, "allowed_worker_ids": list(workers)}
            selection = await self._svc.select({**request, "selection_id": None})
            selection.update(
                sequence_hashes=[],
                isl_tokens=len(request["token_ids"]),
                track_prefill_tokens=True,
            )
            await self._svc.create_reservation(
                {
                    "model_name": _MODEL_NAME,
                    "tenant_id": _TENANT_ID,
                    "selection_id": request["selection_id"],
                    "worker_id": selection["worker_id"],
                    "dp_rank": selection["dp_rank"],
                    "sequence_hashes": [],
                    "isl_tokens": selection["isl_tokens"],
                    "effective_prefill_tokens": selection["effective_prefill_tokens"],
                    "expected_output_tokens": request["expected_output_tokens"],
                    "track_prefill_tokens": True,
                }
            )
            return selection

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

    async def on_reservations_created(
        self, reservations: List[ReservationBroadcast]
    ) -> None:
        """Queue already-selected requests for this ingress's selection service.

        This method runs as a Serve RPC on LLMRouter replicas. Keep it short:
        route handling shares the same replica, so the heavier Dynamo
        ``create_reservation`` calls are applied by a background task.
        """
        if self._svc is None or self._block_size is None:
            return
        pending = []
        for reservation in reservations:
            # The selecting ingress receives its own broadcast after it has
            # already booked the atomic reservation. It must skip even if the
            # request already completed locally before the broadcast arrived.
            source_id = reservation.get("source_ingress_replica_id")
            if source_id is not None and source_id == self._ingress_replica_id:
                continue
            pending.append(reservation)
        if not pending:
            return
        if self._reservation_apply_task is None or self._reservation_apply_task.done():
            self._reservation_apply_task = asyncio.create_task(
                self._apply_reservation_updates()
            )
        self._reservation_updates.put_nowait(pending)

    async def _apply_reservation_updates(self) -> None:
        while True:
            batches = [await self._reservation_updates.get()]
            while not self._reservation_updates.empty():
                batches.append(self._reservation_updates.get_nowait())
            reservations = [reservation for batch in batches for reservation in batch]
            try:
                await self._apply_reservations(reservations)
            except Exception:
                logger.exception("Failed to apply KV reservation broadcast batch.")
            finally:
                for _ in batches:
                    self._reservation_updates.task_done()

    async def _apply_reservations(
        self, reservations: List[ReservationBroadcast]
    ) -> None:
        await self._evict_stale_requests()
        for reservation in reservations:
            request_id = reservation["request_id"]
            if reservation.get("completed"):
                await self.on_request_completed(request_id)
                continue
            if (
                request_id in self._requests
                or request_id in self._completed_request_ids
            ):
                continue
            await self._svc.create_reservation(
                {
                    "model_name": _MODEL_NAME,
                    "tenant_id": _TENANT_ID,
                    "selection_id": request_id,
                    "worker_id": reservation["worker_id"],
                    "dp_rank": reservation["dp_rank"],
                    "sequence_hashes": reservation["sequence_hashes"],
                    "isl_tokens": reservation["isl_tokens"],
                    "expected_output_tokens": reservation["expected_output_tokens"],
                    "effective_prefill_tokens": reservation["effective_prefill_tokens"],
                    "track_prefill_tokens": reservation.get(
                        "track_prefill_tokens", True
                    ),
                }
            )
            if request_id in self._completed_request_ids:
                await self._svc.free_reservation(request_id)
                continue
            self._track_request_state(
                request_id,
                reservation["worker_id"],
                reservation["isl_tokens"],
                reservation["expected_output_tokens"],
            )
            self._requests[request_id].reservation = reservation
            pending = self._lifecycle_cache.pop(request_id, {})
            if pending.get("prefill_completed"):
                await self.on_prefill_complete(request_id)
            output_tokens = pending.get("output_tokens", 0)
            if output_tokens:
                await self.on_decode_progress(request_id, output_tokens)

    async def release_request(self, request_id: str) -> None:
        await self.on_request_completed(request_id)
        if self._reservation_forwarder is not None:
            self._reservation_forwarder.report(
                {
                    "request_id": request_id,
                    "source_ingress_replica_id": self._ingress_replica_id,
                    "deployment_id": self._serve_deployment_id,
                    "completed": True,
                }
            )

    def _cache_lifecycle_event(
        self, request_id: str, **update: Unpack[CachedLifecycle]
    ) -> None:
        # Engine events can arrive before a peer router's reservation broadcast.
        # Replay these updates once that reservation has been applied.
        if request_id in self._completed_request_ids:
            return
        pending = self._lifecycle_cache.setdefault(request_id, {})
        pending.update(update)
        while len(self._lifecycle_cache) > 8192:
            self._lifecycle_cache.popitem(last=False)

    async def on_lifecycle_events(self, events: List[LifecycleEvent]) -> None:
        """Apply a replica's ``(hook_name, args)`` lifecycle events in order.

        The hooks are order-sensitive (e.g. a completion arriving before its
        admission would resurrect an evicted request) so a replica sends its
        events in submission order, batched into one call.
        """
        if self._svc is None or self._block_size is None:
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
            self._cache_lifecycle_event(request_id, prefill_completed=True)
            return
        if state.prefill_completed:
            return
        state.prefill_completed = True
        try:
            await self._svc.prefill_complete(request_id)
        except Exception:
            if self._requests.get(request_id) is state:
                raise

    async def on_decode_progress(
        self, request_id: str, cumulative_output_tokens: int
    ) -> None:
        """Advance ``request_id`` to an exact cumulative output-token count,
        booking one decode block in the selection service per crossed boundary.
        """
        if self._routing_stage == RoutingStage.PREFILL:
            return
        state = self._requests.get(request_id)
        if state is None:
            pending = self._lifecycle_cache.get(request_id, {})
            self._cache_lifecycle_event(
                request_id,
                output_tokens=max(
                    pending.get("output_tokens", 0), cumulative_output_tokens
                ),
            )
            return
        if cumulative_output_tokens <= state.output_tokens:
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
        self._lifecycle_cache.pop(request_id, None)
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


_KV_TOKEN_TRACKERS: Dict[DeploymentID, "KVTokenTracker"] = {}


def set_kv_token_tracker(
    tracker: "KVTokenTracker", deployment_id: DeploymentID
) -> None:
    _KV_TOKEN_TRACKERS[deployment_id] = tracker


def get_kv_token_tracker(deployment_id: DeploymentID) -> Optional["KVTokenTracker"]:
    return _KV_TOKEN_TRACKERS.get(deployment_id)


def get_llm_router_handle():
    """Handle to the in-app LLMRouter deployment, for engine replicas to reach
    its ``on_lifecycle_events`` method. Resolved in the current Serve app.
    """
    app_name = serve.get_replica_context().app_name
    return serve.get_deployment_handle(LLM_ROUTER_DEPLOYMENT_NAME, app_name=app_name)


def build_kv_token_tracker(
    llm_config: "LLMConfig", serve_deployment_id: DeploymentID
) -> "KVTokenTracker":
    """Build the ``KVTokenTracker`` and register it in this process's global
    so the same-process ``KVAwareRouter`` can reach it. Must be called from the
    ingress replica's event loop (the tracker binds a LongPollClient to it).
    """
    stage = llm_config.experimental_configs.get(PD_ROUTING_STAGE)
    tracker = KVTokenTracker(
        indexer_threads=llm_config.experimental_configs.get(
            KV_INDEXER_THREADS_KEY, DEFAULT_KV_INDEXER_THREADS
        ),
        serve_deployment_id=serve_deployment_id,
        routing_stage=RoutingStage(stage) if stage is not None else None,
    )
    set_kv_token_tracker(tracker, serve_deployment_id)
    return tracker
