import asyncio
import hashlib
import logging
import math
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, TypedDict

import ray
from ray import serve
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_INDEXER_THREADS,
    REQUEST_TRACKING_TTL_S,
)
from ray.serve._private.common import DeploymentTargetInfo
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"

# Dynamo's selection service keys all worker, indexer, and load state by
# (model_name, tenant_id). KVRouterActor is a deployment-scoped actor that
# instantiates a selection service and serves exactly one model, so a single
# fixed key scopes all of its workers together.
_MODEL_NAME = "default"
_TENANT_ID = "default"

# Hooks a replica may invoke through ``KVRouterActor.on_lifecycle_events``.
LIFECYCLE_HOOKS = frozenset(
    {
        "on_request_added",
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
    """The worker chosen by ``KVRouterActor.select_worker`` for a request."""

    # The chosen worker.
    worker_id: int
    # Data-parallel rank within the worker.
    dp_rank: int
    # Matched prompt tokens available on the selected worker.
    overlap_tokens: int
    # Prompt tokens that still need prefill on the selected worker.
    effective_prefill_tokens: int


class KVRouterActor:
    """Deployment-scoped Ray actor backing KV-aware routing.

    Attached to the LLMServer deployment via Serve's ``DeploymentActorConfig``,
    independent of any replica's lifetime.

    1. Created once per deployment, attached to the LLMServer deployment via
       Serve's ``DeploymentActorConfig`` (independent of any replica's lifetime).
    2. Owns an in-process Dynamo ``SelectionService``.
    3. Tracks live replicas via a ``LongPollClient`` on ``DEPLOYMENT_TARGETS``,
       mapping each running replica to a Dynamo worker id.
    4. The ``SelectionService`` maintains a global KV index radix tree, fed by
       every replica's KV events; each node records which workers hold that KV block.
    5. Scoring (``select_worker``) ranks candidate workers by KV-cache overlap
       and prefill/decode load.
    6. Books each request's lifecycle into the service's active-load tracker, so
       in-flight load feeds back into scoring for subsequent requests.
    """

    def __init__(self, indexer_threads: int = DEFAULT_KV_INDEXER_THREADS):
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
        # Carries the effective prefill tokens select() computed at routing time to
        # on_request_added, which books them via the explicit create_reservation.
        # TODO(jeffreywang): this map is only needed because create_reservation
        # requires the effective prefill tokens to be passed in explicitly. Once the
        # selection service caches each select() result and create_reservation can
        # look it up by request id, Ray no longer needs to forward it.
        self._effective_prefill_tokens_by_request: Dict[str, int] = {}
        self._pending_tasks: Set[asyncio.Task] = set()
        self._long_poll_client: Optional[LongPollClient] = None
        self._create_selection_service()
        self._start_replica_tracking()

    async def ready(self) -> None:
        """Readiness probe for KVAwareRouter to confirm KVRouterActor is initialized
        before it starts routing requests to it.
        """

    def get_block_size(self) -> int:
        """Return the KV-cache block size used for decode-block accounting."""
        return self._block_size

    def _create_selection_service(self) -> None:
        """Create the in-process Dynamo selection service for this deployment."""
        # Imported here, not at module scope: Ray pickles this actor class by
        # value, and Dynamo's pyo3 classes cannot be pickled as its globals.
        try:
            from dynamo.llm import SelectionService
        except ImportError:
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
        deployment_id = serve.get_deployment_actor_context().deployment_id
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
        self._long_poll_client = LongPollClient(
            controller,
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    deployment_id,
                ): self._on_deployment_targets,
            },
            # Relies on KVRouterActor being an async actor (it defines async
            # methods), so Ray runs __init__ inside the actor's event loop.
            call_in_event_loop=asyncio.get_running_loop(),
            client_id=f"{type(self).__name__}:{deployment_id}",
        )

    def _schedule(self, coro) -> None:
        """Run a coroutine on the actor's event loop, holding a reference until
        it completes.
        """
        task = asyncio.ensure_future(coro)
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
    ) -> WorkerSelection:
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.

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
        selection = await self._svc.select(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "selection_id": request_id,
                "token_ids": token_ids,
                "allowed_worker_ids": allowed_worker_ids,
            }
        )
        self._effective_prefill_tokens_by_request[request_id] = selection[
            "effective_prefill_tokens"
        ]
        return {
            "worker_id": selection["worker_id"],
            "dp_rank": selection["dp_rank"],
            "overlap_tokens": selection["overlap"]["longest_matched"],
            "effective_prefill_tokens": selection["effective_prefill_tokens"],
        }

    async def on_lifecycle_events(self, events: List[tuple]) -> None:
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

    async def on_request_added(
        self,
        request_id: str,
        worker_id: int,
        token_ids: List[int],
        expected_output_tokens: Optional[int] = None,
    ) -> None:
        """Admit a routed request into ``worker_id``'s active load, booking it
        into the selection service which computes the worker's KV overlap from
        ``token_ids``, so the recorded prefill excludes the cached prefix."""
        await self._evict_stale_requests()
        prompt_tokens = len(token_ids)
        self._requests[request_id] = RequestLifecycle(
            worker_id=worker_id,
            prompt_tokens=prompt_tokens,
            expected_output_tokens=expected_output_tokens,
            total_blocks=math.ceil(prompt_tokens / self._block_size),
        )
        self._request_ids_by_worker.setdefault(worker_id, set()).add(request_id)
        effective_prefill_tokens = self._effective_prefill_tokens_by_request.pop(
            request_id, None
        )

        await self._svc.create_reservation(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "reservation_id": request_id,
                "worker_id": worker_id,
                "token_ids": token_ids,
                "expected_output_tokens": expected_output_tokens,
                "effective_prefill_tokens": effective_prefill_tokens,
            }
        )
        if request_id not in self._requests:
            await self._svc.free_reservation(request_id)

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
        self._effective_prefill_tokens_by_request.pop(request_id, None)
        state = self._requests.pop(request_id, None)
        if state is not None:
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
            self._effective_prefill_tokens_by_request.pop(request_id, None)
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
