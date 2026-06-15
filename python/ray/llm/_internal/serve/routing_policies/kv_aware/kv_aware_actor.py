import asyncio
import hashlib
import json
import logging
import math
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

import ray
from ray import serve
from ray.llm._internal.serve.routing_policies.kv_aware.kv_event_broker import (
    KvEventBroker,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_event_plane import (
    configure_kv_event_broker_env,
    configure_kv_event_plane_env,
    create_kv_event_plane_runtime,
    kv_event_namespace,
    kv_events_endpoint_path,
)
from ray.serve._private.common import DeploymentTargetInfo, ReplicaID
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"


def get_worker_id(replica_unique_id: str) -> int:
    """
    Deterministically derive a Dynamo worker id from a replica's unique id.
    """
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


#: Hooks a replica may invoke through ``KVRouterActor.on_lifecycle_events``.
LIFECYCLE_HOOKS = frozenset(
    {
        "on_request_added",
        "on_prefill_complete",
        "on_decode_progress",
        "on_request_completed",
    }
)


@dataclass
class RequestLifecycle:
    """In-flight request load state while the request is served by a replica.

    Mirrors what the Dynamo ``KvRouter`` tracks for the request so the actor can
    compute decode-block boundary crossings (one ``add_output_block`` per block)
    and serve the active-load views without round-tripping to Dynamo.
    """

    worker_id: int
    prompt_tokens: int = 0
    expected_output_tokens: Optional[int] = None
    prefill_completed: bool = False
    output_tokens: int = 0
    output_blocks: int = 0
    total_blocks: int = 0


@ray.remote
class KVRouterActor:
    """Deployment-scoped Ray actor hosting the KV-aware router.

    KVRouterActor, independent of any replica's lifetime, is attached to the LLMServer
    deployment via Serve's DeploymentActorConfig. It owns the deployment's Dynamo
    ``KvRouter``, whose ``KvEventConsumer`` consumes the replicas' KV events from the
    event plane into the global KV indexer, and hosts the ZMQ event broker the
    replicas' publishers connect to (Dynamo discovery is bypassed; replicas
    rendezvous with the broker through ``register_kv_event_worker``). It exposes
    the KV-aware routing interfaces:
    - Replica membership tracking
    - KV-aware scoring (query-only ``select_worker``)
    - Request lifecycle tracking that books active prefill/decode load into the
      ``KvRouter`` (``on_request_added`` -> ``on_prefill_complete`` ->
      ``on_decode_progress`` -> ``on_request_completed``)
    """

    def __init__(self, block_size: int):
        self._block_size = block_size
        self._replica_id_by_worker: Dict[int, str] = {}
        self._long_poll_client: Optional[LongPollClient] = None
        self._dyn_worker_id_to_replica_id: Dict[int, str] = {}
        self._requests: Dict[str, RequestLifecycle] = {}

        namespace = self._kv_event_plane_namespace()
        configure_kv_event_plane_env(namespace)
        # The deployment's event plane runs through this actor's broker. It is
        # created before any registration RPC is served, and its URL is handed
        # to each replica's publisher as the return of register_kv_event_worker,
        # so a replica never connects to a dead incarnation's broker.
        self._kv_event_broker = KvEventBroker()
        configure_kv_event_broker_env(self._kv_event_broker.broker_url)
        self._create_kv_router(namespace, block_size)
        self._start_replica_tracking()

    def get_block_size(self) -> int:
        """Return the KV-cache block size used for decode-block accounting."""
        return self._block_size

    def _create_kv_router(self, namespace: str, block_size: int) -> None:
        """Create the Dynamo ``KvRouter`` consuming this deployment's KV events.

        Eager: its ``KvEventConsumer`` subscribes to the broker before any
        replica exists, so the start of every event stream is observable.
        """
        # Imported here, not at module scope: Ray pickles this actor class by
        # value, and Dynamo's pyo3 classes cannot be pickled as its globals.
        from dynamo.llm import KvRouter, KvRouterConfig

        self._kv_router_runtime = create_kv_event_plane_runtime(
            asyncio.get_running_loop()
        )
        endpoint = self._kv_router_runtime.endpoint(kv_events_endpoint_path(namespace))
        # durable_kv_events=False: events arrive over the event plane, not
        # NATS JetStream.
        self._kv_router = KvRouter(
            endpoint=endpoint,
            block_size=block_size,
            kv_router_config=KvRouterConfig(
                use_kv_events=True,
                durable_kv_events=False,
            ),
        )
        logger.info(
            "Dynamo KvRouter created for namespace %s (block size %d).",
            namespace,
            block_size,
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
            call_in_event_loop=asyncio.get_event_loop(),
            client_id=f"{type(self).__name__}:{deployment_id}",
        )

    def _on_deployment_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: sync the worker mapping to the running replicas.

        Diffs the snapshot against the current mapping and updates the reverse
        worker->replica map.
        """
        new = {
            get_worker_id(r.replica_id.unique_id): r.replica_id.to_full_id_str()
            for r in target_info.running_replicas
        }
        current = set(self._replica_id_by_worker)
        added = new.keys() - current
        removed = current - new.keys()

        for worker_id in removed:
            self.remove_worker(worker_id)
            self._replica_id_by_worker.pop(worker_id, None)
            # A removed replica's KV-event registration goes with it. Keyed
            # by worker id, not the running set: replicas register while
            # still STARTING, before they appear in this snapshot.
            self._dyn_worker_id_to_replica_id.pop(worker_id, None)
        for worker_id in added:
            self._replica_id_by_worker[worker_id] = new[worker_id]

        if added or removed:
            logger.info(
                "KV router replica membership updated: +%d -%d, tracking %d worker(s).",
                len(added),
                len(removed),
                len(self._replica_id_by_worker),
            )

    def get_tracked_worker_id(self, replica_id_full_str: str) -> Optional[int]:
        """Return the worker id for a currently-tracked replica, or ``None``."""
        unique_id = ReplicaID.from_full_id_str(replica_id_full_str).unique_id
        worker_id = get_worker_id(unique_id)
        return worker_id if worker_id in self._replica_id_by_worker else None

    def get_replica_id(self, worker_id: int) -> Optional[str]:
        """Return the replica's full id string for ``worker_id``, or ``None``."""
        return self._replica_id_by_worker.get(worker_id)

    def get_candidate_worker_ids(self) -> List[int]:
        """Return the currently tracked worker ids, sorted ascending."""
        return sorted(self._replica_id_by_worker)

    def remove_worker(self, worker_id: int) -> None:
        """Evict a removed replica's worker from the KvRouter's indexer.

        Scheduled on the actor's event loop rather than awaited: the LongPoll
        listener that calls this is synchronous, while ``KvRouter.remove_worker``
        is async. Workers are added in :meth:`register_kv_event_worker` (awaited,
        before the replica publishes); removal is the only KvRouter membership
        change driven from replica-departure tracking.
        """
        asyncio.ensure_future(self._kv_router.remove_worker(worker_id))

    async def register_kv_event_worker(
        self, worker_id: int, replica_id: str, kv_block_size: int
    ) -> str:
        """Register a replica's KV-event identity before it publishes.

        Called by each replica's ``KvEventPublisher`` on startup with the
        Ray-derived worker id its events are keyed by. Registers the worker
        directly with the ``KvRouter`` so its live KV events are indexed from
        the start with no discovery-based recovery, and returns this actor's
        event broker URL for the publisher to connect to.
        """
        if kv_block_size != self._block_size:
            raise ValueError(
                f"KV event worker {worker_id} (replica {replica_id}) resolved "
                f"block size {kv_block_size}, but the KvRouter indexes at the "
                f"build-time block size {self._block_size}; its events would "
                "never match overlap queries."
            )
        self._dyn_worker_id_to_replica_id[worker_id] = replica_id
        # Add to the router before returning: the publisher only starts emitting
        # events after this RPC resolves, so the worker is always known to the
        # router before its first event arrives.
        await self._kv_router.add_worker(worker_id)
        logger.info(
            "Registered KV event worker %d for replica %s (%d registered).",
            worker_id,
            replica_id,
            len(self._dyn_worker_id_to_replica_id),
        )
        return self._kv_event_broker.broker_url

    def _kv_event_plane_namespace(self) -> str:
        """The Dynamo namespace scoping this deployment's KV events."""
        return kv_event_namespace(serve.get_deployment_actor_context().deployment_id)

    def get_kv_event_worker_replicas(self) -> Dict[int, str]:
        """The registered Dynamo worker id -> replica full id mapping."""
        return dict(self._dyn_worker_id_to_replica_id)

    async def get_kv_indexer_events(self) -> List[Dict[str, Any]]:
        """The KV events applied to the router's global indexer.

        Dynamo's ``KvRouter.dump_events``: each entry carries the worker id,
        storage tier, and the stored/removed event payload (block hashes and
        per-block token hashes).
        """
        return json.loads(await self._kv_router.dump_events())

    async def get_kv_event_worker_ids(self) -> List[int]:
        """Workers with at least one event in the global indexer, sorted."""
        return sorted(
            {event["worker_id"] for event in await self.get_kv_indexer_events()}
        )

    async def get_kv_overlap_blocks(self, token_ids: List[int]) -> Dict[int, int]:
        """Per-worker device-tier KV overlap blocks for a token sequence.

        The global indexer's view of how many leading blocks of ``token_ids``
        each worker has cached: the overlap input to KV-aware scoring.
        """
        scores = await self._kv_router.get_overlap_scores(token_ids)
        return {
            worker["worker_id"]: worker["device_blocks"] for worker in scores["workers"]
        }

    async def select_worker(
        self,
        request_id: str,
        token_ids: List[int],
        allowed_worker_ids: List[int],
    ) -> Dict[str, Any]:
        """Score the allowed workers for a request by KV-cache overlap and load
        and pick the best one, without admitting active load.

        Delegates to the Dynamo ``KvRouter``'s query-only ``select_worker``: it
        ranks only ``allowed_worker_ids`` (Ray's currently-routable replicas) and
        does not book active load (booked later via ``on_request_added``).

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.

        Returns:
            A dict describing the selected worker: ``worker_id``, ``dp_rank``,
            ``overlap_blocks``, ``effective_overlap_blocks``, ``cached_tokens``.
        """
        return await self._kv_router.select_worker(
            request_id, token_ids, allowed_worker_ids
        )

    async def on_lifecycle_events(self, events: List[tuple]) -> None:
        """Apply a replica's ``(hook_name, args)`` lifecycle events in order.

        Replicas deliver events through their reporter's ordered pump (one
        awaited batch in flight per replica) because plain fire-and-forget
        calls to an async actor are not executed in submission order, and the
        lifecycle hooks are order-sensitive (e.g. a completion overtaking the
        admission would resurrect an evicted request).
        """
        for hook_name, args in events:
            if hook_name in LIFECYCLE_HOOKS:
                await getattr(self, hook_name)(*args)
            else:
                logger.debug("Ignoring unknown lifecycle hook %s", hook_name)

    async def on_request_added(
        self,
        request_id: str,
        worker_id: int,
        token_ids: List[int],
        expected_output_tokens: Optional[int] = None,
    ) -> None:
        """Admit a routed request into ``worker_id``'s active load.

        Books the request into the Dynamo ``KvRouter`` with the worker's current
        KV overlap, so the request's counted prefill work excludes the prompt
        prefix already cached there. Pairs 1:1 with :meth:`on_request_completed`.
        """
        prompt_tokens = len(token_ids)
        self._requests[request_id] = RequestLifecycle(
            worker_id=worker_id,
            prompt_tokens=prompt_tokens,
            expected_output_tokens=expected_output_tokens,
            total_blocks=math.ceil(prompt_tokens / self._block_size),
        )
        cached_tokens = await self._cached_tokens(token_ids, worker_id)
        await self._kv_router.add_request(
            request_id,
            token_ids,
            worker_id,
            cached_tokens=cached_tokens,
            expected_output_tokens=expected_output_tokens,
        )

    async def on_prefill_complete(self, request_id: str) -> None:
        """Record a request's prefill->decode transition in Dynamo, dropping its
        prompt-side prefill load."""
        state = self._requests.get(request_id)
        if state is None:
            return
        state.prefill_completed = True
        await self._kv_router.mark_prefill_complete(request_id)

    async def on_decode_progress(
        self, request_id: str, cumulative_output_tokens: int
    ) -> None:
        """Advance ``request_id`` to an exact cumulative output-token count,
        booking one decode block in Dynamo per crossed block boundary."""
        state = self._requests.get(request_id)
        if state is None:
            return
        state.output_tokens = cumulative_output_tokens
        new_total_blocks = math.ceil(
            (state.prompt_tokens + cumulative_output_tokens) / self._block_size
        )
        decay_fraction = self._decay_fraction(state)
        while new_total_blocks > state.total_blocks:
            state.output_blocks += 1
            state.total_blocks += 1
            await self._kv_router.add_output_block(request_id, decay_fraction)

    async def on_request_completed(self, request_id: str) -> None:
        """Free ``request_id`` from Dynamo's active load and the local view."""
        if self._requests.pop(request_id, None) is not None:
            await self._kv_router.free(request_id)

    async def _cached_tokens(self, token_ids: List[int], worker_id: int) -> int:
        """Tokens of ``token_ids`` already cached on ``worker_id`` (whole blocks).

        The admission-time KV overlap for the chosen worker; subtracted from the
        prompt to count only the prefill work this request actually adds.
        """
        scores = await self._kv_router.get_overlap_scores(token_ids)
        for worker in scores["workers"]:
            if worker["worker_id"] == worker_id:
                return worker["device_blocks"] * self._block_size
        return 0

    def _decay_fraction(self, state: RequestLifecycle) -> Optional[float]:
        """Fraction of output still expected, or ``None`` without an estimate.

        Weights a decode block by how much generation remains, so blocks of a
        nearly-finished request count for less (Dynamo's decay model).
        """
        if not state.expected_output_tokens:
            return None
        return max(0.0, 1.0 - state.output_tokens / state.expected_output_tokens)

    async def get_request_lifecycle(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Return a snapshot of an in-flight request's state, or ``None``."""
        state = self._requests.get(request_id)
        return None if state is None else asdict(state)

    async def get_active_request_ids(self) -> List[str]:
        """Return ids of the in-flight requests."""
        return list(self._requests)

    async def get_worker_active_load(self, worker_id: int) -> int:
        """Return the number of in-flight requests attributed to ``worker_id``."""
        return sum(1 for s in self._requests.values() if s.worker_id == worker_id)
