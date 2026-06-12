import asyncio
import hashlib
import json
import logging
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
    materialize_worker_discovery_records,
    remove_worker_discovery_records,
    touch_worker_discovery_records,
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

# Refresh period for materialized discovery records, against Dynamo's
# 10-second file store TTL.
_RECORD_KEEPALIVE_INTERVAL_S = 3


def get_worker_id(replica_unique_id: str) -> int:
    """
    Deterministically derive a Dynamo worker id from a replica's unique id.
    """
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


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
    - KV-aware scoring

    TODO (jeffreywang): Scoring routes to Dynamo once ``rank_workers`` lands.
    """

    def __init__(self, block_size: int):
        self._block_size = block_size
        self._replica_id_by_worker: Dict[int, str] = {}
        self._long_poll_client: Optional[LongPollClient] = None
        self._dyn_worker_id_to_replica_id: Dict[int, str] = {}
        # The KvRouter recovers a worker's cache view by querying that worker's
        # local indexer, resolved through Dynamo's file-KV discovery store. This
        # actor and each replica run in separate processes with private discovery
        # stores, so a replica's worker-query endpoint is invisible here. The
        # replica hands us its discovery records (via activate_kv_event_worker)
        # and we materialize them into this process's store, holding the
        # filenames per worker to keep them alive against Dynamo's TTL and to
        # purge them when the worker leaves.
        self._records_by_kv_event_worker: Dict[int, List[str]] = {}

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
        self._record_keepalive_task = asyncio.get_running_loop().create_task(
            self._keep_worker_records_alive()
        )

    async def _keep_worker_records_alive(self) -> None:
        """Refresh the materialized worker records' mtimes forever.

        Dynamo's file store expires records 10s after their last mtime and
        only keep-alives records written through its own API; without this,
        the router would silently lose every worker-query recovery target.
        Touching and purging both run on this actor's event loop, so a
        record is never touched after its worker is purged.
        """
        while True:
            touch_worker_discovery_records(
                [
                    filename
                    for filenames in self._records_by_kv_event_worker.values()
                    for filename in filenames
                ]
            )
            await asyncio.sleep(_RECORD_KEEPALIVE_INTERVAL_S)

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
            self._purge_kv_event_worker_records(worker_id)
        for worker_id in added:
            self._replica_id_by_worker[worker_id] = new[worker_id]
            self.add_worker(worker_id)

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

    def add_worker(self, worker_id: int) -> None:
        """Register a worker with the KV router when a replica is added."""
        pass

    def remove_worker(self, worker_id: int) -> None:
        """Deregister a worker from the KV router when a replica is removed."""
        pass

    async def register_kv_event_worker(
        self, worker_id: int, replica_id: str, kv_block_size: int
    ) -> str:
        """Register a replica's KV-event identity before it publishes.

        Called by each replica's ``KvEventPublisher`` on startup with the
        Ray-derived worker id its events are keyed by; returns this actor's
        event broker URL for the publisher to connect to. The ``KvRouter``
        consumer exists since this actor's construction, so the start of the
        replica's event stream is observable.
        """
        if kv_block_size != self._block_size:
            raise ValueError(
                f"KV event worker {worker_id} (replica {replica_id}) resolved "
                f"block size {kv_block_size}, but the KvRouter indexes at the "
                f"build-time block size {self._block_size}; its events would "
                "never match overlap queries."
            )
        self._dyn_worker_id_to_replica_id[worker_id] = replica_id
        logger.info(
            "Registered KV event worker %d for replica %s (%d registered).",
            worker_id,
            replica_id,
            len(self._dyn_worker_id_to_replica_id),
        )
        return self._kv_event_broker.broker_url

    async def activate_kv_event_worker(
        self, worker_id: int, records: Dict[str, bytes]
    ) -> None:
        """Feed a worker's discovery records to the router's recovery path.

        Called by the replica once its publisher (and the worker-query
        endpoint serving its local indexer) is up, passing the worker's
        discovery records directly. Materializing them into this process's
        private discovery store lets the router restore the worker's view:
        live events buffer behind a restore until the query endpoint resolves.
        """
        self._records_by_kv_event_worker[
            worker_id
        ] = materialize_worker_discovery_records(records)
        logger.info(
            "Activated KV event worker %d (%d discovery records).",
            worker_id,
            len(records),
        )

    def _purge_kv_event_worker_records(self, worker_id: int) -> None:
        """Drop a removed worker's materialized discovery records.

        A removed replica may have registered without reaching activation, in
        which case there is nothing to purge.
        """
        filenames = self._records_by_kv_event_worker.pop(worker_id, None)
        if filenames is None:
            return
        remove_worker_discovery_records(filenames)

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
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.

        Returns:
            A dict describing the selected worker:
            ``worker_id`` (int): the chosen worker.
            ``dp_rank`` (int): data-parallel rank within the worker.
            ``overlap_blocks`` (int): KV blocks already cached on that worker.
            ``score`` (float): the worker's routing score (higher is better).
        """
        raise NotImplementedError("KVRouterActor.select_worker is not implemented")

    async def on_request_added(
        self,
        request_id: str,
        expected_output_tokens: Optional[int] = None,
    ) -> None:
        """Commit a routed request to the worker chosen by ``select_worker``.

        Args:
            request_id: Unique identifier for the request.
            expected_output_tokens: Predicted number of output tokens.
        """
        raise NotImplementedError("KVRouterActor.on_request_added is not implemented")

    async def on_prefill_complete(self, request_id: str) -> None:
        """Record a request's transition from prefill to decode.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.on_prefill_complete is not implemented"
        )

    async def on_decode_progress(
        self, request_id: str, cumulative_output_tokens: int
    ) -> None:
        """Adds any newly crossed decode blocks to the request's accounted load.

        Args:
            request_id: Unique identifier for the request.
            cumulative_output_tokens: Total output tokens generated so far.
        """
        raise NotImplementedError("KVRouterActor.on_decode_progress is not implemented")

    async def on_request_completed(self, request_id: str) -> None:
        """Release the KV-cache accounting for a finished request.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.on_request_completed is not implemented"
        )
