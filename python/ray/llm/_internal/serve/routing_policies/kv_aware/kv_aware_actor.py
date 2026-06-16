import asyncio
import hashlib
import logging
from typing import Any, Dict, List, Optional, Set

import ray
from ray import serve
from ray.serve._private.common import DeploymentTargetInfo
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"

# The selection service keys all worker, indexer, and load state by
# (model_name, tenant_id). A deployment-scoped actor serves exactly one model,
# so a single fixed key scopes all of its workers together.
_MODEL_NAME = "default"
_TENANT_ID = "default"


def get_worker_id(replica_unique_id: str) -> int:
    """
    Deterministically derive a Dynamo worker id from a replica's unique id.
    """
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


@ray.remote
class KVRouterActor:
    """Deployment-scoped Ray actor owning the Dynamo selection service.

    Rather than running Dynamo's HTTP ``select_service``, the actor consumes the
    runtime-free ``SelectionService`` as an in-process Python object. It indexes
    each replica's KV events by connecting **out** to the replica's engine ZMQ
    endpoint (no broker and no in-replica Dynamo bridge), and exposes replica
    membership tracking and KV-overlap queries.

    Each replica advertises its engine's KV-events endpoint via
    ``record_routing_stats``; the controller propagates it on the ``LongPoll``
    replica snapshot, and the actor reconciles the selection service against that
    snapshot in :meth:`_on_deployment_targets` -- registering newly-advertised
    workers (the service then dials them) and evicting departed ones.
    """

    def __init__(self, block_size: int):
        self._block_size = block_size
        self._replica_id_by_worker: Dict[int, str] = {}
        self._pending_tasks: Set[asyncio.Task] = set()
        self._long_poll_client: Optional[LongPollClient] = None
        self._create_selection_service()
        self._start_replica_tracking()

    def _create_selection_service(self) -> None:
        """Create the in-process Dynamo selection service for this deployment."""
        # Imported here, not at module scope: Ray pickles this actor class by
        # value, and Dynamo's pyo3 classes cannot be pickled as its globals.
        from dynamo.llm import SelectionService

        self._svc = SelectionService(indexer_threads=4)
        logger.info(
            "Dynamo SelectionService created (block size %d).", self._block_size
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

    def _schedule(self, coro) -> None:
        """Run a coroutine on the actor's event loop, holding a reference until
        it completes.

        The LongPoll listener that drives registration/eviction is synchronous
        while the selection service's worker mutations are async; keeping the
        task referenced prevents it from being garbage-collected mid-flight.
        """
        task = asyncio.ensure_future(coro)
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    def _on_deployment_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: reconcile selection-service workers against the
        running-replica snapshot.

        Each replica advertises its KV-events endpoint via
        ``record_routing_stats`` (carried in ``RunningReplicaInfo.routing_stats``
        and rebroadcast when it changes). Replicas newly carrying that endpoint
        are registered with the selection service (which then dials them);
        departed replicas are evicted along with their KV blocks. A replica that
        is running but has not advertised its endpoint yet is skipped until a
        later snapshot carries it.
        """
        members = set()
        advertised: Dict[int, tuple] = {}
        for r in target_info.running_replicas:
            worker_id = get_worker_id(r.replica_id.unique_id)
            members.add(worker_id)
            kv_events = r.routing_stats.get("kv_events")
            if kv_events is not None:
                advertised[worker_id] = (r.replica_id.to_full_id_str(), kv_events)

        registered = set(self._replica_id_by_worker)

        for worker_id in registered - members:
            self.remove_worker(worker_id)
            self._replica_id_by_worker.pop(worker_id, None)

        for worker_id in advertised.keys() - registered:
            replica_id, kv_events = advertised[worker_id]
            # Mark registered synchronously so a re-poll (or back-to-back
            # snapshot) before the async upsert finishes cannot register the
            # same worker -- and re-spawn its listener -- twice.
            self._replica_id_by_worker[worker_id] = replica_id
            self._schedule(self._upsert_worker(worker_id, replica_id, kv_events))

        added = advertised.keys() - registered
        removed = registered - members
        if added or removed:
            logger.info(
                "KV selection membership updated: +%d -%d, tracking %d worker(s).",
                len(added),
                len(removed),
                len(self._replica_id_by_worker),
            )

    def remove_worker(self, worker_id: int) -> None:
        """Evict a departed replica's worker (and its KV blocks) from the
        selection service.

        Scheduled on the actor's event loop rather than awaited: the LongPoll
        listener that calls this is synchronous, while ``delete_worker`` is async.
        """
        self._schedule(self._svc.delete_worker(worker_id))

    async def _upsert_worker(
        self, worker_id: int, replica_id: str, kv_events: Dict[str, Any]
    ) -> None:
        """Register a replica's KV-event endpoint with the selection service.

        The selection service spawns a connect-out ZMQ listener to the
        replica's ``endpoint`` and indexes its live KV events. ``endpoint`` and
        ``max_num_batched_tokens`` make the worker schedulable (required for
        overlap/scoring queries); Ray owns request dispatch, so the worker's
        ``endpoint`` is an opaque Ray-internal handle the service never dials for
        inference.
        """
        dp_rank = kv_events["dp_rank"]
        await self._svc.upsert_worker(
            {
                "worker_id": worker_id,
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "endpoint": f"ray://{replica_id}",
                "block_size": self._block_size,
                "max_num_batched_tokens": kv_events["max_num_batched_tokens"],
                "data_parallel_start_rank": dp_rank,
                "data_parallel_size": 1,
                # In-process depythonize maps HashMap<u32, String> from int keys.
                "kv_events_endpoints": {dp_rank: kv_events["endpoint"]},
            }
        )
        logger.info(
            "Registered KV event worker %d for replica %s at %s.",
            worker_id,
            replica_id,
            kv_events["endpoint"],
        )

    def get_candidate_worker_ids(self) -> List[int]:
        """The workers currently registered with the selection service, sorted.

        A worker appears once its replica has advertised its KV-events endpoint
        and the actor has registered it (connect-out indexing has begun).
        """
        return sorted(self._replica_id_by_worker)

    def get_kv_event_worker_replicas(self) -> Dict[int, str]:
        """The registered Dynamo worker id -> replica full id mapping."""
        return dict(self._replica_id_by_worker)

    def get_registered_worker_ids(self) -> List[int]:
        """Worker ids the selection service can currently schedule, sorted.

        ``delete_worker`` leaves a tombstone record in the catalog marked
        unschedulable, so filter to schedulable workers (registered + serving).
        """
        workers = self._svc.list_workers(_MODEL_NAME, _TENANT_ID)
        return sorted(
            w["worker_id"] for w in workers if w["lifecycle"] == "schedulable"
        )

    async def get_kv_overlap_blocks(self, token_ids: List[int]) -> Dict[int, int]:
        """Per-worker device-tier KV overlap blocks for a token sequence.

        The selection service's view of how many leading blocks of ``token_ids``
        each worker has cached: the overlap signal that drives KV-aware scoring.
        """
        scores = await self._svc.overlap_scores(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "token_ids": list(token_ids),
            }
        )
        return {w["worker_id"]: w["device_blocks"] for w in scores["workers"]}
