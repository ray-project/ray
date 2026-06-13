import asyncio
import logging
from typing import Optional

from dynamo.llm import KvEventPublisher as DynamoKvEventPublisher

from ray import serve
from ray.actor import ActorHandle
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_event_plane import (
    configure_kv_event_broker_env,
    configure_kv_event_plane_env,
    create_kv_event_plane_runtime,
    kv_event_namespace,
    kv_events_endpoint_path,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    resolve_kv_event_source_endpoint,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


class KvEventPublisher:
    """Bridges the engine's ZMQ KV-event stream into Dynamo's ``kv-events``
    event plane, where the deployment's ``KVRouterActor``-hosted ``KvRouter``
    consumes it into the global KV indexer.

    Must be started in a running asyncio event loop.
    """

    def __init__(
        self,
        kv_router_actor: ActorHandle,
        replica_id: str,
        worker_id: int,
        namespace: str,
        zmq_endpoint: str,
        kv_block_size: int,
        dp_rank: int = 0,
    ):
        self._kv_router_actor = kv_router_actor
        self._replica_id = replica_id
        self._worker_id = worker_id
        self._namespace = namespace
        self._zmq_endpoint = zmq_endpoint
        self._kv_block_size = kv_block_size
        self._dp_rank = dp_rank
        self._runtime = None
        self._publisher = None

    @property
    def worker_id(self) -> int:
        """This replica's Dynamo worker id."""
        return self._worker_id

    async def start(self) -> None:
        # Registering returns this deployment's broker URL and adds the worker
        # to the KvRouter, so its events are indexed from the first one (the
        # router applies them directly, with no discovery-based recovery).
        broker_url = await self._kv_router_actor.register_kv_event_worker.remote(
            self._worker_id, self._replica_id, self._kv_block_size
        )

        configure_kv_event_plane_env(self._namespace)
        configure_kv_event_broker_env(broker_url)
        self._runtime = create_kv_event_plane_runtime(asyncio.get_running_loop())
        endpoint = self._runtime.endpoint(kv_events_endpoint_path(self._namespace))

        self._publisher = DynamoKvEventPublisher(
            endpoint,
            worker_id=self._worker_id,
            kv_block_size=self._kv_block_size,
            zmq_endpoint=self._zmq_endpoint,
            zmq_topic="",
            # Required for the publisher to publish through the event plane.
            enable_local_indexer=True,
            dp_rank=self._dp_rank,
        )
        logger.info(
            "KvEventPublisher started for worker %d (replica %s), "
            "consuming KV events from %s.",
            self._worker_id,
            self._replica_id,
            self._zmq_endpoint,
        )

    def close(self) -> None:
        """Shut down the publisher and its event plane runtime."""
        if self._publisher is not None:
            self._publisher.shutdown()
            self._publisher = None
        if self._runtime is not None:
            self._runtime.shutdown()
            self._runtime = None


async def maybe_start_kv_event_publisher(
    llm_config: LLMConfig, engine_block_size: int
) -> Optional[KvEventPublisher]:
    """Start this replica's KvEventPublisher if KV-aware routing is set up."""
    zmq_endpoint = resolve_kv_event_source_endpoint(llm_config)
    if zmq_endpoint is None:
        return None
    replica_context = serve.get_replica_context()
    kv_router_actor = serve.get_deployment_actor(KV_ROUTER_ACTOR_NAME)

    publisher = KvEventPublisher(
        kv_router_actor=kv_router_actor,
        replica_id=replica_context.replica_id.to_full_id_str(),
        worker_id=get_worker_id(replica_context.replica_id.unique_id),
        namespace=kv_event_namespace(replica_context.replica_id.deployment_id),
        zmq_endpoint=zmq_endpoint,
        kv_block_size=engine_block_size,
        dp_rank=llm_config.engine_kwargs.get("data_parallel_rank") or 0,
    )
    await publisher.start()
    return publisher
