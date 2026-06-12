import logging
import os
import re
import tempfile
from typing import Any, Dict

from dynamo.runtime import DistributedRuntime

from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DYN_FILE_KV_ENV,
    DYN_ZMQ_BROKER_URL_ENV,
    KV_EVENT_PLANE_ENV_DEFAULTS,
    KV_EVENTS_ENDPOINT_SUFFIX,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


def kv_event_namespace(deployment_id: DeploymentID) -> str:
    """The Dynamo namespace for a deployment.

    Per-deployment so KV events of different models never share an event
    scope. Derived from the deployment identity alone, so replicas and the
    deployment actor compute it independently.
    """
    raw = f"ray_llm_{deployment_id.app_name}_{deployment_id.name}"
    return re.sub(r"[^A-Za-z0-9_-]", "_", raw)


def kv_events_endpoint_path(namespace: str) -> str:
    """The Dynamo endpoint path scoping a deployment's KV events."""
    return f"{namespace}.{KV_EVENTS_ENDPOINT_SUFFIX}"


def configure_kv_event_plane_env(namespace: str) -> None:
    """Configure this process's Dynamo runtime for the KV event plane.

    NOTE: This private store and the out-of-band relay exist only because
    Dynamo resolves worker-query endpoints (i.e. each replica's local-indexer
    query endpoint) solely through its own discovery backends (file/etcd/NATS),
    with no Ray/GCS provider and no API to register an endpoint directly. So
    each process runs an isolated ``file`` store and we relay the records
    ourselves (read -> RPC -> materialize, then touch to keep alive / remove).

    TODO (jeffreywang): upstream a direct ``KvRouter`` endpoint-registration API,
    then drop the materialize/touch/remove plumbing.
    """
    base = os.path.join(tempfile.gettempdir(), "ray_serve_llm_kv_events")
    os.makedirs(base, exist_ok=True)
    os.environ[DYN_FILE_KV_ENV] = tempfile.mkdtemp(prefix=f"{namespace}_", dir=base)
    for key, value in KV_EVENT_PLANE_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def configure_kv_event_broker_env(broker_url: str) -> None:
    """Point this process's Dynamo event plane at the deployment's broker.

    Must be set before the process creates Dynamo event-plane publishers or
    subscribers (broker mode is resolved at their creation).
    """
    os.environ[DYN_ZMQ_BROKER_URL_ENV] = broker_url


def create_kv_event_plane_runtime(loop) -> DistributedRuntime:
    """Create the Dynamo runtime backing this process's KV event plane.

    Private file-based discovery with the TCP request plane; must be created
    on a running asyncio event loop, after :func:`configure_kv_event_plane_env`.
    """
    return DistributedRuntime(loop, "file", "tcp")


def derive_kv_event_block_size(engine_kwargs: Dict[str, Any]) -> int:
    """The KV-event block size, resolved at deployment build time.

    Runs vLLM's own config resolution for the cache block size, so the
    ``KvRouter`` can be created eagerly with the same chunk size replicas
    later register with.
    """
    # Imported here, not at module scope: this module loads in non-engine
    # processes (Serve controller, router actor) that must not import vLLM.
    from vllm.config import CacheConfig

    return CacheConfig(block_size=engine_kwargs.get("block_size")).block_size
