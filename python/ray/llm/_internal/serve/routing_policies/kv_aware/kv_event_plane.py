import asyncio
import logging
import os
import re
import tempfile
import time
from typing import Any, Dict, List

from dynamo.runtime import DistributedRuntime

from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DISCOVERY_INSTANCES_BUCKET,
    DYN_FILE_KV_ENV,
    DYN_ZMQ_BROKER_URL_ENV,
    KV_EVENT_PLANE_ENV_DEFAULTS,
    KV_EVENTS_ENDPOINT_SUFFIX,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

_DISCOVERY_RECORDS_TIMEOUT_S = 10


def kv_event_namespace(deployment_id: DeploymentID) -> str:
    """The event plane namespace for a deployment.

    Per-deployment so KV events of different models never share an event scope.
    """
    raw = f"ray_llm_{deployment_id.app_name}_{deployment_id.name}"
    return re.sub(r"[^A-Za-z0-9_-]", "_", raw)


def kv_events_endpoint_path(namespace: str) -> str:
    """The event plane endpoint path scoping a deployment's KV events."""
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
    """Point this process's event plane at the deployment-scoped ``KvEventBroker``.

    Must be set before the process creates Dynamo event-plane publishers or
    subscribers as broker mode is resolved at their creation.
    """
    os.environ[DYN_ZMQ_BROKER_URL_ENV] = broker_url


def create_kv_event_plane_runtime(loop) -> DistributedRuntime:
    """Create the Dynamo runtime backing this process's KV event plane."""
    return DistributedRuntime(loop, "file", "tcp")


def derive_kv_event_block_size(engine_kwargs: Dict[str, Any]) -> int:
    # This module loads in non-engine processes (Serve controller, router actor)
    # that must not import vLLM.
    from vllm.config import CacheConfig

    return CacheConfig(block_size=engine_kwargs.get("block_size")).block_size


def _discovery_instances_dir() -> str:
    return os.path.join(os.environ[DYN_FILE_KV_ENV], DISCOVERY_INSTANCES_BUCKET)


async def read_worker_discovery_records() -> Dict[str, bytes]:
    """This process's Dynamo endpoint discovery records, as opaque files.

    Dynamo registers them asynchronously after the publisher is constructed,
    so this polls until they appear.
    """
    instances_dir = _discovery_instances_dir()
    deadline = time.monotonic() + _DISCOVERY_RECORDS_TIMEOUT_S
    while True:
        records = {}
        if os.path.isdir(instances_dir):
            for name in os.listdir(instances_dir):
                # Skip in-flight atomic writes (Dynamo writes temp + rename).
                if name.startswith(".tmp_"):
                    continue
                with open(os.path.join(instances_dir, name), "rb") as f:
                    records[name] = f.read()
        if records:
            return records
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "The Dynamo publisher registered no endpoint discovery records "
                f"in {instances_dir} within {_DISCOVERY_RECORDS_TIMEOUT_S}s."
            )
        await asyncio.sleep(0.05)


def materialize_worker_discovery_records(records: Dict[str, bytes]) -> List[str]:
    """Write a worker's discovery records into this process's store.

    Atomic write (temp + rename) so Dynamo's watcher picks them up. Returns
    the written file names, for later removal.
    """
    instances_dir = _discovery_instances_dir()
    os.makedirs(instances_dir, exist_ok=True)
    for name, content in records.items():
        temp_path = os.path.join(instances_dir, f".tmp_{name}")
        with open(temp_path, "wb") as f:
            f.write(content)
        os.replace(temp_path, os.path.join(instances_dir, name))
    return list(records)


def touch_worker_discovery_records(filenames: List[str]) -> None:
    """Refresh records' mtimes so Dynamo's file store does not expire them.

    Dynamo only keep-alives records written through its own API; these were
    materialized by hand, so they need refreshing against the 10s TTL.
    """
    instances_dir = _discovery_instances_dir()
    for name in filenames:
        os.utime(os.path.join(instances_dir, name))


def remove_worker_discovery_records(filenames: List[str]) -> None:
    """Delete a worker's records; Dynamo's watcher then drops it from the
    router's query directory."""
    instances_dir = _discovery_instances_dir()
    for name in filenames:
        os.remove(os.path.join(instances_dir, name))
