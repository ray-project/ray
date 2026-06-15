import logging
import os
import re
from typing import Any, Dict

from dynamo.runtime import DistributedRuntime

from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DYN_ZMQ_BROKER_URL_ENV,
    KV_EVENT_PLANE_ENV_DEFAULTS,
    KV_EVENTS_ENDPOINT_SUFFIX,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


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

    The runtime uses an in-memory (``mem``) discovery store: nothing in the
    KV-aware path reads it across processes. Worker membership is conveyed to
    the router via ``KvRouter.add_worker`` and event transport runs through the
    deployment's ZMQ broker, so no shared or on-disk discovery store is needed.
    """
    for key, value in KV_EVENT_PLANE_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def configure_kv_event_broker_env(broker_url: str) -> None:
    """Point this process's event plane at the deployment-scoped ``KvEventBroker``.

    Must be set before the process creates Dynamo event-plane publishers or
    subscribers as broker mode is resolved at their creation.
    """
    os.environ[DYN_ZMQ_BROKER_URL_ENV] = broker_url


def create_kv_event_plane_runtime(loop) -> DistributedRuntime:
    """Create the Dynamo runtime hosting this process's KV event plane.

    Dynamo's ``KvRouter`` and ``KvEventPublisher`` must run inside a
    ``DistributedRuntime``. Constructing one takes two arguments that a normal
    Dynamo deployment relies on but the KV-aware path does not, because here all
    traffic instead flows over the ZMQ event plane:

    - The discovery backend is how separate Dynamo processes find each other:
      each publishes its network address to a shared store (``etcd`` or a shared
      ``file`` directory) that the others read. We pass ``"mem"``, a per-process
      in-memory store no other process reads, because nothing here needs to look
      up an address: the router is told its workers directly via
      ``KvRouter.add_worker`` and receives their events over the ZMQ broker.
    - The request-plane transport is how one Dynamo process makes direct RPC
      calls to another. We pass ``"tcp"`` only to satisfy the constructor; the
      KV-aware path makes no such calls, so the transport is set up and left
      unused.
    """
    return DistributedRuntime(loop, "mem", "tcp")


def derive_kv_event_block_size(engine_kwargs: Dict[str, Any]) -> int:
    # This module loads in non-engine processes (Serve controller, router actor)
    # that must not import vLLM.
    from vllm.config import CacheConfig

    return CacheConfig(block_size=engine_kwargs.get("block_size")).block_size
