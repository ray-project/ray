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

    Each process gets a private per-process ``file`` discovery store so the
    DistributedRuntime's endpoints never collide across colocated processes.
    Worker membership is conveyed to the router directly via
    ``KvRouter.add_worker`` (no discovery-record relay); event transport runs
    through the deployment's ZMQ broker.
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
