"""Render HAProxy's direct, batched OpenCensus export to Ray's metrics agent."""


import os
import secrets
from typing import Optional

import ray
from ray._private.node import Node
from ray.core.generated import metrics_pb2


def _lua_bytes(value: bytes) -> str:
    return '"' + "".join(f"\\{byte:03d}" for byte in value) + '"'


def stream_metrics_agent_address() -> str:
    """Return the node-local reporter endpoint used by Ray's native exporters."""
    if ray.is_initialized():
        node = ray._private.worker._global_node
        if node is not None and node.metrics_agent_port:
            return f"127.0.0.1:{node.metrics_agent_port}"
    # Configuration-only tests do not initialize Ray or start the Lua task.
    return "127.0.0.1:1"


def stream_metrics_token_source() -> Optional[str]:
    """Where HAProxy's exporter reads Ray's auth token, never the token itself.

    Mirrors the precedence of Ray's token loader. Returns None when token auth is
    off, "" for the inherited ``RAY_AUTH_TOKEN`` variable, else the path of the
    existing token file, which the exporter rereads so rotated tokens apply.
    """
    from ray._private.authentication.authentication_utils import (
        is_token_auth_enabled,
    )

    if not is_token_auth_enabled():
        return None
    from ray._private.authentication.authentication_token_setup import (
        _get_default_token_path,
    )
    from ray._private.authentication.authentication_utils import (
        is_k8s_token_auth_enabled,
    )

    if os.environ.get("RAY_AUTH_TOKEN"):
        return ""
    if os.environ.get("RAY_AUTH_TOKEN_PATH"):
        return os.path.abspath(os.environ["RAY_AUTH_TOKEN_PATH"])
    if is_k8s_token_auth_enabled():
        k8s_path = "/var/run/secrets/ray.io/serviceaccount/token"
        if os.path.isfile(k8s_path):
            return k8s_path
    return str(_get_default_token_path())


def render_stream_metrics_export_config(
    socket_path: str, generation: str | None = None
) -> str:
    """Precompute protobuf descriptors and buckets once, outside request handling."""
    from ray.llm._internal.serve.observability.metrics.llm_metrics import (
        ITL_BUCKETS,
        REQUEST_DURATION_BUCKETS,
        TTFT_BUCKETS,
    )

    node: Node | None = (
        ray._private.worker._global_node if ray.is_initialized() else None
    )
    proxy_worker_id = (
        bytes.fromhex(ray.get_runtime_context().get_worker_id())
        if ray.is_initialized()
        else b"\xff" * 28
    )
    generation = generation or secrets.token_hex(28)
    from ray_haproxy import get_stream_metrics_module

    from ray.serve._private.haproxy import get_haproxy_binary

    module = get_stream_metrics_module(get_haproxy_binary())
    labels = [
        "Component",
        "NodeAddress",
        "ReplicaId",
        "SessionName",
        "Version",
        "WorkerId",
        "engine_worker_id",
        "engine",
        "model_name",
    ]
    defaults = [
        (node.node_ip_address or "127.0.0.1") if node else "127.0.0.1",
        (node.session_name or "") if node else "",
        ray.__version__,
        proxy_worker_id.hex(),
    ]
    token_source = stream_metrics_token_source()
    lines = [
        "local EXPORT_SOCKET = " + _lua_bytes(socket_path.encode()),
        "local AUTH_TOKEN_SOURCE = "
        + ("nil" if token_source is None else _lua_bytes(token_source.encode())),
        # Reporting identity is independent of ProxyActor: old generations
        # expire even while that actor and the new generation keep reporting.
        "local REPORT_WORKER_ID = " + _lua_bytes(bytes.fromhex(generation)),
        "local EXPORT_GENERATION = " + _lua_bytes(generation.encode()),
        "local STREAM_METRICS_MODULE = " + _lua_bytes(module.encode()),
        "local DEFAULT_LABELS = {"
        + ",".join(_lua_bytes(v.encode()) for v in defaults)
        + "}",
        "local HISTOGRAMS = {",
    ]
    for name, bounds, description in [
        (
            "time_to_first_token",
            TTFT_BUCKETS,
            "Time from HAProxy request entry to first output chunk.",
        ),
        (
            "inter_token_latency",
            ITL_BUCKETS,
            "HAProxy output interval per new token, weighted by token count.",
        ),
        ("router_overhead", TTFT_BUCKETS, "HAProxy round trip to the ingress router."),
        (
            "request_duration",
            REQUEST_DURATION_BUCKETS,
            "Time from HAProxy request entry to response completion.",
        ),
    ]:
        descriptor = metrics_pb2.MetricDescriptor(
            name=f"serve_llm_{name}_seconds",
            description=description,
            unit="s",
            type=metrics_pb2.MetricDescriptor.CUMULATIVE_DISTRIBUTION,
            label_keys=[metrics_pb2.LabelKey(key=key) for key in labels],
        )
        options = metrics_pb2.DistributionValue.BucketOptions()
        options.explicit.bounds.extend(bounds)
        lines.append(
            "{descriptor="
            + _lua_bytes(descriptor.SerializeToString())
            + ",options="
            + _lua_bytes(options.SerializeToString())
            + ",bounds={"
            + ",".join(str(round(b * 1_000_000)) for b in bounds)
            + "}},"
        )
    lines.append("}")
    descriptor = metrics_pb2.MetricDescriptor(
        name="serve_llm_stream_metrics_collection_errors",
        description="Dropped or invalid observations and failed metric exports.",
        type=metrics_pb2.MetricDescriptor.CUMULATIVE_INT64,
        label_keys=[
            metrics_pb2.LabelKey(key=key)
            for key in labels
            if key not in ("ReplicaId", "engine_worker_id", "model_name", "engine")
        ],
    )
    lines.append(
        "local ERROR_DESCRIPTOR = " + _lua_bytes(descriptor.SerializeToString())
    )
    return "\n".join(lines)
