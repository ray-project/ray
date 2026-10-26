"""ModelExpress (MX) integration for the vLLM engine.

With ``load_format="modelexpress"`` a new replica loads its weights (and JIT
caches) from an already-serving peer over NIXL instead of from storage. The MX
client runs inside vLLM's workers and only reads environment variables, so this
module's job is to set those variables in the replica process before the engine
starts, and to expose the readiness signal MX waits for before publishing the
replica's JIT caches.
"""

import http.server
import logging
import os
import threading
from typing import Optional

import ray
from ray import serve
from ray.llm._internal.common.utils.download_utils import MODEL_EXPRESS_LOAD_FORMATS
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.model_express.envs import (
    get_mx_app_name,
    get_mx_metadata_port_base,
    get_mx_ready_port_base,
    get_mx_worker_grpc_port_base,
)
from ray.llm._internal.serve.serving_patterns.model_express.mx_server import (
    MX_SERVER_DEPLOYMENT_NAME,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

# The MX client's env vars. vLLM copies only a fixed set of env prefixes into
# its Ray workers, so these are added to that set.
_MX_ENV_PREFIXES = ("MX_", "MODEL_EXPRESS_")

_engine_ready = threading.Event()
_ready_server: Optional[http.server.ThreadingHTTPServer] = None


def is_model_express(llm_config: LLMConfig) -> bool:
    return llm_config.engine_kwargs.get("load_format") in MODEL_EXPRESS_LOAD_FORMATS


async def _mx_server_address(app_name: str) -> str:
    handle = serve.get_deployment_handle(MX_SERVER_DEPLOYMENT_NAME, app_name=app_name)
    return await handle.address.remote()


def _port_offset(llm_config: LLMConfig) -> int:
    # Ranks are scoped to the deployment, so colocated replicas of one deployment
    # get disjoint port ranges. MX itself adds the worker's local device index.
    rank = serve.get_replica_context().rank
    return rank.local_rank * llm_config.get_engine_config().num_devices


def _set_default(name: str, value: str) -> None:
    if name not in os.environ:
        os.environ[name] = value


def _append_env_prefixes_to_copy() -> None:
    key = "VLLM_RAY_EXTRA_ENV_VAR_PREFIXES_TO_COPY"
    prefixes = [p for p in os.environ.get(key, "").split(",") if p]
    prefixes += [p for p in _MX_ENV_PREFIXES if p not in prefixes]
    os.environ[key] = ",".join(prefixes)


class _ReadyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if _engine_ready.is_set() else 503)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def _start_ready_server(host: str, port: int) -> None:
    global _ready_server
    if _ready_server is not None:
        return
    _ready_server = http.server.ThreadingHTTPServer((host, port), _ReadyHandler)
    threading.Thread(
        target=_ready_server.serve_forever, name="mx-ready-server", daemon=True
    ).start()


async def setup_model_express(llm_config: LLMConfig) -> None:
    """Configure the MX client for this replica. Runs before the engine starts."""
    engine_config = llm_config.get_engine_config()
    node_ip = ray.util.get_node_ip_address()
    offset = _port_offset(llm_config)

    if "MX_SERVER_ADDRESS" not in os.environ:
        os.environ["MX_SERVER_ADDRESS"] = await _mx_server_address(get_mx_app_name())
    _set_default("MX_METADATA_PORT", str(get_mx_metadata_port_base() + offset))
    _set_default("MX_WORKER_GRPC_PORT", str(get_mx_worker_grpc_port_base() + offset))
    _set_default("MX_WORKER_HOST", node_ip)
    # Node init may rewrite hf_model_id to a local path; keep the P2P identity
    # on the model id so every replica matches.
    _set_default("MX_MODEL_NAME_OVERRIDE", engine_config.actual_hf_model_id)
    if engine_config.mirror_config and engine_config.mirror_config.bucket_uri:
        # Cold start with no peer streams the weights from the mirror.
        _set_default("MX_MODEL_URI", engine_config.mirror_config.bucket_uri)

    _set_default("MX_ARTIFACT_TRANSFER", "1")
    _set_default("MX_ARTIFACT_BACKEND", "nixl")
    ready_port = get_mx_ready_port_base() + offset
    _start_ready_server(node_ip, ready_port)
    os.environ["MX_ARTIFACT_READY_URL"] = f"http://{node_ip}:{ready_port}/ready"

    _append_env_prefixes_to_copy()
    mx_env = " ".join(
        f"{name}={value}"
        for name, value in sorted(os.environ.items())
        if name.startswith(_MX_ENV_PREFIXES)
    )
    logger.info("ModelExpress enabled: %s", mx_env)


def mark_engine_ready() -> None:
    """Let MX publish this replica's JIT caches; call once the engine has started."""
    _engine_ready.set()
