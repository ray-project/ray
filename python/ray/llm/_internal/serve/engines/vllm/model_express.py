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
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

# experimental_configs keys and defaults.
MX_SERVER_PORT_KEY = "MX_SERVER_PORT"
MX_METADATA_PORT_BASE_KEY = "MX_METADATA_PORT_BASE"
MX_WORKER_GRPC_PORT_BASE_KEY = "MX_WORKER_GRPC_PORT_BASE"
MX_READY_PORT_BASE_KEY = "MX_READY_PORT_BASE"
DEFAULT_MX_SERVER_PORT = 8001
DEFAULT_MX_METADATA_PORT_BASE = 15555
DEFAULT_MX_WORKER_GRPC_PORT_BASE = 16555
DEFAULT_MX_READY_PORT_BASE = 17555

# vLLM copies only a fixed set of env prefixes into its Ray workers.
_MX_ENV_PREFIXES = ("MX_", "MODEL_EXPRESS_")

_engine_ready = threading.Event()
_ready_server: Optional[http.server.ThreadingHTTPServer] = None


def is_model_express(llm_config: LLMConfig) -> bool:
    return llm_config.engine_kwargs.get("load_format") in MODEL_EXPRESS_LOAD_FORMATS


def _head_node_ip() -> str:
    for node in ray.nodes():
        if node["Alive"] and "node:__internal_head__" in node["Resources"]:
            return node["NodeManagerAddress"]
    raise RuntimeError("Could not find the head node to reach the MX server.")


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


def setup_model_express(llm_config: LLMConfig) -> None:
    """Configure the MX client for this replica. Runs before the engine starts."""
    configs = llm_config.experimental_configs
    engine_config = llm_config.get_engine_config()
    node_ip = ray.util.get_node_ip_address()
    offset = _port_offset(llm_config)

    server_port = int(configs.get(MX_SERVER_PORT_KEY, DEFAULT_MX_SERVER_PORT))
    _set_default("MX_SERVER_ADDRESS", f"{_head_node_ip()}:{server_port}")
    _set_default(
        "MX_METADATA_PORT",
        str(
            int(configs.get(MX_METADATA_PORT_BASE_KEY, DEFAULT_MX_METADATA_PORT_BASE))
            + offset
        ),
    )
    _set_default(
        "MX_WORKER_GRPC_PORT",
        str(
            int(
                configs.get(
                    MX_WORKER_GRPC_PORT_BASE_KEY, DEFAULT_MX_WORKER_GRPC_PORT_BASE
                )
            )
            + offset
        ),
    )
    _set_default("MX_WORKER_HOST", node_ip)
    # Node init may rewrite hf_model_id to a local path; keep the P2P identity
    # on the model id so every replica matches.
    _set_default("MX_MODEL_NAME_OVERRIDE", engine_config.actual_hf_model_id)
    if engine_config.mirror_config and engine_config.mirror_config.bucket_uri:
        # Cold start with no peer streams the weights from the mirror.
        _set_default("MX_MODEL_URI", engine_config.mirror_config.bucket_uri)

    _set_default("MX_ARTIFACT_TRANSFER", "1")
    _set_default("MX_ARTIFACT_BACKEND", "nixl")
    ready_port = (
        int(configs.get(MX_READY_PORT_BASE_KEY, DEFAULT_MX_READY_PORT_BASE)) + offset
    )
    _start_ready_server(node_ip, ready_port)
    os.environ["MX_ARTIFACT_READY_URL"] = f"http://{node_ip}:{ready_port}/ready"

    _append_env_prefixes_to_copy()
    logger.info(
        "ModelExpress enabled: server=%s metadata_port=%s worker_grpc_port=%s "
        "ready_url=%s",
        os.environ["MX_SERVER_ADDRESS"],
        os.environ["MX_METADATA_PORT"],
        os.environ["MX_WORKER_GRPC_PORT"],
        os.environ["MX_ARTIFACT_READY_URL"],
    )


def mark_engine_ready() -> None:
    """Let MX publish this replica's JIT caches; call once the engine has started."""
    _engine_ready.set()
