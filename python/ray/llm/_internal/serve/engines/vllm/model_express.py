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
from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig
from ray.llm._internal.serve.serving_patterns.model_express.envs import (
    get_mx_app_name,
    get_mx_metadata_port_base,
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
    try:
        handle = serve.get_deployment_handle(
            MX_SERVER_DEPLOYMENT_NAME, app_name=app_name
        )
    except KeyError as e:
        raise RuntimeError(
            f"load_format='modelexpress' needs the MX server app {app_name!r}, which "
            "isn't deployed. Deploy ray.serve.llm.mx:build_mx_app under that name, "
            "or set RAY_MX_APP_NAME to its name."
        ) from e
    return await handle.address.remote()


def _check_whole_gpus(llm_config: LLMConfig) -> None:
    """MX registers each worker's GPU with NIXL and transfers whole GPU state, so
    workers can't share a GPU."""
    env_vars = (llm_config.runtime_env or {}).get("env_vars") or {}
    per_worker = env_vars.get("VLLM_RAY_PER_WORKER_GPUS")
    if per_worker is not None and float(per_worker) >= 1:
        return
    fraction = per_worker or VLLMEngineConfig._detect_fractional_gpu_from_pg(
        llm_config.placement_group_config
    )
    if fraction is not None:
        raise ValueError(
            f"load_format={llm_config.engine_kwargs['load_format']!r} doesn't "
            f"support fractional GPUs (got {fraction} GPU per worker). Give each "
            "vLLM worker a whole GPU, or use another load_format."
        )


def _set_default(name: str, value: str) -> None:
    if name not in os.environ:
        os.environ[name] = value


def _append_env_prefixes_to_copy() -> None:
    key = "VLLM_RAY_EXTRA_ENV_VAR_PREFIXES_TO_COPY"
    prefixes = [p for p in os.environ.get(key, "").split(",") if p]
    prefixes += [p for p in _MX_ENV_PREFIXES if p not in prefixes]
    os.environ[key] = ",".join(prefixes)


class _ReadyHandler(http.server.BaseHTTPRequestHandler):
    # http.server dispatches GET requests to a method with exactly this name.
    def do_GET(self):
        self.send_response(200 if _engine_ready.is_set() else 503)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def _start_ready_server(host: str) -> int:
    """Serve the readiness endpoint on a free port; returns the port."""
    global _ready_server
    if _ready_server is None:
        _ready_server = http.server.ThreadingHTTPServer((host, 0), _ReadyHandler)
        threading.Thread(
            target=_ready_server.serve_forever, name="mx-ready-server", daemon=True
        ).start()
    return _ready_server.server_address[1]


async def setup_model_express(llm_config: LLMConfig) -> None:
    """Configure the MX client for this replica. Runs before the engine starts."""
    _check_whole_gpus(llm_config)
    engine_config = llm_config.get_engine_config()
    node_ip = ray.util.get_node_ip_address()

    if "MX_SERVER_ADDRESS" not in os.environ:
        os.environ["MX_SERVER_ADDRESS"] = await _mx_server_address(get_mx_app_name())
    # MX adds each worker's device index to these. vLLM's Ray workers address
    # GPUs by physical index, so the ports are unique per node without an offset.
    _set_default("MX_METADATA_PORT", str(get_mx_metadata_port_base()))
    _set_default("MX_WORKER_GRPC_PORT", str(get_mx_worker_grpc_port_base()))
    # Leave MX_WORKER_HOST unset: vLLM copies the replica's MX_* env into every
    # worker, and workers can span nodes, so each worker's MX client detects
    # its own host instead.
    # Node init may rewrite hf_model_id to a local path; keep the P2P identity
    # on the model id so every replica matches.
    _set_default("MX_MODEL_NAME_OVERRIDE", engine_config.actual_hf_model_id)
    if engine_config.mirror_config and engine_config.mirror_config.bucket_uri:
        # Cold start with no peer streams the weights from the mirror.
        _set_default("MX_MODEL_URI", engine_config.mirror_config.bucket_uri)

    _set_default("MX_ARTIFACT_TRANSFER", "1")
    _set_default("MX_ARTIFACT_BACKEND", "nixl")
    ready_port = _start_ready_server(node_ip)
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
