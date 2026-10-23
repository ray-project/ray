"""The ModelExpress (MX) server as a Serve deployment.

One replica, pinned to the head node like the Serve controller, runs the MX
server binary. LLM replicas reach it at the head node's IP (see
``model_express.py``). The server is only a registry of serving peers; weights
and JIT caches move directly between LLM replicas.
"""

import os
import socket
import subprocess
import time
from typing import Optional

import ray
from ray import serve


@serve.deployment(
    num_replicas=1,
    # Like the Serve controller: on the head node, and schedulable when the head
    # advertises no CPUs.
    ray_actor_options={"num_cpus": 0, "resources": {"node:__internal_head__": 0.001}},
    health_check_period_s=10,
)
class ModelExpressServer:
    def __init__(
        self,
        binary: str,
        port: int = 8001,
        metadata_backend: str = "memory",
        cache_dir: Optional[str] = None,
        log_path: Optional[str] = None,
        startup_timeout_s: float = 30,
    ):
        env = dict(os.environ)
        env.update(
            MX_METADATA_BACKEND=metadata_backend,
            MODEL_EXPRESS_SERVER_PORT=str(port),
            MODEL_EXPRESS_SERVER_METRICS_PORT="0",
        )
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
            env["MODEL_EXPRESS_CACHE_DIRECTORY"] = cache_dir
        out = open(log_path, "ab") if log_path else None
        self._proc = subprocess.Popen(
            [binary], env=env, stdout=out, stderr=subprocess.STDOUT
        )
        self._address = f"{ray.util.get_node_ip_address()}:{port}"

        deadline = time.monotonic() + startup_timeout_s
        while True:
            self.check_health()
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"MX server did not listen on :{port}")
                time.sleep(0.5)

    def check_health(self):
        if self._proc.poll() is not None:
            raise RuntimeError(f"MX server exited with code {self._proc.returncode}")

    def address(self) -> str:
        return self._address

    def __del__(self):
        if getattr(self, "_proc", None) and self._proc.poll() is None:
            self._proc.terminate()


def build_model_express_app(binary: str, **kwargs) -> serve.Application:
    """Build the `mx` app. Deploy it with ``route_prefix=None``."""
    return ModelExpressServer.bind(binary=binary, **kwargs)
