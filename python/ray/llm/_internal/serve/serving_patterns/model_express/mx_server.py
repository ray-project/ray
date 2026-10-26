"""The ModelExpress (MX) server as a Serve deployment.

One replica runs the MX server binary. It is a registry of serving peers: LLM
replicas publish themselves to it and look up sources in it, while weights and
JIT caches move directly between LLM replicas. LLM replicas find the server
through this deployment's ``address()``, so they don't depend on where it runs.
The server's output goes to this replica's log.
"""

import os
import socket
import subprocess
import time

import ray
from ray import serve
from ray.llm._internal.serve.serving_patterns.model_express.envs import (
    MX_SERVER_HEALTH_CHECK_PERIOD_S,
    get_mx_server_binary,
    get_mx_server_port,
)

MX_SERVER_DEPLOYMENT_NAME = "ModelExpressServer"


@serve.deployment(
    name=MX_SERVER_DEPLOYMENT_NAME,
    num_replicas=1,
    # Like the Serve controller: on the head node, and schedulable when the head
    # advertises no CPUs.
    ray_actor_options={"num_cpus": 0, "resources": {"node:__internal_head__": 0.001}},
    health_check_period_s=MX_SERVER_HEALTH_CHECK_PERIOD_S,
)
class ModelExpressServer:
    def __init__(self, metadata_backend: str = "memory", startup_timeout_s: float = 30):
        """Start the MX server.

        Args:
            metadata_backend: "memory" or "redis". For "redis", set REDIS_URL
                in the replica's environment.
            startup_timeout_s: How long to wait for the server to listen.
        """
        binary = get_mx_server_binary()
        if not os.access(binary, os.X_OK):
            raise FileNotFoundError(
                f"MX server binary not found at {binary}. Set "
                "RAY_MX_SERVER_BINARY_PATH when not using the ray-llm image."
            )
        port = get_mx_server_port()
        env = dict(os.environ)
        env.update(
            MX_METADATA_BACKEND=metadata_backend,
            MODEL_EXPRESS_SERVER_PORT=str(port),
            MODEL_EXPRESS_SERVER_METRICS_PORT="0",
        )
        self._proc = subprocess.Popen([binary], env=env)
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
