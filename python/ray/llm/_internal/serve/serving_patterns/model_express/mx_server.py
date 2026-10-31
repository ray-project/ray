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
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.llm._internal.serve.serving_patterns.model_express.envs import (
    MX_SERVER_HEALTH_CHECK_PERIOD_S,
    get_mx_server_binary,
    get_mx_server_port,
)

MX_SERVER_DEPLOYMENT_NAME = "ModelExpressServer"


def _port_in_use(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@serve.deployment(
    name=MX_SERVER_DEPLOYMENT_NAME,
    num_replicas=1,
    # Like the Serve controller: on the head node, and schedulable when the head
    # advertises no CPUs.
    ray_actor_options={"num_cpus": 0, "resources": {HEAD_NODE_RESOURCE_NAME: 0.001}},
    health_check_period_s=MX_SERVER_HEALTH_CHECK_PERIOD_S,
)
class ModelExpressServer:
    def __init__(self, metadata_backend: str = "memory", startup_timeout_s: float = 60):
        """Start the MX server.

        Args:
            metadata_backend: "memory" or "redis". For "redis", set REDIS_URL
                in the replica's environment.
            startup_timeout_s: How long to wait for the port to be free and
                then for the server to listen on it.
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
        deadline = time.monotonic() + startup_timeout_s
        # A rolling update starts this replica while the one it replaces may
        # still hold the port; starting now would fail to bind.
        while _port_in_use(port):
            if time.monotonic() > deadline:
                raise TimeoutError(f"Port {port} for the MX server is still in use")
            time.sleep(0.5)

        self._proc = subprocess.Popen([binary], env=env)
        self._address = f"{ray.util.get_node_ip_address()}:{port}"
        while not _port_in_use(port):
            self.check_health()
            if time.monotonic() > deadline:
                raise TimeoutError(f"MX server did not listen on :{port}")
            time.sleep(0.5)
        self.check_health()

    def check_health(self):
        if self._proc.poll() is not None:
            raise RuntimeError(f"MX server exited with code {self._proc.returncode}")

    def address(self) -> str:
        return self._address

    def __del__(self):
        if getattr(self, "_proc", None) and self._proc.poll() is None:
            self._proc.terminate()
