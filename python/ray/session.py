import atexit
from dataclasses import dataclass
import importlib
import json
import logging
import sys
import os
from typing import Any, Dict, Optional

import ray as real_ray
from ray.job_config import JobConfig
import ray.ray_constants as ray_constants
from ray._private.services import create_redis_client, find_redis_address
from ray.util.client import ray

logger = logging.getLogger(__name__)


@dataclass
class SessionInfo:
    dashboard_url: str
    python_version: str
    ray_version: str
    ray_commit: str
    protocol_version: str


class SessionBuilder:
    def __init__(self, address: Optional[str] = None):
        self.address = address
        self.job_config = JobConfig()

    def connect(self) -> "real_ray.SessionInfo":
        connection_info = real_ray.util.connect(
            self.address, job_config=self.job_config)
        return SessionInfo(
            dashboard_url=None,
            python_version=connection_info["python_version"],
            ray_version=connection_info["ray_version"],
            ray_commit=connection_info["ray_commit"],
            protocol_version=connection_info["protocol_version"],
        )

    def env(self, env: Dict[str, Any]) -> "real_ray.SessionBuilder":
        self.job_config.set_runtime_env(env)
        return self

    def namespace(self, namespace: str) -> "real_ray.SessionBuilder":
        self.job_config.set_namespace(namespace)
        return self


class LocalSessionBuilder(SessionBuilder):
    def __init__(self, address: Optional[str] = None):
        super().__init__(address)
        self.kwargs = {"job_config": self.job_config}
        if address == "auto":
            self.kwargs["address"] = "auto"

    def connect(self) -> "real_ray.SessionInfo":
        real_ray.init(**self.kwargs)
        session_info = SessionInfo(
            dashboard_url=real_ray.worker.get_dashboard_url(),
            python_version=".".join(str(x) for x in sys.version_info[:3]),
            ray_version=real_ray.__version__,
            ray_commit=real_ray.__commit__,
            protocol_version=real_ray.util.client.CURRENT_PROTOCOL_VERSION,
        )
        atexit.register(real_ray.shutdown, True)
        return session_info

    def resources(
            self,
            num_cpus: Optional[int] = None,
            num_gpus: Optional[int] = None,
            resources: Optional[Dict[str, float]] = None,
    ):
        if num_cpus:
            self.kwargs["num_cpus"] = num_cpus
        if num_gpus:
            self.kwargs["num_gpus"] = num_gpus
        if resources:
            self.kwargs["resources"] = resources
        return self


class ConnectOrCreateSessionBuilder(LocalSessionBuilder):
    def connect(self):
        redis_addresses = find_redis_address()
        if len(redis_addresses) == 1:
            redis_client = create_redis_client(
                redis_addresses.pop(), ray_constants.REDIS_DEFAULT_PASSWORD)
            self.address = json.loads(
                redis_client.hget("healthcheck:ray_client_server",
                                  "value"))["location"]
            super(LocalSessionBuilder, self).connect()
        elif len(redis_addresses) == 0:
            # Start up a Ray Client Server & Connect
            return super().connect()
        else:
            raise ConnectionError(
                f"Found multiple active Ray instances: {redis_addresses}.")


def parse_protocol_from_string(address: str) -> str:
    if address.find("://") == -1:
        return "ray"
    package = address.split("://")[0]
    return package


def session(address: Optional[str] = None) -> Any:
    overwrite_address = os.environ.get("RAY_OVERWRITE_ADDRESS")
    if overwrite_address:
        if address:
            logger.info(f"Overwriting address with: {overwrite_address}")
        address = overwrite_address

    if address is None:
        return ConnectOrCreateSessionBuilder(address)
    elif address == "local" or address == "auto":
        return LocalSessionBuilder(address)
    else:
        protocol = parse_protocol_from_string(address)
        plugin = importlib.import_module(protocol)
        return plugin.SessionBuilder(address)
