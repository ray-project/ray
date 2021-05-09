import os
import importlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ray.ray_constants import RAY_ADDRESS_ENVIRONMENT_VARIABLE
from ray.job_config import JobConfig
import ray.util

logger = logging.getLogger(__name__)


@dataclass
class ClientInfo:
    dashboard_url: str
    python_version: str
    ray_version: str
    ray_commit: str
    protocol_version: str


class ClientBuilder:
    def __init__(self, address: Optional[str]) -> None:
        self.address = address
        self.job_config = JobConfig()

    def env(self, env: Dict[str, Any]) -> "ClientBuilder":
        self.job_config = JobConfig(runtime_env=env)
        return self

    def connect(self) -> ClientInfo:
        client_info_tuple = ray.util.connect(
            self.address, job_config=self.job_config)
        return ClientInfo(
            dashboard_url=None,
            python_version=client_info_tuple["python_version"],
            ray_version=client_info_tuple["ray_version"],
            ray_commit=client_info_tuple["ray_commit"],
            protocol_version=client_info_tuple["protocol_version"])


class _LocalClientBuilder(ClientBuilder):
    pass


def _get_builder_from_address(address: Optional[str]) -> ClientBuilder:
    if address is None or address == "local":
        return _LocalClientBuilder(address)
    elif address.find("://") == -1:
        return ClientBuilder(address)
    else:
        module = importlib.import_module(address.split("://")[0])
        return module.ClientBuilder(address)


def client(address: Optional[str] = None) -> ClientBuilder:
    override_address = os.environ.get(RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if override_address:
        logger.debug(
            f"Using address ({override_address}) instead of "
            f"({address}) because {RAY_ADDRESS_ENVIRONMENT_VARIABLE} is set")
        address = override_address

    return _get_builder_from_address(address)
