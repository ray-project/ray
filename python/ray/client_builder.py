import os
import importlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from ray.ray_constants import RAY_ADDRESS_ENVIRONMENT_VARIABLE
from ray.job_config import JobConfig
import ray.util

logger = logging.getLogger(__name__)


@dataclass
class ClientInfo:
    """
    Basic information of the remote server for a given Ray Client connection.
    """
    dashboard_url: str
    python_version: str
    ray_version: str
    ray_commit: str
    protocol_version: str


class ClientBuilder:
    """
    Builder for a Ray Client connection.
    """

    def __init__(self, address: Optional[str]) -> None:
        self.address = address
        self.job_config = JobConfig()

    def env(self, env: Dict[str, Any]) -> "ClientBuilder":
        """
        Set an environment for the session.
        """
        self.job_config = JobConfig(runtime_env=env)
        return self

    def connect(self) -> ClientInfo:
        """
        Begin a connection to the address passed in via ray.client(...).
        """
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


def _split_address(address: str) -> Tuple[str, str]:
    """
    Splits address by "://" into a module string and an inner_address.
    NOTE: inner_address may contain "://"
    """
    module_start_idx = address.find("://")
    module_string = address[0:module_start_idx]
    inner_address_start_idx = module_start_idx + len("://")
    inner_address = address[inner_address_start_idx:]
    return (module_string, inner_address)


def _get_builder_from_address(address: Optional[str]) -> ClientBuilder:
    if address is None or address == "local":
        return _LocalClientBuilder(address)
    elif address.find("://") == -1:
        return ClientBuilder(address)
    else:
        module_string, inner_address = _split_address(address)
        module = importlib.import_module(module_string)
        return module.ClientBuilder(inner_address)


def client(address: Optional[str] = None) -> ClientBuilder:
    """
    Creates a ClientBuilder based on the provided address. The address can be
    of the following forms:
    * None -> Connects to or creates a local cluster and connects to it.
    * local -> Creates a new cluster locally and connects to it.
    * IP:Port -> Connects to a Ray Client Server at the given address.
    * module://inner_addres -> load module.ClientBuilder and pass inner_address
    """
    override_address = os.environ.get(RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if override_address:
        logger.debug(
            f"Using address ({override_address}) instead of "
            f"({address}) because {RAY_ADDRESS_ENVIRONMENT_VARIABLE} is set")
        address = override_address

    return _get_builder_from_address(address)
