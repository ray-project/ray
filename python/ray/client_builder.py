import os
import importlib
import logging
from dataclasses import dataclass
import sys

from typing import Any, Dict, Optional, Tuple

from ray.ray_constants import RAY_ADDRESS_ENVIRONMENT_VARIABLE
from ray.job_config import JobConfig
import ray.util.client_connect

logger = logging.getLogger(__name__)


@dataclass
class ClientContext:
    """
    Basic context manager for a ClientBuilder connection.
    """
    dashboard_url: Optional[str]
    python_version: str
    ray_version: str
    ray_commit: str
    protocol_version: Optional[str]
    _num_clients: int

    def __enter__(self) -> "ClientContext":
        return self

    def __exit__(self, *exc) -> None:
        self.disconnect()

    def disconnect(self) -> None:
        """
        Disconnect Ray. This either disconnects from the remote Client Server
        or shuts the current driver down.
        """
        if ray.util.client.ray.is_connected():
            # This is only a client connected to a server.
            ray.util.client_connect.disconnect()
            ray._private.client_mode_hook._explicitly_disable_client_mode()
        elif ray.worker.global_worker.node is None:
            # Already disconnected.
            return
        elif ray.worker.global_worker.node.is_head():
            logger.debug(
                "The current Ray Cluster is scoped to this process. "
                "Disconnecting is not possible as it will shutdown the "
                "cluster.")
        else:
            # This is only a driver connected to an existing cluster.
            ray.shutdown()


class ClientBuilder:
    """
    Builder for a Ray Client connection.
    """

    def __init__(self, address: Optional[str]) -> None:
        self.address = address
        self._job_config = JobConfig()

    def env(self, env: Dict[str, Any]) -> "ClientBuilder":
        """
        Set an environment for the session.
        """
        self._job_config.set_runtime_env(env)
        return self

    def namespace(self, namespace: str) -> "ClientBuilder":
        self._job_config.set_ray_namespace(namespace)
        return self

    def connect(self) -> ClientContext:
        """
        Begin a connection to the address passed in via ray.client(...).
        """
        client_info_dict = ray.util.client_connect.connect(
            self.address, job_config=self._job_config)
        dashboard_url = ray.get(
            ray.remote(ray.worker.get_dashboard_url).remote())
        return ClientContext(
            dashboard_url=dashboard_url,
            python_version=client_info_dict["python_version"],
            ray_version=client_info_dict["ray_version"],
            ray_commit=client_info_dict["ray_commit"],
            protocol_version=client_info_dict["protocol_version"],
            _num_clients=client_info_dict["num_clients"])


class _LocalClientBuilder(ClientBuilder):
    def connect(self) -> ClientContext:
        """
        Begin a connection to the address passed in via ray.client(...).
        """
        connection_dict = ray.init(
            address=self.address, job_config=self._job_config)
        return ClientContext(
            dashboard_url=connection_dict["webui_url"],
            python_version="{}.{}.{}".format(
                sys.version_info[0], sys.version_info[1], sys.version_info[2]),
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
            protocol_version=None,
            _num_clients=1)


def _split_address(address: str) -> Tuple[str, str]:
    """
    Splits address into a module string (scheme) and an inner_address.
    """
    if "://" not in address:
        address = "ray://" + address
    # NOTE: We use a custom splitting function instead of urllib because
    # PEP allows "underscores" in a module names, while URL schemes do not
    # allow them.
    module_string, inner_address = address.split("://", maxsplit=1)
    return (module_string, inner_address)


def _get_builder_from_address(address: Optional[str]) -> ClientBuilder:
    if address == "local":
        return _LocalClientBuilder(None)
    if address is None:
        try:
            # NOTE: This is not placed in `Node::get_temp_dir_path`, because
            # this file is accessed before the `Node` object is created.
            cluster_file = os.path.join(ray._private.utils.get_user_temp_dir(),
                                        "ray_current_cluster")
            with open(cluster_file, "r") as f:
                address = f.read()
                print(address)
        except FileNotFoundError:
            # `address` won't be set and we'll create a new cluster.
            pass
        return _LocalClientBuilder(address)
    module_string, inner_address = _split_address(address)
    try:
        module = importlib.import_module(module_string)
    except Exception:
        raise RuntimeError(
            f"Module: {module_string} does not exist.\n"
            f"This module was parsed from Address: {address}") from None
    assert "ClientBuilder" in dir(module), (f"Module: {module_string} does "
                                            "not have ClientBuilder.")
    return module.ClientBuilder(inner_address)


def client(address: Optional[str] = None) -> ClientBuilder:
    """
    Creates a ClientBuilder based on the provided address. The address can be
    of the following forms:
    * None -> Connects to or creates a local cluster and connects to it.
    * local -> Creates a new cluster locally and connects to it.
    * IP:Port -> Connects to a Ray Client Server at the given address.
    * module://inner_address -> load module.ClientBuilder & pass inner_address
    """
    env_address = os.environ.get(RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if env_address and address is None:
        logger.debug(
            f"Using address ({env_address}) instead of auto-detection "
            f"because {RAY_ADDRESS_ENVIRONMENT_VARIABLE} is set.")
        address = env_address

    return _get_builder_from_address(address)
