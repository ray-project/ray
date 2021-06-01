import os
import importlib
import logging
from dataclasses import dataclass
from urllib.parse import urlparse
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
    Builder for a Ray Client connection. This class can be subclassed by
    custom builder classes to modify connection behavior to include additional
    features or altered semantics. One example is the ``_LocalClientBuilder``.
    """

    def __init__(self, address: Optional[str]) -> None:
        self.address = address
        self._job_config = JobConfig()

    def env(self, env: Dict[str, Any]) -> "ClientBuilder":
        """
        Set an environment for the session.
        Args:
            env (Dict[st, Any]): A runtime environment to use for this
            connection. See ``runtime_env.py`` for what values are
            accepted in this dict.
        """
        self._job_config.set_runtime_env(env)
        return self

    def namespace(self, namespace: str) -> "ClientBuilder":
        """
        Sets the namespace for the session.
        Args:
            namespace (str): Namespace to use.
        """
        self._job_config.set_ray_namespace(namespace)
        return self

    def connect(self) -> ClientContext:
        """
        Begin a connection to the address passed in via ray.client(...).

        Returns:
            ClientInfo: Dataclass with information about the setting. This
                includes the server's version of Python & Ray as well as the
                dashboard_url.
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
            protocol_version=None)


def _split_address(address: str) -> Tuple[str, str]:
    """
    Splits address into a module string (scheme) and an inner_address.
    """
    if "://" not in address:
        address = "ray://" + address
    url_object = urlparse(address)
    module_string = url_object.scheme
    inner_address = address.replace(module_string + "://", "", 1)
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
    module = importlib.import_module(module_string)
    return module.ClientBuilder(inner_address)


def client(address: Optional[str] = None) -> ClientBuilder:
    """
    Creates a ClientBuilder based on the provided address. The address can be
    of the following forms:

        * None: Connects to or creates a local cluster and connects to it.
        * ``"local"``: Creates a new cluster locally and connects to it.
        * ``"IP:Port"``: Connects to a Ray Client Server at the given address.
        * ``"module://inner_address"``: load module.ClientBuilder & pass
            inner_address
    """
    override_address = os.environ.get(RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if override_address:
        logger.debug(
            f"Using address ({override_address}) instead of "
            f"({address}) because {RAY_ADDRESS_ENVIRONMENT_VARIABLE} is set")
        address = override_address

    return _get_builder_from_address(address)
