import atexit
import os
import importlib
import logging
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Any, Dict, Optional, Tuple

import ray
from ray.ray_constants import RAY_ADDRESS_ENVIRONMENT_VARIABLE
from ray.ray_constants import RAY_CLIENT_SERVER_PORT_REDIS_KEY
from ray.ray_constants import REDIS_DEFAULT_PASSWORD
from ray.job_config import JobConfig
from ray._private.client_mode_hook import disable_client_hook
from ray._private.services import create_redis_client, find_redis_address
from ray._private.utils import get_unused_port
import ray.util.client.server as client_server
import ray.util.client_connect

logger = logging.getLogger(__name__)


@dataclass
class ClientInfo:
    """
    Basic information of the remote server for a given Ray Client connection.
    """
    dashboard_url: Optional[str]
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
        self._job_config: Optional[JobConfig] = None

    def env(self, env: Dict[str, Any]) -> "ClientBuilder":
        """
        Set an environment for the session.
        """
        self._job_config = JobConfig(runtime_env=env)
        return self

    def connect(self) -> ClientInfo:
        """
        Begin a connection to the address passed in via ray.client(...).
        """
        client_info_dict = ray.util.client_connect.connect(
            self.address, job_config=self._job_config)
        dashboard_url = ray.get(
            ray.remote(ray.worker.get_dashboard_url).remote())
        return ClientInfo(
            dashboard_url=dashboard_url,
            python_version=client_info_dict["python_version"],
            ray_version=client_info_dict["ray_version"],
            ray_commit=client_info_dict["ray_commit"],
            protocol_version=client_info_dict["protocol_version"])


class _LocalClientBuilder(ClientBuilder):
    """
    Connects/or Creates to a local Ray Cluster with Ray Client.
    """

    def __init__(self, address: Optional[str]) -> None:
        super().__init__(address)
        # If local, we should always create a new cluster.
        self.create_cluster_without_searching = (address == "local")
        self._init_args = {}
        self._server = None

    def resources(
            self,
            *,
            num_cpus: Optional[int] = None,
            num_gpus: Optional[int] = None,
            resources: Optional[Dict[str, str]] = None,
            object_store_memory: Optional[int] = None) -> "ClientBuilder":
        self._init_args["num_cpus"] = num_cpus
        self._init_args["num_gpus"] = num_gpus
        self._init_args["resources"] = resources
        self._init_args["object_store_memory"] = object_store_memory
        return self

    def dashboard(self,
                  *,
                  include: bool = True,
                  host: str = "127.0.0.1",
                  port: Optional[int] = None) -> "ClientBuilder":
        assert self.create_cluster_without_searching
        self._init_args["include_dashboard"] = include
        self._init_args["dashboard_host"] = host
        self._init_args["dashboard_port"] = port
        return self

    def _create_local_cluster(self):
        def ray_connect_handler(job_config=None):
            with disable_client_hook():
                if not ray.is_initialized():
                    ray.init(job_config=job_config, **self._init_args)

        port, _ = get_unused_port()
        self.address = f"localhost:{port}"
        logger.info(f"Creating a server on {self.address}")
        self._server = client_server.serve(self.address, ray_connect_handler)
        atexit.register(ray.shutdown, True)
        atexit.register(self._server.grpc_server.stop, 0)

    def _find_or_create_cluster(self):
        """
        Searches for a local cluster, and looks in redis for the port
        of Ray Client Server. If this fails, a cluster is manually launched.
        """
        redis_addresses = find_redis_address()
        if len(redis_addresses) > 1:
            raise ConnectionError(
                f"Found multiple active Ray instances: {redis_addresses}.")

        if len(redis_addresses) == 1:
            address = redis_addresses.pop()
            redis_client = create_redis_client(address, REDIS_DEFAULT_PASSWORD)
            port = redis_client.get(RAY_CLIENT_SERVER_PORT_REDIS_KEY)
            if port is not None:
                self.address = "localhost:" + port.decode("UTF-8")
                return
            else:
                logger.warning(
                    f"Found cluster at: {address}, but unable to "
                    "determine the Ray Client Server Port in Redis. "
                    "Starting a new cluster instead.")

        self._create_local_cluster()

    def connect(self) -> ClientInfo:
        """
        Connects to a local Ray cluster.
        If address="local":
            Always create a new local cluster.
        If address="None"
           Search for a locally running cluster with the Ray Client Server
           running or create one.
        """
        if self.create_cluster_without_searching:
            self._create_local_cluster()
        else:
            self._find_or_create_cluster()
        return super().connect()


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
    if address is None or address == "local":
        local_builder = _LocalClientBuilder(address)
        return local_builder
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
    * module://inner_address -> load module.ClientBuilder & pass inner_address
    """
    override_address = os.environ.get(RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if override_address:
        logger.debug(
            f"Using address ({override_address}) instead of "
            f"({address}) because {RAY_ADDRESS_ENVIRONMENT_VARIABLE} is set")
        address = override_address

    return _get_builder_from_address(address)
