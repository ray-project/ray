import os
import importlib
import inspect
import json
import logging
import sys

from typing import Any, Dict, Optional, Tuple

from ray.ray_constants import (RAY_ADDRESS_ENVIRONMENT_VARIABLE,
                               RAY_NAMESPACE_ENVIRONMENT_VARIABLE,
                               RAY_RUNTIME_ENV_ENVIRONMENT_VARIABLE)
from ray.job_config import JobConfig
import ray.util.client_connect
from ray.worker import init as ray_driver_init
from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)


class ClientBuilder:
    """
    Builder for a Ray Client connection. This class can be subclassed by
    custom builder classes to modify connection behavior to include additional
    features or altered semantics. One example is the ``_LocalClientBuilder``.
    """

    def __init__(self, address: Optional[str]) -> None:
        self.address = address
        self._job_config = JobConfig()
        self._fill_defaults_from_env()
        self._remote_init_kwargs = {}
        # Whether to allow connections to multiple clusters"
        # " (allow_multiple=True).
        self._allow_multiple_connections = False
        self._credentials = None

    def env(self, env: Dict[str, Any]) -> "ClientBuilder":
        """
        Set an environment for the session.
        Args:
            env (Dict[st, Any]): A runtime environment to use for this
            connection. See :ref:`runtime-environments` for what values are
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

    def connect(self) -> ray.util.client.ManagedContext:
        """
        Begin a connection to the address passed in via ray.client(...).

        Returns:
            ClientInfo: Dataclass with information about the setting. This
                includes the server's version of Python & Ray as well as the
                dashboard_url.
        """
        # If it has already connected to the cluster with allow_multiple=True,
        # connect to the default one is not allowed.
        # But if it has connected to the default one, connect to other clients
        # with allow_multiple=True is allowed
        default_cli_connected = ray.util.client.ray.is_connected()
        has_cli_connected = ray.util.client.num_connected_contexts() > 0
        if not self._allow_multiple_connections and \
           not default_cli_connected and has_cli_connected:
            raise ValueError(
                "The client has already connected to the cluster "
                "with allow_multiple=True. Please set allow_multiple=True"
                " to proceed")

        old_ray_cxt = None
        if self._allow_multiple_connections:
            old_ray_cxt = ray.util.client.ray.set_context(None)

        ray.util.client_connect.connect(
            self.address,
            job_config=self._job_config,
            _credentials=self._credentials,
            ray_init_kwargs=self._remote_init_kwargs)
        dashboard_url = ray.get(
            ray.remote(ray.worker.get_dashboard_url).remote())
        ctx = ray.util.client.ray.get_context()
        ctx._conn_info["dashboard_url"] = dashboard_url
        managed_ctx = ray.util.client.ManagedContext(ctx._conn_info, ctx)
        if self._allow_multiple_connections:
            ray.util.client.ray.set_context(old_ray_cxt)
        return managed_ctx

    def _fill_defaults_from_env(self):
        # Check environment variables for default values
        namespace_env_var = os.environ.get(RAY_NAMESPACE_ENVIRONMENT_VARIABLE)
        if namespace_env_var and self._job_config.ray_namespace is None:
            self.namespace(namespace_env_var)

        runtime_env_var = os.environ.get(RAY_RUNTIME_ENV_ENVIRONMENT_VARIABLE)
        if runtime_env_var and self._job_config.runtime_env is None:
            self.env(json.loads(runtime_env_var))

    def _init_args(self, **kwargs) -> "ClientBuilder":
        """
        When a client builder is constructed through ray.init, for example
        `ray.init(ray://..., namespace=...)`, all of the
        arguments passed into ray.init are passed again into this method.
        Custom client builders can override this method to do their own
        handling/validation of arguments.
        """
        # Use namespace and runtime_env from ray.init call
        if kwargs.get("namespace") is not None:
            self.namespace(kwargs["namespace"])
            del kwargs["namespace"]
        if kwargs.get("runtime_env") is not None:
            self.env(kwargs["runtime_env"])
            del kwargs["runtime_env"]

        if kwargs.get("allow_multiple") is True:
            self._allow_multiple_connections = True
            del kwargs["allow_multiple"]

        if "_credentials" in kwargs.keys():
            self._credentials = kwargs["_credentials"]
            del kwargs["_credentials"]

        if kwargs:
            expected_sig = inspect.signature(ray_driver_init)
            extra_args = set(kwargs.keys()).difference(
                expected_sig.parameters.keys())
            if len(extra_args) > 0:
                raise RuntimeError("Got unexpected kwargs: {}".format(
                    ", ".join(extra_args)))
            self._remote_init_kwargs = kwargs
            unknown = ", ".join(kwargs)
            logger.info("Passing the following kwargs to ray.init() "
                        f"on the server: {unknown}")
        return self


class _LocalClientBuilder(ClientBuilder):
    def connect(self) -> ray.util.client.ManagedContext:
        """
        Begin a connection to the address passed in via ray.client(...)
        """
        connection_dict = ray.init(
            address=self.address, job_config=self._job_config)
        return ray.util.client.ManagedContext(
            dict(
                dashboard_url=connection_dict["webui_url"],
                python_version="{}.{}.{}".format(sys.version_info[0],
                                                 sys.version_info[1],
                                                 sys.version_info[2]),
                ray_version=ray.__version__,
                ray_commit=ray.__commit__,
                protocol_version=None,
                num_clients=1),
            _context_to_restore=None)


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
                address = f.read().strip()
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


@Deprecated
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
    env_address = os.environ.get(RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if env_address and address is None:
        logger.debug(
            f"Using address ({env_address}) instead of auto-detection "
            f"because {RAY_ADDRESS_ENVIRONMENT_VARIABLE} is set.")
        address = env_address

    return _get_builder_from_address(address)
