import importlib
import inspect
import json
import logging
import os
import sys
import warnings
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import ray.util.client_connect
from ray._private.ray_constants import (
    RAY_ADDRESS_ENVIRONMENT_VARIABLE,
    RAY_NAMESPACE_ENVIRONMENT_VARIABLE,
    RAY_RUNTIME_ENV_ENVIRONMENT_VARIABLE,
)
from ray._private.utils import check_ray_client_dependencies_installed, split_address
from ray._private.worker import BaseContext
from ray._private.worker import init as ray_driver_init
from ray.job_config import JobConfig
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(__name__)

CLIENT_DOCS_URL = (
    "https://docs.ray.io/en/latest/cluster/running-applications/"
    "job-submission/ray-client.html"
)


@dataclass
@PublicAPI
class ClientContext(BaseContext):
    """
    Basic context manager for a ClientBuilder connection.

    `protocol_version` is no longer used.
    """

    dashboard_url: Optional[str]
    python_version: str
    ray_version: str
    ray_commit: str
    _num_clients: int
    _context_to_restore: Optional[ray.util.client.RayAPIStub]
    protocol_version: Optional[str] = None  # Deprecated

    def __enter__(self) -> "ClientContext":
        self._swap_context()
        return self

    def __exit__(self, *exc) -> None:
        self._disconnect_with_context(False)
        self._swap_context()

    def disconnect(self) -> None:
        self._swap_context()
        self._disconnect_with_context(True)
        self._swap_context()

    def _swap_context(self):
        if self._context_to_restore is not None:
            self._context_to_restore = ray.util.client.ray.set_context(
                self._context_to_restore
            )

    def _disconnect_with_context(self, force_disconnect: bool) -> None:
        """
        Disconnect Ray. If it's a ray client and created with `allow_multiple`,
        it will do nothing. For other cases this either disconnects from the
        remote Client Server or shuts the current driver down.
        """
        if ray.util.client.ray.is_connected():
            if ray.util.client.ray.is_default() or force_disconnect:
                # This is the only client connection
                ray.util.client_connect.disconnect()
        elif ray._private.worker.global_worker.node is None:
            # Already disconnected.
            return
        elif ray._private.worker.global_worker.node.is_head():
            logger.debug(
                "The current Ray Cluster is scoped to this process. "
                "Disconnecting is not possible as it will shutdown the "
                "cluster."
            )
        else:
            # This is only a driver connected to an existing cluster.
            ray.shutdown()


@Deprecated
class ClientBuilder:
    """
    Builder for a Ray Client connection. This class can be subclassed by
    custom builder classes to modify connection behavior to include additional
    features or altered semantics. One example is the ``_LocalClientBuilder``.
    """

    def __init__(self, address: Optional[str]) -> None:
        if not check_ray_client_dependencies_installed():
            raise ValueError(
                "Ray Client requires pip package `ray[client]`. "
                "If you installed the minimal Ray (e.g. `pip install ray`), "
                "please reinstall by executing `pip install ray[client]`."
            )
        self.address = address
        self._job_config = JobConfig()
        self._remote_init_kwargs = {}
        # Whether to allow connections to multiple clusters"
        # " (allow_multiple=True).
        self._allow_multiple_connections = False
        self._credentials = None
        self._metadata = None
        # Set to False if ClientBuilder is being constructed by internal
        # methods
        self._deprecation_warn_enabled = True

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
            namespace: Namespace to use.
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
        if self._deprecation_warn_enabled:
            self._client_deprecation_warn()
        # Fill runtime env/namespace from environment if not already set.
        # Should be done *after* the deprecation warning, since warning will
        # check if those values are already set.
        self._fill_defaults_from_env()

        # If it has already connected to the cluster with allow_multiple=True,
        # connect to the default one is not allowed.
        # But if it has connected to the default one, connect to other clients
        # with allow_multiple=True is allowed
        default_cli_connected = ray.util.client.ray.is_connected()
        has_cli_connected = ray.util.client.num_connected_contexts() > 0
        if (
            not self._allow_multiple_connections
            and not default_cli_connected
            and has_cli_connected
        ):
            raise ValueError(
                "The client has already connected to the cluster "
                "with allow_multiple=True. Please set allow_multiple=True"
                " to proceed"
            )

        old_ray_cxt = None
        if self._allow_multiple_connections:
            old_ray_cxt = ray.util.client.ray.set_context(None)

        client_info_dict = ray.util.client_connect.connect(
            self.address,
            job_config=self._job_config,
            _credentials=self._credentials,
            ray_init_kwargs=self._remote_init_kwargs,
            metadata=self._metadata,
        )

        dashboard_url = ray.util.client.ray._get_dashboard_url()

        cxt = ClientContext(
            dashboard_url=dashboard_url,
            python_version=client_info_dict["python_version"],
            ray_version=client_info_dict["ray_version"],
            ray_commit=client_info_dict["ray_commit"],
            _num_clients=client_info_dict["num_clients"],
            _context_to_restore=ray.util.client.ray.get_context(),
        )
        if self._allow_multiple_connections:
            ray.util.client.ray.set_context(old_ray_cxt)
        return cxt

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
        arguments passed into ray.init with non-default values are passed
        again into this method. Custom client builders can override this method
        to do their own handling/validation of arguments.
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

        if "_metadata" in kwargs.keys():
            self._metadata = kwargs["_metadata"]
            del kwargs["_metadata"]

        if kwargs:
            expected_sig = inspect.signature(ray_driver_init)
            extra_args = set(kwargs.keys()).difference(expected_sig.parameters.keys())
            if len(extra_args) > 0:
                raise RuntimeError(
                    "Got unexpected kwargs: {}".format(", ".join(extra_args))
                )
            self._remote_init_kwargs = kwargs
            unknown = ", ".join(kwargs)
            logger.info(
                "Passing the following kwargs to ray.init() "
                f"on the server: {unknown}"
            )
        return self

    def _client_deprecation_warn(self) -> None:
        """
        Generates a warning for user's if this ClientBuilder instance was
        created directly or through ray.client, instead of relying on
        internal methods (ray.init, or auto init)
        """
        namespace = self._job_config.ray_namespace
        runtime_env = self._job_config.runtime_env
        replacement_args = []
        if self.address:
            if isinstance(self, _LocalClientBuilder):
                # Address might be set for LocalClientBuilder if ray.client()
                # is called while ray_current_cluster is set
                # (see _get_builder_from_address). In this case,
                # leave off the ray:// so the user attaches the driver directly
                replacement_args.append(f'"{self.address}"')
            else:
                replacement_args.append(f'"ray://{self.address}"')
        if namespace:
            replacement_args.append(f'namespace="{namespace}"')
        if runtime_env:
            # Use a placeholder here, since the real runtime_env would be
            # difficult to read if formatted in directly
            replacement_args.append("runtime_env=<your_runtime_env>")
        args_str = ", ".join(replacement_args)
        replacement_call = f"ray.init({args_str})"

        # Note: stack level is set to 3 since we want the warning to reach the
        # call to ray.client(...).connect(). The intervening frames are
        # connect() -> client_deprecation_warn() -> warnings.warn()
        # https://docs.python.org/3/library/warnings.html#available-functions
        warnings.warn(
            "Starting a connection through `ray.client` will be deprecated "
            "in future ray versions in favor of `ray.init`. See the docs for "
            f"more details: {CLIENT_DOCS_URL}. You can replace your call to "
            "`ray.client().connect()` with the following:\n"
            f"      {replacement_call}\n",
            DeprecationWarning,
            stacklevel=3,
        )


class _LocalClientBuilder(ClientBuilder):
    def connect(self) -> ClientContext:
        """
        Begin a connection to the address passed in via ray.client(...)
        """
        if self._deprecation_warn_enabled:
            self._client_deprecation_warn()
        # Fill runtime env/namespace from environment if not already set.
        # Should be done *after* the deprecation warning, since warning will
        # check if those values are already set.
        self._fill_defaults_from_env()

        connection_dict = ray.init(address=self.address, job_config=self._job_config)
        return ClientContext(
            dashboard_url=connection_dict["webui_url"],
            python_version="{}.{}.{}".format(
                sys.version_info[0], sys.version_info[1], sys.version_info[2]
            ),
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
            _num_clients=1,
            _context_to_restore=None,
        )


def _split_address(address: str) -> Tuple[str, str]:
    """
    Splits address into a module string (scheme) and an inner_address.

    If the scheme is not present, then "ray://" is prepended to the address.
    """
    if "://" not in address:
        address = "ray://" + address
    return split_address(address)


def _get_builder_from_address(address: Optional[str]) -> ClientBuilder:
    if address == "local":
        return _LocalClientBuilder("local")
    if address is None:
        # NOTE: This is not placed in `Node::get_temp_dir_path`, because
        # this file is accessed before the `Node` object is created.
        address = ray._private.services.canonicalize_bootstrap_address(address)
        return _LocalClientBuilder(address)
    module_string, inner_address = _split_address(address)
    try:
        module = importlib.import_module(module_string)
    except Exception as e:
        raise RuntimeError(
            f"Module: {module_string} does not exist.\n"
            f"This module was parsed from Address: {address}"
        ) from e
    assert "ClientBuilder" in dir(
        module
    ), f"Module: {module_string} does not have ClientBuilder."
    return module.ClientBuilder(inner_address)


@Deprecated
def client(
    address: Optional[str] = None, _deprecation_warn_enabled: bool = True
) -> ClientBuilder:
    """
    Creates a ClientBuilder based on the provided address. The address can be
    of the following forms:

        * None: Connects to or creates a local cluster and connects to it.
        * ``"local"``: Creates a new cluster locally and connects to it.
        * ``"IP:Port"``: Connects to a Ray Client Server at the given address.
        * ``"module://inner_address"``: load module.ClientBuilder & pass
            inner_address

    The _deprecation_warn_enabled flag enables deprecation warnings, and is
    for internal use only. Set it to False to suppress client deprecation
    warnings.
    """
    env_address = os.environ.get(RAY_ADDRESS_ENVIRONMENT_VARIABLE)
    if env_address and address is None:
        logger.debug(
            f"Using address ({env_address}) instead of auto-detection "
            f"because {RAY_ADDRESS_ENVIRONMENT_VARIABLE} is set."
        )
        address = env_address

    builder = _get_builder_from_address(address)
    # Disable client deprecation warn when ray.client is used internally
    builder._deprecation_warn_enabled = _deprecation_warn_enabled
    return builder
