import logging
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union
from zlib import crc32

from ray._common.pydantic_compat import (
    BaseModel,
    Extra,
    Field,
    NonNegativeInt,
    PositiveInt,
    StrictInt,
    root_validator,
    validator,
)
from ray._private.ray_logging.constants import LOGRECORD_STANDARD_ATTRS
from ray._private.runtime_env.packaging import parse_uri
from ray.serve._private.common import (
    DeploymentStatus,
    DeploymentStatusTrigger,
    ReplicaState,
    RequestProtocol,
    ServeDeployMode,
)
from ray.serve._private.constants import (
    DEFAULT_CONSUMER_CONCURRENCY,
    DEFAULT_GRPC_PORT,
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    RAY_SERVE_LOG_ENCODING,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.utils import DEFAULT, validate_ssl_config
from ray.serve.config import ProxyLocation, RequestRouterConfig
from ray.util.annotations import PublicAPI

# Shared amongst multiple schemas.
TARGET_CAPACITY_FIELD = Field(
    default=None,
    description=(
        "[EXPERIMENTAL]: the target capacity percentage for all replicas across the "
        "cluster. The `num_replicas`, `min_replicas`, `max_replicas`, and "
        "`initial_replicas` for each deployment will be scaled by this percentage."
    ),
    ge=0,
    le=100,
)


def _route_prefix_format(cls, v):
    """
    The route_prefix
    1. must start with a / character
    2. must not end with a / character (unless the entire prefix is just /)
    3. cannot contain wildcards (must not have "{" or "}")
    """

    if v is None:
        return v

    if not v.startswith("/"):
        raise ValueError(
            f'Got "{v}" for route_prefix. Route prefix must start with "/".'
        )
    if len(v) > 1 and v.endswith("/"):
        raise ValueError(
            f'Got "{v}" for route_prefix. Route prefix '
            'cannot end with "/" unless the '
            'entire prefix is just "/".'
        )
    if "{" in v or "}" in v:
        raise ValueError(
            f'Got "{v}" for route_prefix. Route prefix '
            "cannot contain wildcards, so it cannot "
            'contain "{" or "}".'
        )

    return v


@PublicAPI(stability="alpha")
class EncodingType(str, Enum):
    """Encoding type for the serve logs."""

    TEXT = "TEXT"
    JSON = "JSON"


@PublicAPI(stability="alpha")
class LoggingConfig(BaseModel):
    """Logging config schema for configuring serve components logs.

    Example:

        .. code-block:: python

            from ray import serve
            from ray.serve.schema import LoggingConfig
            # Set log level for the deployment.
            @serve.deployment(LoggingConfig(log_level="DEBUG"))
            class MyDeployment:
                def __call__(self) -> str:
                    return "Hello world!"
            # Set log directory for the deployment.
            @serve.deployment(LoggingConfig(logs_dir="/my_dir"))
            class MyDeployment:
                def __call__(self) -> str:
                    return "Hello world!"
    """

    class Config:
        extra = Extra.forbid

    encoding: Union[str, EncodingType] = Field(
        default_factory=lambda: RAY_SERVE_LOG_ENCODING,
        description=(
            "Encoding type for the serve logs. Defaults to 'TEXT'. The default can be "
            "overwritten using the `RAY_SERVE_LOG_ENCODING` environment variable. "
            "'JSON' is also supported for structured logging."
        ),
    )
    log_level: Union[int, str] = Field(
        default="INFO",
        description=(
            "Log level for the serve logs. Defaults to INFO. You can set it to "
            "'DEBUG' to get more detailed debug logs."
        ),
    )
    logs_dir: Union[str, None] = Field(
        default=None,
        description=(
            "Directory to store the logs. Default to None, which means "
            "logs will be stored in the default directory "
            "('/tmp/ray/session_latest/logs/serve/...')."
        ),
    )
    enable_access_log: bool = Field(
        default=True,
        description=(
            "Whether to enable access logs for each request. Default to True."
        ),
    )
    additional_log_standard_attrs: List[str] = Field(
        default_factory=list,
        description=(
            "Default attributes from the Python standard logger that will be "
            "added to all log records. "
            "See https://docs.python.org/3/library/logging.html#logrecord-attributes "
            "for a list of available attributes."
        ),
    )

    @validator("encoding")
    def valid_encoding_format(cls, v):
        if v not in list(EncodingType):
            raise ValueError(
                f"Got '{v}' for encoding. Encoding must be one "
                f"of {set(EncodingType)}."
            )

        return v

    @validator("log_level")
    def valid_log_level(cls, v):
        if isinstance(v, int):
            if v not in logging._levelToName:
                raise ValueError(
                    f'Got "{v}" for log_level. log_level must be one of '
                    f"{list(logging._levelToName.keys())}."
                )
            return logging._levelToName[v]

        if v not in logging._nameToLevel:
            raise ValueError(
                f'Got "{v}" for log_level. log_level must be one of '
                f"{list(logging._nameToLevel.keys())}."
            )
        return v

    @validator("additional_log_standard_attrs")
    def valid_additional_log_standard_attrs(cls, v):
        for attr in v:
            if attr not in LOGRECORD_STANDARD_ATTRS:
                raise ValueError(
                    f"Unknown attribute '{attr}'. "
                    f"Additional log standard attributes must be one of {LOGRECORD_STANDARD_ATTRS}."
                )
        return list(set(v))

    def _compute_hash(self) -> int:
        return crc32(
            (
                str(self.encoding)
                + str(self.log_level)
                + str(self.logs_dir)
                + str(self.enable_access_log)
            ).encode("utf-8")
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LoggingConfig):
            return False
        return self._compute_hash() == other._compute_hash()


@PublicAPI(stability="stable")
class RayActorOptionsSchema(BaseModel):
    """Options with which to start a replica actor."""

    runtime_env: dict = Field(
        default={},
        description=(
            "This deployment's runtime_env. working_dir and "
            "py_modules may contain only remote URIs."
        ),
    )
    num_cpus: float = Field(
        default=None,
        description=(
            "The number of CPUs required by the deployment's "
            "application per replica. This is the same as a ray "
            "actor's num_cpus. Uses a default if null."
        ),
        ge=0,
    )
    num_gpus: float = Field(
        default=None,
        description=(
            "The number of GPUs required by the deployment's "
            "application per replica. This is the same as a ray "
            "actor's num_gpus. Uses a default if null."
        ),
        ge=0,
    )
    memory: float = Field(
        default=None,
        description=(
            "Restrict the heap memory usage of each replica. Uses a default if null."
        ),
        ge=0,
    )
    resources: Dict = Field(
        default={},
        description=("The custom resources required by each replica."),
    )
    accelerator_type: str = Field(
        default=None,
        description=(
            "Forces replicas to run on nodes with the specified accelerator type."
            "See :ref:`accelerator types <accelerator_types>`."
        ),
    )

    @validator("runtime_env")
    def runtime_env_contains_remote_uris(cls, v):
        # Ensure that all uris in py_modules and working_dir are remote

        if v is None:
            return

        uris = v.get("py_modules", [])
        if "working_dir" in v:
            uris = [*uris, v["working_dir"]]

        for uri in uris:
            if uri is not None:
                try:
                    parse_uri(uri)
                except ValueError as e:
                    raise ValueError(
                        "runtime_envs in the Serve config support only "
                        "remote URIs in working_dir and py_modules. Got "
                        f"error when parsing URI: {e}"
                    )

        return v


@PublicAPI(stability="stable")
class DeploymentSchema(BaseModel, allow_population_by_field_name=True):
    """
    Specifies options for one deployment within a Serve application. For each deployment
    this can optionally be included in `ServeApplicationSchema` to override deployment
    options specified in code.
    """

    name: str = Field(
        ..., description=("Globally-unique name identifying this deployment.")
    )
    num_replicas: Optional[Union[PositiveInt, str]] = Field(
        default=DEFAULT.VALUE,
        description=(
            "The number of processes that handle requests to this "
            "deployment. Uses a default if null. Can also be set to "
            "`auto` for a default autoscaling configuration "
            "(experimental)."
        ),
    )
    max_ongoing_requests: int = Field(
        default=DEFAULT.VALUE,
        description=(
            "Maximum number of requests that are sent in parallel "
            "to each replica of this deployment. The limit is enforced across all "
            "callers (HTTP requests or DeploymentHandles). Defaults to "
            f"{DEFAULT_MAX_ONGOING_REQUESTS}."
        ),
        gt=0,
    )
    max_queued_requests: StrictInt = Field(
        default=DEFAULT.VALUE,
        description=(
            "[DEPRECATED] The max number of requests that will be executed at once in "
            f"each replica. Defaults to {DEFAULT_MAX_ONGOING_REQUESTS}."
        ),
    )
    user_config: Optional[Dict] = Field(
        default=DEFAULT.VALUE,
        description=(
            "Config to pass into this deployment's "
            "reconfigure method. This can be updated dynamically "
            "without restarting replicas"
        ),
    )
    autoscaling_config: Optional[Dict] = Field(
        default=DEFAULT.VALUE,
        description=(
            "Config specifying autoscaling "
            "parameters for the deployment's number of replicas. "
            "If null, the deployment won't autoscale its number of "
            "replicas; the number of replicas will be fixed at "
            "num_replicas."
        ),
    )
    graceful_shutdown_wait_loop_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Duration that deployment replicas will wait until there "
            "is no more work to be done before shutting down. Uses a "
            "default if null."
        ),
        ge=0,
    )
    graceful_shutdown_timeout_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Serve controller waits for this duration before "
            "forcefully killing the replica for shutdown. Uses a "
            "default if null."
        ),
        ge=0,
    )
    health_check_period_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Frequency at which the controller will health check "
            "replicas. Uses a default if null."
        ),
        gt=0,
    )
    health_check_timeout_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Timeout that the controller will wait for a response "
            "from the replica's health check before marking it "
            "unhealthy. Uses a default if null."
        ),
        gt=0,
    )
    ray_actor_options: RayActorOptionsSchema = Field(
        default=DEFAULT.VALUE, description="Options set for each replica actor."
    )

    placement_group_bundles: List[Dict[str, float]] = Field(
        default=DEFAULT.VALUE,
        description=(
            "Define a set of placement group bundles to be "
            "scheduled *for each replica* of this deployment. The replica actor will "
            "be scheduled in the first bundle provided, so the resources specified in "
            "`ray_actor_options` must be a subset of the first bundle's resources. All "
            "actors and tasks created by the replica actor will be scheduled in the "
            "placement group by default (`placement_group_capture_child_tasks` is set "
            "to True)."
        ),
    )

    placement_group_strategy: str = Field(
        default=DEFAULT.VALUE,
        description=(
            "Strategy to use for the replica placement group "
            "specified via `placement_group_bundles`. Defaults to `PACK`."
        ),
    )

    max_replicas_per_node: int = Field(
        default=DEFAULT.VALUE,
        description=(
            "The max number of replicas of this deployment that can run on a single "
            "Valid values are None (default, no limit) or an integer in the range of "
            "[1, 100]. "
        ),
    )
    logging_config: LoggingConfig = Field(
        default=DEFAULT.VALUE,
        description="Logging config for configuring serve deployment logs.",
    )
    request_router_config: Union[Dict, RequestRouterConfig] = Field(
        default=DEFAULT.VALUE,
        description="Config for the request router used for this deployment.",
    )

    @root_validator
    def validate_num_replicas_and_autoscaling_config(cls, values):
        num_replicas = values.get("num_replicas", None)
        autoscaling_config = values.get("autoscaling_config", None)

        # Cannot have `num_replicas` be an int and a non-null
        # autoscaling config
        if isinstance(num_replicas, int):
            if autoscaling_config not in [None, DEFAULT.VALUE]:
                raise ValueError(
                    "Manually setting num_replicas is not allowed "
                    "when autoscaling_config is provided."
                )
        # A null `num_replicas` or `num_replicas="auto"` can be paired
        # with a non-null autoscaling_config
        elif num_replicas not in ["auto", None, DEFAULT.VALUE]:
            raise ValueError(
                f'`num_replicas` must be an int or "auto", but got: {num_replicas}'
            )

        return values

    @root_validator
    def validate_max_replicas_per_node_and_placement_group_bundles(cls, values):
        max_replicas_per_node = values.get("max_replicas_per_node", None)
        placement_group_bundles = values.get("placement_group_bundles", None)

        if max_replicas_per_node not in [
            DEFAULT.VALUE,
            None,
        ] and placement_group_bundles not in [DEFAULT.VALUE, None]:
            raise ValueError(
                "Setting max_replicas_per_node is not allowed when "
                "placement_group_bundles is provided."
            )

        return values

    @root_validator
    def validate_max_queued_requests(cls, values):
        max_queued_requests = values.get("max_queued_requests", None)
        if max_queued_requests is None or max_queued_requests == DEFAULT.VALUE:
            return values

        if max_queued_requests < 1 and max_queued_requests != -1:
            raise ValueError(
                "max_queued_requests must be -1 (no limit) or a positive integer."
            )

        return values

    def _get_user_configured_option_names(self) -> Set[str]:
        """Get set of names for all user-configured options.

        Any field not set to DEFAULT.VALUE is considered a user-configured option.
        """

        return {
            field for field, value in self.dict().items() if value is not DEFAULT.VALUE
        }

    def is_autoscaling_configured(self) -> bool:
        return self.num_replicas == "auto" or self.autoscaling_config not in [
            None,
            DEFAULT.VALUE,
        ]


def _deployment_info_to_schema(name: str, info: DeploymentInfo) -> DeploymentSchema:
    """Converts a DeploymentInfo object to DeploymentSchema."""

    schema = DeploymentSchema(
        name=name,
        max_ongoing_requests=info.deployment_config.max_ongoing_requests,
        max_queued_requests=info.deployment_config.max_queued_requests,
        user_config=info.deployment_config.user_config,
        graceful_shutdown_wait_loop_s=(
            info.deployment_config.graceful_shutdown_wait_loop_s
        ),
        graceful_shutdown_timeout_s=info.deployment_config.graceful_shutdown_timeout_s,
        health_check_period_s=info.deployment_config.health_check_period_s,
        health_check_timeout_s=info.deployment_config.health_check_timeout_s,
        ray_actor_options=info.replica_config.ray_actor_options,
        request_router_config=info.deployment_config.request_router_config,
    )

    if info.deployment_config.autoscaling_config is not None:
        schema.autoscaling_config = info.deployment_config.autoscaling_config.dict()
    else:
        schema.num_replicas = info.deployment_config.num_replicas

    return schema


@PublicAPI(stability="stable")
class ServeApplicationSchema(BaseModel):
    """
    Describes one Serve application, and currently can also be used as a standalone
    config to deploy a single application to a Ray cluster.
    """

    name: str = Field(
        default=SERVE_DEFAULT_APP_NAME,
        description=(
            "Application name, the name should be unique within the serve instance"
        ),
    )
    route_prefix: Optional[str] = Field(
        default="/",
        description=(
            "Route prefix for HTTP requests. If not provided, it will use"
            "route_prefix of the ingress deployment. By default, the ingress route "
            "prefix is '/'."
        ),
    )
    import_path: str = Field(
        ...,
        description=(
            "An import path to a bound deployment node. Should be of the "
            'form "module.submodule_1...submodule_n.'
            'dag_node". This is equivalent to '
            '"from module.submodule_1...submodule_n import '
            'dag_node". Only works with Python '
            "applications. This field is REQUIRED when deploying Serve config "
            "to a Ray cluster."
        ),
    )
    runtime_env: dict = Field(
        default={},
        description=(
            "The runtime_env that the deployment graph will be run in. "
            "Per-deployment runtime_envs will inherit from this. working_dir "
            "and py_modules may contain only remote URIs."
        ),
    )
    host: str = Field(
        default="0.0.0.0",
        description=(
            "Host for HTTP servers to listen on. Defaults to "
            '"0.0.0.0", which exposes Serve publicly. Cannot be updated once '
            "your Serve application has started running. The Serve application "
            "must be shut down and restarted with the new host instead."
        ),
    )
    port: int = Field(
        default=8000,
        description=(
            "Port for HTTP server. Defaults to 8000. Cannot be updated once "
            "your Serve application has started running. The Serve application "
            "must be shut down and restarted with the new port instead."
        ),
    )
    deployments: List[DeploymentSchema] = Field(
        default=[],
        description="Deployment options that override options specified in the code.",
    )
    autoscaling_policy: Optional[dict] = Field(
        default=None,
        description=(
            "Application-level autoscaling policy. "
            "If null, serve fallbacks to autoscaling policy in each deployment. "
            "This option is under development and not yet supported."
        ),
    )

    args: Dict = Field(
        default={},
        description="Arguments that will be passed to the application builder.",
    )
    logging_config: LoggingConfig = Field(
        default=None,
        description="Logging config for configuring serve application logs.",
    )
    external_scaler_enabled: bool = Field(
        default=False,
        description=(
            "If True, indicates that an external autoscaler will manage replica scaling for this application. "
            "When enabled, Serve's built-in autoscaling cannot be used for any deployments in this application."
        ),
    )

    @property
    def deployment_names(self) -> List[str]:
        return [d.name for d in self.deployments]

    @validator("runtime_env")
    def runtime_env_contains_remote_uris(cls, v):
        # Ensure that all uris in py_modules and working_dir are remote.
        if v is None:
            return

        uris = v.get("py_modules", [])
        if "working_dir" in v:
            uris = [*uris, v["working_dir"]]

        for uri in uris:
            if uri is not None:
                try:
                    parse_uri(uri)
                except ValueError as e:
                    raise ValueError(
                        "runtime_envs in the Serve config support only "
                        "remote URIs in working_dir and py_modules. Got "
                        f"error when parsing URI: {e}"
                    )

        return v

    @validator("import_path")
    def import_path_format_valid(cls, v: str):
        if v is None:
            return

        if ":" in v:
            if v.count(":") > 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path may have at most one colon."
                )
            if v.rfind(":") == 0 or v.rfind(":") == len(v) - 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path may not start or end with a colon."
                )
            return v
        else:
            if v.count(".") < 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path must contain at least on dot or colon "
                    "separating the module (and potentially submodules) from "
                    'the deployment graph. E.g.: "module.deployment_graph".'
                )
            if v.rfind(".") == 0 or v.rfind(".") == len(v) - 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path may not start or end with a dot."
                )

        return v

    @root_validator
    def validate_external_scaler_and_autoscaling(cls, values):
        external_scaler_enabled = values.get("external_scaler_enabled", False)
        deployments = values.get("deployments", [])

        if external_scaler_enabled:
            deployments_with_autoscaling = []
            for deployment in deployments:
                if deployment.is_autoscaling_configured():
                    deployments_with_autoscaling.append(deployment.name)

            if deployments_with_autoscaling:
                deployment_names = ", ".join(
                    f'"{name}"' for name in deployments_with_autoscaling
                )
                raise ValueError(
                    f"external_scaler_enabled is set to True, but the following "
                    f"deployment(s) have autoscaling configured: {deployment_names}. "
                    "When using an external autoscaler, Serve's built-in autoscaling must "
                    "be disabled for all deployments in the application."
                )

        return values

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Returns an empty app schema dictionary.

        Schema can be used as a representation of an empty Serve application config.
        """

        return {
            "import_path": "",
            "runtime_env": {},
            "deployments": [],
        }


@PublicAPI(stability="alpha")
class gRPCOptionsSchema(BaseModel):
    """Options to start the gRPC Proxy with."""

    port: int = Field(
        default=DEFAULT_GRPC_PORT,
        description=(
            "Port for gRPC server. Defaults to 9000. Cannot be updated once "
            "Serve has started running. Serve must be shut down and restarted "
            "with the new port instead."
        ),
    )
    grpc_servicer_functions: List[str] = Field(
        default=[],
        description=(
            "List of import paths for gRPC `add_servicer_to_server` functions to add "
            "to Serve's gRPC proxy. Default to empty list, which means no gRPC methods "
            "will be added and no gRPC server will be started. The servicer functions "
            "need to be importable from the context of where Serve is running."
        ),
    )
    request_timeout_s: float = Field(
        default=None,
        description="The timeout for gRPC requests. Defaults to no timeout.",
    )


@PublicAPI(stability="stable")
class HTTPOptionsSchema(BaseModel):
    """Options to start the HTTP Proxy with.

    NOTE: This config allows extra parameters to make it forward-compatible (ie
          older versions of Serve are able to accept configs from a newer versions,
          simply ignoring new parameters).
    """

    host: str = Field(
        default="0.0.0.0",
        description=(
            "Host for HTTP servers to listen on. Defaults to "
            '"0.0.0.0", which exposes Serve publicly. Cannot be updated once '
            "Serve has started running. Serve must be shut down and restarted "
            "with the new host instead."
        ),
    )
    port: int = Field(
        default=8000,
        description=(
            "Port for HTTP server. Defaults to 8000. Cannot be updated once "
            "Serve has started running. Serve must be shut down and restarted "
            "with the new port instead."
        ),
    )
    root_path: str = Field(
        default="",
        description=(
            'Root path to mount the serve application (for example, "/serve"). All '
            'deployment routes will be prefixed with this path. Defaults to "".'
        ),
    )
    request_timeout_s: float = Field(
        default=None,
        description="The timeout for HTTP requests. Defaults to no timeout.",
    )
    keep_alive_timeout_s: int = Field(
        default=DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
        description="The HTTP proxy will keep idle connections alive for this duration "
        "before closing them when no requests are ongoing. Defaults to "
        f"{DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S} seconds.",
    )
    ssl_keyfile: Optional[str] = Field(
        default=None,
        description="Path to the SSL key file for HTTPS. If provided with ssl_certfile, "
        "the HTTP server will use HTTPS. Cannot be updated once Serve has started.",
    )
    ssl_certfile: Optional[str] = Field(
        default=None,
        description="Path to the SSL certificate file for HTTPS. If provided with "
        "ssl_keyfile, the HTTP server will use HTTPS. Cannot be updated once Serve "
        "has started.",
    )
    ssl_keyfile_password: Optional[str] = Field(
        default=None,
        description="Password for the SSL key file, if encrypted.",
    )
    ssl_ca_certs: Optional[str] = Field(
        default=None,
        description="Path to the CA certificate file for verifying client certificates.",
    )

    @validator("ssl_certfile")
    def validate_ssl_certfile(cls, v, values):
        ssl_keyfile = values.get("ssl_keyfile")
        validate_ssl_config(v, ssl_keyfile)
        return v


@PublicAPI(stability="stable")
class ServeDeploySchema(BaseModel):
    """
    Multi-application config for deploying a list of Serve applications to the Ray
    cluster.

    This is the request JSON schema for the v2 REST API
    `PUT "/api/serve/applications/"`.

    NOTE: This config allows extra parameters to make it forward-compatible (ie
          older versions of Serve are able to accept configs from a newer versions,
          simply ignoring new parameters)
    """

    proxy_location: ProxyLocation = Field(
        default=ProxyLocation.EveryNode,
        description=(
            "Config for where to run proxies for ingress traffic to the cluster."
        ),
    )
    http_options: HTTPOptionsSchema = Field(
        default=HTTPOptionsSchema(), description="Options to start the HTTP Proxy with."
    )
    grpc_options: gRPCOptionsSchema = Field(
        default=gRPCOptionsSchema(), description="Options to start the gRPC Proxy with."
    )
    logging_config: LoggingConfig = Field(
        default=None,
        description="Logging config for configuring serve components logs.",
    )
    applications: List[ServeApplicationSchema] = Field(
        ..., description="The set of applications to run on the Ray cluster."
    )
    target_capacity: Optional[float] = TARGET_CAPACITY_FIELD

    @validator("applications")
    def application_names_unique(cls, v):
        # Ensure there are no duplicate applications listed
        names = [app.name for app in v]
        duplicates = {f'"{name}"' for name in names if names.count(name) > 1}
        if len(duplicates):
            apps_str = ("application " if len(duplicates) == 1 else "applications ") + (
                ", ".join(duplicates)
            )
            raise ValueError(
                f"Found multiple configs for {apps_str}. Please remove all duplicates."
            )
        return v

    @validator("applications")
    def application_routes_unique(cls, v):
        # Ensure each application with a non-null route prefix has unique route prefixes
        routes = [app.route_prefix for app in v if app.route_prefix is not None]
        duplicates = {f'"{route}"' for route in routes if routes.count(route) > 1}
        if len(duplicates):
            routes_str = (
                "route prefix " if len(duplicates) == 1 else "route prefixes "
            ) + (", ".join(duplicates))
            raise ValueError(
                f"Found duplicate applications for {routes_str}. Please ensure each "
                "application's route_prefix is unique."
            )
        return v

    @validator("applications")
    def application_names_nonempty(cls, v):
        for app in v:
            if len(app.name) == 0:
                raise ValueError("Application names must be nonempty.")
        return v

    @root_validator
    def nested_host_and_port(cls, values):
        # TODO (zcin): ServeApplicationSchema still needs to have host and port
        # fields to support single-app mode, but in multi-app mode the host and port
        # fields at the top-level deploy config is used instead. Eventually, after
        # migration, we should remove these fields from ServeApplicationSchema.
        for app_config in values.get("applications"):
            if "host" in app_config.dict(exclude_unset=True):
                raise ValueError(
                    f'Host "{app_config.host}" is set in the config for application '
                    f"`{app_config.name}`. Please remove it and set host in the top "
                    "level deploy config only."
                )
            if "port" in app_config.dict(exclude_unset=True):
                raise ValueError(
                    f"Port {app_config.port} is set in the config for application "
                    f"`{app_config.name}`. Please remove it and set port in the top "
                    "level deploy config only."
                )
        return values

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Returns an empty deploy schema dictionary.

        Schema can be used as a representation of an empty Serve deploy config.
        """

        return {"applications": []}


# Keep in sync with ServeSystemActorStatus in
# python/ray/dashboard/client/src/type/serve.ts
@PublicAPI(stability="stable")
class ProxyStatus(str, Enum):
    """The current status of the proxy."""

    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    DRAINING = "DRAINING"
    # The DRAINED status is a momentary state
    # just before the proxy is removed
    # so this status won't show up on the dashboard.
    DRAINED = "DRAINED"


@PublicAPI(stability="alpha")
@dataclass
class DeploymentStatusOverview:
    """Describes the status of a deployment.

    Attributes:
        status: The current status of the deployment.
        replica_states: A map indicating how many replicas there are of
            each replica state.
        message: A message describing the deployment status in more
            detail.
    """

    status: DeploymentStatus
    status_trigger: DeploymentStatusTrigger
    replica_states: Dict[ReplicaState, int]
    message: str


@PublicAPI(stability="stable")
class ApplicationStatus(str, Enum):
    """The current status of the application."""

    NOT_STARTED = "NOT_STARTED"
    DEPLOYING = "DEPLOYING"
    DEPLOY_FAILED = "DEPLOY_FAILED"
    RUNNING = "RUNNING"
    UNHEALTHY = "UNHEALTHY"
    DELETING = "DELETING"


@PublicAPI(stability="alpha")
@dataclass
class ApplicationStatusOverview:
    """Describes the status of an application and all its deployments.

    Attributes:
        status: The current status of the application.
        message: A message describing the application status in more
            detail.
        last_deployed_time_s: The time at which the application was
            deployed. A Unix timestamp in seconds.
        deployments: The deployments in this application.
    """

    status: ApplicationStatus
    message: str
    last_deployed_time_s: float
    deployments: Dict[str, DeploymentStatusOverview]


@PublicAPI(stability="alpha")
@dataclass(eq=True)
class ServeStatus:
    """Describes the status of Serve.

    Attributes:
        proxies: The proxy actors running on each node in the cluster.
            A map from node ID to proxy status.
        applications: The live applications in the cluster.
        target_capacity: the target capacity percentage for all replicas across the
            cluster.
    """

    proxies: Dict[str, ProxyStatus] = field(default_factory=dict)
    applications: Dict[str, ApplicationStatusOverview] = field(default_factory=dict)
    target_capacity: Optional[float] = TARGET_CAPACITY_FIELD


@PublicAPI(stability="stable")
class ServeActorDetails(BaseModel, frozen=True):
    """Detailed info about a Ray Serve actor.

    Attributes:
        node_id: ID of the node that the actor is running on.
        node_ip: IP address of the node that the actor is running on.
        node_instance_id: Cloud provider instance id of the node that the actor is running on.
        actor_id: Actor ID.
        actor_name: Actor name.
        worker_id: Worker ID.
        log_file_path: The relative path to the Serve actor's log file from the ray logs
            directory.
    """

    node_id: Optional[str] = Field(
        description="ID of the node that the actor is running on."
    )
    node_ip: Optional[str] = Field(
        description="IP address of the node that the actor is running on."
    )
    node_instance_id: Optional[str] = Field(
        description="Cloud provider instance id of the node that the actor is running on."
    )
    actor_id: Optional[str] = Field(description="Actor ID.")
    actor_name: Optional[str] = Field(description="Actor name.")
    worker_id: Optional[str] = Field(description="Worker ID.")
    log_file_path: Optional[str] = Field(
        description=(
            "The relative path to the Serve actor's log file from the ray logs "
            "directory."
        )
    )


@PublicAPI(stability="stable")
class ReplicaDetails(ServeActorDetails, frozen=True):
    """Detailed info about a single deployment replica."""

    replica_id: str = Field(description="Unique ID for the replica.")
    state: ReplicaState = Field(description="Current state of the replica.")
    pid: Optional[int] = Field(description="PID of the replica actor process.")
    start_time_s: float = Field(
        description=(
            "The time at which the replica actor was started. If the controller dies, "
            "this is the time at which the controller recovers and retrieves replica "
            "state from the running replica actor."
        )
    )


@PublicAPI(stability="alpha")
class AutoscalingMetricsHealth(str, Enum):
    HEALTHY = "healthy"
    DELAYED = "delayed"
    UNAVAILABLE = "unavailable"


@PublicAPI(stability="alpha")
class AutoscalingStatus(str, Enum):
    UPSCALING = "UPSCALING"
    DOWNSCALING = "DOWNSCALING"
    STABLE = "STABLE"


@PublicAPI(stability="alpha")
class ScalingDecision(BaseModel):
    """One autoscaling decision with minimal provenance."""

    timestamp_s: float = Field(
        ..., description="Unix time (seconds) when the decision was made."
    )
    reason: str = Field(
        ..., description="Short, human-readable reason for the decision."
    )
    prev_num_replicas: int = Field(
        ..., ge=0, description="Replica count before the decision."
    )
    curr_num_replicas: int = Field(
        ..., ge=0, description="Replica count after the decision."
    )
    policy: Optional[str] = Field(
        None, description="Policy name or identifier (if applicable)."
    )


@PublicAPI(stability="alpha")
class DeploymentAutoscalingDetail(BaseModel):
    """Deployment-level autoscaler observability."""

    scaling_status: AutoscalingStatus = Field(
        ..., description="Current scaling direction or stability."
    )
    decisions: List[ScalingDecision] = Field(
        default_factory=list, description="Recent scaling decisions."
    )
    metrics: Optional[Dict[str, Any]] = Field(
        None, description="Aggregated metrics for this deployment."
    )
    metrics_health: AutoscalingMetricsHealth = Field(
        AutoscalingMetricsHealth.HEALTHY,
        description="Health of metrics collection pipeline.",
    )
    errors: List[str] = Field(
        default_factory=list, description="Recent errors/abnormal events."
    )


@PublicAPI(stability="stable")
class DeploymentDetails(BaseModel, extra=Extra.forbid, frozen=True):
    """
    Detailed info about a deployment within a Serve application.
    """

    name: str = Field(description="Deployment name.")
    status: DeploymentStatus = Field(
        description="The current status of the deployment."
    )
    status_trigger: DeploymentStatusTrigger = Field(
        description="[EXPERIMENTAL] The trigger for the current status.",
    )
    message: str = Field(
        description=(
            "If there are issues with the deployment, this will describe the issue in "
            "more detail."
        )
    )
    deployment_config: DeploymentSchema = Field(
        description=(
            "The set of deployment config options that are currently applied to this "
            "deployment. These options may come from the user's code, config file "
            "options, or Serve default values."
        )
    )
    target_num_replicas: NonNegativeInt = Field(
        description=(
            "The current target number of replicas for this deployment. This can "
            "change over time for autoscaling deployments, but will remain a constant "
            "number for other deployments."
        )
    )
    required_resources: Dict = Field(
        description="The resources required per replica of this deployment."
    )
    replicas: List[ReplicaDetails] = Field(
        description="Details about the live replicas of this deployment."
    )

    autoscaling_detail: Optional[DeploymentAutoscalingDetail] = Field(
        default=None,
        description="[EXPERIMENTAL] Deployment-level autoscaler observability for this deployment.",
    )


@PublicAPI(stability="alpha")
class APIType(str, Enum):
    """Tracks the type of API that an application originates from."""

    UNKNOWN = "unknown"
    IMPERATIVE = "imperative"
    DECLARATIVE = "declarative"

    @classmethod
    def get_valid_user_values(cls):
        """Get list of valid APIType values that users can explicitly pass.

        Excludes 'unknown' which is for internal use only.
        """
        return [cls.IMPERATIVE.value, cls.DECLARATIVE.value]


@PublicAPI(stability="alpha")
class DeploymentNode(BaseModel):
    """Represents a node in the deployment topology.

    Each node represents a deployment and tracks which other deployments it calls.
    """

    name: str = Field(description="The name of the deployment.")
    app_name: str = Field(
        description="The name of the application this deployment belongs to."
    )
    # using name and app_name instead of just deployment name because outbound dependencies can be in different apps
    outbound_deployments: List[dict] = Field(
        default_factory=list,
        description="The deployment IDs that this deployment calls (outbound dependencies).",
    )
    is_ingress: bool = Field(
        default=False, description="Whether this is the ingress deployment."
    )


@PublicAPI(stability="alpha")
class DeploymentTopology(BaseModel):
    """Represents the dependency graph of deployments in an application.

    The topology shows which deployments call which other deployments,
    with the ingress deployment as the entry point.
    """

    app_name: str = Field(
        description="The name of the application this topology belongs to."
    )
    nodes: Dict[str, DeploymentNode] = Field(
        description="The adjacency list of deployment nodes."
    )
    ingress_deployment: Optional[str] = Field(
        default=None, description="The name of the ingress deployment (entry point)."
    )


@PublicAPI(stability="stable")
class ApplicationDetails(BaseModel, extra=Extra.forbid, frozen=True):
    """Detailed info about a Serve application."""

    name: str = Field(description="Application name.")
    route_prefix: Optional[str] = Field(
        ...,
        description=(
            "This is the `route_prefix` of the ingress deployment in the application. "
            "Requests to paths under this HTTP path prefix will be routed to this "
            "application. This value may be null if the application is deploying "
            "and app information has not yet fully propagated in the backend; or "
            "if the user explicitly set the prefix to `None`, so the application isn't "
            "exposed over HTTP. Routing is done based on longest-prefix match, so if "
            'you have deployment A with a prefix of "/a" and deployment B with a '
            'prefix of "/a/b", requests to "/a", "/a/", and "/a/c" go to A and '
            'requests to "/a/b", "/a/b/", and "/a/b/c" go to B. Routes must not end '
            'with a "/" unless they\'re the root (just "/"), which acts as a catch-all.'
        ),
    )
    docs_path: Optional[str] = Field(
        ...,
        description=(
            "The path at which the docs for this application is served, for instance "
            "the `docs_url` for FastAPI-integrated applications."
        ),
    )
    status: ApplicationStatus = Field(
        description="The current status of the application."
    )
    message: str = Field(
        description="A message that gives more insight into the application status."
    )
    last_deployed_time_s: float = Field(
        description="The time at which the application was deployed."
    )
    deployed_app_config: Optional[ServeApplicationSchema] = Field(
        description=(
            "The exact copy of the application config that was submitted to the "
            "cluster. This will include all of, and only, the options that were "
            "explicitly specified in the submitted config. Default values for "
            "unspecified options will not be displayed, and deployments that are part "
            "of the application but unlisted in the config will also not be displayed. "
            "Note that default values for unspecified options are applied to the "
            "cluster under the hood, and deployments that were unlisted will still be "
            "deployed. This config simply avoids cluttering with unspecified fields "
            "for readability."
        )
    )
    source: APIType = Field(
        description=(
            "The type of API that the application originates from. "
            "This is a Developer API that is subject to change."
        ),
    )
    deployments: Dict[str, DeploymentDetails] = Field(
        description="Details about the deployments in this application."
    )
    external_scaler_enabled: bool = Field(
        description="Whether external scaling is enabled for this application.",
    )

    application_details_route_prefix_format = validator(
        "route_prefix", allow_reuse=True
    )(_route_prefix_format)

    deployment_topology: Optional[DeploymentTopology] = Field(
        default=None,
        description="The deployment topology showing how deployments in this application call each other.",
    )


@PublicAPI(stability="stable")
class ProxyDetails(ServeActorDetails, frozen=True):
    """Detailed info about a Ray Serve ProxyActor.

    Attributes:
        status: The current status of the proxy.
    """

    status: ProxyStatus = Field(description="Current status of the proxy.")


@PublicAPI(stability="alpha")
class Target(BaseModel, frozen=True):
    ip: str = Field(description="IP address of the target.")
    port: int = Field(description="Port of the target.")
    instance_id: str = Field(description="Instance ID of the target.")
    name: str = Field(description="Name of the target.")


@PublicAPI(stability="alpha")
class TargetGroup(BaseModel, frozen=True):
    targets: List[Target] = Field(description="List of targets for the given route.")
    route_prefix: str = Field(description="Prefix route of the targets.")
    protocol: RequestProtocol = Field(description="Protocol of the targets.")


@PublicAPI(stability="stable")
class ServeInstanceDetails(BaseModel, extra=Extra.forbid):
    """
    Serve metadata with system-level info and details on all applications deployed to
    the Ray cluster.

    This is the response JSON schema for v2 REST API `GET /api/serve/applications`.
    """

    controller_info: ServeActorDetails = Field(
        description="Details about the Serve controller actor."
    )
    proxy_location: Optional[ProxyLocation] = Field(
        description=(
            "Config for where to run proxies for ingress traffic to the cluster.\n"
            '- "Disabled": disable the proxies entirely.\n'
            '- "HeadOnly": run only one proxy on the head node.\n'
            '- "EveryNode": run proxies on every node that has at least one replica.\n'
        ),
    )
    http_options: Optional[HTTPOptionsSchema] = Field(description="HTTP Proxy options.")
    grpc_options: Optional[gRPCOptionsSchema] = Field(description="gRPC Proxy options.")
    proxies: Dict[str, ProxyDetails] = Field(
        description=(
            "Mapping from node_id to details about the Proxy running on that node."
        )
    )
    deploy_mode: ServeDeployMode = Field(
        default=ServeDeployMode.MULTI_APP,
        description=(
            "[DEPRECATED]: single-app configs are removed, so this is always "
            "MULTI_APP. This field will be removed in a future release."
        ),
    )
    applications: Dict[str, ApplicationDetails] = Field(
        description="Details about all live applications running on the cluster."
    )
    target_capacity: Optional[float] = TARGET_CAPACITY_FIELD

    target_groups: List[TargetGroup] = Field(
        default_factory=list,
        description=(
            "List of target groups, each containing target info for a given route and "
            "protocol."
        ),
    )

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Empty Serve instance details dictionary.

        Represents no Serve instance running on the cluster.
        """

        return {
            "deploy_mode": "MULTI_APP",
            "controller_info": {},
            "proxies": {},
            "applications": {},
            "target_capacity": None,
        }

    def _get_status(self) -> ServeStatus:
        return ServeStatus(
            target_capacity=self.target_capacity,
            proxies={node_id: proxy.status for node_id, proxy in self.proxies.items()},
            applications={
                app_name: ApplicationStatusOverview(
                    status=app.status,
                    message=app.message,
                    last_deployed_time_s=app.last_deployed_time_s,
                    deployments={
                        deployment_name: DeploymentStatusOverview(
                            status=deployment.status,
                            status_trigger=deployment.status_trigger,
                            replica_states=dict(
                                Counter([r.state.value for r in deployment.replicas])
                            ),
                            message=deployment.message,
                        )
                        for deployment_name, deployment in app.deployments.items()
                    },
                )
                for app_name, app in self.applications.items()
            },
        )

    def _get_user_facing_json_serializable_dict(
        self, *args, **kwargs
    ) -> Dict[str, Any]:
        """Generates json serializable dictionary with user facing data."""
        values = super().dict(*args, **kwargs)

        # `serialized_policy_def` and internal router config fields are only used
        # internally and should not be exposed to the REST api. This method iteratively
        # removes them from each deployment config if exists.
        for app_name, application in values["applications"].items():
            for deployment_name, deployment in application["deployments"].items():
                if "deployment_config" in deployment:
                    # Remove internal fields from request_router_config if it exists
                    if "request_router_config" in deployment["deployment_config"]:
                        deployment["deployment_config"]["request_router_config"].pop(
                            "_serialized_request_router_cls", None
                        )
                    if "autoscaling_config" in deployment["deployment_config"]:
                        deployment["deployment_config"]["autoscaling_config"].pop(
                            "_serialized_policy_def", None
                        )

        return values


@PublicAPI(stability="alpha")
class CeleryAdapterConfig(BaseModel):
    """
    Celery adapter config. You can use it to configure the Celery task processor for your Serve application.
    """

    app_custom_config: Optional[Dict[str, Any]] = Field(
        default=None, description="The custom configurations to use for the Celery app."
    )
    task_custom_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="""
        The custom configurations to use for the Celery task.
        This custom configurations will get applied to all the celery tasks.
        """,
    )
    broker_url: str = Field(..., description="The URL of the broker to use for Celery.")
    backend_url: str = Field(
        ..., description="The URL of the backend to use for Celery."
    )
    broker_transport_options: Optional[Dict[str, Any]] = Field(
        default=None, description="The broker transport options to use for Celery."
    )


@PublicAPI(stability="alpha")
class TaskProcessorConfig(BaseModel):
    """
    Task processor config. You can use it to configure the task processor for your Serve application.
    """

    queue_name: str = Field(
        ..., description="The name of the queue to use for task processing."
    )
    adapter: Union[str, Callable] = Field(
        default="ray.serve.task_processor.CeleryTaskProcessorAdapter",
        description="The adapter to use for task processing. By default, Celery is used.",
    )
    adapter_config: Any = Field(..., description="The adapter config.")
    max_retries: Optional[int] = Field(
        default=3,
        description="The maximum number of times to retry a task before marking it as failed.",
    )
    failed_task_queue_name: Optional[str] = Field(
        default=None,
        description="The name of the failed task queue. This is used to move failed tasks to a dead-letter queue after max retries.",
    )
    unprocessable_task_queue_name: Optional[str] = Field(
        default=None,
        description="The name of the unprocessable task queue. This is used to move unprocessable tasks(like tasks with serialization issue, or missing handler) to a dead-letter queue.",
    )


@PublicAPI(stability="alpha")
class TaskResult(BaseModel):
    """
    Task result Model.
    """

    id: str = Field(..., description="The ID of the task.")
    status: str = Field(..., description="The status of the task.")
    created_at: Optional[float] = Field(
        default=None, description="The timestamp of the task creation."
    )
    result: Any = Field(..., description="The result of the task.")


@PublicAPI(stability="alpha")
class TaskProcessorAdapter(ABC):
    """
    Abstract base class for task processing adapters.

    Subclasses can support different combinations of sync and async operations.
    Use supports_async_capability() to check if a specific async operation is supported.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the TaskProcessorAdapter.

        """
        pass

    @abstractmethod
    def initialize(self, consumer_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY):
        """
        Initialize the task processor.
        """
        pass

    @abstractmethod
    def register_task_handle(self, func: Callable, name: Optional[str] = None):
        """
        Register a function as a task handler.

        Args:
            func: The function to register as a task handler.
            name: Custom name for the task.
        """
        pass

    @abstractmethod
    def enqueue_task_sync(
        self,
        task_name: str,
        args: Optional[Any] = None,
        kwargs: Optional[Any] = None,
        **options,
    ) -> TaskResult:
        """
        Enqueue a task for execution synchronously.

        Args:
            task_name: Name of the registered task to execute.
            args: Positional arguments to pass to the task function.
            kwargs: Keyword arguments to pass to the task function.
            **options: Additional adapter-specific options for task execution.

        Returns:
            TaskResult: Object containing task ID, status, and other metadata.
        """
        pass

    @abstractmethod
    def get_task_status_sync(self, task_id: str) -> TaskResult:
        """
        Retrieve the current status of a task synchronously.

        Args:
            task_id: Unique identifier of the task to query.

        Returns:
            TaskResult: Object containing current task status, result, and other metadata.
        """
        pass

    @abstractmethod
    def start_consumer(self, **kwargs):
        """
        Start the task consumer/worker process.
        """
        pass

    @abstractmethod
    def stop_consumer(self, timeout: float = 10.0):
        """
        Stop the task consumer gracefully.

        Args:
            timeout: Maximum time in seconds to wait for the consumer to stop.
        """
        pass

    @abstractmethod
    def shutdown(self):
        """
        Shutdown the task processor and clean up resources.
        """
        pass

    @abstractmethod
    def cancel_task_sync(self, task_id: str):
        """
        Cancel a task synchronously.

        Args:
            task_id: Unique identifier of the task to cancel.
        """
        pass

    @abstractmethod
    def get_metrics_sync(self) -> Dict[str, Any]:
        """
        Get metrics synchronously.

        Returns:
            Dict[str, Any]: Adapter-specific metrics data.
        """
        pass

    @abstractmethod
    def health_check_sync(self) -> List[Dict]:
        """
        Perform health check synchronously.

        Returns:
            List[Dict]: Health status information for workers/components.
        """
        pass

    async def enqueue_task_async(
        self,
        task_name: str,
        args: Optional[Any] = None,
        kwargs: Optional[Any] = None,
        **options,
    ) -> TaskResult:
        """
        Enqueue a task asynchronously.

        Args:
            task_name: Name of the registered task to execute.
            args: Positional arguments to pass to the task function.
            kwargs: Keyword arguments to pass to the task function.
            **options: Additional adapter-specific options for task execution.

        Returns:
            TaskResult: Object containing task ID, status, and other metadata.

        Raises:
            NotImplementedError: If subclass didn't implement enqueue_task_async function
        """

        raise NotImplementedError("Subclass must implement enqueue_task_async function")

    async def get_task_status_async(self, task_id: str) -> TaskResult:
        """
        Get task status asynchronously.

        Args:
            task_id: Unique identifier of the task to query.

        Returns:
            TaskResult: Object containing current task status, result, and other metadata.

        Raises:
            NotImplementedError: If subclass didn't implement get_task_status_async function
        """

        raise NotImplementedError(
            "Subclass must implement get_task_status_async function"
        )

    async def cancel_task_async(self, task_id: str):
        """
        Cancel a task.

        Args:
            task_id: Unique identifier of the task to cancel.

        Raises:
            NotImplementedError: If subclass didn't implement cancel_task_async function
        """

        raise NotImplementedError("Subclass must implement cancel_task_async function")

    async def get_metrics_async(self) -> Dict[str, Any]:
        """
        Get metrics asynchronously.

        Returns:
            Dict[str, Any]: Adapter-specific metrics data.

        Raises:
            NotImplementedError: If subclass didn't implement get_metrics_async function
        """

        raise NotImplementedError("Subclass must implement get_metrics_async function")

    async def health_check_async(self) -> List[Dict]:
        """
        Perform health check asynchronously.

        Returns:
            List[Dict]: Health status information for workers/components.

        Raises:
            NotImplementedError: If subclass didn't implement health_check_async function
        """

        raise NotImplementedError("Subclass must implement health_check_async function")


@PublicAPI(stability="alpha")
class ScaleDeploymentRequest(BaseModel):
    """Request schema for scaling a deployment's replicas."""

    target_num_replicas: NonNegativeInt = Field(
        description="The target number of replicas for the deployment."
    )


@PublicAPI(stability="alpha")
class ReplicaRank(BaseModel):
    """Replica rank model."""

    rank: int = Field(
        description="Global rank of the replica across all nodes scoped to the deployment."
    )

    node_rank: int = Field(description="Rank of the node in the deployment.")

    local_rank: int = Field(
        description="Rank of the replica on the node scoped to the deployment."
    )
