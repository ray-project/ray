from collections import Counter
from dataclasses import dataclass, field
import json
from pydantic import BaseModel, Field, Extra, root_validator, validator
from typing import Union, List, Dict, Set, Optional

from ray._private.runtime_env.packaging import parse_uri
from ray.serve._private.common import (
    DeploymentStatusInfo,
    ApplicationStatusInfo,
    ApplicationStatus,
    DeploymentStatus,
    DeploymentInfo,
    StatusOverview,
    ReplicaState,
    ServeDeployMode,
    ProxyStatus,
)
from ray.serve.config import ProxyLocation
from ray.serve._private.utils import DEFAULT, dict_keys_snake_to_camel_case
from ray.util.annotations import PublicAPI
from ray.serve._private.constants import (
    DEFAULT_GRPC_PORT,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    SERVE_DEFAULT_APP_NAME,
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
    object_store_memory: float = Field(
        default=None,
        description=(
            "Restrict the object store memory used per replica when "
            "creating objects. Uses a default if null."
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
        ),
    )

    @validator("runtime_env")
    def runtime_env_contains_remote_uris(cls, v):
        # Ensure that all uris in py_modules and working_dir are remote

        if v is None:
            return

        uris = v.get("py_modules", [])
        if "working_dir" in v:
            uris.append(v["working_dir"])

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
    num_replicas: Optional[int] = Field(
        default=DEFAULT.VALUE,
        description=(
            "The number of processes that handle requests to this "
            "deployment. Uses a default if null."
        ),
        gt=0,
    )
    # route_prefix of None means the deployment is not exposed over HTTP.
    route_prefix: Union[str, None] = Field(
        default=DEFAULT.VALUE,
        description=(
            "[DEPRECATED] Please use route_prefix under ServeApplicationSchema instead."
        ),
    )
    max_concurrent_queries: int = Field(
        default=DEFAULT.VALUE,
        description=(
            "The max number of pending queries in a single replica. "
            "Uses a default if null."
        ),
        gt=0,
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
            "[EXPERIMENTAL] The max number of deployment replicas can "
            "run on a single node. Valid values are None (no limitation) "
            "or an integer in the range of [1, 100]. "
            "Defaults to no limitation."
        ),
    )

    is_driver_deployment: bool = Field(
        default=DEFAULT.VALUE,
        description="Indicate Whether the deployment is driver deployment "
        "Driver deployments are spawned one per node.",
    )

    @root_validator
    def num_replicas_and_autoscaling_config_mutually_exclusive(cls, values):
        if values.get("num_replicas", None) not in [DEFAULT.VALUE, None] and values.get(
            "autoscaling_config", None
        ) not in [DEFAULT.VALUE, None]:
            raise ValueError(
                "Manually setting num_replicas is not allowed "
                "when autoscaling_config is provided."
            )

        return values

    deployment_schema_route_prefix_format = validator("route_prefix", allow_reuse=True)(
        _route_prefix_format
    )

    def get_user_configured_option_names(self) -> Set[str]:
        """Get set of names for all user-configured options.

        Any field not set to DEFAULT.VALUE is considered a user-configured option.
        """

        return {
            field for field, value in self.dict().items() if value is not DEFAULT.VALUE
        }


def _deployment_info_to_schema(name: str, info: DeploymentInfo) -> DeploymentSchema:
    """Converts a DeploymentInfo object to DeploymentSchema.

    Route_prefix will not be set in the returned DeploymentSchema, since starting in 2.x
    route_prefix is an application-level concept. (This should only be used on the 2.x
    codepath)
    """

    schema = DeploymentSchema(
        name=name,
        max_concurrent_queries=info.deployment_config.max_concurrent_queries,
        user_config=info.deployment_config.user_config,
        graceful_shutdown_wait_loop_s=(
            info.deployment_config.graceful_shutdown_wait_loop_s
        ),
        graceful_shutdown_timeout_s=info.deployment_config.graceful_shutdown_timeout_s,
        health_check_period_s=info.deployment_config.health_check_period_s,
        health_check_timeout_s=info.deployment_config.health_check_timeout_s,
        ray_actor_options=info.replica_config.ray_actor_options,
        is_driver_deployment=info.is_driver_deployment,
    )

    if info.deployment_config.autoscaling_config is not None:
        schema.autoscaling_config = info.deployment_config.autoscaling_config
    else:
        schema.num_replicas = info.deployment_config.num_replicas

    return schema


@PublicAPI(stability="stable")
class ServeApplicationSchema(BaseModel):
    """
    Describes one Serve application, and currently can also be used as a standalone
    config to deploy a single application to a Ray cluster.

    This is the request JSON schema for the v1 REST API `PUT "/api/serve/deployments/"`.
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
    args: Dict = Field(
        default={},
        description="Arguments that will be passed to the application builder.",
    )

    @property
    def deployment_names(self) -> List[str]:
        return [d.name for d in self.deployments]

    @validator("runtime_env")
    def runtime_env_contains_remote_uris(cls, v):
        # Ensure that all uris in py_modules and working_dir are remote

        if v is None:
            return

        uris = v.get("py_modules", [])
        if "working_dir" in v:
            uris.append(v["working_dir"])

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

    def kubernetes_dict(self, **kwargs) -> Dict:
        """Returns dictionary in Kubernetes format.

        Dictionary can be yaml-dumped to a Serve config file directly and then
        copy-pasted into a RayService Kubernetes config.

        Args: all kwargs are passed directly into schema's dict() function.
        """

        config = self.dict(**kwargs)
        for idx, deployment in enumerate(config["deployments"]):

            if isinstance(deployment.get("ray_actor_options"), dict):

                # JSON-serialize ray_actor_options' resources dictionary
                if isinstance(deployment["ray_actor_options"].get("resources"), dict):
                    deployment["ray_actor_options"]["resources"] = json.dumps(
                        deployment["ray_actor_options"]["resources"]
                    )

                # JSON-serialize ray_actor_options' runtime_env dictionary
                if isinstance(deployment["ray_actor_options"].get("runtime_env"), dict):
                    deployment["ray_actor_options"]["runtime_env"] = json.dumps(
                        deployment["ray_actor_options"]["runtime_env"]
                    )

                # Convert ray_actor_options' keys
                deployment["ray_actor_options"] = dict_keys_snake_to_camel_case(
                    deployment["ray_actor_options"]
                )

            # JSON-serialize user_config dictionary
            if isinstance(deployment.get("user_config"), dict):
                deployment["user_config"] = json.dumps(deployment["user_config"])

            # Convert deployment's keys
            config["deployments"][idx] = dict_keys_snake_to_camel_case(deployment)

        # Convert top-level runtime_env
        if isinstance(config.get("runtime_env"), dict):
            config["runtime_env"] = json.dumps(config["runtime_env"])

        # Convert top-level option's keys
        config = dict_keys_snake_to_camel_case(config)

        return config


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
    applications: List[ServeApplicationSchema] = Field(
        ..., description="The set of applications to run on the Ray cluster."
    )

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
    replica_states: Dict[ReplicaState, int]
    message: str


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
    """

    proxies: Dict[str, ProxyStatus] = field(default_factory=dict)
    applications: Dict[str, ApplicationStatusOverview] = field(default_factory=dict)


@PublicAPI(stability="stable")
class ServeActorDetails(BaseModel, frozen=True):
    node_id: Optional[str] = Field(
        description="ID of the node that the actor is running on."
    )
    node_ip: Optional[str] = Field(
        description="IP address of the node that the actor is running on."
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

    replica_id: str = Field(
        description=(
            "Unique ID for the replica. By default, this will be "
            '"<deployment name>#<replica suffix>", where the replica suffix is a '
            "randomly generated unique string."
        )
    )
    state: ReplicaState = Field(description="Current state of the replica.")
    pid: Optional[int] = Field(description="PID of the replica actor process.")
    start_time_s: float = Field(
        description=(
            "The time at which the replica actor was started. If the controller dies, "
            "this is the time at which the controller recovers and retrieves replica "
            "state from the running replica actor."
        )
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
    replicas: List[ReplicaDetails] = Field(
        description="Details about the live replicas of this deployment."
    )

    @validator("deployment_config")
    def deployment_route_prefix_not_set(cls, v: DeploymentSchema):
        # Route prefix should not be set at the deployment level. Deployment-level route
        # prefix is outdated, there should be one route prefix per application
        if "route_prefix" in v.dict(exclude_unset=True):
            raise ValueError(
                "Unexpectedly found a deployment-level route_prefix in the "
                f'deployment_config for deployment "{cls.name}". The route_prefix in '
                "deployment_config within DeploymentDetails should not be set; please "
                "set it at the application level."
            )
        return v


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
    deployments: Dict[str, DeploymentDetails] = Field(
        description="Details about the deployments in this application."
    )

    application_details_route_prefix_format = validator(
        "route_prefix", allow_reuse=True
    )(_route_prefix_format)


@PublicAPI(stability="stable")
class ProxyDetails(ServeActorDetails, frozen=True):
    status: ProxyStatus = Field(description="Current status of the proxy.")


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
            "Mapping from node_id to details about the HTTP Proxy running on that node."
        )
    )
    deploy_mode: ServeDeployMode = Field(
        description=(
            "Whether a single-app config of format ServeApplicationSchema or multi-app "
            "config of format ServeDeploySchema was deployed to the cluster."
        )
    )
    applications: Dict[str, ApplicationDetails] = Field(
        description="Details about all live applications running on the cluster."
    )

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Empty Serve instance details dictionary.

        Represents no Serve instance running on the cluster.
        """

        return {
            "deploy_mode": "UNSET",
            "controller_info": {},
            "proxies": {},
            "applications": {},
        }

    def _get_status(self) -> ServeStatus:
        return ServeStatus(
            proxies={node_id: proxy.status for node_id, proxy in self.proxies.items()},
            applications={
                app_name: ApplicationStatusOverview(
                    status=app.status,
                    message=app.message,
                    last_deployed_time_s=app.last_deployed_time_s,
                    deployments={
                        deployment_name: DeploymentStatusOverview(
                            status=deployment.status,
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


@PublicAPI(stability="beta")
class ServeStatusSchema(BaseModel, extra=Extra.forbid):
    """
    Describes the status of an application and all its deployments.

    This is the response JSON schema for the v1 REST API
    `GET /api/serve/deployments/status`.
    """

    name: str = Field(description="Application name", default="")
    app_status: ApplicationStatusInfo = Field(
        ...,
        description=(
            "Describes if the Serve application is DEPLOYING, if the "
            "DEPLOY_FAILED, or if the app is RUNNING. Includes a timestamp of "
            "when the application was deployed."
        ),
    )
    deployment_statuses: List[DeploymentStatusInfo] = Field(
        default=[],
        description=(
            "List of statuses for all the deployments running in this Serve "
            "application. Each status contains the deployment name, the "
            "deployment's status, and a message providing extra context on "
            "the status."
        ),
    )

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Returns an empty status schema dictionary.

        Schema represents Serve status for a Ray cluster where Serve hasn't
        started yet.
        """

        return {
            "app_status": {
                "status": ApplicationStatus.NOT_STARTED.value,
                "message": "",
                "deployment_timestamp": 0,
            },
            "deployment_statuses": [],
        }


def _serve_status_to_schema(serve_status: StatusOverview) -> ServeStatusSchema:
    return ServeStatusSchema(
        name=serve_status.name,
        app_status=serve_status.app_status,
        deployment_statuses=serve_status.deployment_statuses,
    )
