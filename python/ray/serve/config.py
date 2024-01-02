import json
import logging
import warnings
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from typing_extensions import Annotated

from ray._private.pydantic_compat import (
    BaseModel,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    validator,
)
from ray._private.utils import import_attr
from ray.serve._private.constants import (
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S,
    DEFAULT_GRPC_PORT,
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_MAX_CONCURRENT_QUERIES,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    MAX_REPLICAS_PER_NODE_MAX_VALUE,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.utils import DeploymentOptionUpdateType
from ray.util.annotations import Deprecated, PublicAPI
from ray.util.placement_group import PlacementGroupStrategy

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="stable")
class AutoscalingConfig(BaseModel):
    """Config for the Serve Autoscaler."""

    # Please keep these options in sync with those in
    # `src/ray/protobuf/serve.proto`.

    # Publicly exposed options
    min_replicas: NonNegativeInt = 1
    initial_replicas: Optional[NonNegativeInt] = None
    max_replicas: PositiveInt = 1
    target_num_ongoing_requests_per_replica: PositiveFloat = 1.0

    # Private options below.

    # Metrics scraping options

    # How often to scrape for metrics
    metrics_interval_s: PositiveFloat = 10.0
    # Time window to average over for metrics.
    look_back_period_s: PositiveFloat = 30.0

    # Internal autoscaling configuration options

    # Multiplicative "gain" factor to limit scaling decisions
    smoothing_factor: PositiveFloat = 1.0
    upscale_smoothing_factor: Optional[PositiveFloat] = None
    downscale_smoothing_factor: Optional[PositiveFloat] = None

    # How frequently to make autoscaling decisions
    # loop_period_s: float = CONTROL_LOOP_PERIOD_S
    # How long to wait before scaling down replicas
    downscale_delay_s: NonNegativeFloat = 600.0
    # How long to wait before scaling up replicas
    upscale_delay_s: NonNegativeFloat = 30.0

    @validator("max_replicas", always=True)
    def replicas_settings_valid(cls, max_replicas, values):
        min_replicas = values.get("min_replicas")
        initial_replicas = values.get("initial_replicas")
        if min_replicas is not None and max_replicas < min_replicas:
            raise ValueError(
                f"max_replicas ({max_replicas}) must be greater than "
                f"or equal to min_replicas ({min_replicas})!"
            )

        if initial_replicas is not None:
            if initial_replicas < min_replicas:
                raise ValueError(
                    f"min_replicas ({min_replicas}) must be less than "
                    f"or equal to initial_replicas ({initial_replicas})!"
                )
            elif initial_replicas > max_replicas:
                raise ValueError(
                    f"max_replicas ({max_replicas}) must be greater than "
                    f"or equal to initial_replicas ({initial_replicas})!"
                )

        return max_replicas

    def get_upscale_smoothing_factor(self) -> PositiveFloat:
        return self.upscale_smoothing_factor or self.smoothing_factor

    def get_downscale_smoothing_factor(self) -> PositiveFloat:
        return self.downscale_smoothing_factor or self.smoothing_factor

    # TODO(architkulkarni): implement below
    # The num_ongoing_requests_per_replica error ratio (desired / current)
    # threshold for overriding `upscale_delay_s`
    # panic_mode_threshold: float = 2.0

    # TODO(architkulkarni): Add reasonable defaults


# Keep in sync with ServeDeploymentMode in dashboard/client/src/type/serve.ts
@Deprecated
class DeploymentMode(str, Enum):
    NoServer = "NoServer"
    HeadOnly = "HeadOnly"
    EveryNode = "EveryNode"


@PublicAPI(stability="stable")
class ProxyLocation(str, Enum):
    """Config for where to run proxies to receive ingress traffic to the cluster.

    Options:

        - Disabled: don't run proxies at all. This should be used if you are only
          making calls to your applications via deployment handles.
        - HeadOnly: only run a single proxy on the head node.
        - EveryNode: run a proxy on every node in the cluster that has at least one
          replica actor. This is the default.
    """

    Disabled = "Disabled"
    HeadOnly = "HeadOnly"
    EveryNode = "EveryNode"

    @classmethod
    def _to_deployment_mode(cls, v: Union["ProxyLocation", str]) -> DeploymentMode:
        if not isinstance(v, (cls, str)):
            raise TypeError(f"Must be a `ProxyLocation` or str, got: {type(v)}.")
        elif v == ProxyLocation.Disabled:
            return DeploymentMode.NoServer
        elif v == ProxyLocation.HeadOnly:
            return DeploymentMode.HeadOnly
        elif v == ProxyLocation.EveryNode:
            return DeploymentMode.EveryNode
        else:
            raise ValueError(f"Unrecognized `ProxyLocation`: {v}.")


@PublicAPI(stability="stable")
class RayActorOptionsConfig(BaseModel):
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


@PublicAPI(stability="stable")
class BaseDeploymentModel(BaseModel):
    """Defines options that can be used to configure a Serve deployment."""

    num_replicas: Annotated[
        Optional[PositiveInt],
        Field(
            description=(
                "The number of processes that handle requests to this "
                "deployment. Uses a default if null."
            ),
            update_type=DeploymentOptionUpdateType.LightWeight,
        ),
    ] = 1
    max_concurrent_queries: Annotated[
        PositiveInt,
        Field(
            description=(
                "The max number of pending queries in a single replica. "
                "Uses a default if null."
            ),
            update_type=DeploymentOptionUpdateType.NeedsReconfigure,
        ),
    ] = DEFAULT_MAX_CONCURRENT_QUERIES
    user_config: Annotated[
        Optional[Union[Dict, bytes]],
        Field(
            description=(
                "Config to pass into this deployment's "
                "reconfigure method. This can be updated dynamically "
                "without restarting replicas"
            ),
            update_type=DeploymentOptionUpdateType.NeedsActorReconfigure,
        ),
    ] = None
    autoscaling_config: Annotated[
        Optional[AutoscalingConfig],
        Field(
            description=(
                "Config specifying autoscaling parameters for the "
                "deployment's number of replicas. If null, the deployment "
                "won't autoscale; the number of replicas will be fixed at "
                "`num_replicas`."
            ),
            update_type=DeploymentOptionUpdateType.LightWeight,
        ),
    ] = None
    graceful_shutdown_wait_loop_s: Annotated[
        NonNegativeFloat,
        Field(
            description=(
                "Duration that deployment replicas will wait until there "
                "is no more work to be done before shutting down. Uses a "
                "default if null."
            ),
            update_type=DeploymentOptionUpdateType.NeedsActorReconfigure,
        ),
    ] = DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S
    graceful_shutdown_timeout_s: Annotated[
        NonNegativeFloat,
        Field(
            description=(
                "Serve controller waits for this duration before "
                "forcefully killing the replica for shutdown. Uses a "
                "default if null."
            ),
            update_type=DeploymentOptionUpdateType.NeedsReconfigure,
        ),
    ] = DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S
    health_check_period_s: Annotated[
        NonNegativeFloat,
        Field(
            description=(
                "Frequency at which the controller will health check "
                "replicas. Uses a default if null."
            ),
            update_type=DeploymentOptionUpdateType.NeedsReconfigure,
        ),
    ] = DEFAULT_HEALTH_CHECK_PERIOD_S
    health_check_timeout_s: Annotated[
        PositiveFloat,
        Field(
            description=(
                "Timeout that the controller will wait for a response "
                "from the replica's health check before marking it "
                "unhealthy. Uses a default if null."
            ),
            update_type=DeploymentOptionUpdateType.NeedsReconfigure,
        ),
    ] = DEFAULT_HEALTH_CHECK_TIMEOUT_S
    ray_actor_options: Annotated[
        RayActorOptionsConfig,
        Field(
            description="Options set for each replica actor.",
            update_type=DeploymentOptionUpdateType.HeavyWeight,
        ),
    ] = RayActorOptionsConfig()
    placement_group_bundles: Annotated[
        Optional[List[Dict[str, float]]],
        Field(
            description=(
                "Define a set of placement group bundles to be scheduled *for each "
                "replica* of this deployment. The replica actor will be scheduled in "
                "the first bundle provided, so the resources specified in "
                "`ray_actor_options` must be a subset of the first bundle's resources. "
                "All actors and tasks created by the replica actor will be scheduled "
                "in the placement group by default "
                "(`placement_group_capture_child_tasks` is set to True)."
            ),
            update_type=DeploymentOptionUpdateType.HeavyWeight,
        ),
    ] = None
    placement_group_strategy: Annotated[
        Optional[PlacementGroupStrategy],
        Field(
            description=(
                "Strategy to use for the replica placement group "
                "specified via `placement_group_bundles`. Defaults to `PACK`."
            ),
            update_type=DeploymentOptionUpdateType.HeavyWeight,
        ),
    ] = None
    max_replicas_per_node: Annotated[
        Optional[int],
        Field(
            description=(
                "[EXPERIMENTAL] The max number of deployment replicas can "
                "run on a single node. Valid values are None (no limitation) "
                "or an integer in the range of [1, 100]. "
                "Defaults to no limitation."
            ),
            ge=1,
            le=MAX_REPLICAS_PER_NODE_MAX_VALUE,
            update_type=DeploymentOptionUpdateType.HeavyWeight,
        ),
    ] = None

    @validator("user_config", always=True)
    def user_config_json_serializable(cls, v):
        if isinstance(v, bytes):
            return v
        if v is not None:
            try:
                json.dumps(v)
            except TypeError as e:
                raise ValueError(f"user_config is not JSON-serializable: {str(e)}.")

        return v


@PublicAPI(stability="stable")
class HTTPOptions(BaseModel):
    """HTTP options for the proxies. Supported fields:

    - host: Host that the proxies listens for HTTP on. Defaults to
      "127.0.0.1". To expose Serve publicly, you probably want to set
      this to "0.0.0.0".
    - port: Port that the proxies listen for HTTP on. Defaults to 8000.
    - root_path: An optional root path to mount the serve application
      (for example, "/prefix"). All deployment routes are prefixed
      with this path.
    - request_timeout_s: End-to-end timeout for HTTP requests.
    - keep_alive_timeout_s: Duration to keep idle connections alive when no
      requests are ongoing.

    - location: [DEPRECATED: use `proxy_location` field instead] The deployment
      location of HTTP servers:

        - "HeadOnly": start one HTTP server on the head node. Serve
          assumes the head node is the node you executed serve.start
          on. This is the default.
        - "EveryNode": start one HTTP server per node.
        - "NoServer": disable HTTP server.

    - num_cpus: [DEPRECATED] The number of CPU cores to reserve for each
      internal Serve HTTP proxy actor.
    """

    host: Optional[str] = DEFAULT_HTTP_HOST
    port: int = DEFAULT_HTTP_PORT
    middlewares: List[Any] = []
    location: Optional[DeploymentMode] = DeploymentMode.HeadOnly
    num_cpus: int = 0
    root_url: str = ""
    root_path: str = ""
    request_timeout_s: Optional[float] = None
    keep_alive_timeout_s: int = DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S

    @validator("location", always=True)
    def location_backfill_no_server(cls, v, values):
        if values["host"] is None or v is None:
            return DeploymentMode.NoServer

        return v

    @validator("middlewares", always=True)
    def warn_for_middlewares(cls, v, values):
        if v:
            warnings.warn(
                "Passing `middlewares` to HTTPOptions is deprecated and will be "
                "removed in a future version. Consider using the FastAPI integration "
                "to configure middlewares on your deployments: "
                "https://docs.ray.io/en/latest/serve/http-guide.html#fastapi-http-deployments"  # noqa 501
            )
        return v

    @validator("num_cpus", always=True)
    def warn_for_num_cpus(cls, v, values):
        if v:
            warnings.warn(
                "Passing `num_cpus` to HTTPOptions is deprecated and will be "
                "removed in a future version."
            )
        return v

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


@PublicAPI(stability="alpha")
class gRPCOptions(BaseModel):
    """gRPC options for the proxies. Supported fields:

    Args:
        port (int):
            Port for gRPC server if started. Default to 9000. Cannot be
            updated once Serve has started running. Serve must be shut down and
            restarted with the new port instead.
        grpc_servicer_functions (List[str]):
            List of import paths for gRPC `add_servicer_to_server` functions to add to
            Serve's gRPC proxy. Default to empty list, which means no gRPC methods will
            be added and no gRPC server will be started. The servicer functions need to
            be importable from the context of where Serve is running.
    """

    port: int = DEFAULT_GRPC_PORT
    grpc_servicer_functions: List[str] = []

    @property
    def grpc_servicer_func_callable(self) -> List[Callable]:
        """Return a list of callable functions from the grpc_servicer_functions.

        If the function is not callable or not found, it will be ignored and a warning
        will be logged.
        """
        callables = []
        for func in self.grpc_servicer_functions:
            try:
                imported_func = import_attr(func)
                if callable(imported_func):
                    callables.append(imported_func)
                else:
                    message = (
                        f"{func} is not a callable function! Please make sure "
                        "the function is imported correctly."
                    )
                    raise ValueError(message)
            except ModuleNotFoundError as e:
                message = (
                    f"{func} can't be imported! Please make sure there are no typo "
                    "in those functions. Or you might want to rebuild service "
                    "definitions if .proto file is changed."
                )
                raise ModuleNotFoundError(message) from e

        return callables
