import json
import logging
import warnings
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from ray import cloudpickle
from ray._common.pydantic_compat import (
    BaseModel,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    PrivateAttr,
    validator,
)
from ray._common.utils import import_attr
from ray.serve._private.constants import (
    DEFAULT_AUTOSCALING_POLICY_NAME,
    DEFAULT_GRPC_PORT,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_REQUEST_ROUTER_PATH,
    DEFAULT_REQUEST_ROUTING_STATS_PERIOD_S,
    DEFAULT_REQUEST_ROUTING_STATS_TIMEOUT_S,
    DEFAULT_TARGET_ONGOING_REQUESTS,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="alpha")
class RequestRouterConfig(BaseModel):
    """Config for the Serve request router.

    This class configures how Ray Serve routes requests to deployment replicas. The router is
    responsible for selecting which replica should handle each incoming request based on the
    configured routing policy. You can customize the routing behavior by specifying a custom
    request router class and providing configuration parameters.

    The router also manages periodic health checks and scheduling statistics collection from
    replicas to make informed routing decisions.

    Example:
        .. code-block:: python

            from ray.serve.config import RequestRouterConfig, DeploymentConfig
            from ray import serve

            # Use default router with custom stats collection interval
            request_router_config = RequestRouterConfig(
                request_routing_stats_period_s=5.0,
                request_routing_stats_timeout_s=15.0
            )

            # Use custom router class
            request_router_config = RequestRouterConfig(
                request_router_class="ray.serve.llm.request_router.PrefixCacheAffinityRouter",
                request_router_kwargs={"imbalanced_threshold": 20}
            )
            deployment_config = DeploymentConfig(
                request_router_config=request_router_config
            )
            deployment = serve.deploy(
                "my_deployment",
                deployment_config=deployment_config
            )
    """

    _serialized_request_router_cls: bytes = PrivateAttr(default=b"")

    request_router_class: Union[str, Callable] = Field(
        default=DEFAULT_REQUEST_ROUTER_PATH,
        description=(
            "The class of the request router that Ray Serve uses for this deployment. This value can be "
            "a string or a class. All the deployment handles that you create for this "
            "deployment use the routing policy defined by the request router. "
            "Default to Serve's PowerOfTwoChoicesRequestRouter."
        ),
    )
    request_router_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Keyword arguments that Ray Serve passes to the request router class "
            "initialize_state method."
        ),
    )

    request_routing_stats_period_s: PositiveFloat = Field(
        default=DEFAULT_REQUEST_ROUTING_STATS_PERIOD_S,
        description=(
            "Duration between record scheduling stats calls for the replica. "
            "Defaults to 10s. The health check is by default a no-op Actor call "
            "to the replica, but you can define your own request scheduling stats "
            "using the 'record_scheduling_stats' method in your deployment."
        ),
    )

    request_routing_stats_timeout_s: PositiveFloat = Field(
        default=DEFAULT_REQUEST_ROUTING_STATS_TIMEOUT_S,
        description=(
            "Duration in seconds, that replicas wait for a request scheduling "
            "stats method to return before considering it as failed. Defaults to 30s."
        ),
    )

    @validator("request_router_kwargs", always=True)
    def request_router_kwargs_json_serializable(cls, v):
        if isinstance(v, bytes):
            return v
        if v is not None:
            try:
                json.dumps(v)
            except TypeError as e:
                raise ValueError(
                    f"request_router_kwargs is not JSON-serializable: {str(e)}."
                )

        return v

    def __init__(self, **kwargs: dict[str, Any]):
        """Initialize RequestRouterConfig with the given parameters.

        Needed to serialize the request router class since validators are not called
        for attributes that begin with an underscore.

        Args:
            **kwargs: Keyword arguments to pass to BaseModel.
        """
        super().__init__(**kwargs)
        self._serialize_request_router_cls()

    def _serialize_request_router_cls(self) -> None:
        """Import and serialize request router class with cloudpickle.

        Import the request router if you pass it in as a string import path.
        Then cloudpickle the request router and set to
        `_serialized_request_router_cls`.
        """
        request_router_class = self.request_router_class
        if isinstance(request_router_class, Callable):
            request_router_class = (
                f"{request_router_class.__module__}.{request_router_class.__name__}"
            )

        request_router_path = request_router_class or DEFAULT_REQUEST_ROUTER_PATH
        request_router_class = import_attr(request_router_path)

        self._serialized_request_router_cls = cloudpickle.dumps(request_router_class)
        # Update the request_router_class field to be the string path
        self.request_router_class = request_router_path

    def get_request_router_class(self) -> Callable:
        """Deserialize the request router from cloudpickled bytes."""
        return cloudpickle.loads(self._serialized_request_router_cls)


DEFAULT_METRICS_INTERVAL_S = 10.0


@PublicAPI(stability="alpha")
class AutoscalingPolicy(BaseModel):
    name: Union[str, Callable] = Field(
        default=DEFAULT_AUTOSCALING_POLICY_NAME,
        description="Name of the policy function or the import path of the policy. "
        "Will be the concatenation of the policy module and the policy name if user passed a callable.",
    )


@PublicAPI(stability="stable")
class AutoscalingConfig(BaseModel):
    """Config for the Serve Autoscaler."""

    # Please keep these options in sync with those in
    # `src/ray/protobuf/serve.proto`.

    # Publicly exposed options
    min_replicas: NonNegativeInt = 1
    initial_replicas: Optional[NonNegativeInt] = None
    max_replicas: PositiveInt = 1

    target_ongoing_requests: Optional[PositiveFloat] = DEFAULT_TARGET_ONGOING_REQUESTS

    metrics_interval_s: PositiveFloat = Field(
        default=DEFAULT_METRICS_INTERVAL_S,
        description="[DEPRECATED] How often to scrape for metrics. "
        "Will be replaced by the environment variables "
        "`RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S` and "
        "`RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S` in a future release.",
    )
    look_back_period_s: PositiveFloat = Field(
        default=30.0, description="Time window to average over for metrics."
    )

    smoothing_factor: PositiveFloat = Field(
        default=1.0,
        description="[DEPRECATED] Smoothing factor for autoscaling decisions.",
    )
    # DEPRECATED: replaced by `downscaling_factor`
    upscale_smoothing_factor: Optional[PositiveFloat] = Field(
        default=None, description="[DEPRECATED] Please use `upscaling_factor` instead."
    )
    # DEPRECATED: replaced by `upscaling_factor`
    downscale_smoothing_factor: Optional[PositiveFloat] = Field(
        default=None,
        description="[DEPRECATED] Please use `downscaling_factor` instead.",
    )

    upscaling_factor: Optional[PositiveFloat] = Field(
        default=None,
        description='Multiplicative "gain" factor to limit upscaling decisions.',
    )
    downscaling_factor: Optional[PositiveFloat] = Field(
        default=None,
        description='Multiplicative "gain" factor to limit downscaling decisions.',
    )

    # How frequently to make autoscaling decisions
    # loop_period_s: float = CONTROL_LOOP_PERIOD_S
    downscale_delay_s: NonNegativeFloat = Field(
        default=600.0, description="How long to wait before scaling down replicas."
    )
    upscale_delay_s: NonNegativeFloat = Field(
        default=30.0, description="How long to wait before scaling up replicas."
    )

    # Cloudpickled policy definition.
    _serialized_policy_def: bytes = PrivateAttr(default=b"")

    # Autoscaling policy. This policy is deployment scoped. Defaults to the request-based autoscaler.
    _policy: AutoscalingPolicy = Field(default_factory=AutoscalingPolicy)

    # This is to make `_policy` a normal field until its GA ready.
    class Config:
        underscore_attrs_are_private = True

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

    @validator("metrics_interval_s")
    def metrics_interval_s_deprecation_warning(cls, v: PositiveFloat) -> PositiveFloat:
        if v != DEFAULT_METRICS_INTERVAL_S:
            warnings.warn(
                "The `metrics_interval_s` field in AutoscalingConfig is deprecated and "
                "will be replaced by the environment variables "
                "`RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S` and "
                "`RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S` in a future release.",
                DeprecationWarning,
            )
        return v

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.serialize_policy()

    def serialize_policy(self) -> None:
        """Serialize policy with cloudpickle.

        Import the policy if it's passed in as a string import path. Then cloudpickle
        the policy and set `serialized_policy_def` if it's empty.
        """
        values = self.dict()
        policy = values.get("_policy")

        policy_name = None
        if isinstance(policy, dict):
            policy_name = policy.get("name")

        if isinstance(policy_name, Callable):
            policy_name = f"{policy_name.__module__}.{policy_name.__name__}"

        if not policy_name:
            policy_name = DEFAULT_AUTOSCALING_POLICY_NAME

        if not self._serialized_policy_def:
            self._serialized_policy_def = cloudpickle.dumps(import_attr(policy_name))

        self._policy = AutoscalingPolicy(name=policy_name)

    @classmethod
    def default(cls):
        return cls(
            target_ongoing_requests=DEFAULT_TARGET_ONGOING_REQUESTS,
            min_replicas=1,
            max_replicas=100,
        )

    def get_policy(self) -> Callable:
        """Deserialize policy from cloudpickled bytes."""
        return cloudpickle.loads(self._serialized_policy_def)

    def get_upscaling_factor(self) -> PositiveFloat:
        if self.upscaling_factor:
            return self.upscaling_factor

        return self.upscale_smoothing_factor or self.smoothing_factor

    def get_downscaling_factor(self) -> PositiveFloat:
        if self.downscaling_factor:
            return self.downscaling_factor

        return self.downscale_smoothing_factor or self.smoothing_factor

    def get_target_ongoing_requests(self) -> PositiveFloat:
        return self.target_ongoing_requests


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
    def _to_deployment_mode(
        cls, proxy_location: Union["ProxyLocation", str]
    ) -> DeploymentMode:
        if isinstance(proxy_location, str):
            proxy_location = ProxyLocation(proxy_location)
        elif not isinstance(proxy_location, ProxyLocation):
            raise TypeError(
                f"Must be a `ProxyLocation` or str, got: {type(proxy_location)}."
            )

        if proxy_location == ProxyLocation.Disabled:
            return DeploymentMode.NoServer
        else:
            return DeploymentMode(proxy_location.value)

    @classmethod
    def _from_deployment_mode(
        cls, deployment_mode: Optional[Union[DeploymentMode, str]]
    ) -> Optional["ProxyLocation"]:
        """Converts DeploymentMode enum into ProxyLocation enum.

        DeploymentMode is a deprecated version of ProxyLocation that's still
        used internally throughout Serve.
        """

        if deployment_mode is None:
            return None
        elif isinstance(deployment_mode, str):
            deployment_mode = DeploymentMode(deployment_mode)
        elif not isinstance(deployment_mode, DeploymentMode):
            raise TypeError(
                f"Must be a `DeploymentMode` or str, got: {type(deployment_mode)}."
            )

        if deployment_mode == DeploymentMode.NoServer:
            return ProxyLocation.Disabled
        else:
            return ProxyLocation(deployment_mode.value)


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
        request_timeout_s: End-to-end timeout for gRPC requests.
    """

    port: int = DEFAULT_GRPC_PORT
    grpc_servicer_functions: List[str] = []
    request_timeout_s: Optional[float] = None

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
