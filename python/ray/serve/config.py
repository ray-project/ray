import json
import logging
import warnings
from enum import Enum
from functools import cached_property
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
from ray._common.utils import import_attr, import_module_and_attr

# Import types needed for AutoscalingContext
from ray.serve._private.common import DeploymentID, ReplicaID, TimeSeries
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
from ray.serve._private.utils import validate_ssl_config
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="alpha")
class AutoscalingContext:
    """Rich context provided to custom autoscaling policies.

    This class provides comprehensive information about a deployment's current state,
    metrics, and configuration that can be used by custom autoscaling policies to
    make intelligent scaling decisions.

    The context includes deployment metadata, current replica state, built-in and
    custom metrics, capacity bounds, policy state, and timing information.

    Note: The aggregated_metrics and raw_metrics fields support lazy evaluation.
    You can pass callables that will be evaluated only when accessed, with results
    cached for subsequent accesses.
    """

    def __init__(
        self,
        deployment_id: DeploymentID,
        deployment_name: str,
        app_name: Optional[str],
        current_num_replicas: int,
        target_num_replicas: int,
        running_replicas: List[ReplicaID],
        total_num_requests: Union[float, Callable[[], float]],
        total_queued_requests: Optional[Union[float, Callable[[], float]]],
        aggregated_metrics: Optional[
            Union[
                Dict[str, Dict[ReplicaID, float]],
                Callable[[], Dict[str, Dict[ReplicaID, float]]],
            ]
        ],
        raw_metrics: Optional[
            Union[
                Dict[str, Dict[ReplicaID, TimeSeries]],
                Callable[[], Dict[str, Dict[ReplicaID, TimeSeries]]],
            ]
        ],
        capacity_adjusted_min_replicas: int,
        capacity_adjusted_max_replicas: int,
        policy_state: Dict[str, Any],
        last_scale_up_time: Optional[float],
        last_scale_down_time: Optional[float],
        current_time: Optional[float],
        config: Optional[Any],
    ):
        # Deployment information
        self.deployment_id = deployment_id  #: Unique identifier for the deployment.
        self.deployment_name = deployment_name  #: Name of the deployment.
        self.app_name = app_name  #: Name of the application containing this deployment.

        # Current state
        self.current_num_replicas = (
            current_num_replicas  #: Current number of running replicas.
        )
        self.target_num_replicas = (
            target_num_replicas  #: Target number of replicas set by the autoscaler.
        )
        self.running_replicas = (
            running_replicas  #: List of currently running replica IDs.
        )

        # Built-in metrics
        self._total_num_requests_value = (
            total_num_requests  #: Total number of requests across all replicas.
        )
        self._total_queued_requests_value = (
            total_queued_requests  #: Number of requests currently queued.
        )

        # Custom metrics - store potentially lazy callables privately
        self._aggregated_metrics_value = aggregated_metrics
        self._raw_metrics_value = raw_metrics

        # Capacity and bounds
        self.capacity_adjusted_min_replicas = capacity_adjusted_min_replicas  #: Minimum replicas adjusted for cluster capacity.
        self.capacity_adjusted_max_replicas = capacity_adjusted_max_replicas  #: Maximum replicas adjusted for cluster capacity.

        # Policy state
        self.policy_state = (
            policy_state  #: Persistent state dictionary for the autoscaling policy.
        )

        # Timing
        self.last_scale_up_time = (
            last_scale_up_time  #: Timestamp of last scale-up action.
        )
        self.last_scale_down_time = (
            last_scale_down_time  #: Timestamp of last scale-down action.
        )
        self.current_time = current_time  #: Current timestamp.

        # Config
        self.config = config  #: Autoscaling configuration for this deployment.

    @cached_property
    def aggregated_metrics(self) -> Optional[Dict[str, Dict[ReplicaID, float]]]:
        if callable(self._aggregated_metrics_value):
            return self._aggregated_metrics_value()
        return self._aggregated_metrics_value

    @cached_property
    def raw_metrics(self) -> Optional[Dict[str, Dict[ReplicaID, TimeSeries]]]:
        if callable(self._raw_metrics_value):
            return self._raw_metrics_value()
        return self._raw_metrics_value

    @cached_property
    def total_num_requests(self) -> float:
        if callable(self._total_num_requests_value):
            return self._total_num_requests_value()
        return self._total_num_requests_value

    @cached_property
    def total_queued_requests(self) -> float:
        if callable(self._total_queued_requests_value):
            return self._total_queued_requests_value()
        return self._total_queued_requests_value

    @property
    def total_running_requests(self) -> float:
        # NOTE: for non-additive aggregation functions, total_running_requests is not
        # accurate, consider this is an approximation.
        return self.total_num_requests - self.total_queued_requests


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
        serialized_request_router_cls = kwargs.pop(
            "_serialized_request_router_cls", None
        )
        super().__init__(**kwargs)
        if serialized_request_router_cls:
            self._serialized_request_router_cls = serialized_request_router_cls
        else:
            self._serialize_request_router_cls()

    def set_serialized_request_router_cls(
        self, serialized_request_router_cls: bytes
    ) -> None:
        self._serialized_request_router_cls = serialized_request_router_cls

    @classmethod
    def from_serialized_request_router_cls(
        cls, request_router_config: dict, serialized_request_router_cls: bytes
    ) -> "RequestRouterConfig":
        config = request_router_config.copy()
        config["_serialized_request_router_cls"] = serialized_request_router_cls
        return cls(**config)

    def get_serialized_request_router_cls(self) -> Optional[bytes]:
        return self._serialized_request_router_cls

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
        request_router_module, request_router_class = import_module_and_attr(
            request_router_path
        )
        cloudpickle.register_pickle_by_value(request_router_module)
        self.set_serialized_request_router_cls(cloudpickle.dumps(request_router_class))
        cloudpickle.unregister_pickle_by_value(request_router_module)

        # Update the request_router_class field to be the string path
        self.request_router_class = request_router_path

    def get_request_router_class(self) -> Callable:
        """Deserialize the request router from cloudpickled bytes."""
        try:
            return cloudpickle.loads(self._serialized_request_router_cls)
        except (ModuleNotFoundError, ImportError) as e:
            raise ImportError(
                f"Failed to deserialize custom request router: {e}\n\n"
                "This typically happens when the router depends on external modules "
                "that aren't available in the current environment. To fix this:\n"
                "  - Ensure all dependencies are installed in your Docker image or environment\n"
                "  - Package your router as a Python package and install it\n"
                "  - Place the router module in PYTHONPATH\n\n"
                "For more details, see: https://docs.ray.io/en/latest/serve/advanced-guides/"
                "custom-request-router.html#gotchas-and-limitations"
            ) from e


DEFAULT_METRICS_INTERVAL_S = 10.0


@PublicAPI(stability="alpha")
class AggregationFunction(str, Enum):
    MEAN = "mean"
    MAX = "max"
    MIN = "min"


@PublicAPI(stability="alpha")
class AutoscalingPolicy(BaseModel):
    # Cloudpickled policy definition.
    _serialized_policy_def: bytes = PrivateAttr(default=b"")

    policy_function: Union[str, Callable] = Field(
        default=DEFAULT_AUTOSCALING_POLICY_NAME,
        description="Policy function can be a string import path or a function callable. "
        "If it's a string import path, it must be of the form `path.to.module:function_name`. ",
    )

    def __init__(self, **kwargs):
        serialized_policy_def = kwargs.pop("_serialized_policy_def", None)
        super().__init__(**kwargs)
        if serialized_policy_def:
            self._serialized_policy_def = serialized_policy_def
        else:
            self.serialize_policy()

    def set_serialized_policy_def(self, serialized_policy_def: bytes) -> None:
        self._serialized_policy_def = serialized_policy_def

    @classmethod
    def from_serialized_policy_def(
        cls, policy_config: dict, serialized_policy_def: bytes
    ) -> "AutoscalingPolicy":
        config = policy_config.copy()
        config["_serialized_policy_def"] = serialized_policy_def
        return cls(**config)

    def get_serialized_policy_def(self) -> Optional[bytes]:
        return self._serialized_policy_def

    def serialize_policy(self) -> None:
        """Serialize policy with cloudpickle.

        Import the policy if it's passed in as a string import path. Then cloudpickle
        the policy and set `serialized_policy_def` if it's empty.
        """
        policy_path = self.policy_function

        if isinstance(policy_path, Callable):
            policy_path = f"{policy_path.__module__}.{policy_path.__name__}"

        if not self._serialized_policy_def:
            policy_module, policy_function = import_module_and_attr(policy_path)
            cloudpickle.register_pickle_by_value(policy_module)
            self.set_serialized_policy_def(cloudpickle.dumps(policy_function))
            cloudpickle.unregister_pickle_by_value(policy_module)

        self.policy_function = policy_path

    def is_default_policy_function(self) -> bool:
        return self.policy_function == DEFAULT_AUTOSCALING_POLICY_NAME

    def get_policy(self) -> Callable:
        """Deserialize policy from cloudpickled bytes."""
        try:
            return cloudpickle.loads(self._serialized_policy_def)
        except (ModuleNotFoundError, ImportError) as e:
            raise ImportError(
                f"Failed to deserialize custom autoscaling policy: {e}\n\n"
                "This typically happens when the policy depends on external modules "
                "that aren't available in the current environment. To fix this:\n"
                "  - Ensure all dependencies are installed in your Docker image or environment\n"
                "  - Package your policy as a Python package and install it\n"
                "  - Place the policy module in PYTHONPATH\n\n"
                "For more details, see: https://docs.ray.io/en/latest/serve/advanced-guides/"
                "advanced-autoscaling.html#gotchas-and-limitations"
            ) from e


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
        default=600.0,
        description="How long to wait before scaling down replicas to a value greater than 0.",
    )
    # Optionally set for 1->0 transition
    downscale_to_zero_delay_s: Optional[NonNegativeFloat] = Field(
        default=None,
        description="How long to wait before scaling down replicas from 1 to 0. If not set, the value of `downscale_delay_s` will be used.",
    )
    upscale_delay_s: NonNegativeFloat = Field(
        default=30.0, description="How long to wait before scaling up replicas."
    )

    aggregation_function: Union[str, AggregationFunction] = Field(
        default=AggregationFunction.MEAN,
        description="Function used to aggregate metrics across a time window.",
    )

    # Autoscaling policy. This policy is deployment scoped. Defaults to the request-based autoscaler.
    policy: AutoscalingPolicy = Field(
        default_factory=AutoscalingPolicy,
        description="The autoscaling policy for the deployment. This option is experimental.",
    )

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

    @validator("aggregation_function", always=True)
    def aggregation_function_valid(cls, v: Union[str, AggregationFunction]):
        if isinstance(v, AggregationFunction):
            return v
        return AggregationFunction(str(v).lower())

    @classmethod
    def default(cls):
        return cls(
            target_ongoing_requests=DEFAULT_TARGET_ONGOING_REQUESTS,
            min_replicas=1,
            max_replicas=100,
        )

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
    - ssl_keyfile: Path to the SSL key file for HTTPS. If provided with
      ssl_certfile, the HTTP server will use HTTPS.
    - ssl_certfile: Path to the SSL certificate file for HTTPS. If provided
      with ssl_keyfile, the HTTP server will use HTTPS.
    - ssl_keyfile_password: Optional password for the SSL key file.
    - ssl_ca_certs: Optional path to CA certificate file for client certificate
      verification.

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
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile_password: Optional[str] = None
    ssl_ca_certs: Optional[str] = None

    @validator("location", always=True)
    def location_backfill_no_server(cls, v, values):
        if values["host"] is None or v is None:
            return DeploymentMode.NoServer

        return v

    @validator("ssl_certfile")
    def validate_ssl_certfile(cls, v, values):
        ssl_keyfile = values.get("ssl_keyfile")
        validate_ssl_config(v, ssl_keyfile)
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
