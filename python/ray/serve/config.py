import logging
from enum import Enum
from typing import Any, Callable, List, Optional, Union
import warnings

import pydantic
from pydantic import (
    BaseModel,
    NonNegativeFloat,
    PositiveFloat,
    NonNegativeInt,
    PositiveInt,
    validator,
)

from ray.serve._private.constants import (
    DEFAULT_GRPC_PORT,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray._private.utils import import_attr
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="stable")
class AutoscalingConfig(BaseModel):
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
    FixedNumber = "FixedNumber"


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
class HTTPOptions(pydantic.BaseModel):
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
    fixed_number_replicas: Optional[int] = None
    fixed_number_selection_seed: int = 0
    request_timeout_s: Optional[float] = None
    keep_alive_timeout_s: int = DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S

    @validator("location", always=True)
    def location_backfill_no_server(cls, v, values):
        if values["host"] is None or v is None:
            return DeploymentMode.NoServer

        if v == DeploymentMode.FixedNumber:
            warnings.warn(
                "`DeploymentMode.FixedNumber` is deprecated and will be removed in a "
                "future version."
            )

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

    @validator("fixed_number_replicas", always=True)
    def fixed_number_replicas_should_exist(cls, v, values):
        if values.get("location") == DeploymentMode.FixedNumber and v is None:
            raise ValueError(
                "When location='FixedNumber', you must specify "
                "the `fixed_number_replicas` parameter."
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
                    logger.warning(
                        f"{func} is not a callable function! Please make sure "
                        "the function is imported correctly."
                    )
            except ModuleNotFoundError:
                logger.warning(
                    f"{func} can't be imported! Please make sure there are no typo "
                    "in those functions. Or you might want to rebuild service "
                    "definitions if .proto file is changed."
                )

        return callables
