import inspect
import json
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pydantic
from google.protobuf.json_format import MessageToDict
from pydantic import (
    BaseModel,
    NonNegativeFloat,
    PositiveFloat,
    NonNegativeInt,
    PositiveInt,
    validator,
)

from ray import cloudpickle
from ray.serve.constants import (
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S,
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentConfig as DeploymentConfigProto,
    DeploymentLanguage,
    AutoscalingConfig as AutoscalingConfigProto,
    ReplicaConfig as ReplicaConfigProto,
)
from ray._private import ray_option_utils
from ray._private.utils import resources_from_ray_options


class AutoscalingConfig(BaseModel):
    # Please keep these options in sync with those in
    # `src/ray/protobuf/serve.proto`.

    # Publicly exposed options
    min_replicas: NonNegativeInt = 1
    max_replicas: PositiveInt = 1
    target_num_ongoing_requests_per_replica: NonNegativeInt = 1

    # Private options below.

    # Metrics scraping options

    # How often to scrape for metrics
    metrics_interval_s: PositiveFloat = 10.0
    # Time window to average over for metrics.
    look_back_period_s: PositiveFloat = 30.0

    # Internal autoscaling configuration options

    # Multiplicative "gain" factor to limit scaling decisions
    smoothing_factor: PositiveFloat = 1.0

    # How frequently to make autoscaling decisions
    # loop_period_s: float = CONTROL_LOOP_PERIOD_S
    # How long to wait before scaling down replicas
    downscale_delay_s: NonNegativeFloat = 600.0
    # How long to wait before scaling up replicas
    upscale_delay_s: NonNegativeFloat = 30.0

    @validator("max_replicas")
    def max_replicas_greater_than_or_equal_to_min_replicas(cls, v, values):
        if "min_replicas" in values and v < values["min_replicas"]:
            raise ValueError(
                f"""max_replicas ({v}) must be greater than """
                f"""or equal to min_replicas """
                f"""({values["min_replicas"]})!"""
            )
        return v

    # TODO(architkulkarni): implement below
    # The number of replicas to start with when creating the deployment
    # initial_replicas: int = 1
    # The num_ongoing_requests_per_replica error ratio (desired / current)
    # threshold for overriding `upscale_delay_s`
    # panic_mode_threshold: float = 2.0

    # TODO(architkulkarni): Add reasonable defaults


class DeploymentConfig(BaseModel):
    """Configuration options for a deployment, to be set by the user.

    Args:
        num_replicas (Optional[int]): The number of processes to start up that
            will handle requests to this deployment. Defaults to 1.
        max_concurrent_queries (Optional[int]): The maximum number of queries
            that will be sent to a replica of this deployment without receiving
            a response. Defaults to 100.
        user_config (Optional[Any]): Arguments to pass to the reconfigure
            method of the deployment. The reconfigure method is called if
            user_config is not None.
        graceful_shutdown_wait_loop_s (Optional[float]): Duration
            that deployment replicas will wait until there is no more work to
            be done before shutting down.
        graceful_shutdown_timeout_s (Optional[float]):
            Controller waits for this duration to forcefully kill the replica
            for shutdown.
        health_check_period_s (Optional[float]):
            Frequency at which the controller will health check replicas.
        health_check_timeout_s (Optional[float]):
            Timeout that the controller will wait for a response from the
            replica's health check before marking it unhealthy.
    """

    num_replicas: NonNegativeInt = 1
    max_concurrent_queries: Optional[int] = None
    user_config: Any = None

    graceful_shutdown_timeout_s: NonNegativeFloat = (
        DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S  # noqa: E501
    )
    graceful_shutdown_wait_loop_s: NonNegativeFloat = (
        DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S  # noqa: E501
    )

    health_check_period_s: PositiveFloat = DEFAULT_HEALTH_CHECK_PERIOD_S
    health_check_timeout_s: PositiveFloat = DEFAULT_HEALTH_CHECK_TIMEOUT_S

    autoscaling_config: Optional[AutoscalingConfig] = None

    # This flag is used to let replica know they are deplyed from
    # a different language.
    is_cross_language: bool = False

    # This flag is used to let controller know which language does
    # the deploymnent use.
    deployment_language: Any = DeploymentLanguage.PYTHON

    version: Optional[str] = None
    prev_version: Optional[str] = None

    class Config:
        validate_assignment = True
        extra = "forbid"
        arbitrary_types_allowed = True

    # Dynamic default for max_concurrent_queries
    @validator("max_concurrent_queries", always=True)
    def set_max_queries_by_mode(cls, v, values):  # noqa 805
        if v is None:
            v = 100
        else:
            if v <= 0:
                raise ValueError("max_concurrent_queries must be >= 0")
        return v

    def to_proto(self):
        data = self.dict()
        if data.get("user_config"):
            data["user_config"] = cloudpickle.dumps(data["user_config"])
        if data.get("autoscaling_config"):
            data["autoscaling_config"] = AutoscalingConfigProto(
                **data["autoscaling_config"]
            )
        return DeploymentConfigProto(**data)

    def to_proto_bytes(self):
        return self.to_proto().SerializeToString()

    @classmethod
    def from_proto(cls, proto: DeploymentConfigProto):
        data = MessageToDict(
            proto,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
        )
        if "user_config" in data:
            if data["user_config"] != "":
                data["user_config"] = cloudpickle.loads(proto.user_config)
            else:
                data["user_config"] = None
        if "autoscaling_config" in data:
            data["autoscaling_config"] = AutoscalingConfig(**data["autoscaling_config"])
        if "prev_version" in data:
            if data["prev_version"] == "":
                data["prev_version"] = None
        if "version" in data:
            if data["version"] == "":
                data["version"] = None
        return cls(**data)

    @classmethod
    def from_proto_bytes(cls, proto_bytes: bytes):
        proto = DeploymentConfigProto.FromString(proto_bytes)
        return cls.from_proto(proto)

    @classmethod
    def from_default(cls, ignore_none: bool = False, **kwargs):
        """Creates a default DeploymentConfig and overrides it with kwargs.

        Only accepts the same keywords as the class. Passing in any other
        keyword raises a ValueError.

        Args:
            ignore_none: When True, any valid keywords with value None
                are ignored, and their values stay default. Invalid keywords
                still raise a TypeError.

        Raises:
            TypeError: when a keyword that's not an argument to the class is
                passed in.
        """

        config = cls()
        valid_config_options = set(config.dict().keys())

        # Friendly error if a non-DeploymentConfig kwarg was passed in
        for key, val in kwargs.items():
            if key not in valid_config_options:
                raise TypeError(
                    f'Got invalid Deployment config option "{key}" '
                    f"(with value {val}) as keyword argument. All Deployment "
                    "config options must come from this list: "
                    f"{list(valid_config_options)}."
                )

        if ignore_none:
            kwargs = {key: val for key, val in kwargs.items() if val is not None}

        for key, val in kwargs.items():
            config.__setattr__(key, val)

        return config


class ReplicaConfig:
    """Configuration for a deployment's replicas.

    Provides five main properties (see property docstrings for more info):
        deployment_def: the code, or a reference to the code, that this
            replica should run.
        init_args: the deployment_def's init_args.
        init_kwargs: the deployment_def's init_kwargs.
        ray_actor_options: the Ray actor options to pass into the replica's
            actor.
        resource_dict: contains info on this replica's actor's resource needs.

    Offers a serialized equivalent (e.g. serialized_deployment_def) for
    deployment_def, init_args, and init_kwargs. Deserializes these properties
    when they're first accessed, if they were not passed in directly through
    create().

    Use the classmethod create() to make a ReplicaConfig with the deserialized
    properties.

    Note: overwriting or setting any property after the ReplicaConfig has been
    constructed is currently undefined behavior. The config's fields should not
    be modified externally after it is created.
    """

    def __init__(
        self,
        deployment_def_name: str,
        serialized_deployment_def: bytes,
        serialized_init_args: bytes,
        serialized_init_kwargs: bytes,
        ray_actor_options: Dict,
    ):
        """Construct a ReplicaConfig with serialized properties.

        All parameters are required. See classmethod create() for defaults.
        """
        self.deployment_def_name = deployment_def_name

        # Store serialized versions of code properties.
        self.serialized_deployment_def = serialized_deployment_def
        self.serialized_init_args = serialized_init_args
        self.serialized_init_kwargs = serialized_init_kwargs

        # Deserialize properties when first accessed. See @property methods.
        self._deployment_def = None
        self._init_args = None
        self._init_kwargs = None

        # Configure ray_actor_options. These are the Ray options ultimately
        # passed into the replica's actor when it's created.
        self.ray_actor_options = ray_actor_options
        self._validate_ray_actor_options()

        # Create resource_dict. This contains info about the replica's resource
        # needs. It does NOT set the replica's resource usage. That's done by
        # the ray_actor_options.
        self.resource_dict = resources_from_ray_options(self.ray_actor_options)

    @classmethod
    def create(
        cls,
        deployment_def: Union[Callable, str],
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        ray_actor_options: Optional[Dict] = None,
        deployment_def_name: Optional[str] = None,
    ):
        """Create a ReplicaConfig from deserialized parameters."""

        if inspect.isfunction(deployment_def):
            if init_args:
                raise ValueError("init_args not supported for function deployments.")
            elif init_kwargs:
                raise ValueError("init_kwargs not supported for function deployments.")

        if not isinstance(deployment_def, (Callable, str)):
            raise TypeError(
                f'Got invalid type "{type(deployment_def)}" for '
                "deployment_def. Expected deployment_def to be a "
                "class, function, or string."
            )

        # Set defaults
        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}
        if ray_actor_options is None:
            ray_actor_options = {}
        if deployment_def_name is None:
            if isinstance(deployment_def, str):
                deployment_def_name = deployment_def
            else:
                deployment_def_name = deployment_def.__name__

        config = cls(
            deployment_def_name,
            cloudpickle.dumps(deployment_def),
            cloudpickle.dumps(init_args),
            cloudpickle.dumps(init_kwargs),
            ray_actor_options,
        )

        config._deployment_def = deployment_def
        config._init_args = init_args
        config._init_kwargs = init_kwargs

        return config

    def _validate_ray_actor_options(self) -> None:

        if not isinstance(self.ray_actor_options, dict):
            raise TypeError(
                f'Got invalid type "{type(self.ray_actor_options)}" for '
                "ray_actor_options. Expected a dictionary."
            )

        allowed_ray_actor_options = {
            # Resource options
            "accelerator_type",
            "memory",
            "num_cpus",
            "num_gpus",
            "object_store_memory",
            "resources",
            # Other options
            "runtime_env",
        }

        for option in self.ray_actor_options:
            if option not in allowed_ray_actor_options:
                raise ValueError(
                    f"Specifying '{option}' in ray_actor_options is not allowed. "
                    f"Allowed options: {allowed_ray_actor_options}"
                )
        ray_option_utils.validate_actor_options(self.ray_actor_options, in_options=True)

        # Set Serve replica defaults
        if self.ray_actor_options.get("num_cpus") is None:
            self.ray_actor_options["num_cpus"] = 1

    @property
    def deployment_def(self) -> Union[Callable, str]:
        """The code, or a reference to the code, that this replica runs.

        For Python replicas, this can be one of the following:
            - Function (Callable)
            - Class (Callable)
            - Import path (str)

        For Java replicas, this can be one of the following:
            - Class path (str)
        """
        if self._deployment_def is None:
            self._deployment_def = cloudpickle.loads(self.serialized_deployment_def)

        return self._deployment_def

    @property
    def init_args(self) -> Optional[Tuple[Any]]:
        """The init_args for a Python class.

        This property is only meaningful if deployment_def is a Python class.
        Otherwise, it is None.
        """
        if self._init_args is None:
            self._init_args = cloudpickle.loads(self.serialized_init_args)

        return self._init_args

    @property
    def init_kwargs(self) -> Optional[Tuple[Any]]:
        """The init_kwargs for a Python class.

        This property is only meaningful if deployment_def is a Python class.
        Otherwise, it is None.
        """

        if self._init_kwargs is None:
            self._init_kwargs = cloudpickle.loads(self.serialized_init_kwargs)

        return self._init_kwargs

    @classmethod
    def from_proto(
        cls, proto: ReplicaConfigProto, deployment_language: DeploymentLanguage
    ):
        if deployment_language == DeploymentLanguage.PYTHON:
            deployment_def = proto.deployment_def
        else:
            # TODO use messagepack
            deployment_def = proto.deployment_def

        return ReplicaConfig(
            proto.deployment_def_name,
            deployment_def,
            proto.init_args,
            proto.init_kwargs,
            json.loads(proto.ray_actor_options),
        )

    @classmethod
    def from_proto_bytes(
        cls, proto_bytes: bytes, deployment_language: DeploymentLanguage
    ):
        proto = ReplicaConfigProto.FromString(proto_bytes)
        return cls.from_proto(proto, deployment_language)

    def to_proto(self):
        return ReplicaConfigProto(
            deployment_def_name=self.deployment_def_name,
            deployment_def=self.serialized_deployment_def,
            init_args=self.serialized_init_args,
            init_kwargs=self.serialized_init_kwargs,
            ray_actor_options=json.dumps(self.ray_actor_options),
        )

    def to_proto_bytes(self):
        return self.to_proto().SerializeToString()


class DeploymentMode(str, Enum):
    NoServer = "NoServer"
    HeadOnly = "HeadOnly"
    EveryNode = "EveryNode"
    FixedNumber = "FixedNumber"


class HTTPOptions(pydantic.BaseModel):
    # Documentation inside serve.start for user's convenience.
    host: Optional[str] = DEFAULT_HTTP_HOST
    port: int = DEFAULT_HTTP_PORT
    middlewares: List[Any] = []
    location: Optional[DeploymentMode] = DeploymentMode.HeadOnly
    num_cpus: int = 0
    root_url: str = ""
    root_path: str = ""
    fixed_number_replicas: Optional[int] = None
    fixed_number_selection_seed: int = 0

    @validator("location", always=True)
    def location_backfill_no_server(cls, v, values):
        if values["host"] is None or v is None:
            return DeploymentMode.NoServer
        return v

    @validator("fixed_number_replicas", always=True)
    def fixed_number_replicas_should_exist(cls, v, values):
        if values["location"] == DeploymentMode.FixedNumber and v is None:
            raise ValueError(
                "When location='FixedNumber', you must specify "
                "the `fixed_number_replicas` parameter."
            )
        return v

    class Config:
        validate_assignment = True
        extra = "forbid"
        arbitrary_types_allowed = True
