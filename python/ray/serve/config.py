import inspect
import pickle
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

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
from ray.serve.constants import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT
from ray.serve.generated.serve_pb2 import (
    DeploymentConfig as DeploymentConfigProto,
    AutoscalingConfig as AutoscalingConfigProto,
)
from ray.serve.generated.serve_pb2 import DeploymentLanguage

from ray import cloudpickle as cloudpickle


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
            be done before shutting down. Defaults to 2s.
        graceful_shutdown_timeout_s (Optional[float]):
            Controller waits for this duration to forcefully kill the replica
            for shutdown. Defaults to 20s.
    """

    num_replicas: PositiveInt = 1
    max_concurrent_queries: Optional[int] = None
    user_config: Any = None

    graceful_shutdown_wait_loop_s: NonNegativeFloat = 2.0
    graceful_shutdown_timeout_s: NonNegativeFloat = 20.0

    autoscaling_config: Optional[AutoscalingConfig] = None

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

    def to_proto_bytes(self):
        data = self.dict()
        if data.get("user_config"):
            data["user_config"] = pickle.dumps(data["user_config"])
        if data.get("autoscaling_config"):
            data["autoscaling_config"] = AutoscalingConfigProto(
                **data["autoscaling_config"]
            )
        return DeploymentConfigProto(
            is_cross_language=False,
            deployment_language=DeploymentLanguage.PYTHON,
            **data,
        ).SerializeToString()

    @classmethod
    def from_proto_bytes(cls, proto_bytes: bytes):
        proto = DeploymentConfigProto.FromString(proto_bytes)
        data = MessageToDict(
            proto, including_default_value_fields=True, preserving_proto_field_name=True
        )
        if "user_config" in data:
            if data["user_config"] != "":
                data["user_config"] = pickle.loads(proto.user_config)
            else:
                data["user_config"] = None
        if "autoscaling_config" in data:
            data["autoscaling_config"] = AutoscalingConfig(**data["autoscaling_config"])

        # Delete fields which are only used in protobuf, not in Python.
        del data["is_cross_language"]
        del data["deployment_language"]

        return cls(**data)


class ReplicaConfig:
    def __init__(
        self,
        deployment_def: Callable,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        ray_actor_options=None,
    ):
        # Validate that deployment_def is an import path, function, or class.
        if isinstance(deployment_def, str):
            self.func_or_class_name = deployment_def
        elif inspect.isfunction(deployment_def):
            self.func_or_class_name = deployment_def.__name__
            if init_args:
                raise ValueError("init_args not supported for function deployments.")
            if init_kwargs:
                raise ValueError("init_kwargs not supported for function deployments.")
        elif inspect.isclass(deployment_def):
            self.func_or_class_name = deployment_def.__name__
        else:
            raise TypeError(
                "Deployment must be a function or class, it is {}.".format(
                    type(deployment_def)
                )
            )

        self.serialized_deployment_def = cloudpickle.dumps(deployment_def)
        self.init_args = init_args if init_args is not None else ()
        self.init_kwargs = init_kwargs if init_kwargs is not None else {}
        if ray_actor_options is None:
            self.ray_actor_options = {}
        else:
            self.ray_actor_options = ray_actor_options

        self.resource_dict = {}
        self._validate()

    def _validate(self):

        if "placement_group" in self.ray_actor_options:
            raise ValueError(
                "Providing placement_group for deployment actors "
                "is not currently supported."
            )

        if not isinstance(self.ray_actor_options, dict):
            raise TypeError("ray_actor_options must be a dictionary.")
        elif "lifetime" in self.ray_actor_options:
            raise ValueError("Specifying lifetime in ray_actor_options is not allowed.")
        elif "name" in self.ray_actor_options:
            raise ValueError("Specifying name in ray_actor_options is not allowed.")
        elif "max_restarts" in self.ray_actor_options:
            raise ValueError(
                "Specifying max_restarts in " "ray_actor_options is not allowed."
            )
        else:
            # Ray defaults to zero CPUs for placement, we default to one here.
            if "num_cpus" not in self.ray_actor_options:
                self.ray_actor_options["num_cpus"] = 1
            num_cpus = self.ray_actor_options["num_cpus"]
            if not isinstance(num_cpus, (int, float)):
                raise TypeError(
                    "num_cpus in ray_actor_options must be an int or a float."
                )
            elif num_cpus < 0:
                raise ValueError("num_cpus in ray_actor_options must be >= 0.")
            self.resource_dict["CPU"] = num_cpus

            num_gpus = self.ray_actor_options.get("num_gpus", 0)
            if not isinstance(num_gpus, (int, float)):
                raise TypeError(
                    "num_gpus in ray_actor_options must be an int or a float."
                )
            elif num_gpus < 0:
                raise ValueError("num_gpus in ray_actor_options must be >= 0.")
            self.resource_dict["GPU"] = num_gpus

            memory = self.ray_actor_options.get("memory", 0)
            if not isinstance(memory, (int, float)):
                raise TypeError(
                    "memory in ray_actor_options must be an int or a float."
                )
            elif memory < 0:
                raise ValueError("num_gpus in ray_actor_options must be >= 0.")
            self.resource_dict["memory"] = memory

            object_store_memory = self.ray_actor_options.get("object_store_memory", 0)
            if not isinstance(object_store_memory, (int, float)):
                raise TypeError(
                    "object_store_memory in ray_actor_options must be "
                    "an int or a float."
                )
            elif object_store_memory < 0:
                raise ValueError(
                    "object_store_memory in ray_actor_options must be >= 0."
                )
            self.resource_dict["object_store_memory"] = object_store_memory

            custom_resources = self.ray_actor_options.get("resources", {})
            if not isinstance(custom_resources, dict):
                raise TypeError("resources in ray_actor_options must be a dictionary.")
            self.resource_dict.update(custom_resources)


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
