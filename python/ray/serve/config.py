import inspect
import pickle
from enum import Enum
from typing import Any, List, Optional

import pydantic
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, NonNegativeFloat, PositiveInt, validator
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT)
from ray.serve.generated.serve_pb2 import (BackendConfig as BackendConfigProto,
                                           AutoscalingConfig as
                                           AutoscalingConfigProto)
from ray.serve.generated.serve_pb2 import BackendLanguage

from ray import cloudpickle as cloudpickle


class AutoscalingConfig(BaseModel):
    # Publicly exposed options
    min_replicas: int
    max_replicas: int
    target_num_ongoing_requests_per_replica: int = 1

    # Private options below

    # Metrics scraping options
    metrics_interval_s: float = 10.0
    loop_period_s: float = 30.0
    look_back_period_s: float = 30.0

    # Internal autoscaling configuration options

    # Multiplicative "gain" factor to limit scaling decisions
    smoothing_factor: float = 1.0

    # TODO(architkulkarni): implement below
    # How long to wait before scaling down replicas
    # downscale_delay_s: float = 600.0
    # How long to wait before scaling up replicas
    # upscale_delay_s: float = 30.0

    # The number of replicas to start with when creating the deployment
    # initial_replicas: int = 1
    # The num_ongoing_requests_per_replica error ratio (desired / current)
    # threshold for overriding `upscale_delay_s`
    # panic_mode_threshold: float = 2.0

    # TODO(architkulkarni): Add reasonable defaults
    # TODO(architkulkarni): Add pydantic validation.  E.g. max_replicas>=min


class BackendConfig(BaseModel):
    """Configuration options for a backend, to be set by the user.

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

    Args:
        num_replicas (Optional[int]): The number of processes to start up that
            will handle requests to this backend. Defaults to 1.
        max_concurrent_queries (Optional[int]): The maximum number of queries
            that will be sent to a replica of this backend without receiving a
            response. Defaults to 100.
        user_config (Optional[Any]): Arguments to pass to the reconfigure
            method of the backend. The reconfigure method is called if
            user_config is not None.
        experimental_graceful_shutdown_wait_loop_s (Optional[float]): Duration
            that backend workers will wait until there is no more work to be
            done before shutting down. Defaults to 2s.
        experimental_graceful_shutdown_timeout_s (Optional[float]):
            Controller waits for this duration to forcefully kill the replica
            for shutdown. Defaults to 20s.
    """

    num_replicas: PositiveInt = 1
    max_concurrent_queries: Optional[int] = None
    user_config: Any = None

    experimental_graceful_shutdown_wait_loop_s: NonNegativeFloat = 2.0
    experimental_graceful_shutdown_timeout_s: NonNegativeFloat = 20.0

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
                **data["autoscaling_config"])
        return BackendConfigProto(
            is_cross_language=False,
            backend_language=BackendLanguage.PYTHON,
            **data,
        ).SerializeToString()

    @classmethod
    def from_proto_bytes(cls, proto_bytes: bytes):
        proto = BackendConfigProto.FromString(proto_bytes)
        data = MessageToDict(proto, preserving_proto_field_name=True)
        if "user_config" in data:
            data["user_config"] = pickle.loads(proto.user_config)
        if "autoscaling_config" in data:
            data["autoscaling_config"] = AutoscalingConfig(
                **data["autoscaling_config"])
        return cls(**data)


class ReplicaConfig:
    def __init__(self, backend_def, *init_args, ray_actor_options=None):
        # Validate that backend_def is an import path, function, or class.
        if isinstance(backend_def, str):
            self.func_or_class_name = backend_def
            pass
        elif inspect.isfunction(backend_def):
            self.func_or_class_name = backend_def.__name__
            if len(init_args) != 0:
                raise ValueError(
                    "init_args not supported for function backend.")
        elif inspect.isclass(backend_def):
            self.func_or_class_name = backend_def.__name__
        else:
            raise TypeError(
                "Backend must be an import path, function or class, it is {}.".
                format(type(backend_def)))

        self.serialized_backend_def = cloudpickle.dumps(backend_def)
        self.init_args = init_args
        if ray_actor_options is None:
            self.ray_actor_options = {}
        else:
            self.ray_actor_options = ray_actor_options

        self.resource_dict = {}
        self._validate()

    def _validate(self):

        if "placement_group" in self.ray_actor_options:
            raise ValueError("Providing placement_group for backend actors "
                             "is not currently supported.")

        if not isinstance(self.ray_actor_options, dict):
            raise TypeError("ray_actor_options must be a dictionary.")
        elif "lifetime" in self.ray_actor_options:
            raise ValueError(
                "Specifying lifetime in init_args is not allowed.")
        elif "name" in self.ray_actor_options:
            raise ValueError("Specifying name in init_args is not allowed.")
        elif "max_restarts" in self.ray_actor_options:
            raise ValueError("Specifying max_restarts in "
                             "init_args is not allowed.")
        else:
            # Ray defaults to zero CPUs for placement, we default to one here.
            if "num_cpus" not in self.ray_actor_options:
                self.ray_actor_options["num_cpus"] = 1
            num_cpus = self.ray_actor_options["num_cpus"]
            if not isinstance(num_cpus, (int, float)):
                raise TypeError(
                    "num_cpus in ray_actor_options must be an int or a float.")
            elif num_cpus < 0:
                raise ValueError("num_cpus in ray_actor_options must be >= 0.")
            self.resource_dict["CPU"] = num_cpus

            num_gpus = self.ray_actor_options.get("num_gpus", 0)
            if not isinstance(num_gpus, (int, float)):
                raise TypeError(
                    "num_gpus in ray_actor_options must be an int or a float.")
            elif num_gpus < 0:
                raise ValueError("num_gpus in ray_actor_options must be >= 0.")
            self.resource_dict["GPU"] = num_gpus

            memory = self.ray_actor_options.get("memory", 0)
            if not isinstance(memory, (int, float)):
                raise TypeError(
                    "memory in ray_actor_options must be an int or a float.")
            elif memory < 0:
                raise ValueError("num_gpus in ray_actor_options must be >= 0.")
            self.resource_dict["memory"] = memory

            object_store_memory = self.ray_actor_options.get(
                "object_store_memory", 0)
            if not isinstance(object_store_memory, (int, float)):
                raise TypeError(
                    "object_store_memory in ray_actor_options must be "
                    "an int or a float.")
            elif object_store_memory < 0:
                raise ValueError(
                    "object_store_memory in ray_actor_options must be >= 0.")
            self.resource_dict["object_store_memory"] = object_store_memory

            custom_resources = self.ray_actor_options.get("resources", {})
            if not isinstance(custom_resources, dict):
                raise TypeError(
                    "resources in ray_actor_options must be a dictionary.")
            self.resource_dict.update(custom_resources)


class DeploymentMode(str, Enum):
    NoServer = "NoServer"
    HeadOnly = "HeadOnly"
    EveryNode = "EveryNode"


class HTTPOptions(pydantic.BaseModel):
    # Documentation inside serve.start for user's convenience.
    host: Optional[str] = DEFAULT_HTTP_HOST
    port: int = DEFAULT_HTTP_PORT
    middlewares: List[Any] = []
    location: Optional[DeploymentMode] = DeploymentMode.HeadOnly
    num_cpus: int = 0
    root_url: str = ""

    @validator("location", always=True)
    def location_backfill_no_server(cls, v, values):
        if values["host"] is None or v is None:
            return DeploymentMode.NoServer
        return v

    class Config:
        validate_assignment = True
        extra = "forbid"
        arbitrary_types_allowed = True
