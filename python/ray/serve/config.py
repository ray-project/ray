import inspect
from enum import Enum
from typing import Any, List, Optional

import pydantic
from pydantic import BaseModel, PositiveInt, validator, NonNegativeFloat

from ray import cloudpickle as cloudpickle
from ray.serve.constants import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT


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


class ReplicaConfig:
    def __init__(self, backend_def, *init_args, ray_actor_options=None):
        # Validate that backend_def is an import path, function, or class.
        if isinstance(backend_def, str):
            pass
        elif inspect.isfunction(backend_def):
            if len(init_args) != 0:
                raise ValueError(
                    "init_args not supported for function backend.")
        elif not inspect.isclass(backend_def):
            raise TypeError(
                "Backend must be a function or class, it is {}.".format(
                    type(backend_def)))

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

    @validator("location", always=True)
    def location_backfill_no_server(cls, v, values):
        if values["host"] is None or v is None:
            return DeploymentMode.NoServer
        return v

    class Config:
        validate_assignment = True
        extra = "forbid"
        arbitrary_types_allowed = True
