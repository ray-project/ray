import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import pydantic
from pydantic import BaseModel, confloat, PositiveFloat, PositiveInt, validator
from ray.serve.constants import (ASYNC_CONCURRENCY, DEFAULT_HTTP_HOST,
                                 DEFAULT_HTTP_PORT)


def _callable_accepts_batch(func_or_class):
    if inspect.isfunction(func_or_class):
        return hasattr(func_or_class, "_serve_accept_batch")
    elif inspect.isclass(func_or_class):
        return hasattr(func_or_class.__call__, "_serve_accept_batch")


def _callable_is_blocking(func_or_class):
    if inspect.isfunction(func_or_class):
        return not inspect.iscoroutinefunction(func_or_class)
    elif inspect.isclass(func_or_class):
        return not inspect.iscoroutinefunction(func_or_class.__call__)


@dataclass
class BackendMetadata:
    accepts_batches: bool = False
    is_blocking: bool = True
    autoscaling_config: Optional[Dict[str, Any]] = None


class BackendConfig(BaseModel):
    """Configuration options for a backend, to be set by the user.

    Args:
        num_replicas (Optional[int]): The number of processes to start up that
            will handle requests to this backend. Defaults to 0.
        max_batch_size (Optional[int]): The maximum number of requests that
            will be processed in one batch by this backend. Defaults to None
            (no maximium).
        batch_wait_timeout (Optional[float]): The time in seconds that backend
            replicas will wait for a full batch of requests before processing a
            partial batch. Defaults to 0.
        max_concurrent_queries (Optional[int]): The maximum number of queries
            that will be sent to a replica of this backend without receiving a
            response. Defaults to None (no maximum).
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

    internal_metadata: BackendMetadata = BackendMetadata()
    num_replicas: PositiveInt = 1
    max_batch_size: Optional[PositiveInt] = None
    batch_wait_timeout: float = 0
    max_concurrent_queries: Optional[int] = None
    user_config: Any = None

    experimental_graceful_shutdown_wait_loop_s: PositiveFloat = 2.0
    experimental_graceful_shutdown_timeout_s: confloat(ge=0) = 20.0

    class Config:
        validate_assignment = True
        extra = "forbid"
        arbitrary_types_allowed = True

    def _validate_batch_size(self):
        if (self.max_batch_size is not None
                and not self.internal_metadata.accepts_batches
                and self.max_batch_size > 1):
            raise ValueError(
                "max_batch_size is set in config but the function or "
                "method does not accept batching. Please use "
                "@serve.accept_batch to explicitly mark that the function or "
                "method accepts a list of requests as an argument.")

    # This is not a pydantic validator, so that we may skip this method when
    # creating partially filled BackendConfig objects to pass as updates--for
    # example, BackendConfig(max_batch_size=5).
    def _validate_complete(self):
        self._validate_batch_size()

    # Dynamic default for max_concurrent_queries
    @validator("max_concurrent_queries", always=True)
    def set_max_queries_by_mode(cls, v, values):  # noqa 805
        if v is None:
            # Model serving mode: if the servable is blocking and the wait
            # timeout is default zero seconds, then we keep the existing
            # behavior to allow at most max batch size queries.
            if (values["internal_metadata"].is_blocking
                    and values["batch_wait_timeout"] == 0):
                if ("max_batch_size" in values
                        and values["max_batch_size"] is not None):
                    v = 2 * values["max_batch_size"]
                else:
                    v = 8

            # Pipeline/async mode: if the servable is not blocking,
            # router should just keep pushing queries to the replicas
            # until a high limit.
            if not values["internal_metadata"].is_blocking:
                v = ASYNC_CONCURRENCY

            # Batch inference mode: user specifies non zero timeout to wait for
            # full batch. We will use 2*max_batch_size to perform double
            # buffering to keep the replica busy.
            if ("max_batch_size" in values
                    and values["max_batch_size"] is not None
                    and values["batch_wait_timeout"] > 0):
                v = 2 * values["max_batch_size"]
        return v


class ReplicaConfig:
    def __init__(self, func_or_class, *actor_init_args,
                 ray_actor_options=None):
        self.func_or_class = func_or_class
        self.accepts_batches = _callable_accepts_batch(func_or_class)
        self.is_blocking = _callable_is_blocking(func_or_class)
        self.actor_init_args = list(actor_init_args)
        if ray_actor_options is None:
            self.ray_actor_options = {}
        else:
            self.ray_actor_options = ray_actor_options

        self.resource_dict = {}
        self._validate()

    def _validate(self):
        # Validate that func_or_class is a function or class.
        if inspect.isfunction(self.func_or_class):
            if len(self.actor_init_args) != 0:
                raise ValueError(
                    "actor_init_args not supported for function backend.")
        elif not inspect.isclass(self.func_or_class):
            raise TypeError(
                "Backend must be a function or class, it is {}.".format(
                    type(self.func_or_class)))

        if not isinstance(self.ray_actor_options, dict):
            raise TypeError("ray_actor_options must be a dictionary.")
        elif "lifetime" in self.ray_actor_options:
            raise ValueError(
                "Specifying lifetime in actor_init_args is not allowed.")
        elif "name" in self.ray_actor_options:
            raise ValueError(
                "Specifying name in actor_init_args is not allowed.")
        elif "max_restarts" in self.ray_actor_options:
            raise ValueError("Specifying max_restarts in "
                             "actor_init_args is not allowed.")
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

    @validator("location", always=True)
    def location_backfill_no_server(cls, v, values):
        if values["host"] is None or v is None:
            return DeploymentMode.NoServer
        return v

    class Config:
        validate_assignment = True
        extra = "forbid"
        arbitrary_types_allowed = True
