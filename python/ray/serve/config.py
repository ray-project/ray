import inspect
import pickle
import copy
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pydantic
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, NonNegativeFloat, PositiveInt, validator
from ray.serve.constants import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT
from ray.serve.generated.serve_pb2 import (
    DeploymentConfig as DeploymentConfigProto, AutoscalingConfig as
    AutoscalingConfigProto)
from ray.serve.generated.serve_pb2 import DeploymentLanguage

from ray import cloudpickle as cloudpickle


class AutoscalingConfig(BaseModel):
    # Please keep these options in sync with those in
    # `src/ray/protobuf/serve.proto`.

    # Publicly exposed options
    min_replicas: int = 1
    max_replicas: int = 1
    target_num_ongoing_requests_per_replica: int = 1

    # Private options below.

    # Metrics scraping options

    # How often to scrape for metrics
    metrics_interval_s: float = 10.0
    # Time window to average over for metrics.
    look_back_period_s: float = 30.0

    # Internal autoscaling configuration options

    # Multiplicative "gain" factor to limit scaling decisions
    smoothing_factor: float = 1.0

    # How frequently to make autoscaling decisions
    # loop_period_s: float = CONTROL_LOOP_PERIOD_S
    # How long to wait before scaling down replicas
    downscale_delay_s: float = 600.0
    # How long to wait before scaling up replicas
    upscale_delay_s: float = 30.0

    # TODO(architkulkarni): implement below
    # The number of replicas to start with when creating the deployment
    # initial_replicas: int = 1
    # The num_ongoing_requests_per_replica error ratio (desired / current)
    # threshold for overriding `upscale_delay_s`
    # panic_mode_threshold: float = 2.0

    # TODO(architkulkarni): Add reasonable defaults
    # TODO(architkulkarni): Add pydantic validation.  E.g. max_replicas>=min


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
                **data["autoscaling_config"])
        return DeploymentConfigProto(
            is_cross_language=False,
            deployment_language=DeploymentLanguage.PYTHON,
            **data,
        ).SerializeToString()

    @classmethod
    def from_proto_bytes(cls, proto_bytes: bytes):
        proto = DeploymentConfigProto.FromString(proto_bytes)
        data = MessageToDict(
            proto,
            including_default_value_fields=True,
            preserving_proto_field_name=True)
        if "user_config" in data:
            if data["user_config"] != "":
                data["user_config"] = pickle.loads(proto.user_config)
            else:
                data["user_config"] = None
        if "autoscaling_config" in data:
            data["autoscaling_config"] = AutoscalingConfig(
                **data["autoscaling_config"])

        # Delete fields which are only used in protobuf, not in Python.
        del data["is_cross_language"]
        del data["deployment_language"]

        return cls(**data)


class SerializedFuncOrClass:
    def __init__(self, func_or_class: Callable):
        self.is_function = False
        if inspect.isfunction(func_or_class):
            self.is_function = True
        elif not inspect.isclass(func_or_class):
            raise TypeError("@serve.deployment must be called on a class or "
                            f"function, got {type(func_or_class)}.")

        self.name: str = func_or_class.__name__
        self.is_function: bool = inspect.isfunction(func_or_class)
        self.serialized_bytes: bytes = cloudpickle.dumps(func_or_class)


class ReplicaConfig:
    """Holds metadata for creating Deployment replicas."""

    def __init__(self,
                 func_or_class: Union[Callable, SerializedFuncOrClass],
                 init_args: Optional[Tuple[Any]] = None,
                 init_kwargs: Optional[Dict[Any, Any]] = None,
                 num_cpus: Optional[float] = None,
                 num_gpus: Optional[float] = None,
                 resources: Optional[Dict[str, float]] = None,
                 accelerator_type: Optional[str] = None,
                 runtime_env: Optional[Dict[str, Any]] = None):
        self.set_func_or_class(func_or_class)

        self.init_args = tuple()
        if init_args is not None:
            self.set_init_args(init_args)

        self.init_kwargs = dict()
        if init_kwargs is not None:
            self.set_init_kwargs(init_kwargs)

        self.num_cpus = 1.0
        if num_cpus is not None:
            self.set_num_cpus(num_cpus)

        self.num_gpus = 0.0
        if num_gpus is not None:
            self.set_num_gpus(num_gpus)

        self.resources = None
        if resources is not None:
            self.set_resources(resources)

        self.accelerator_type = None
        if accelerator_type is not None:
            self.set_accelerator_type(accelerator_type)

        self.runtime_env = None
        if runtime_env is not None:
            self.set_runtime_env(runtime_env)

    def copy(self) -> "ReplicaConfig":
        return ReplicaConfig(
            func_or_class=self.serialized_func_or_class,
            num_cpus=self.num_cpus,
            num_gpus=self.num_gpus,
            resources=copy.copy(self.resources),
            accelerator_type=self.accelerator_type,
            runtime_env=copy.copy(self.runtime_env))

    def set_func_or_class(
            self, func_or_class: Union[Callable, SerializedFuncOrClass]):
        if isinstance(func_or_class, SerializedFuncOrClass):
            serialized_func_or_class = func_or_class
        else:
            serialized_func_or_class = SerializedFuncOrClass(func_or_class)

        # Serialize the function or class to make this datastructure
        # environment-independent.
        self.serialized_func_or_class = serialized_func_or_class

    def set_init_args(self, init_args: Tuple[Any]):
        if self.serialized_func_or_class.is_function:
            raise ValueError(
                "init_args not supported for function deployments.")

        if not isinstance(init_args, tuple):
            raise TypeError("init_args must be a tuple.")

        self.init_args = init_args

    def set_init_kwargs(self, init_kwargs: Dict[Any, Any]):
        if self.serialized_func_or_class.is_function:
            raise ValueError(
                "init_kwargs not supported for function deployments.")

        if not isinstance(init_kwargs, dict):
            raise TypeError("init_kwargs must be a dict.")

        self.init_kwargs = init_kwargs

    def set_num_cpus(self, num_cpus: Union[int, float]):
        if not isinstance(num_cpus, (int, float)):
            raise TypeError("num_cpus_per_replica must be int or float, "
                            f"got {type(num_cpus)}.")
        elif num_cpus < 0:
            raise ValueError("num_cpus_per_replica must be >= 0.")

        self.num_cpus = float(num_cpus)

    def set_num_gpus(self, num_gpus: Union[int, float]) -> float:
        if not isinstance(num_gpus, (int, float)):
            raise TypeError("num_gpus_per_replica must be int or float, "
                            f"got {type(num_gpus)}.")
        elif num_gpus < 0:
            raise ValueError("num_gpus_per_replica must be >= 0.")

        self.num_gpus = float(num_gpus)

    def set_resources(self, resources: Dict[str, float]):
        if not isinstance(resources, dict):
            raise TypeError(
                "resources_per_replica must be a Dict[str, float], "
                f"got {type(resources)}.")
        if "CPU" in resources:
            raise ValueError(
                "CPU cannot be passed in resources_per_replica, use "
                "num_cpus_per_replica instead.")
        if "GPU" in resources:
            raise ValueError(
                "GPU cannot be passed in resources_per_replica, use "
                "num_gpus_per_replica instead.")

        for k, v in resources.items():
            if not isinstance(k, str):
                raise TypeError("resource keys must be strings, got "
                                f"{type(k)} for key '{k}'.")
            if not isinstance(v, (int, float)):
                raise TypeError("resource values must be ints or floats, "
                                f"got {type(v)} for key '{k}'.")
            if v < 0:
                raise ValueError("resource values must be >= 0, "
                                 f"got {v} for key '{k}'.")

        self.resources = resources

    def get_resource_dict(self) -> Dict[str, float]:
        """Return a dictionary of all resources required by the replicas.

        Includes num_cpus and num_gpus, which will be 'CPU' and 'GPU',
        respectively. This is suitable to be passed to create a placement
        group or print warnings about resources.
        """
        d = self.resources.copy() if self.resources is not None else {}
        if self.num_cpus != 0:
            d["CPU"] = self.num_cpus
        if self.num_gpus != 0:
            d["GPU"] = self.num_gpus
        return d

    def set_accelerator_type(self, accelerator_type: str):
        if not isinstance(accelerator_type, str):
            raise TypeError("accelerator_type must be a string.")

        self.accelerator_type = accelerator_type

    def set_runtime_env(self, runtime_env: Dict[Any, str]):
        if runtime_env is not None and not isinstance(runtime_env, dict):
            raise TypeError("runtime_env must be a Dict, "
                            f"got {type(runtime_env)}.")

        self.runtime_env = runtime_env

    def set_ray_actor_options(self, ray_actor_options: Dict[str, Any]):
        """Sets the options specified in ray_actor_options in the replica_config.

        This is used to support the legacy `ray_actor_options` API and will be
        removed in a future release.
        """
        if not isinstance(ray_actor_options, dict):
            raise TypeError("ray_actor_options must be a dictionary.")
        elif "placement_group" in ray_actor_options:
            raise ValueError(
                "Specifying placement_group in ray_actor_options is not "
                "allowed.")
        elif "lifetime" in ray_actor_options:
            raise ValueError(
                "Specifying lifetime in ray_actor_options is not allowed.")
        elif "name" in ray_actor_options:
            raise ValueError(
                "Specifying name in ray_actor_options is not allowed.")
        elif "max_restarts" in ray_actor_options:
            raise ValueError("Specifying max_restarts in "
                             "ray_actor_options is not allowed.")

        if "num_cpus" in ray_actor_options:
            self.set_num_cpus(ray_actor_options["num_cpus"])

        if "num_gpus" in ray_actor_options:
            self.set_num_gpus(ray_actor_options["num_gpus"])

        resources = ray_actor_options.get("resources", {})
        if "memory" in ray_actor_options:
            resources["memory"] = ray_actor_options["memory"]
        if "object_store_memory" in ray_actor_options:
            resources["object_store_memory"] = ray_actor_options[
                "object_store_memory"]

        self.set_resources(resources)

    def override_runtime_env(self, curr_job_env: Dict[str, Any]):
        if self.runtime_env is None:
            self.runtime_env = curr_job_env
        else:
            self.runtime_env.setdefault("uris", curr_job_env.get("uris"))

        # working_dir cannot be passed per-actor.
        # TODO(edoakes): we should remove this once we refactor working_dir to
        # not get rewritten into the "uris" field.
        if "working_dir" in self.runtime_env:
            del self.runtime_env["working_dir"]


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
            raise ValueError("When location='FixedNumber', you must specify "
                             "the `fixed_number_replicas` parameter.")
        return v

    class Config:
        validate_assignment = True
        extra = "forbid"
        arbitrary_types_allowed = True
