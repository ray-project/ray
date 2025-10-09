from abc import ABC, abstractmethod
from dataclasses import dataclass, fields

import ray
from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.constants import RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP
from ray.serve._private.utils import DEFAULT


@dataclass(frozen=True)
class InitHandleOptionsBase(ABC):
    """Init options for each ServeHandle instance.

    These fields can be set by calling `.init()` on a handle before
    sending the first request.
    """

    _prefer_local_routing: bool = False
    _source: DeploymentHandleSource = DeploymentHandleSource.UNKNOWN
    _run_router_in_separate_loop: bool = RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP

    @classmethod
    @abstractmethod
    def create(cls, **kwargs) -> "InitHandleOptionsBase":
        raise NotImplementedError


@dataclass(frozen=True)
class InitHandleOptions(InitHandleOptionsBase):
    @classmethod
    def create(cls, **kwargs) -> "InitHandleOptions":
        for k in list(kwargs.keys()):
            if kwargs[k] == DEFAULT.VALUE:
                # Use default value
                del kwargs[k]

        # Detect replica source for handles
        if (
            "_source" not in kwargs
            and ray.serve.context._get_internal_replica_context() is not None
        ):
            kwargs["_source"] = DeploymentHandleSource.REPLICA

        return cls(**kwargs)


@dataclass(frozen=True)
class DynamicHandleOptionsBase(ABC):
    """Dynamic options for each ServeHandle instance.

    These fields can be changed by calling `.options()` on a handle.
    """

    method_name: str = "__call__"
    multiplexed_model_id: str = ""
    stream: bool = False

    @abstractmethod
    def copy_and_update(self, **kwargs) -> "DynamicHandleOptionsBase":
        pass


@dataclass(frozen=True)
class DynamicHandleOptions(DynamicHandleOptionsBase):
    _by_reference: bool = True

    def copy_and_update(self, **kwargs) -> "DynamicHandleOptions":
        new_kwargs = {}

        for f in fields(self):
            if f.name not in kwargs or kwargs[f.name] == DEFAULT.VALUE:
                new_kwargs[f.name] = getattr(self, f.name)
            else:
                new_kwargs[f.name] = kwargs[f.name]

        return DynamicHandleOptions(**new_kwargs)
