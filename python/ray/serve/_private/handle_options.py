from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import Callable, Optional

import ray
from ray._common.utils import import_attr
from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.replica_scheduler import PowerOfTwoChoicesReplicaScheduler
from ray.serve._private.utils import DEFAULT


@dataclass(frozen=True)
class InitHandleOptionsBase(ABC):
    """Init options for each ServeHandle instance.

    These fields can be set by calling `.init()` on a handle before
    sending the first request.
    """

    _prefer_local_routing: bool = False
    _source: DeploymentHandleSource = DeploymentHandleSource.UNKNOWN

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
    replica_scheduler: Callable = PowerOfTwoChoicesReplicaScheduler
    on_request_scheduled: Optional[Callable] = None

    @abstractmethod
    def copy_and_update(self, **kwargs) -> "DynamicHandleOptionsBase":
        pass


@dataclass(frozen=True)
class DynamicHandleOptions(DynamicHandleOptionsBase):
    def copy_and_update(self, **kwargs) -> "DynamicHandleOptions":
        new_kwargs = {}

        for f in fields(self):
            if f.name not in kwargs or kwargs[f.name] == DEFAULT.VALUE:
                new_kwargs[f.name] = getattr(self, f.name)
            elif f.name == "replica_scheduler":
                replica_scheduler = kwargs[f.name]
                if isinstance(replica_scheduler, str):
                    replica_scheduler = import_attr(kwargs[f.name])
                new_kwargs[f.name] = replica_scheduler
            else:
                new_kwargs[f.name] = kwargs[f.name]

        return DynamicHandleOptions(**new_kwargs)
