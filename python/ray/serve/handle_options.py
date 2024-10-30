from abc import ABC
from dataclasses import dataclass, fields

import ray
from ray.serve._private.common import DeploymentHandleSource, RequestProtocol
from ray.serve._private.utils import DEFAULT


@dataclass(frozen=True)
class _InitHandleOptionsBase:
    """Init options for each ServeHandle instance.

    These fields can be set by calling `.init()` on a handle before
    sending the first request.
    """

    _prefer_local_routing: bool = False
    _source: DeploymentHandleSource = DeploymentHandleSource.UNKNOWN


@dataclass(frozen=True)
class _InitHandleOptions(_InitHandleOptionsBase):
    @classmethod
    def create(cls, **kwargs) -> "_InitHandleOptions":
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
class _DynamicHandleOptionsBase(ABC):
    """Dynamic options for each ServeHandle instance.

    These fields can be changed by calling `.options()` on a handle.
    """

    method_name: str = "__call__"
    multiplexed_model_id: str = ""
    stream: bool = False
    _request_protocol: str = RequestProtocol.UNDEFINED

    def copy_and_update(self, **kwargs) -> "_DynamicHandleOptionsBase":
        new_kwargs = {}

        for f in fields(self):
            if f.name not in kwargs or kwargs[f.name] == DEFAULT.VALUE:
                new_kwargs[f.name] = getattr(self, f.name)
            else:
                new_kwargs[f.name] = kwargs[f.name]

        return _DynamicHandleOptions(**new_kwargs)


@dataclass(frozen=True)
class _DynamicHandleOptions(_DynamicHandleOptionsBase):
    pass
