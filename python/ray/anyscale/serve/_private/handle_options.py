from dataclasses import dataclass, fields

import ray
from ray.anyscale.serve._private.constants import ANYSCALE_RAY_SERVE_USE_GRPC_BY_DEFAULT
from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.handle_options import (
    DynamicHandleOptionsBase,
    InitHandleOptionsBase,
)
from ray.serve._private.utils import DEFAULT


@dataclass(frozen=True)
class AnyscaleInitHandleOptions(InitHandleOptionsBase):
    _run_router_in_separate_loop: bool = True

    @classmethod
    def create(cls, **kwargs) -> "AnyscaleInitHandleOptions":
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
class AnyscaleDynamicHandleOptions(DynamicHandleOptionsBase):
    _by_reference: bool = not ANYSCALE_RAY_SERVE_USE_GRPC_BY_DEFAULT

    def copy_and_update(self, **kwargs) -> "AnyscaleDynamicHandleOptions":
        new_kwargs = {}

        for f in fields(self):
            if f.name not in kwargs or kwargs[f.name] == DEFAULT.VALUE:
                new_kwargs[f.name] = getattr(self, f.name)
            else:
                new_kwargs[f.name] = kwargs[f.name]

        return AnyscaleDynamicHandleOptions(**new_kwargs)
