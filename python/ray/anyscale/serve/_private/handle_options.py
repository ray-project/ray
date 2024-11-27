from dataclasses import dataclass

import ray
from ray.anyscale.serve._private.constants import ANYSCALE_RAY_SERVE_USE_GRPC_BY_DEFAULT
from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.handle_options import InitHandleOptionsBase
from ray.serve._private.utils import DEFAULT


@dataclass(frozen=True)
class _AnyscaleInitHandleOptions(InitHandleOptionsBase):
    _by_reference: bool = not ANYSCALE_RAY_SERVE_USE_GRPC_BY_DEFAULT
    _run_router_in_separate_loop: bool = True

    @classmethod
    def create(cls, **kwargs) -> "_AnyscaleInitHandleOptions":
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
