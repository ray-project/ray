# TODO (Kourosh): Internally rename PrefixAwarePow2ReplicaRouter to something else.
from ray.llm._internal.serve.request_router.prefix_aware.prefix_aware_router import (
    PrefixAwarePow2ReplicaRouter,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class KVRouter(PrefixAwarePow2ReplicaRouter):
    pass
