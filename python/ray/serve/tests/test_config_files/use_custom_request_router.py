import random
from typing import (
    List,
    Optional,
)

from ray import serve
from ray.serve.context import _get_internal_replica_context
from ray.serve.request_router import (
    PendingRequest,
    ReplicaID,
    ReplicaResult,
    RequestRouter,
    RunningReplica,
)


class UniformRequestRouter(RequestRouter):
    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        print("UniformRequestRouter routing request")
        index = random.randint(0, len(candidate_replicas) - 1)
        return [[candidate_replicas[index]]]

    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        print("on_request_routed callback is called!!")


@serve.deployment
class UniformRequestRouterApp:
    def __init__(self):
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id

    async def __call__(self):
        return "hello_from_custom_request_router"


app = UniformRequestRouterApp.bind()
