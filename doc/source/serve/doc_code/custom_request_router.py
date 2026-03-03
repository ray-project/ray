# flake8: noqa
# __begin_define_uniform_request_router__
import random
from ray.serve.request_router import (
    PendingRequest,
    RequestRouter,
    ReplicaID,
    ReplicaResult,
    RunningReplica,
)
from typing import (
    List,
    Optional,
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


# __end_define_uniform_request_router__


# __begin_define_throughput_aware_request_router__
from ray.serve.request_router import (
    FIFOMixin,
    LocalityMixin,
    MultiplexMixin,
    PendingRequest,
    RequestRouter,
    ReplicaID,
    ReplicaResult,
    RunningReplica,
)
from typing import (
    Dict,
    List,
    Optional,
)


class ThroughputAwareRequestRouter(
    FIFOMixin, MultiplexMixin, LocalityMixin, RequestRouter
):
    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """
        This method chooses the best replica for the request based on
        multiplexed, locality, and custom throughput stats. The algorithm
        works as follows:

        1. Populate top_ranked_replicas based on available replicas based on
          multiplex_id
        2. Populate and override top_ranked_replicas info based on locality
         information of replicas (we want to prefer replicas that are in the
          same vicinity to this deployment)
        3. Select the replica with minimum throughput.
        """

        # Dictionary to hold the top-ranked replicas
        top_ranked_replicas: Dict[ReplicaID, RunningReplica] = {}
        # Take the best set of replicas for the multiplexed model
        if (
            pending_request is not None
            and pending_request.metadata.multiplexed_model_id
        ):
            ranked_replicas_multiplex: List[RunningReplica] = (
                self.rank_replicas_via_multiplex(
                    replicas=candidate_replicas,
                    multiplexed_model_id=pending_request.metadata.multiplexed_model_id,
                )
            )[0]

            # Filter out replicas that are not available (queue length exceed max ongoing request)
            ranked_replicas_multiplex = self.select_available_replicas(
                candidates=ranked_replicas_multiplex
            )

            for replica in ranked_replicas_multiplex:
                top_ranked_replicas[replica.replica_id] = replica

        # Take the best set of replicas in terms of locality
        ranked_replicas_locality: List[
            RunningReplica
        ] = self.rank_replicas_via_locality(replicas=candidate_replicas)[0]

        # Filter out replicas that are not available (queue length exceed max ongoing request)
        ranked_replicas_locality = self.select_available_replicas(
            candidates=ranked_replicas_locality
        )

        for replica in ranked_replicas_locality:
            top_ranked_replicas[replica.replica_id] = replica

        print("ThroughputAwareRequestRouter routing request")

        # Take the replica with minimum throughput.
        min_throughput_replicas = min(
            [replica for replica in top_ranked_replicas.values()],
            key=lambda r: r.routing_stats.get("throughput", 0),
        )
        return [[min_throughput_replicas]]


# __end_define_throughput_aware_request_router__
