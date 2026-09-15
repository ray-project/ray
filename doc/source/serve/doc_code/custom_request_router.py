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
        """Rank by model affinity, then locality, then reported throughput."""
        if pending_request is not None:
            # All fallback ranks are returned together, so back off after this pass.
            pending_request.routing_context.should_backoff = True
        model_ranks = [candidate_replicas]
        if (
            pending_request is not None
            and pending_request.metadata.multiplexed_model_id
        ):
            model_ranks = self.rank_replicas_via_multiplex(
                replicas=candidate_replicas,
                multiplexed_model_id=pending_request.metadata.multiplexed_model_id,
            )

        ranked_replicas = []
        for model_rank in model_ranks:
            for locality_rank in self.rank_replicas_via_locality(model_rank):
                available = self.select_available_replicas(candidates=locality_rank)
                by_throughput = sorted(
                    available,
                    key=lambda replica: replica.routing_stats.get("throughput", 0),
                )
                # Return singleton ranks so throughput determines the attempt order.
                ranked_replicas.extend([[replica] for replica in by_throughput])
        return ranked_replicas


# __end_define_throughput_aware_request_router__


if __name__ == "__main__":
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from ray.serve._private.common import (
        DeploymentHandleSource,
        DeploymentID,
        RequestMetadata,
    )

    deployment_id = DeploymentID("routing-example")

    def replica(name, *, node="local", zone="zone-1", models=(), throughput=0):
        return SimpleNamespace(
            replica_id=ReplicaID(unique_id=name, deployment_id=deployment_id),
            node_id=node,
            availability_zone=zone,
            multiplexed_model_ids=list(models),
            max_ongoing_requests=1,
            routing_stats={"throughput": throughput},
        )

    async def check(replicas, expected, *, model_id="", full=()):
        router = ThroughputAwareRequestRouter(
            deployment_id=deployment_id,
            handle_source=DeploymentHandleSource.REPLICA,
            self_node_id="local",
            self_availability_zone="zone-1",
            get_curr_time_s=lambda: 0.0,
        )
        # Populate the real mixins and queue cache without starting replica actors.
        router._replicas = {item.replica_id: item for item in replicas}
        router._replicas_list = replicas
        router._update_colocated_replica_ids_with_replicas(replicas)
        router._update_multiplexed_model_ids_with_replicas(replicas)
        for item in replicas:
            router._replica_queue_len_cache.update(
                item.replica_id, int(item.replica_id.unique_id in full)
            )
        request = PendingRequest(
            args=[],
            kwargs={},
            metadata=RequestMetadata(
                request_id="example-request",
                internal_request_id="example-request",
                multiplexed_model_id=model_id,
            ),
        )
        ranks = await router.choose_replicas(replicas, request)
        assert [[item.replica_id.unique_id for item in rank] for rank in ranks] == [
            [name] for name in expected
        ]
        if replicas and not expected:
            # Check the flag first so a regression cannot spin the generator forever.
            assert request.routing_context.should_backoff
            router._backoff = AsyncMock(side_effect=RuntimeError("backoff reached"))
            try:
                await anext(router._choose_replicas_with_backoff(request))
            except RuntimeError as error:
                assert str(error) == "backoff reached"
            else:
                raise AssertionError("Expected the full replicas to trigger backoff")
            router._backoff.assert_awaited_once_with(0)

    async def test_routing():
        local = replica("local")
        remote = replica("remote", node="remote", zone="zone-2")
        await check([remote], ["remote"])
        await check([local, remote], ["remote"], full=("local",))
        await check([local, remote], [], full=("local", "remote"))
        await check([], [])

        cached = replica("cached", node="remote", models=("model-a",), throughput=10)
        await check([local, cached], ["cached", "local"], model_id="model-a")
        await check([local, cached], ["local", "cached"], model_id="new-model")

        slow = replica("slow", node="remote", throughput=10)
        fast = replica("fast", node="remote", throughput=1)
        await check([slow, fast], ["fast", "slow"])

    asyncio.run(test_routing())
