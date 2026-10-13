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

        model_ranks: List[List[RunningReplica]] = [candidate_replicas]
        if (
            pending_request is not None
            and pending_request.metadata.multiplexed_model_id
        ):
            model_ranks = self.rank_replicas_via_multiplex(
                replicas=candidate_replicas,
                multiplexed_model_id=pending_request.metadata.multiplexed_model_id,
            )

        ranked_replicas: List[List[RunningReplica]] = []
        for model_rank in model_ranks:
            for locality_rank in self.rank_replicas_via_locality(model_rank):
                # Keep replicas that the cache reports as full. The cache can be
                # stale, so the base router probes them before it tries the next rank.
                by_throughput: List[RunningReplica] = sorted(
                    locality_rank,
                    key=lambda replica: replica.routing_stats.get("throughput", 0),
                )
                # Return singleton ranks so throughput determines the attempt order.
                ranked_replicas.extend([[replica] for replica in by_throughput])
        return ranked_replicas


# __end_define_throughput_aware_request_router__


if __name__ == "__main__":
    import asyncio
    from types import SimpleNamespace

    from ray.serve._private.common import (
        DeploymentHandleSource,
        DeploymentID,
        RequestMetadata,
    )

    deployment_id = DeploymentID("routing-example")

    class FakeReplica(SimpleNamespace):
        async def get_queue_len(self, *, deadline_s):
            self.num_probes += 1
            return self.queue_len

    def replica(
        name, *, node="local", zone="zone-1", models=(), throughput=0, queue_len=0
    ):
        return FakeReplica(
            replica_id=ReplicaID(unique_id=name, deployment_id=deployment_id),
            node_id=node,
            availability_zone=zone,
            multiplexed_model_ids=list(models),
            max_ongoing_requests=1,
            routing_stats={"throughput": throughput},
            queue_len=queue_len,
            num_probes=0,
        )

    def make_router(replicas):
        router = ThroughputAwareRequestRouter(
            deployment_id=deployment_id,
            handle_source=DeploymentHandleSource.REPLICA,
            self_node_id="local",
            self_availability_zone="zone-1",
            use_replica_queue_len_cache=True,
            get_curr_time_s=lambda: 0.0,
        )

        # Skip the background probe of `update_replicas` so that each test
        # controls the queue length cache.
        router._replicas = {item.replica_id: item for item in replicas}
        router._replicas_list = replicas
        router._update_colocated_replica_ids_with_replicas(replicas)
        router._update_multiplexed_model_ids_with_replicas(replicas)
        return router

    def make_request(model_id=""):
        return PendingRequest(
            args=[],
            kwargs={},
            metadata=RequestMetadata(
                request_id="example-request",
                internal_request_id="example-request",
                multiplexed_model_id=model_id,
            ),
        )

    async def check(replicas, expected, *, model_id=""):
        router = make_router(replicas)
        ranks = await router.choose_replicas(replicas, make_request(model_id))
        assert [[item.replica_id.unique_id for item in rank] for rank in ranks] == [
            [name] for name in expected
        ]

    async def route(router, *, model_id=""):
        # Fail instead of hanging when the router never selects a replica.
        return await asyncio.wait_for(
            router._choose_replica_for_request(make_request(model_id)), timeout=5
        )

    async def test_ranking():
        local = replica("local")
        remote = replica("remote", node="remote", zone="zone-2")
        await check([remote], ["remote"])
        await check([remote, local], ["local", "remote"])
        await check([], [])

        cached = replica("cached", node="remote", models=("model-a",), throughput=10)
        await check([local, cached], ["cached", "local"], model_id="model-a")
        await check([local, cached], ["local", "cached"], model_id="new-model")

        slow = replica("slow", node="remote", throughput=10)
        fast = replica("fast", node="remote", throughput=1)
        await check([slow, fast], ["fast", "slow"])

    async def test_probe_cached_full_replica():
        # The cache reports a full queue, but the replica has capacity again.
        stale = replica("stale")
        router = make_router([stale])
        router.replica_queue_len_cache.update(stale.replica_id, 1)

        assert await route(router) is stale
        assert stale.num_probes == 1

    async def test_fall_back_from_full_model_replica():
        preferred = replica("preferred", models=("model-a",), queue_len=1)
        fallback = replica("fallback")
        router = make_router([fallback, preferred])

        assert await route(router, model_id="model-a") is fallback
        assert preferred.num_probes == 1

    async def test_back_off_until_replica_frees_up():
        busy = replica("busy", queue_len=1)
        freed = replica("freed", queue_len=1)
        router = make_router([busy, freed])

        # The router backs off once a retry finds every replica full. Free one
        # replica then, so that the next retry selects it.
        backoff = router._backoff
        backoff_attempts = []

        async def free_replica_on_backoff(attempt):
            backoff_attempts.append(attempt)
            freed.queue_len = 0
            await backoff(attempt)

        router._backoff = free_replica_on_backoff

        assert await route(router) is freed
        assert backoff_attempts == [0]

    for test in (
        test_ranking,
        test_probe_cached_full_replica,
        test_fall_back_from_full_model_replica,
        test_back_off_until_replica_frees_up,
    ):
        asyncio.run(test())
