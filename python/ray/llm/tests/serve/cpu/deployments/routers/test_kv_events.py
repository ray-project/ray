import sys

import pytest
from vllm.distributed.kv_events import (
    BlockRemoved,
    BlockStored,
    KVEventBatch,
    ZmqEventPublisher,
)

import ray
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KVRouterActor,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    configure_kv_events_for_kv_routing,
    derive_kv_event_block_size,
    kv_event_routing_stats,
    resolve_kv_event_source_endpoint,
)
from ray.serve._private.common import (
    DeploymentID,
    DeploymentTargetInfo,
    ReplicaID,
    RunningReplicaInfo,
)
from ray.serve.llm.request_router import KVAwareRouter

BLOCK_SIZE = 16
MAX_NUM_BATCHED_TOKENS = 8192


def make_kv_aware_llm_config(**kwargs) -> LLMConfig:
    return LLMConfig(
        model_loading_config={
            "model_id": "qwen-0.5b",
            "model_source": "Qwen/Qwen2.5-0.5B-Instruct",
        },
        accelerator_type=None,
        deployment_config={
            "autoscaling_config": {"min_replicas": 1, "max_replicas": 1},
            "request_router_config": {"request_router_class": KVAwareRouter},
        },
        **kwargs,
    )


@pytest.fixture(scope="module")
def ray_instance():
    if not ray.is_initialized():
        ray.init(address="auto")
    yield


class TestConfigureKvEvents:
    def test_configure_enables_events_and_pins_seed(self):
        """KV-aware config turns on engine ZMQ KV events and pins the hash seed."""
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)

        assert llm_config.engine_kwargs["kv_events_config"] == {
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": "tcp://*:5557",
        }
        assert llm_config.runtime_env["env_vars"]["PYTHONHASHSEED"] == "0"

    def test_derive_block_size(self):
        """The actor's block size comes from the engine's resolved config."""
        from vllm.config import CacheConfig

        assert derive_kv_event_block_size({"block_size": 32}) == 32
        assert derive_kv_event_block_size({}) == CacheConfig.DEFAULT_BLOCK_SIZE

    def test_resolve_endpoint_is_node_routable(self, ray_instance):
        """The advertised endpoint is the replica's node IP (the selection
        service may dial it from another node), not loopback."""
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)

        endpoint = resolve_kv_event_source_endpoint(llm_config)
        node_ip = ray.util.get_node_ip_address()
        assert endpoint == f"tcp://{node_ip}:5557"
        assert "127.0.0.1" not in endpoint and "*" not in endpoint

    def test_routing_stats_advertise_endpoint(self, ray_instance):
        """The replica advertises its node-routable endpoint plus the engine
        facts the selection service needs to schedule it -- via record_routing_stats."""
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)

        stats = kv_event_routing_stats(llm_config, max_num_batched_tokens=4096)
        node_ip = ray.util.get_node_ip_address()
        assert stats == {
            "kv_events": {
                "endpoint": f"tcp://{node_ip}:5557",
                "max_num_batched_tokens": 4096,
                "dp_rank": 0,
            }
        }

    def test_routing_stats_empty_without_kv_events(self):
        """Nothing to advertise when KV-cache events are not enabled."""
        llm_config = make_kv_aware_llm_config()
        assert kv_event_routing_stats(llm_config, max_num_batched_tokens=4096) == {}


@ray.remote(num_cpus=0)
class LocalKVRouterActor(KVRouterActor.__ray_actor_class__):
    """The real KVRouterActor with Serve LongPoll tracking disabled (there is no
    Serve controller in these unit tests); tests drive _on_deployment_targets
    directly with synthetic replica snapshots."""

    def _start_replica_tracking(self) -> None:
        pass


@ray.remote(num_cpus=0)
class ReplicaStandIn:
    """A worker stand-in: vLLM's production ZmqEventPublisher binding a
    node-routable KV-events PUB, exactly as a real replica's engine does."""

    def __init__(self, port: int):
        self._port = port
        self._pub = ZmqEventPublisher(
            data_parallel_rank=0, endpoint=f"tcp://*:{port}", topic=""
        )

    def endpoint(self) -> str:
        return f"tcp://{ray.util.get_node_ip_address()}:{self._port}"

    def publish_stored(self, block_hashes, token_ids) -> None:
        self._pub.publish(
            KVEventBatch(
                ts=1.0,
                events=[
                    BlockStored(
                        block_hashes=list(block_hashes),
                        parent_block_hash=None,
                        token_ids=list(token_ids),
                        block_size=BLOCK_SIZE,
                        lora_id=None,
                        medium="GPU",
                        lora_name=None,
                    )
                ],
            )
        )

    def publish_removed(self, block_hashes) -> None:
        self._pub.publish(
            KVEventBatch(
                ts=2.0,
                events=[BlockRemoved(block_hashes=list(block_hashes), medium="GPU")],
            )
        )

    def close(self) -> None:
        self._pub.shutdown()


def running_replica(unique_id: str, endpoint: str, dp_rank: int = 0):
    """A RunningReplicaInfo whose routing_stats advertise a KV-events endpoint,
    exactly as a real replica's record_routing_stats would surface it."""
    return RunningReplicaInfo(
        replica_id=ReplicaID(
            unique_id=unique_id,
            deployment_id=DeploymentID(name="llm", app_name="app"),
        ),
        node_id="node-1",
        node_ip="127.0.0.1",
        availability_zone=None,
        actor_name=f"actor-{unique_id}",
        max_ongoing_requests=10,
        routing_stats={
            "kv_events": {
                "endpoint": endpoint,
                "max_num_batched_tokens": MAX_NUM_BATCHED_TOKENS,
                "dp_rank": dp_rank,
            }
        },
    )


def targets(*replicas):
    return DeploymentTargetInfo(is_available=True, running_replicas=list(replicas))


async def wait_registered(actor, worker_ids):
    await async_wait_for_condition(
        lambda: ray.get(actor.get_registered_worker_ids.remote()) == sorted(worker_ids),
        timeout=15,
    )


async def wait_for_overlap(actor, token_ids, predicate, publish=None, timeout=30):
    """Poll the actor's overlap view until ``predicate`` holds.

    ``publish`` re-publishes each retry and must be idempotent: ZMQ PUB/SUB is a
    slow joiner, so events sent before the selection service's listener connects
    are dropped; overlap is also unavailable until the worker is schedulable.
    """

    async def condition():
        if publish is not None:
            publish()
        try:
            overlap = await actor.get_kv_overlap_blocks.remote(list(token_ids))
        except Exception:
            return False
        return predicate(overlap)

    await async_wait_for_condition(condition, timeout=timeout, retry_interval_ms=500)


class TestSelectionServiceEventFlow:
    """End-to-end over the connect-out event plane: a replica advertises its
    vLLM ZMQ PUB endpoint on the LongPoll snapshot -> the actor registers it ->
    the selection service's connect-out listener -> its KV indexer, with no
    broker and no in-replica Dynamo bridge."""

    @pytest.mark.asyncio
    async def test_register_then_ingest_events(self, ray_instance):
        """A replica appearing on the snapshot with a KV-events endpoint makes the
        selection service dial it and index the KV events it publishes."""
        actor = LocalKVRouterActor.remote(block_size=BLOCK_SIZE)
        replica = ReplicaStandIn.remote(23901)
        try:
            worker_id = get_worker_id("replica-A")
            endpoint = await replica.endpoint.remote()
            # The LongPoll snapshot now carries this replica's KV-events endpoint.
            await actor._on_deployment_targets.remote(
                targets(running_replica("replica-A", endpoint))
            )
            await wait_registered(actor, [worker_id])

            # Two full blocks of prompt; once ingested both overlap the query.
            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                actor,
                token_ids,
                lambda overlap: overlap.get(worker_id) == 2,
                publish=lambda: replica.publish_stored.remote([101, 102], token_ids),
            )

            # Removing the leaf block drops it from the worker's overlap.
            await wait_for_overlap(
                actor,
                token_ids,
                lambda overlap: overlap.get(worker_id) == 1,
                publish=lambda: replica.publish_removed.remote([102]),
            )
        finally:
            await replica.close.remote()
            for a in (replica, actor):
                ray.kill(a, no_restart=True)

    @pytest.mark.asyncio
    async def test_per_worker_isolation(self, ray_instance):
        """Each worker's overlap reflects only the blocks its own replica cached."""
        actor = LocalKVRouterActor.remote(block_size=BLOCK_SIZE)
        a = ReplicaStandIn.remote(23902)
        b = ReplicaStandIn.remote(23903)
        workers = {
            "replica-A": get_worker_id("replica-A"),
            "replica-B": get_worker_id("replica-B"),
        }
        try:
            # Both replicas advertise their endpoints in a single snapshot.
            await actor._on_deployment_targets.remote(
                targets(
                    running_replica("replica-A", await a.endpoint.remote()),
                    running_replica("replica-B", await b.endpoint.remote()),
                )
            )
            await wait_registered(actor, list(workers.values()))

            tokens_a = list(range(BLOCK_SIZE))
            tokens_b = list(range(BLOCK_SIZE, 2 * BLOCK_SIZE))

            await wait_for_overlap(
                actor,
                tokens_a,
                lambda overlap: overlap.get(workers["replica-A"]) == 1,
                publish=lambda: a.publish_stored.remote([201], tokens_a),
            )
            await wait_for_overlap(
                actor,
                tokens_b,
                lambda overlap: overlap.get(workers["replica-B"]) == 1,
                publish=lambda: b.publish_stored.remote([301], tokens_b),
            )
            # A's content does not overlap B's worker and vice versa.
            overlap_a = await actor.get_kv_overlap_blocks.remote(tokens_a)
            assert overlap_a.get(workers["replica-B"], 0) == 0
        finally:
            for standin in (a, b):
                await standin.close.remote()
            for handle in (a, b, actor):
                ray.kill(handle, no_restart=True)

    @pytest.mark.asyncio
    async def test_departed_replica_is_evicted(self, ray_instance):
        """A replica dropping off the snapshot tears down its listener and drops
        it from the selection service catalog."""
        actor = LocalKVRouterActor.remote(block_size=BLOCK_SIZE)
        replica = ReplicaStandIn.remote(23904)
        worker_id = get_worker_id("replica-A")
        try:
            endpoint = await replica.endpoint.remote()
            await actor._on_deployment_targets.remote(
                targets(running_replica("replica-A", endpoint))
            )
            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                actor,
                token_ids,
                lambda overlap: overlap.get(worker_id) == 2,
                publish=lambda: replica.publish_stored.remote([401, 402], token_ids),
            )

            # The replica departs: an empty snapshot evicts its worker.
            await actor._on_deployment_targets.remote(targets())
            await wait_registered(actor, [])
            assert await actor.get_candidate_worker_ids.remote() == []
        finally:
            await replica.close.remote()
            for a in (replica, actor):
                ray.kill(a, no_restart=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
