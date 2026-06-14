import sys
import uuid

import pytest
from dynamo.llm import compute_block_hash_for_seq
from vllm.distributed.kv_events import (
    AllBlocksCleared,
    BlockRemoved,
    BlockStored,
    KVEventBatch,
    ZmqEventPublisher,
)

import ray
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.builder import (
    _maybe_setup_kv_aware_routing,
    build_llm_deployment,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KVRouterActor,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_event_plane import (
    derive_kv_event_block_size,
    kv_event_namespace,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_event_publisher import (
    KvEventPublisher,
    maybe_start_kv_event_publisher,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    assign_replica_kv_events_endpoint,
    configure_kv_events_for_kv_routing,
    resolve_kv_event_source_endpoint,
)
from ray.serve._private.common import (
    DeploymentID,
    DeploymentTargetInfo,
    ReplicaID,
    RunningReplicaInfo,
)
from ray.serve.config import RequestRouterConfig
from ray.serve.llm.request_router import KVAwareRouter

BLOCK_SIZE = 16


def make_llm_config(**kwargs) -> LLMConfig:
    return LLMConfig(
        model_loading_config={
            "model_id": "qwen-0.5b",
            "model_source": "Qwen/Qwen2.5-0.5B-Instruct",
        },
        accelerator_type=None,
        **kwargs,
    )


def make_kv_aware_llm_config(**kwargs) -> LLMConfig:
    return make_llm_config(
        deployment_config={
            "autoscaling_config": {"min_replicas": 1, "max_replicas": 1},
            "request_router_config": {"request_router_class": KVAwareRouter},
        },
        **kwargs,
    )


class TestConfigureKvEvents:
    def test_build_enables_kv_events(self):
        """Building a KVAwareRouter deployment enables engine KV events."""
        llm_config = make_kv_aware_llm_config()
        build_llm_deployment(llm_config)

        assert llm_config.engine_kwargs["kv_events_config"] == {
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": "tcp://*:5557",
        }

    def test_build_attaches_router_actor_with_block_size(self):
        """The actor's init_kwargs carry the build-time derived block size."""
        llm_config = make_kv_aware_llm_config(engine_kwargs={"block_size": 32})
        deployment_options = {
            "request_router_config": RequestRouterConfig(
                request_router_class=KVAwareRouter
            )
        }
        _maybe_setup_kv_aware_routing(deployment_options, llm_config)

        (actor_config,) = deployment_options["deployment_actors"]
        assert actor_config.init_kwargs == {"block_size": 32}

    def test_build_without_kv_aware_router_is_untouched(self):
        llm_config = make_llm_config(
            deployment_config={
                "autoscaling_config": {"min_replicas": 1, "max_replicas": 1}
            },
        )
        build_llm_deployment(llm_config)

        assert "kv_events_config" not in llm_config.engine_kwargs

    def test_port_base_override(self):
        llm_config = make_kv_aware_llm_config(
            experimental_configs={"KV_EVENTS_PORT_BASE": 21000},
        )
        configure_kv_events_for_kv_routing(llm_config)

        assert llm_config.engine_kwargs["kv_events_config"]["endpoint"] == (
            "tcp://*:21000"
        )

    def test_block_hash_seed_pinned(self):
        """Engine block hashes must be content-deterministic across replicas."""
        llm_config = make_kv_aware_llm_config()
        build_llm_deployment(llm_config)

        assert llm_config.runtime_env["env_vars"]["PYTHONHASHSEED"] == "0"

        user_config = make_kv_aware_llm_config(
            runtime_env={"env_vars": {"PYTHONHASHSEED": "7"}},
        )
        configure_kv_events_for_kv_routing(user_config)

        assert user_config.runtime_env["env_vars"]["PYTHONHASHSEED"] == "7"


class TestReplicaEndpoints:
    @pytest.fixture
    def replica_rank(self, monkeypatch):
        def set_rank(rank):
            monkeypatch.setattr(
                "ray.llm._internal.serve.routing_policies.kv_aware.kv_events."
                "_replica_rank",
                lambda: rank,
            )

        return set_rank

    def test_no_kv_events_is_noop(self):
        llm_config = make_llm_config()
        assign_replica_kv_events_endpoint(llm_config)

        assert "kv_events_config" not in llm_config.engine_kwargs
        assert resolve_kv_event_source_endpoint(llm_config) is None

    def test_replica_rank_offsets_port(self, replica_rank):
        """Colocated replicas must bind distinct KV-events ports; the
        publisher consumes the rank-offset engine endpoint."""
        replica_rank(2)
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)
        assign_replica_kv_events_endpoint(llm_config)

        assert llm_config.engine_kwargs["kv_events_config"]["endpoint"] == (
            "tcp://*:5559"
        )
        assert resolve_kv_event_source_endpoint(llm_config) == "tcp://127.0.0.1:5559"

    def test_data_parallel_rank_is_offset_by_vllm(self, replica_rank):
        """vLLM offsets the bind port by dp rank itself, so the configured
        endpoint stays at the base."""
        replica_rank(5)
        llm_config = make_kv_aware_llm_config(
            engine_kwargs={"data_parallel_rank": 3},
        )
        configure_kv_events_for_kv_routing(llm_config)
        assign_replica_kv_events_endpoint(llm_config)

        endpoint = llm_config.engine_kwargs["kv_events_config"]["endpoint"]
        assert endpoint == "tcp://*:5557"
        offset_by_vllm = ZmqEventPublisher.offset_endpoint_port(endpoint, 3)
        assert offset_by_vllm == "tcp://*:5560"
        # The publisher consumes the dp-offset engine endpoint.
        assert resolve_kv_event_source_endpoint(llm_config) == "tcp://127.0.0.1:5560"


class TestKvEventPlaneConfig:
    def test_namespace_is_deployment_scoped_and_sanitized(self):
        deployment_id = DeploymentID(name="LLMServer:qwen-0.5b", app_name="my.app")
        assert kv_event_namespace(deployment_id) == "ray_llm_my_app_LLMServer_qwen-0_5b"

    def test_derive_kv_event_block_size_at_build_time(self):
        """vLLM's own config resolution, including its default."""
        assert derive_kv_event_block_size({}) == 16
        assert derive_kv_event_block_size({"block_size": 32}) == 32

    @pytest.mark.asyncio
    async def test_no_publisher_without_kv_events(self):
        assert await maybe_start_kv_event_publisher(make_llm_config(), 16) is None


@pytest.fixture(scope="module")
def ray_instance():
    if not ray.is_initialized():
        ray.init(address="auto")
    yield


@ray.remote(num_cpus=0)
class LocalKVRouterActor(KVRouterActor.__ray_actor_class__):
    """The real KVRouterActor with a fixed Dynamo namespace and replica
    tracking disabled (no Serve controller in these tests)."""

    def __init__(self, namespace: str, block_size: int = BLOCK_SIZE):
        self._namespace = namespace
        super().__init__(block_size=block_size)

    def _start_replica_tracking(self) -> None:
        pass

    def _kv_event_plane_namespace(self) -> str:
        return self._namespace

    def apply_running_replicas(self, replica_full_ids) -> None:
        """Feed a replica-membership snapshot as the LongPoll listener would."""
        self._on_deployment_targets(
            DeploymentTargetInfo(
                is_available=True,
                running_replicas=[
                    RunningReplicaInfo(
                        replica_id=ReplicaID.from_full_id_str(full_id),
                        node_id=None,
                        node_ip=None,
                        availability_zone=None,
                        actor_name=f"actor-{full_id}",
                        max_ongoing_requests=10,
                    )
                    for full_id in replica_full_ids
                ],
            )
        )


@ray.remote(num_cpus=0)
class ReplicaStandIn:
    """A replica stand-in: vLLM's production ZmqEventPublisher as the engine
    and the real KvEventPublisher bridging it to the event plane."""

    def __init__(self, kv_router_actor, replica_id, worker_id, namespace, port):
        self._engine_pub = ZmqEventPublisher(
            data_parallel_rank=0, endpoint=f"tcp://*:{port}", topic=""
        )
        self._publisher = KvEventPublisher(
            kv_router_actor=kv_router_actor,
            replica_id=replica_id,
            worker_id=worker_id,
            namespace=namespace,
            zmq_endpoint=f"tcp://127.0.0.1:{port}",
            kv_block_size=BLOCK_SIZE,
        )

    async def start(self) -> int:
        await self._publisher.start()
        return self._publisher.worker_id

    def publish_stored(self, block_hashes, token_ids):
        self._engine_pub.publish(
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

    def publish_removed(self, block_hashes):
        self._engine_pub.publish(
            KVEventBatch(
                ts=2.0,
                events=[BlockRemoved(block_hashes=list(block_hashes), medium="GPU")],
            )
        )

    def publish_cleared(self):
        self._engine_pub.publish(KVEventBatch(ts=3.0, events=[AllBlocksCleared()]))

    def close(self):
        self._publisher.close()
        self._engine_pub.shutdown()


def stored_block_hashes(indexer_events, worker_id):
    """Engine block hashes currently stored for a worker in the indexer dump."""
    hashes = set()
    for entry in indexer_events:
        if entry["worker_id"] != worker_id:
            continue
        for block in entry["event"]["data"]["stored"]["blocks"]:
            hashes.add(block["block_hash"])
    return hashes


def stored_tokens_hashes(indexer_events, worker_id):
    """Dynamo per-block token hashes stored for a worker in the indexer dump."""
    return {
        block["tokens_hash"]
        for entry in indexer_events
        if entry["worker_id"] == worker_id
        for block in entry["event"]["data"]["stored"]["blocks"]
    }


async def wait_for_indexer(actor, predicate, publish=None, timeout=20):
    """Wait until the actor's indexer dump satisfies ``predicate``.

    ``publish`` re-publishes the step's event each retry and must be
    idempotent: delivery is asynchronous and events published before the
    subscription is live can drop.
    """

    async def condition():
        if publish is not None:
            publish()
        return predicate(await actor.get_kv_indexer_events.remote())

    await async_wait_for_condition(condition, timeout=timeout, retry_interval_ms=500)


class TestDynamoKvEventPipeline:
    """End-to-end over Dynamo primitives: vLLM's production ZMQ publisher ->
    dynamo KvEventPublisher -> the KVRouterActor's ZMQ broker ->
    KvRouter's KvEventConsumer."""

    @pytest.fixture
    def namespace(self):
        return f"test_kv_events_{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_eager_router_and_block_size_validation(
        self, ray_instance, namespace
    ):
        """The router exists from construction and rejects mismatched block
        sizes."""
        actor = LocalKVRouterActor.remote(namespace)
        try:
            # Queryable before any worker registers: created eagerly.
            assert await actor.get_kv_indexer_events.remote() == []
            with pytest.raises(ValueError, match="block size"):
                await actor.register_kv_event_worker.remote(
                    7001, "replica-A", 2 * BLOCK_SIZE
                )
            assert await actor.get_kv_event_worker_replicas.remote() == {}
        finally:
            ray.kill(actor, no_restart=True)

    @pytest.mark.asyncio
    async def test_stored_and_removed_reach_router_indexer(
        self, ray_instance, namespace
    ):
        worker_id = 7001
        actor = LocalKVRouterActor.remote(namespace)
        replica = ReplicaStandIn.remote(actor, "replica-A", worker_id, namespace, 23817)
        try:
            # Events are keyed by the Ray-supplied worker id.
            await replica.start.remote()
            assert await actor.get_kv_event_worker_replicas.remote() == {
                worker_id: "replica-A"
            }

            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_indexer(
                actor,
                lambda events: stored_block_hashes(events, worker_id) == {11, 22},
                publish=lambda: replica.publish_stored.remote([11, 22], token_ids),
            )
            assert await actor.get_kv_event_worker_ids.remote() == [worker_id]

            # Per-block content hashes match hashing the engine's tokens.
            events = await actor.get_kv_indexer_events.remote()
            assert stored_tokens_hashes(events, worker_id) == set(
                compute_block_hash_for_seq(token_ids, BLOCK_SIZE)
            )

            # Removing the leaf block drops only it from the chain.
            await wait_for_indexer(
                actor,
                lambda events: stored_block_hashes(events, worker_id) == {11},
                publish=lambda: replica.publish_removed.remote([22]),
            )

            # AllBlocksCleared (e.g. /reset_prefix_cache) empties the view.
            await wait_for_indexer(
                actor,
                lambda events: stored_block_hashes(events, worker_id) == set(),
                publish=lambda: replica.publish_cleared.remote(),
            )
        finally:
            await replica.close.remote()
            for a in (replica, actor):
                ray.kill(a, no_restart=True)

    @pytest.mark.asyncio
    async def test_per_worker_isolation(self, ray_instance, namespace):
        """Two replicas' events land in the same indexer keyed by worker."""
        actor = LocalKVRouterActor.remote(namespace)
        worker_ids = {"replica-A": 7001, "replica-B": 7002}
        replicas = {
            replica_id: ReplicaStandIn.remote(
                actor, replica_id, worker_id, namespace, 21813 + i
            )
            for i, (replica_id, worker_id) in enumerate(worker_ids.items())
        }
        try:
            for replica in replicas.values():
                await replica.start.remote()
            assert await actor.get_kv_event_worker_replicas.remote() == {
                worker_id: replica_id for replica_id, worker_id in worker_ids.items()
            }

            blocks = {"replica-A": 100, "replica-B": 200}
            tokens = {
                "replica-A": list(range(BLOCK_SIZE)),
                "replica-B": list(range(BLOCK_SIZE, 2 * BLOCK_SIZE)),
            }
            for replica_id, replica in replicas.items():

                def consumed(events, worker_id=worker_ids[replica_id]):
                    return stored_block_hashes(events, worker_id) == {
                        blocks[replica_id]
                    }

                await wait_for_indexer(
                    actor,
                    consumed,
                    publish=lambda replica=replica, replica_id=replica_id: (
                        replica.publish_stored.remote(
                            [blocks[replica_id]], tokens[replica_id]
                        )
                    ),
                )
            assert await actor.get_kv_event_worker_ids.remote() == sorted(
                worker_ids.values()
            )

            # Each worker overlaps only the tokens its replica cached.
            for replica_id, worker_id in worker_ids.items():
                overlaps = await actor.get_kv_overlap_blocks.remote(tokens[replica_id])
                assert overlaps[worker_id] == 1
                other = (worker_ids.keys() - {replica_id}).pop()
                assert overlaps.get(worker_ids[other], 0) == 0
        finally:
            for replica in replicas.values():
                await replica.close.remote()
                ray.kill(replica, no_restart=True)
            ray.kill(actor, no_restart=True)

    @pytest.mark.asyncio
    async def test_worker_registration_purged_with_replica(
        self, ray_instance, namespace
    ):
        """Removal of a tracked replica purges its KV-event registration; each
        registration adds the worker directly to the KvRouter."""
        actor = LocalKVRouterActor.remote(namespace)
        replicas = {
            get_worker_id(f"u{i}"): ReplicaID(
                unique_id=f"u{i}", deployment_id=DeploymentID("d", "a")
            ).to_full_id_str()
            for i in range(2)
        }
        (keep_worker, keep_replica), (drop_worker, drop_replica) = replicas.items()
        try:
            # Registration adds the worker directly to the KvRouter (no discovery
            # records) before the controller reports the replica running.
            for worker_id, replica_id in replicas.items():
                await actor.register_kv_event_worker.remote(
                    worker_id, replica_id, BLOCK_SIZE
                )
            await actor.apply_running_replicas.remote(list(replicas.values()))
            assert await actor.get_kv_event_worker_replicas.remote() == replicas

            await actor.apply_running_replicas.remote([keep_replica])
            assert await actor.get_kv_event_worker_replicas.remote() == {
                keep_worker: keep_replica
            }
        finally:
            ray.kill(actor, no_restart=True)


class TestSelectWorker:
    """KV-aware scoring: the KVRouterActor's query-only select_worker ranks a
    caller-provided candidate set by KV overlap (Dynamo KvRouter.select_worker)."""

    @pytest.fixture
    def namespace(self):
        return f"test_kv_scoring_{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_select_worker_returns_allowed_candidate(
        self, ray_instance, namespace
    ):
        """select_worker returns a worker from the allowed candidate set."""
        actor = LocalKVRouterActor.remote(namespace)
        a = ReplicaStandIn.remote(actor, "replica-A", 7001, namespace, 22101)
        b = ReplicaStandIn.remote(actor, "replica-B", 7002, namespace, 22102)
        try:
            worker_a = await a.start.remote()
            worker_b = await b.start.remote()
            token_ids = list(range(2 * BLOCK_SIZE))
            selection = await actor.select_worker.remote(
                "req-1", token_ids, [worker_a, worker_b]
            )
            assert selection["worker_id"] in (worker_a, worker_b)
        finally:
            for replica in (a, b):
                await replica.close.remote()
                ray.kill(replica, no_restart=True)
            ray.kill(actor, no_restart=True)

    @pytest.mark.asyncio
    async def test_select_worker_prefers_overlap(self, ray_instance, namespace):
        """The candidate whose cached blocks overlap the prompt is selected."""
        actor = LocalKVRouterActor.remote(namespace)
        a = ReplicaStandIn.remote(actor, "replica-A", 7001, namespace, 22103)
        b = ReplicaStandIn.remote(actor, "replica-B", 7002, namespace, 22104)
        try:
            worker_a = await a.start.remote()
            worker_b = await b.start.remote()
            token_ids = list(range(2 * BLOCK_SIZE))
            # Worker A caches the prompt's blocks; B caches nothing.
            await wait_for_indexer(
                actor,
                lambda events: stored_block_hashes(events, worker_a) == {301, 302},
                publish=lambda: a.publish_stored.remote([301, 302], token_ids),
            )
            selection = await actor.select_worker.remote(
                "req-2", token_ids, [worker_a, worker_b]
            )
            assert selection["worker_id"] == worker_a
            assert selection["overlap_blocks"] >= 1
        finally:
            for replica in (a, b):
                await replica.close.remote()
                ray.kill(replica, no_restart=True)
            ray.kill(actor, no_restart=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
