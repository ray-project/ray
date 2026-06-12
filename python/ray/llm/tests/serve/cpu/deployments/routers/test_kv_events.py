import sys
import uuid

import pytest
from vllm.distributed.kv_events import ZmqEventPublisher

import ray
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
