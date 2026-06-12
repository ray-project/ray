import sys

import pytest
from vllm.distributed.kv_events import ZmqEventPublisher

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    assign_replica_kv_events_endpoint,
    configure_kv_events_for_kv_routing,
    resolve_kv_event_source_endpoint,
)
from ray.serve.llm.request_router import KVAwareRouter


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
        """Replicas must hash identical content identically: the router's
        global indexer chains blocks by the engines' block hashes."""
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
        """Colocated replicas must bind distinct KV-events ports."""
        replica_rank(2)
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)
        assign_replica_kv_events_endpoint(llm_config)

        assert llm_config.engine_kwargs["kv_events_config"]["endpoint"] == (
            "tcp://*:5559"
        )
        assert resolve_kv_event_source_endpoint(llm_config) == "tcp://127.0.0.1:5559"

    def test_data_parallel_rank_is_offset_by_vllm(self, replica_rank):
        """With data_parallel_rank, vLLM offsets the bind port internally, so
        the configured endpoint stays at the base and only the subscriber
        endpoint is offset."""
        replica_rank(5)
        llm_config = make_kv_aware_llm_config(
            engine_kwargs={"data_parallel_rank": 3},
        )
        configure_kv_events_for_kv_routing(llm_config)
        assign_replica_kv_events_endpoint(llm_config)

        endpoint = llm_config.engine_kwargs["kv_events_config"]["endpoint"]
        assert endpoint == "tcp://*:5557"
        assert resolve_kv_event_source_endpoint(llm_config) == "tcp://127.0.0.1:5560"
        offset_by_vllm = ZmqEventPublisher.offset_endpoint_port(endpoint, 3)
        assert offset_by_vllm == "tcp://*:5560"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
