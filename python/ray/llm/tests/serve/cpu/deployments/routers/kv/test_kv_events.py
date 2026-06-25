import sys

import pytest
from vllm.config import CacheConfig

import ray
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    configure_kv_events_for_kv_routing,
    derive_kv_event_block_size,
    get_kv_event_routing_stats,
    resolve_kv_event_source_endpoint,
)
from ray.serve.llm.request_router import KVAwareRouter


def make_kv_aware_llm_config(**kwargs) -> LLMConfig:
    return LLMConfig(
        model_loading_config={
            "model_id": "qwen3-0.6b",
            "model_source": "Qwen/Qwen3-0.6B",
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
    started = not ray.is_initialized()
    if started:
        ray.init()
    yield
    if started:
        ray.shutdown()


class TestConfigureKvEvents:
    def test_configure_enables_events_and_pins_seed(self):
        """KV-aware config turns on engine ZMQ KV events and pins the hash seed."""
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)

        assert llm_config.engine_kwargs["kv_events_config"] == {
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": "tcp://*:5557",
            "replay_endpoint": "tcp://*:6557",
        }
        assert llm_config.runtime_env["env_vars"]["PYTHONHASHSEED"] == "0"

    def test_derive_block_size(self):
        """The actor's block size comes from the engine's resolved config."""
        assert derive_kv_event_block_size({"block_size": 32}) == 32
        assert derive_kv_event_block_size({}) == CacheConfig.DEFAULT_BLOCK_SIZE

    def test_resolve_endpoint_is_node_routable(self, ray_instance):
        """The advertised endpoint is the replica's node IP."""
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)

        endpoint = resolve_kv_event_source_endpoint(llm_config)
        node_ip = ray.util.get_node_ip_address()
        assert endpoint == f"tcp://{node_ip}:5557"

    def test_routing_stats_advertise_endpoint(self, ray_instance):
        """The replica advertises its node-routable endpoint plus the engine
        facts the selection service needs to schedule it via record_routing_stats."""
        llm_config = make_kv_aware_llm_config()
        configure_kv_events_for_kv_routing(llm_config)

        stats = get_kv_event_routing_stats(llm_config, max_num_batched_tokens=4096)
        node_ip = ray.util.get_node_ip_address()
        assert stats == {
            "kv_event_metadata": {
                "endpoint": f"tcp://{node_ip}:5557",
                "max_num_batched_tokens": 4096,
                "dp_rank": 0,
                "replay_endpoint": f"tcp://{node_ip}:6557",
            }
        }

    def test_routing_stats_empty_without_kv_events(self):
        """Nothing to advertise when KV-cache events are not enabled."""
        llm_config = make_kv_aware_llm_config()
        assert get_kv_event_routing_stats(llm_config, max_num_batched_tokens=4096) == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
