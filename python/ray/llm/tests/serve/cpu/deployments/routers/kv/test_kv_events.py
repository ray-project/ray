import sys
from types import SimpleNamespace
from unittest import mock

import pytest

import ray
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.utils import (
    _maybe_setup_kv_aware_routing,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    assign_replica_kv_events_endpoint,
    configure_kv_events_for_kv_routing,
    enable_native_kv_offload_events,
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
    def test_configure_enables_events_and_pins_runtime_env(self):
        """KV-aware config enables events and required vLLM process settings."""
        llm_config = make_kv_aware_llm_config(
            runtime_env={
                "env_vars": {
                    "EXISTING_ENV": "value",
                    "VLLM_USE_SIMPLE_KV_OFFLOAD": "1",
                }
            }
        )
        configure_kv_events_for_kv_routing(llm_config)

        assert llm_config.engine_kwargs["kv_events_config"] == {
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": "tcp://*:5557",
            "replay_endpoint": "tcp://*:6557",
        }
        assert llm_config.runtime_env["env_vars"]["PYTHONHASHSEED"] == "0"
        assert llm_config.runtime_env["env_vars"]["VLLM_USE_SIMPLE_KV_OFFLOAD"] == "0"
        assert llm_config.runtime_env["env_vars"]["EXISTING_ENV"] == "value"

    def test_non_kv_aware_router_preserves_kv_offload_config(self):
        """Non-KV-aware routers retain the user's KV offload configuration."""
        llm_config = make_kv_aware_llm_config(
            runtime_env={
                "env_vars": {
                    "EXISTING_ENV": "value",
                    "VLLM_USE_SIMPLE_KV_OFFLOAD": "1",
                }
            },
            engine_kwargs={
                "kv_offloading_size": 2.0,
                "kv_offloading_backend": "native",
            },
        )
        llm_config.deployment_config["request_router_config"][
            "request_router_class"
        ] = "ray.serve.experimental.consistent_hash_router:ConsistentHashRouter"

        _maybe_setup_kv_aware_routing(llm_config.deployment_config, llm_config)

        assert "kv_events_config" not in llm_config.engine_kwargs
        assert llm_config.engine_kwargs["kv_offloading_size"] == 2.0
        assert llm_config.engine_kwargs["kv_offloading_backend"] == "native"
        assert llm_config.runtime_env["env_vars"]["VLLM_USE_SIMPLE_KV_OFFLOAD"] == "1"

    @pytest.mark.parametrize(
        "offload_size, expected",
        [
            (2.0, {"existing": "value", "self_describing_kv_events": True}),
            (None, {"existing": "value"}),
        ],
    )
    def test_native_offload_event_configuration(self, offload_size, expected):
        """Only native CPU offload enables complete CPU-tier KV events."""
        extra_config = {"existing": "value"}
        vllm_config = SimpleNamespace(
            cache_config=SimpleNamespace(
                kv_offloading_size=offload_size,
                kv_offloading_backend="native",
            ),
            kv_transfer_config=SimpleNamespace(kv_connector_extra_config=extra_config),
        )

        enable_native_kv_offload_events(vllm_config)

        assert extra_config == expected

    @pytest.mark.parametrize(
        "engine_kwargs, local_rank, expected_port, expected_replay_port",
        [
            # Non-DP: offset the base port by the replica's node-local rank so
            # colocated replicas don't bind the same ZMQ PUB port.
            ({}, 2, 5559, 6559),
            # DP: data_parallel_rank set -> offset 0 (the engine offsets the
            # bound port by dp_rank itself), so local_rank must be ignored.
            ({"data_parallel_rank": 2}, 2, 5557, 6557),
        ],
    )
    def test_assign_replica_endpoint_offsets_port(
        self, engine_kwargs, local_rank, expected_port, expected_replay_port
    ):
        """Per-replica endpoint offset: by node-local rank without DP, 0 with DP."""
        llm_config = make_kv_aware_llm_config(engine_kwargs=dict(engine_kwargs))
        configure_kv_events_for_kv_routing(llm_config)  # base ports 5557 / 6557
        replica_context = SimpleNamespace(rank=SimpleNamespace(local_rank=local_rank))
        with mock.patch("ray.serve.get_replica_context", return_value=replica_context):
            assign_replica_kv_events_endpoint(llm_config)
        kv_events_config = llm_config.engine_kwargs["kv_events_config"]
        assert kv_events_config["endpoint"] == f"tcp://*:{expected_port}"
        assert kv_events_config["replay_endpoint"] == f"tcp://*:{expected_replay_port}"

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

        stats = get_kv_event_routing_stats(
            llm_config, block_size=16, max_num_batched_tokens=4096
        )
        node_ip = ray.util.get_node_ip_address()
        assert stats == {
            "kv_event_metadata": {
                "endpoint": f"tcp://{node_ip}:5557",
                "block_size": 16,
                "max_num_batched_tokens": 4096,
                "dp_rank": 0,
                "replay_endpoint": f"tcp://{node_ip}:6557",
            }
        }

    def test_routing_stats_empty_without_kv_events(self):
        """Nothing to advertise when KV-cache events are not enabled."""
        llm_config = make_kv_aware_llm_config()
        assert (
            get_kv_event_routing_stats(
                llm_config, block_size=16, max_num_batched_tokens=4096
            )
            == {}
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
