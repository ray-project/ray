import sys
from types import SimpleNamespace
from unittest import mock

import pytest

import ray
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    assign_replica_kv_events_endpoint,
    configure_kv_events_for_kv_routing,
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
        with mock.patch(
            "ray.serve.get_replica_context", return_value=replica_context
        ):
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
