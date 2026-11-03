import copy
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
    get_prompt_token_routing_stats,
    get_token_channel_endpoints,
    resolve_kv_event_block_size,
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


def _tiering_kwargs(
    secondary_tiers,
    spec_name="TieringOffloadingSpec",
    kv_connector="OffloadingConnector",
):
    extra_config = {"secondary_tiers": secondary_tiers}
    if spec_name is not None:
        extra_config["spec_name"] = spec_name
    return {
        "kv_transfer_config": {
            "kv_connector": kv_connector,
            "kv_connector_extra_config": extra_config,
        }
    }


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
        "kv_connector, expected",
        [
            # Native offloading, whether enabled through kv_offloading_size or
            # configured directly in kv_transfer_config.
            (
                "OffloadingConnector",
                {"existing": "value", "self_describing_kv_events": True},
            ),
            ("LMCacheConnectorV1", {"existing": "value"}),
        ],
    )
    def test_native_offload_event_configuration(self, kv_connector, expected):
        """Only OffloadingConnector enables complete offload-tier KV events."""
        extra_config = {"existing": "value"}
        vllm_config = SimpleNamespace(
            kv_transfer_config=SimpleNamespace(
                kv_connector=kv_connector, kv_connector_extra_config=extra_config
            ),
        )

        enable_native_kv_offload_events(vllm_config)

        assert extra_config == expected

    def test_native_offload_events_without_kv_transfer(self):
        """No KV connector means no offload tier to describe."""
        enable_native_kv_offload_events(SimpleNamespace(kv_transfer_config=None))

    def test_storage_tier_events_default_on_for_fs(self):
        """KV routing turns on STORAGE events for filesystem tiers that don't set
        enable_kv_events, keeps explicit values, and leaves object-store tiers."""
        tiers = [
            {"type": "fs", "root_dir": "/a"},
            {"type": "fs", "root_dir": "/b", "enable_kv_events": False},
            {"type": "obj", "bucket": "c"},
        ]
        llm_config = make_kv_aware_llm_config(
            engine_kwargs={
                "kv_transfer_config": {
                    "kv_connector": "OffloadingConnector",
                    "kv_connector_extra_config": {
                        "spec_name": "TieringOffloadingSpec",
                        "secondary_tiers": tiers,
                    },
                }
            }
        )
        configure_kv_events_for_kv_routing(llm_config)

        extra_config = llm_config.engine_kwargs["kv_transfer_config"][
            "kv_connector_extra_config"
        ]
        assert extra_config["spec_name"] == "TieringOffloadingSpec"
        assert extra_config["secondary_tiers"] == [
            {"type": "fs", "root_dir": "/a", "enable_kv_events": True},
            {"type": "fs", "root_dir": "/b", "enable_kv_events": False},
            {"type": "obj", "bucket": "c"},
        ]
        # The caller's tier dicts are not mutated.
        assert "enable_kv_events" not in tiers[0]

    def test_storage_tier_defaults_skip_non_kv_aware(self):
        """Without KVAwareRouter, filesystem tiers keep the user's settings."""
        tiers = [{"type": "fs", "root_dir": "/a"}]
        llm_config = make_kv_aware_llm_config(
            engine_kwargs={
                "kv_transfer_config": {
                    "kv_connector": "OffloadingConnector",
                    "kv_connector_extra_config": {
                        "spec_name": "TieringOffloadingSpec",
                        "secondary_tiers": tiers,
                    },
                }
            }
        )
        llm_config.deployment_config["request_router_config"][
            "request_router_class"
        ] = "ray.serve.experimental.consistent_hash_router:ConsistentHashRouter"

        _maybe_setup_kv_aware_routing(llm_config.deployment_config, llm_config)

        assert llm_config.engine_kwargs["kv_transfer_config"][
            "kv_connector_extra_config"
        ]["secondary_tiers"] == [{"type": "fs", "root_dir": "/a"}]

    def test_storage_tier_defaults_ignore_malformed_tiers(self):
        """Non-dict tier entries are left for vLLM's validation to report."""
        llm_config = make_kv_aware_llm_config(
            engine_kwargs={
                "kv_transfer_config": {
                    "kv_connector": "OffloadingConnector",
                    "kv_connector_extra_config": {
                        "spec_name": "TieringOffloadingSpec",
                        "secondary_tiers": ["fs", {"type": "fs", "root_dir": "/a"}],
                    },
                }
            }
        )
        configure_kv_events_for_kv_routing(llm_config)
        assert llm_config.engine_kwargs["kv_transfer_config"][
            "kv_connector_extra_config"
        ]["secondary_tiers"] == [
            "fs",
            {"type": "fs", "root_dir": "/a", "enable_kv_events": True},
        ]

    @pytest.mark.parametrize(
        "engine_kwargs",
        [
            _tiering_kwargs([{"type": "fs"}], spec_name=None),
            _tiering_kwargs([{"type": "fs"}], kv_connector="LMCacheConnectorV1"),
            _tiering_kwargs({"type": "fs"}),
            {
                "kv_transfer_config": {
                    "kv_connector": "OffloadingConnector",
                    "kv_connector_extra_config": "{}",
                }
            },
        ],
    )
    def test_storage_tier_defaults_skip_non_tiering_configs(self, engine_kwargs):
        """Tiers are only defaulted for TieringOffloadingSpec; other connectors,
        specs, and malformed configs are left as given."""
        llm_config = make_kv_aware_llm_config(
            engine_kwargs=copy.deepcopy(engine_kwargs)
        )
        configure_kv_events_for_kv_routing(llm_config)
        assert (
            llm_config.engine_kwargs["kv_transfer_config"]
            == engine_kwargs["kv_transfer_config"]
        )

    @pytest.mark.parametrize(
        "engine_kwargs, local_rank, expected_port, expected_replay_port",
        [
            # Non-DP: offset the base port by the replica's node-local rank so
            # colocated replicas don't bind the same ZMQ PUB port.
            ({}, 2, 5559, 6559),
            # DP: equal local and DP ranks need no pre-offset; the engine
            # adds dp_rank to reach base + local_rank.
            ({"data_parallel_rank": 2}, 2, 5557, 6557),
        ],
    )
    def test_assign_replica_endpoint_offsets_port(
        self, engine_kwargs, local_rank, expected_port, expected_replay_port
    ):
        """Pre-offset by node-local rank minus the engine's own DP offset."""
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

    @pytest.mark.parametrize(
        "block_size,parallel_config,expected",
        [
            (16, SimpleNamespace(decode_context_parallel_size=1), 16),
            (64, SimpleNamespace(decode_context_parallel_size=8), 512),
            (16, SimpleNamespace(decode_context_parallel_size=2), 32),
            # PCP shards KV only through DCP (DCP spans the PCP ranks), so
            # prefill context parallelism alone doesn't scale the block span.
            (
                64,
                SimpleNamespace(
                    decode_context_parallel_size=2, prefill_context_parallel_size=2
                ),
                128,
            ),
            (
                64,
                SimpleNamespace(
                    decode_context_parallel_size=1, prefill_context_parallel_size=2
                ),
                64,
            ),
            # Older vLLM configs without the DCP field keep the per-rank size.
            (16, SimpleNamespace(), 16),
        ],
    )
    def test_kv_event_block_size_scales_with_dcp(
        self, block_size, parallel_config, expected
    ):
        """Under DCP the engine hashes and publishes blocks spanning
        block_size * dcp tokens, so the router must be pinned at that size or
        the selection service drops every KV event."""
        vllm_config = SimpleNamespace(
            cache_config=SimpleNamespace(block_size=block_size),
            parallel_config=parallel_config,
        )
        assert resolve_kv_event_block_size(vllm_config) == expected

    @pytest.mark.parametrize(
        "effective_block_size,expected",
        [
            # The engine's resolved size wins over the derived one.
            (512, 512),
            (128, 128),
            # Unreported (None) falls back to block_size * dcp.
            (None, 512),
        ],
    )
    def test_kv_event_block_size_prefers_engine_resolved_size(
        self, effective_block_size, expected
    ):
        """vLLM syncs its resolved full-attention block size (DCP included)
        back to the frontend config; use it when the engine reports one."""
        vllm_config = SimpleNamespace(
            cache_config=SimpleNamespace(
                block_size=64, effective_attention_block_size=effective_block_size
            ),
            parallel_config=SimpleNamespace(decode_context_parallel_size=8),
        )
        assert resolve_kv_event_block_size(vllm_config) == expected

    def test_routing_stats_empty_without_kv_events(self):
        """Nothing to advertise when KV-cache events are not enabled."""
        llm_config = make_kv_aware_llm_config()
        assert (
            get_kv_event_routing_stats(
                llm_config, block_size=16, max_num_batched_tokens=4096
            )
            == {}
        )

    def test_prompt_token_endpoint_offsets_by_replica_rank(self, ray_instance):
        """The token channel gets its own per-replica ZMQ port."""
        llm_config = make_kv_aware_llm_config(
            experimental_configs={"KV_TOKEN_PORT_BASE": 7700}
        )
        replica_context = SimpleNamespace(rank=SimpleNamespace(local_rank=3))
        with mock.patch("ray.serve.get_replica_context", return_value=replica_context):
            endpoints = get_token_channel_endpoints(llm_config)

        node_ip = ray.util.get_node_ip_address()
        assert endpoints == ("tcp://*:7703", f"tcp://{node_ip}:7703")

    def test_prompt_token_routing_stats_advertise_endpoint(self):
        assert get_prompt_token_routing_stats("tcp://10.0.0.1:7557") == {
            "kv_token_metadata": {"endpoint": "tcp://10.0.0.1:7557"}
        }
        assert get_prompt_token_routing_stats(None) == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
