import asyncio
import sys
from copy import deepcopy
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.data_parallel.builder import (
    build_dp_openai_app,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    DPServer,
    GangMasterInfoRegistry,
)
from ray.serve._private.http_util import ASGIAppReplicaWrapper
from ray.serve.config import (
    GangPlacementStrategy,
    GangRuntimeFailurePolicy,
    GangSchedulingConfig,
    RequestRouterConfig,
)
from ray.serve.experimental.consistent_hash_router import ConsistentHashRouter
from ray.serve.experimental.round_robin_router import RoundRobinRouter


class TestGetDeploymentOptions:
    """Mirrors test_dp_server.py but verifies gang scheduling config."""

    @pytest.mark.parametrize(
        "data_parallel_size,num_replicas",
        [
            (None, 1),
            (2, None),
            (1, 1),
            (2, 4),
        ],
    )
    def test_num_replicas_dp_validation(self, data_parallel_size, num_replicas):
        engine_kwargs = (
            {}
            if data_parallel_size is None
            else {"data_parallel_size": data_parallel_size}
        )
        deployment_config = (
            {} if num_replicas is None else {"num_replicas": num_replicas}
        )
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs=deepcopy(engine_kwargs),
            deployment_config=deepcopy(deployment_config),
        )

        opts = DPServer.get_deployment_options(llm_config)
        dp_size = data_parallel_size or 1
        if dp_size > 1:
            expected_replicas = (
                num_replicas * dp_size if num_replicas is not None else dp_size
            )
            assert opts["num_replicas"] == expected_replicas
            assert isinstance(opts["gang_scheduling_config"], GangSchedulingConfig)
            assert opts["gang_scheduling_config"].gang_size == dp_size
            assert (
                opts["gang_scheduling_config"].gang_placement_strategy
                == GangPlacementStrategy.PACK
            )
            assert (
                opts["gang_scheduling_config"].runtime_failure_policy
                == GangRuntimeFailurePolicy.RESTART_GANG
            )
        else:
            assert "gang_scheduling_config" not in opts

    def test_autoscaling_config(self):
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs={"data_parallel_size": 4},
            deployment_config={
                "autoscaling_config": {
                    "target_ongoing_requests": 10,
                    "min_replicas": 2,
                    "max_replicas": 8,
                    "initial_replicas": 3,
                }
            },
        )
        opts = DPServer.get_deployment_options(llm_config)
        assert isinstance(opts["gang_scheduling_config"], GangSchedulingConfig)
        assert opts["gang_scheduling_config"].gang_size == 4
        # Autoscaling config should have min/max/initial replicas multiplied by dp_size
        autoscaling_config = opts["autoscaling_config"]
        assert autoscaling_config["target_ongoing_requests"] == 10
        assert autoscaling_config["min_replicas"] == 2 * 4
        assert autoscaling_config["max_replicas"] == 8 * 4
        assert autoscaling_config["initial_replicas"] == 3 * 4


class TestGangMasterInfoRegistry:
    _KV_MODULE = "ray.llm._internal.serve.serving_patterns.data_parallel.dp_server"

    def _make_kv_store(self):
        # Mocks GCS KV store
        store = {}
        return (
            store,
            lambda key, value, overwrite=False: store.__setitem__(key, value),
            lambda key: store.get(key),
            lambda key: store.pop(key, None) is not None,
            lambda key: key in store,
        )

    @patch(f"{_KV_MODULE}._internal_kv_get")
    @patch(f"{_KV_MODULE}._internal_kv_put")
    def test_get_timeout(self, mock_put, mock_get):
        mock_get.return_value = None
        with pytest.raises(TimeoutError, match="Timed out"):
            asyncio.get_event_loop().run_until_complete(
                GangMasterInfoRegistry.get(
                    "gang-missing", timeout=0.5, poll_interval=0.1
                )
            )

    @patch(f"{_KV_MODULE}._internal_kv_get")
    @patch(f"{_KV_MODULE}._internal_kv_put")
    def test_gang_isolation(self, mock_put, mock_get):
        _, fake_put, fake_get, _, _ = self._make_kv_store()
        mock_put.side_effect = fake_put
        mock_get.side_effect = fake_get

        GangMasterInfoRegistry.register("gang-1", "10.0.0.1", 1111)
        GangMasterInfoRegistry.register("gang-2", "10.0.0.2", 2222)

        loop = asyncio.get_event_loop()
        addr1, port1 = loop.run_until_complete(GangMasterInfoRegistry.get("gang-1"))
        addr2, port2 = loop.run_until_complete(GangMasterInfoRegistry.get("gang-2"))

        assert (addr1, port1) == ("10.0.0.1", 1111)
        assert (addr2, port2) == ("10.0.0.2", 2222)


class TestBundleIndices:
    @pytest.mark.parametrize(
        "engine_kwargs,placement_group_config,dp_rank,sorted_indices,expected",
        [
            # TP=1: 1 bundle per replica, identity ordering
            ({"tensor_parallel_size": 1}, None, 0, list(range(4)), "0"),
            ({"tensor_parallel_size": 1}, None, 3, list(range(4)), "3"),
            (
                {"tensor_parallel_size": 1},
                {"bundles": [{"GPU": 1, "CPU": 1}]},
                2,
                list(range(4)),
                "2",
            ),
            # TP=2: 2 bundles per replica, identity ordering
            ({"tensor_parallel_size": 2}, None, 0, list(range(8)), "0,1"),
            ({"tensor_parallel_size": 2}, None, 2, list(range(8)), "4,5"),
            (
                {"tensor_parallel_size": 2},
                {"bundles": [{"GPU": 1, "CPU": 1}, {"GPU": 1}]},
                1,
                list(range(4)),
                "2,3",
            ),
            # TP=2, PP=2: 4 bundles per replica, identity ordering
            (
                {"tensor_parallel_size": 2, "pipeline_parallel_size": 2},
                None,
                0,
                list(range(8)),
                "0,1,2,3",
            ),
            (
                {"tensor_parallel_size": 2, "pipeline_parallel_size": 2},
                None,
                1,
                list(range(8)),
                "4,5,6,7",
            ),
            # Out-of-order sorted_indices: bundles reordered by node
            ({"tensor_parallel_size": 2}, None, 1, [0, 2, 1, 3], "1,3"),
            ({"tensor_parallel_size": 1}, None, 0, [2, 0, 3, 1], "2"),
        ],
    )
    def test_bundle_indices(
        self, engine_kwargs, placement_group_config, dp_rank, sorted_indices, expected
    ):
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs=engine_kwargs,
            placement_group_config=placement_group_config,
        )
        engine_config = llm_config.get_engine_config()
        bundles_per_replica = len(engine_config.placement_bundles)

        result = DPServer._compute_bundle_indices(
            dp_rank, bundles_per_replica, sorted_indices
        )
        assert result == expected


class TestBuildDPOpenAiAppDirectStreaming:
    def _make_llm_config(self) -> LLMConfig:
        return LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs={"data_parallel_size": 2},
        )

    def test_direct_streaming_builds_dp_ingress_with_router_attached(
        self, disable_placement_bundles, monkeypatch, router_class_path
    ):
        monkeypatch.setattr(
            "ray.llm._internal.serve.serving_patterns.data_parallel.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )

        app = build_dp_openai_app({"llm_config": self._make_llm_config()})
        deployment = app._bound_deployment
        ingress_request_router = app._ingress_request_router

        assert deployment.name == "DPServer:test_model"
        assert issubclass(deployment.func_or_class, ASGIAppReplicaWrapper)
        assert issubclass(deployment.func_or_class, DPServer)
        assert ingress_request_router is not None
        assert ingress_request_router._bound_deployment.name == "LLMRouter"
        assert ingress_request_router._bound_deployment.init_kwargs["server"] is app

        request_router_config = deployment._deployment_config.request_router_config
        assert request_router_config.request_router_class == router_class_path(
            RoundRobinRouter
        )

    def test_direct_streaming_user_request_router_config_wins(
        self, disable_placement_bundles, monkeypatch, router_class_path
    ):
        monkeypatch.setattr(
            "ray.llm._internal.serve.serving_patterns.data_parallel.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )
        llm_config = self._make_llm_config()
        llm_config.deployment_config["request_router_config"] = RequestRouterConfig(
            request_router_class=ConsistentHashRouter,
        )

        app = build_dp_openai_app({"llm_config": llm_config})
        request_router_config = (
            app._bound_deployment._deployment_config.request_router_config
        )
        assert request_router_config.request_router_class == router_class_path(
            ConsistentHashRouter
        )

    @pytest.mark.parametrize(
        ("builder_kwargs", "match"),
        [
            (
                {"ingress_deployment_config": {"num_replicas": 2}},
                "does not support ingress_deployment_config",
            ),
            (
                {"ingress_cls_config": {"ingress_extra_kwargs": {"key": "value"}}},
                "does not support ingress_cls_config",
            ),
        ],
    )
    def test_direct_streaming_rejects_ingress_config(
        self, disable_placement_bundles, monkeypatch, builder_kwargs, match
    ):
        monkeypatch.setattr(
            "ray.llm._internal.serve.serving_patterns.data_parallel.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )

        with pytest.raises(ValueError, match=match):
            build_dp_openai_app(
                {"llm_config": self._make_llm_config(), **builder_kwargs}
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
