import asyncio
import sys
from copy import deepcopy
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server import (
    GangDPServer,
    GangMasterInfoRegistry,
)
from ray.serve.config import (
    GangPlacementStrategy,
    GangRuntimeFailurePolicy,
    GangSchedulingConfig,
)


class TestGetDeploymentOptions:
    """Mirrors test_dp_server.py but verifies gang scheduling config."""

    @pytest.mark.parametrize(
        "data_parallel_size,num_replicas,allowed,error_match",
        [
            (None, 1, True, None),
            (1, 1, True, None),
            (2, None, True, None),
            (4, None, True, None),
            # Multi-gang: num_replicas is a valid multiple of dp_size
            (4, 8, True, None),
            (2, 6, True, None),
            # Invalid: num_replicas is not a multiple of dp_size
            (4, 6, False, "must be a multiple of"),
            (4, 3, False, "must be a multiple of"),
        ],
    )
    def test_num_replicas_dp_validation(
        self, data_parallel_size, num_replicas, allowed, error_match
    ):
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

        if allowed:
            opts = GangDPServer.get_deployment_options(llm_config)
            dp_size = data_parallel_size or 1
            if dp_size > 1:
                expected_replicas = (
                    num_replicas if num_replicas is not None else dp_size
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
        else:
            with pytest.raises(ValueError, match=error_match):
                GangDPServer.get_deployment_options(llm_config)

    def test_autoscaling_unsupported(self):
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs={"data_parallel_size": 4},
            deployment_config={"autoscaling_config": {"target_ongoing_requests": 10}},
        )
        with pytest.raises(ValueError, match="autoscaling_config is not supported"):
            GangDPServer.get_deployment_options(llm_config)


class TestGangMasterInfoRegistry:
    """Test suite for GangMasterInfoRegistry using mocked KV store."""

    def _make_kv_store(self):
        """Return a dict-backed fake KV store with put/get functions."""
        store = {}

        def fake_put(key, value, overwrite=False):
            store[key] = value

        def fake_get(key):
            return store.get(key)

        return store, fake_put, fake_get

    @patch(
        "ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server._internal_kv_get"
    )
    @patch(
        "ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server._internal_kv_put"
    )
    def test_register_and_get(self, mock_put, mock_get):
        """Test round-trip: register master info then retrieve it."""
        store, fake_put, fake_get = self._make_kv_store()
        mock_put.side_effect = fake_put
        mock_get.side_effect = fake_get

        GangMasterInfoRegistry.register("gang-abc", "10.0.0.1", 12345)
        addr, port = asyncio.get_event_loop().run_until_complete(
            GangMasterInfoRegistry.get("gang-abc")
        )
        assert addr == "10.0.0.1"
        assert port == 12345

    @patch(
        "ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server._internal_kv_get"
    )
    @patch(
        "ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server._internal_kv_put"
    )
    def test_get_timeout(self, mock_put, mock_get):
        """Test that get raises TimeoutError when key is never registered."""
        mock_get.return_value = None

        with pytest.raises(TimeoutError, match="Timed out"):
            asyncio.get_event_loop().run_until_complete(
                GangMasterInfoRegistry.get(
                    "gang-missing", timeout=0.5, poll_interval=0.1
                )
            )

    @patch(
        "ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server._internal_kv_get"
    )
    @patch(
        "ray.llm._internal.serve.serving_patterns.data_parallel.gang_dp_server._internal_kv_put"
    )
    def test_multiple_gangs_isolated(self, mock_put, mock_get):
        """Test that different gang_ids store and retrieve independently."""
        _, fake_put, fake_get = self._make_kv_store()
        mock_put.side_effect = fake_put
        mock_get.side_effect = fake_get

        GangMasterInfoRegistry.register("gang-1", "10.0.0.1", 1111)
        GangMasterInfoRegistry.register("gang-2", "10.0.0.2", 2222)

        loop = asyncio.get_event_loop()
        addr1, port1 = loop.run_until_complete(GangMasterInfoRegistry.get("gang-1"))
        addr2, port2 = loop.run_until_complete(GangMasterInfoRegistry.get("gang-2"))

        assert (addr1, port1) == ("10.0.0.1", 1111)
        assert (addr2, port2) == ("10.0.0.2", 2222)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
