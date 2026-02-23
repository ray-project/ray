import asyncio
import sys
from copy import deepcopy
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    DPServer,
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
            opts = DPServer.get_deployment_options(llm_config)
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
                DPServer.get_deployment_options(llm_config)

    def test_autoscaling_unsupported(self):
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs={"data_parallel_size": 4},
            deployment_config={"autoscaling_config": {"target_ongoing_requests": 10}},
        )
        with pytest.raises(ValueError, match="autoscaling_config is not supported"):
            DPServer.get_deployment_options(llm_config)


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
        "engine_kwargs,placement_group_config,dp_rank,expected",
        [
            # TP=1: 1 bundle per replica
            ({"tensor_parallel_size": 1}, None, 0, "0"),
            ({"tensor_parallel_size": 1}, None, 3, "3"),
            ({"tensor_parallel_size": 1}, {"bundles": [{"GPU": 1, "CPU": 1}]}, 2, "2"),
            # TP=2: 2 bundles per replica
            ({"tensor_parallel_size": 2}, None, 0, "0,1"),
            ({"tensor_parallel_size": 2}, None, 1, "2,3"),
            ({"tensor_parallel_size": 2}, None, 3, "6,7"),
            (
                {"tensor_parallel_size": 2},
                {"bundles": [{"GPU": 1, "CPU": 1}, {"GPU": 1}]},
                1,
                "2,3",
            ),
            # TP=2, PP=2: 4 bundles per replica
            (
                {"tensor_parallel_size": 2, "pipeline_parallel_size": 2},
                None,
                0,
                "0,1,2,3",
            ),
            (
                {"tensor_parallel_size": 2, "pipeline_parallel_size": 2},
                None,
                1,
                "4,5,6,7",
            ),
        ],
    )
    def test_bundle_indices(
        self, engine_kwargs, placement_group_config, dp_rank, expected
    ):
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs=engine_kwargs,
            placement_group_config=placement_group_config,
        )
        engine_config = llm_config.get_engine_config()
        bundles_per_replica = len(engine_config.placement_bundles)

        result = DPServer._compute_bundle_indices(dp_rank, bundles_per_replica)
        assert result == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
