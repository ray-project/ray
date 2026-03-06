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
