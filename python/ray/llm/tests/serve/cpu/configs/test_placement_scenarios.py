import sys
from unittest.mock import Mock

import pytest


class MockLLMConfig:
    """Mock LLMConfig for testing placement group logic."""

    def __init__(self, rank_to_bundle_index=None):
        self.rank_to_bundle_index = rank_to_bundle_index

    def _has_custom_placement_config(self, deployment_config):
        return (
            "placement_group_bundles" in deployment_config
            or "placement_group_strategy" in deployment_config
        )

    def _apply_default_placement_config(
        self, deployment_config, replica_actor_resources, engine_config
    ):
        """Apply topology-aware default placement."""
        tp_size = getattr(engine_config, "tensor_parallel_degree", 1)
        pp_size = getattr(engine_config, "pipeline_parallel_degree", 1)

        # TP ranks colocated, PP cross-node allowed
        replica_bundle = replica_actor_resources
        worker_bundles = [{"GPU": tp_size}] * pp_size

        deployment_config.update(
            {
                "placement_group_bundles": [replica_bundle] + worker_bundles,
                "placement_group_strategy": "PACK",
            }
        )

    def process_placement_config(
        self, deployment_config, replica_actor_resources, engine_config
    ):
        """Main logic for placement group processing."""
        if self._has_custom_placement_config(deployment_config):
            # CUSTOM PATH: Use as-is, no modifications
            pass  # Don't modify user config
        else:
            # DEFAULT PATH: Generate topology-aware placement groups
            self._apply_default_placement_config(
                deployment_config, replica_actor_resources, engine_config
            )
        return deployment_config


class TestPlacementGroupScenarios:
    """Test placement group scenarios for distributed tensor parallelism."""

    def test_tp2_single_node_default(self):
        """Test TP=2 single node (default behavior)."""
        config = MockLLMConfig()
        deployment_config = {}
        replica_resources = {"CPU": 1, "GPU": 0}
        engine_config = Mock()
        engine_config.tensor_parallel_degree = 2
        engine_config.pipeline_parallel_degree = 1

        result = config.process_placement_config(
            deployment_config, replica_resources, engine_config
        )

        # Should use default: replica + 1 bundle with 2 GPUs
        expected_bundles = [{"CPU": 1, "GPU": 0}, {"GPU": 2}]
        assert result["placement_group_bundles"] == expected_bundles
        assert result["placement_group_strategy"] == "PACK"

    def test_tp2_multi_node_custom(self):
        """Test TP=2 multi node (custom override)."""
        config = MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [{"CPU": 1, "GPU": 0}, {"GPU": 1}, {"GPU": 1}],
            "placement_group_strategy": "PACK",
        }
        replica_resources = {"CPU": 1, "GPU": 0}
        engine_config = Mock()

        result = config.process_placement_config(
            deployment_config, replica_resources, engine_config
        )

        # Should use custom config unchanged
        expected_bundles = [{"CPU": 1, "GPU": 0}, {"GPU": 1}, {"GPU": 1}]
        assert result["placement_group_bundles"] == expected_bundles
        assert result["placement_group_strategy"] == "PACK"

    def test_tp2_pp3_all_same_node_custom(self):
        """Test TP=2, PP=3 all ranks on same node (custom)."""
        config = MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [{"CPU": 1, "GPU": 6}],
            "placement_group_strategy": "STRICT_PACK",
        }
        replica_resources = {"CPU": 1, "GPU": 0}
        engine_config = Mock()

        result = config.process_placement_config(
            deployment_config, replica_resources, engine_config
        )

        # Should use custom config unchanged
        expected_bundles = [{"CPU": 1, "GPU": 6}]
        assert result["placement_group_bundles"] == expected_bundles
        assert result["placement_group_strategy"] == "STRICT_PACK"

    def test_tp2_pp3_tp_same_node_pp_cross_node_default(self):
        """Test TP=2, PP=3 with TP same node, PP cross-node (default)."""
        config = MockLLMConfig()
        deployment_config = {}
        replica_resources = {"CPU": 1, "GPU": 0}
        engine_config = Mock()
        engine_config.tensor_parallel_degree = 2
        engine_config.pipeline_parallel_degree = 3

        result = config.process_placement_config(
            deployment_config, replica_resources, engine_config
        )

        # Should use default: replica + 3 bundles (one per PP stage), each with 2 GPUs
        expected_bundles = [
            {"CPU": 1, "GPU": 0},  # Replica
            {"GPU": 2},  # PP stage 0
            {"GPU": 2},  # PP stage 1
            {"GPU": 2},  # PP stage 2
        ]
        assert result["placement_group_bundles"] == expected_bundles
        assert result["placement_group_strategy"] == "PACK"

    def test_tp2_pp3_all_multi_node_custom(self):
        """Test TP=2, PP=3 all multi-node (custom for 1×GPU nodes)."""
        config = MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [{"CPU": 1, "GPU": 0}] + [{"GPU": 1}] * 6,
            "placement_group_strategy": "PACK",
        }
        replica_resources = {"CPU": 1, "GPU": 0}
        engine_config = Mock()

        result = config.process_placement_config(
            deployment_config, replica_resources, engine_config
        )

        # Should use custom config unchanged
        expected_bundles = [{"CPU": 1, "GPU": 0}] + [{"GPU": 1}] * 6
        assert result["placement_group_bundles"] == expected_bundles
        assert result["placement_group_strategy"] == "PACK"

    def test_no_replica_resource_merging_in_custom_path(self):
        """Test that custom placement configs are not modified."""
        config = MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [{"GPU": 1}, {"GPU": 1}],
            "placement_group_strategy": "PACK",
        }
        replica_resources = {"CPU": 2, "GPU": 0, "memory": 1000}
        engine_config = Mock()

        original_bundles = deployment_config["placement_group_bundles"].copy()
        result = config.process_placement_config(
            deployment_config, replica_resources, engine_config
        )

        # User bundles should be completely unchanged
        assert result["placement_group_bundles"] == original_bundles
        assert result["placement_group_bundles"][0] == {"GPU": 1}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
