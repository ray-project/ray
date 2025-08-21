import sys

import pytest

from ray.llm._internal.serve.configs.server_models import LLMConfig, ModelLoadingConfig


class TestPlacementGroupScenarios:
    """Test placement group scenarios for tensor and pipeline parallelism."""

    def test_tensor_parallel_default_placement(self, disable_placement_bundles):
        """Test TP=2 default placement strategy (single node)."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
        )

        serve_options = config.get_serve_options()

        # Should use default: replica + 1 bundle with 2 GPUs
        expected_bundles = [{"CPU": 1, "GPU": 0}, {"GPU": 2}]
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "PACK"

    def test_tensor_parallel_cross_node_custom_placement(
        self, disable_placement_bundles
    ):
        """Test TP=2 cross-node placement (custom override for single GPU nodes)."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            deployment_config={
                "placement_group_bundles": [
                    {"CPU": 1, "GPU": 0},
                    {"GPU": 1},
                    {"GPU": 1},
                ],
                "placement_group_strategy": "PACK",
            },
        )

        serve_options = config.get_serve_options()

        # Should use custom config unchanged
        expected_bundles = [{"CPU": 1, "GPU": 0}, {"GPU": 1}, {"GPU": 1}]
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "PACK"

    def test_mixed_parallelism_single_node_custom_placement(
        self, disable_placement_bundles
    ):
        """Test TP=2, PP=3 all ranks on same node (custom configuration)."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 3,
            },
            deployment_config={
                "placement_group_bundles": [{"CPU": 1, "GPU": 6}],
                "placement_group_strategy": "STRICT_PACK",
            },
        )

        serve_options = config.get_serve_options()

        # Should use custom config unchanged
        expected_bundles = [{"CPU": 1, "GPU": 6}]
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "STRICT_PACK"

    def test_mixed_parallelism_default_placement(self, disable_placement_bundles):
        """Test TP=2, PP=3 default placement (TP colocated, PP cross-node allowed)."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 3,
            },
        )

        serve_options = config.get_serve_options()

        # Should use default: replica + 3 bundles (one per PP stage), each with 2 GPUs
        expected_bundles = [
            {"CPU": 1, "GPU": 0},  # Replica
            {"GPU": 2},  # PP stage 0
            {"GPU": 2},  # PP stage 1
            {"GPU": 2},  # PP stage 2
        ]
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "PACK"

    def test_mixed_parallelism_fully_distributed_custom_placement(
        self, disable_placement_bundles
    ):
        """Test TP=2, PP=3 fully distributed (custom for single GPU per node scenario)."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 3,
            },
            deployment_config={
                "placement_group_bundles": [{"CPU": 1, "GPU": 0}] + [{"GPU": 1}] * 6,
                "placement_group_strategy": "PACK",
            },
        )

        serve_options = config.get_serve_options()

        # Should use custom config unchanged
        expected_bundles = [{"CPU": 1, "GPU": 0}] + [{"GPU": 1}] * 6
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "PACK"

    def test_custom_placement_config_preservation(self, disable_placement_bundles):
        """Test that custom placement configurations are preserved without modification."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}, {"GPU": 1}],
                "placement_group_strategy": "PACK",
            },
        )

        serve_options = config.get_serve_options()

        # User bundles should be completely unchanged (no replica resource merging)
        expected_bundles = [{"GPU": 1}, {"GPU": 1}]
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "PACK"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
