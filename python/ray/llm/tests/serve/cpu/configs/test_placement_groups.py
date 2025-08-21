"""Comprehensive tests for placement group configuration and validation.

This module contains all tests related to custom placement group functionality,
including validation, scenarios, edge cases, and integration testing.
"""
import sys

import pytest

from ray.llm._internal.serve.configs.server_models import LLMConfig, ModelLoadingConfig


class TestPlacementGroupValidation:
    """Test placement group validation logic for error detection."""

    def test_missing_placement_strategy(self, disable_placement_bundles):
        """Test that missing placement_group_strategy raises appropriate error."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={"placement_group_bundles": [{"GPU": 1}]},
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError,
            match="placement_group_bundles specified without placement_group_strategy",
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_missing_placement_bundles(self, disable_placement_bundles):
        """Test that missing placement_group_bundles raises appropriate error."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={"placement_group_strategy": "PACK"},
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError,
            match="placement_group_strategy specified without placement_group_bundles",
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_placement_strategy(self, disable_placement_bundles):
        """Test that invalid placement strategy is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}],
                "placement_group_strategy": "INVALID_STRATEGY",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="Invalid placement_group_strategy: INVALID_STRATEGY"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_empty_placement_bundles(self, disable_placement_bundles):
        """Test that empty placement_group_bundles list is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="placement_group_bundles must be a non-empty list"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_non_dict_bundle_entry(self, disable_placement_bundles):
        """Test that non-dictionary bundle entries are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": ["not_a_dict"],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(ValueError, match="Bundle 0 must be a dictionary"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_negative_bundle_resources(self, disable_placement_bundles):
        """Test that negative resource amounts in bundles are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"GPU": -1}],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="Bundle 0 resource 'GPU' must be a non-negative number"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_insufficient_gpu_resources_for_parallelism(
        self, disable_placement_bundles
    ):
        """Test that insufficient GPU resources for TP/PP configuration are caught."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 4,
                "pipeline_parallel_size": 2,
            },
            deployment_config={
                "placement_group_bundles": [{"GPU": 2}],  # Only 2 GPUs, need 8
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(ValueError, match="Insufficient GPU resources"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_valid_custom_placement_configuration(self, disable_placement_bundles):
        """Test that valid custom placement group configuration passes validation."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 2,
            },
            rank_to_bundle_index=[0, 0, 1, 1],  # Valid mapping
            deployment_config={
                "placement_group_bundles": [{"GPU": 2}, {"GPU": 2}],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        # Should not raise any exception
        config._validate_user_placement_config(deployment_config, engine_config)


class TestRankMappingValidation:
    """Test rank_to_bundle_index validation logic."""

    def test_invalid_rank_mapping_length(self, disable_placement_bundles):
        """Test that rank_to_bundle_index with incorrect length is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 2,
            },
            rank_to_bundle_index=[0, 1],  # Wrong length, need 4 entries
            deployment_config={
                "placement_group_bundles": [{"GPU": 2}, {"GPU": 2}],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(ValueError, match="rank_to_bundle_index must have length 4"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_rank_mapping_index(self, disable_placement_bundles):
        """Test that rank_to_bundle_index with invalid bundle indices is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            rank_to_bundle_index=[0, 5],  # Bundle index 5 doesn't exist
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}, {"GPU": 1}],  # Only indices 0,1
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="rank_to_bundle_index\\[1\\] = 5 is invalid"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_negative_rank_to_bundle_index_validation(self, disable_placement_bundles):
        """Test that negative rank_to_bundle_index values are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            rank_to_bundle_index=[0, -1],  # Negative index
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}, {"GPU": 1}],
                "placement_group_strategy": "PACK",
            },
        )

        with pytest.raises(
            ValueError, match="rank_to_bundle_index\\[1\\] = -1 is invalid"
        ):
            config.get_serve_options()

    def test_out_of_bounds_rank_to_bundle_index_validation(
        self, disable_placement_bundles
    ):
        """Test that out-of-bounds rank_to_bundle_index values are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            rank_to_bundle_index=[0, 2],  # Index 2 doesn't exist (only 0,1)
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}, {"GPU": 1}],  # Only indices 0,1
                "placement_group_strategy": "PACK",
            },
        )

        with pytest.raises(
            ValueError, match="rank_to_bundle_index\\[1\\] = 2 is invalid"
        ):
            config.get_serve_options()

    def test_maximum_valid_rank_to_bundle_index_boundary(
        self, disable_placement_bundles
    ):
        """Test rank mapping at maximum valid bundle index."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            rank_to_bundle_index=[0, 1],  # Max valid index is 1 (bundles 0,1)
            deployment_config={
                "placement_group_bundles": [
                    {"GPU": 1},
                    {"GPU": 1},
                ],  # 2 bundles (indices 0,1)
                "placement_group_strategy": "PACK",
            },
        )

        # Should not raise an exception - maximum valid indices
        serve_options = config.get_serve_options()
        expected_bundles = [{"GPU": 1}, {"GPU": 1}]
        assert serve_options["placement_group_bundles"] == expected_bundles


class TestPlacementGroupScenarios:
    """Test placement group scenarios for different TP/PP configurations."""

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

        # New default: replica + 1 bundle with 2 GPUs (TP colocated)
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

        # New default: replica + 3 bundles (one per PP stage), each with 2 GPUs (TP colocated)
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


class TestPlacementGroupEdgeCases:
    """Test edge cases and boundary conditions for placement group validation."""

    def test_pydantic_validation_prevents_non_integer_rank_mapping(
        self, disable_placement_bundles
    ):
        """Test that Pydantic validation catches non-integer rank_to_bundle_index values."""
        # This tests that our Pydantic field validation works
        with pytest.raises(Exception):  # Could be ValidationError or TypeError
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test-model"),
                rank_to_bundle_index=[0, 1.5],  # Float should be rejected by Pydantic
                deployment_config={
                    "placement_group_bundles": [{"GPU": 1}, {"GPU": 1}],
                    "placement_group_strategy": "PACK",
                },
            )

    def test_integration_zero_parallelism_edge_case(self, disable_placement_bundles):
        """Test behavior with TP=1, PP=1 (minimal parallelism)."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 1,
                "pipeline_parallel_size": 1,
            },
        )

        serve_options = config.get_serve_options()

        # For TP=1, PP=1, we should get: replica bundle + 1 worker bundle with 1 GPU
        expected_bundles = [{"CPU": 1, "GPU": 0}, {"GPU": 1}]
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "PACK"

    def test_case_sensitivity_in_placement_strategy(self, disable_placement_bundles):
        """Test that placement strategy validation is case-sensitive."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}],
                "placement_group_strategy": "pack",  # Lowercase should fail
            },
        )

        # This should fail during get_serve_options() when validation occurs
        with pytest.raises(ValueError, match="Invalid placement_group_strategy"):
            config.get_serve_options()

    def test_non_list_placement_bundles_validation(self, disable_placement_bundles):
        """Test that non-list placement_group_bundles are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": {"GPU": 1},  # Dict instead of list
                "placement_group_strategy": "PACK",
            },
        )

        with pytest.raises(
            ValueError, match="placement_group_bundles must be a non-empty list"
        ):
            config.get_serve_options()

    def test_none_placement_bundles_validation(self, disable_placement_bundles):
        """Test that None placement_group_bundles are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": None,
                "placement_group_strategy": "PACK",
            },
        )

        with pytest.raises(
            ValueError, match="placement_group_bundles must be a non-empty list"
        ):
            config.get_serve_options()

    def test_string_resource_amounts_validation(self, disable_placement_bundles):
        """Test that string resource amounts are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"GPU": "invalid"}],
                "placement_group_strategy": "PACK",
            },
        )

        with pytest.raises(
            ValueError, match="Bundle 0 resource 'GPU' must be a non-negative number"
        ):
            config.get_serve_options()

    def test_zero_gpu_with_tensor_parallelism_validation(
        self, disable_placement_bundles
    ):
        """Test that zero GPU resources fail when TP > 1."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            deployment_config={
                "placement_group_bundles": [{"CPU": 1}],  # No GPU resources
                "placement_group_strategy": "PACK",
            },
        )

        with pytest.raises(ValueError, match="Insufficient GPU resources"):
            config.get_serve_options()

    def test_boundary_exactly_sufficient_gpu_resources(self, disable_placement_bundles):
        """Test boundary condition: exactly sufficient GPU resources."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 2,  # Need exactly 4 GPUs
            },
            deployment_config={
                "placement_group_bundles": [{"GPU": 4}],  # Exactly 4 GPUs
                "placement_group_strategy": "PACK",
            },
        )

        # Should not raise an exception - exactly sufficient is valid
        serve_options = config.get_serve_options()
        assert serve_options["placement_group_bundles"] == [{"GPU": 4}]
        assert serve_options["placement_group_strategy"] == "PACK"

    def test_excess_gpu_resources_allowed(self, disable_placement_bundles):
        """Test that excess GPU resources are allowed."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,  # Need 2 GPUs
            },
            deployment_config={
                "placement_group_bundles": [{"GPU": 8}],  # Much more than needed
                "placement_group_strategy": "PACK",
            },
        )

        # Should not raise an exception - excess resources are fine
        serve_options = config.get_serve_options()
        assert serve_options["placement_group_bundles"] == [{"GPU": 8}]

    def test_large_scale_parallelism_validation(self, disable_placement_bundles):
        """Test validation with large TP/PP values."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 8,
                "pipeline_parallel_size": 4,  # Need 32 GPUs total
            },
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}]
                * 32,  # 32 individual GPU bundles
                "placement_group_strategy": "PACK",
            },
        )

        # Should not raise an exception - valid large configuration
        serve_options = config.get_serve_options()
        expected_bundles = [{"GPU": 1}] * 32
        assert serve_options["placement_group_bundles"] == expected_bundles

    def test_malformed_resource_keys_accepted(self, disable_placement_bundles):
        """Test that unusual resource keys are accepted (Ray is flexible)."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"unusual_resource": 1, "GPU": 1}],
                "placement_group_strategy": "PACK",
            },
        )

        # Should not raise an exception - Ray allows any resource key
        serve_options = config.get_serve_options()
        expected_bundles = [{"unusual_resource": 1, "GPU": 1}]
        assert serve_options["placement_group_bundles"] == expected_bundles


class TestPlacementGroupIntegration:
    """Test integration scenarios and feature interactions."""

    def test_custom_placement_overrides_resources_per_bundle_conflict(
        self, disable_placement_bundles
    ):
        """Test that custom placement groups take precedence over resources_per_bundle."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={"tensor_parallel_size": 2, "pipeline_parallel_size": 1},
            resources_per_bundle={"XPU": 1},  # This should be ignored
            deployment_config={
                # Custom placement should take precedence
                "placement_group_bundles": [{"GPU": 2}],
                "placement_group_strategy": "STRICT_PACK",
            },
        )

        serve_options = config.get_serve_options()

        # Should use custom placement, not resources_per_bundle
        expected_bundles = [{"GPU": 2}]
        assert serve_options["placement_group_bundles"] == expected_bundles
        assert serve_options["placement_group_strategy"] == "STRICT_PACK"

    def test_resources_per_bundle_compatibility_path(self, disable_placement_bundles):
        """Test that resources_per_bundle uses compatibility path when no custom placement."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            resources_per_bundle={"XPU": 1},  # This should trigger compatibility path
            deployment_config={
                # No custom placement - should use compatibility path
            },
        )

        serve_options = config.get_serve_options()

        # Should use compatibility path (resources_per_bundle logic)
        # Not our new default path
        assert "placement_group_bundles" in serve_options
        assert "placement_group_strategy" in serve_options


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
